package com.saga.orchestrator.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.saga.orchestrator.entity.SagaInstance;
import com.saga.orchestrator.entity.SagaStep;
import com.saga.orchestrator.entity.enums.SagaStatus;
import com.saga.orchestrator.entity.enums.SagaStepName;
import com.saga.orchestrator.entity.enums.SagaStepStatus;
import com.saga.orchestrator.model.SagaPayload;
import com.saga.orchestrator.repository.SagaInstanceRepository;
import com.saga.orchestrator.repository.SagaStepRepository;
import com.saga.shared.event.*;
import com.saga.shared.kafka.KafkaTopics;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class SagaOrchestrationService {

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaStepRepository sagaStepRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // ─── Saga Start ───────────────────────────────────────────────────────────

    /**
     * Entry point triggered by the gRPC endpoint.
     * Creates the saga and sends a command to order-service to create the order.
     */
    @Transactional
    public SagaInstance startSaga(String userId, String productId,
                                  Integer quantity, BigDecimal amount, String currency) {
        SagaPayload payload = SagaPayload.builder()
                .userId(userId)
                .productId(productId)
                .quantity(quantity)
                .amount(amount)
                .currency(currency)
                .build();

        SagaInstance saga = SagaInstance.builder()
                .currentStep(SagaStepName.ORDER_CREATED)
                .status(SagaStatus.STARTED)
                .payload(toJson(payload))
                .build();
        saga = sagaInstanceRepository.save(saga);
        log.info("Saga started: id={}", saga.getId());

        // Command: tell order-service to create the order
        kafkaTemplate.send(KafkaTopics.ORDER_CREATED, saga.getId().toString(),
                Map.of("sagaId", saga.getId().toString(),
                       "userId", userId,
                       "productId", productId,
                       "quantity", quantity,
                       "amount", amount,
                       "currency", currency));

        return saga;
    }

    // ─── Forward Flow ─────────────────────────────────────────────────────────

    /**
     * Order has been created. Reserve stock next.
     * Called by SagaEventListener when order.created event is received.
     */
    @Transactional
    public void processOrderCreated(OrderCreatedEvent event) {
        SagaInstance saga = findBySagaId(event.getSagaId());
        SagaPayload payload = fromJson(saga.getPayload());
        payload.setOrderId(event.getOrderId());

        recordStep(saga, SagaStepName.ORDER_CREATED, SagaStepStatus.COMPLETED, null);
        saga.setCurrentStep(SagaStepName.STOCK_RESERVED);
        saga.setPayload(toJson(payload));
        sagaInstanceRepository.save(saga);
        log.info("Saga [{}] → ORDER_CREATED done, reserving stock", saga.getId());

        // Command: tell inventory-service to reserve stock
        kafkaTemplate.send(KafkaTopics.STOCK_RESERVE_REQUEST, saga.getId().toString(),
                Map.of("sagaId", saga.getId().toString(),
                       "orderId", payload.getOrderId().toString(),
                       "productId", payload.getProductId(),
                       "quantity", payload.getQuantity()));
    }

    /**
     * Stock reserved. Process payment next.
     * Called by SagaEventListener when stock.reserved event is received.
     */
    @Transactional
    public void processStockReserved(StockReservedEvent event) {
        SagaInstance saga = findBySagaId(event.getSagaId());
        SagaPayload payload = fromJson(saga.getPayload());
        payload.setReservationId(event.getReservationId());

        recordStep(saga, SagaStepName.STOCK_RESERVED, SagaStepStatus.COMPLETED, null);
        saga.setCurrentStep(SagaStepName.PAYMENT_PROCESSED);
        saga.setPayload(toJson(payload));
        sagaInstanceRepository.save(saga);
        log.info("Saga [{}] → STOCK_RESERVED done, processing payment", saga.getId());

        // Command: tell payment-service to process payment
        kafkaTemplate.send(KafkaTopics.PAYMENT_PROCESS_REQUEST, saga.getId().toString(),
                Map.of("sagaId", saga.getId().toString(),
                       "orderId", payload.getOrderId().toString(),
                       "userId", payload.getUserId(),
                       "amount", payload.getAmount(),
                       "currency", payload.getCurrency()));
    }

    /**
     * Payment processed. Confirm the order and complete the saga.
     * Called by SagaEventListener when payment.processed event is received.
     */
    @Transactional
    public void processPaymentProcessed(PaymentProcessedEvent event) {
        SagaInstance saga = findBySagaId(event.getSagaId());
        SagaPayload payload = fromJson(saga.getPayload());
        payload.setPaymentId(event.getPaymentId());

        recordStep(saga, SagaStepName.PAYMENT_PROCESSED, SagaStepStatus.COMPLETED, null);
        saga.setCurrentStep(SagaStepName.COMPLETED);
        saga.setStatus(SagaStatus.COMPLETED);
        saga.setPayload(toJson(payload));
        sagaInstanceRepository.save(saga);
        log.info("Saga [{}] COMPLETED successfully", saga.getId());

        // Command: tell order-service to confirm the order
        OrderConfirmedEvent confirmed = OrderConfirmedEvent.builder()
                .eventId(UUID.randomUUID())
                .sagaId(saga.getId())
                .orderId(payload.getOrderId())
                .paymentId(payload.getPaymentId())
                .reservationId(payload.getReservationId())
                .build();
        confirmed.init();
        kafkaTemplate.send(KafkaTopics.ORDER_CONFIRMED, payload.getOrderId().toString(), confirmed);
    }

    // ─── Compensation Flow ────────────────────────────────────────────────────

    /**
     * Stock reservation failed. Cancel the order directly (nothing to compensate yet).
     * Called by SagaEventListener when stock.reservation.failed event is received.
     */
    @Transactional
    public void processStockReservationFailed(StockReservationFailedEvent event) {
        SagaInstance saga = findBySagaId(event.getSagaId());
        SagaPayload payload = fromJson(saga.getPayload());

        recordStep(saga, SagaStepName.STOCK_RESERVED, SagaStepStatus.FAILED, event.getReason());
        saga.setCurrentStep(SagaStepName.ORDER_CANCELLED);
        saga.setStatus(SagaStatus.COMPENSATING);
        sagaInstanceRepository.save(saga);
        log.warn("Saga [{}] → stock reservation failed, cancelling order", saga.getId());

        cancelOrder(saga.getId(), payload.getOrderId(), event.getReason());
    }

    /**
     * Payment failed. Release stock first (compensation), then cancel the order.
     * Called by SagaEventListener when payment.failed event is received.
     */
    @Transactional
    public void processPaymentFailed(PaymentFailedEvent event) {
        SagaInstance saga = findBySagaId(event.getSagaId());
        SagaPayload payload = fromJson(saga.getPayload());
        payload.setPaymentId(event.getPaymentId());

        recordStep(saga, SagaStepName.PAYMENT_PROCESSED, SagaStepStatus.FAILED, event.getReason());
        saga.setCurrentStep(SagaStepName.STOCK_RELEASED);
        saga.setStatus(SagaStatus.COMPENSATING);
        saga.setPayload(toJson(payload));
        sagaInstanceRepository.save(saga);
        log.warn("Saga [{}] → payment failed, releasing stock", saga.getId());

        // Compensation: release the reserved stock
        kafkaTemplate.send(KafkaTopics.STOCK_RELEASE_REQUEST, saga.getId().toString(),
                Map.of("sagaId", saga.getId().toString(),
                       "orderId", payload.getOrderId().toString(),
                       "reservationId", payload.getReservationId().toString(),
                       "reason", event.getReason()));
    }

    /**
     * Stock released after payment failure. Now cancel the order.
     * Called by SagaEventListener when stock.released event is received.
     */
    @Transactional
    public void processStockReleased(StockReleasedEvent event) {
        SagaInstance saga = findBySagaId(event.getSagaId());
        SagaPayload payload = fromJson(saga.getPayload());

        recordStep(saga, SagaStepName.STOCK_RELEASED, SagaStepStatus.COMPENSATED, null);
        saga.setCurrentStep(SagaStepName.ORDER_CANCELLED);
        sagaInstanceRepository.save(saga);
        log.info("Saga [{}] → stock released, cancelling order", saga.getId());

        cancelOrder(saga.getId(), payload.getOrderId(), "Payment failed");
    }

    /**
     * Payment cancelled during compensation. Release stock next.
     * Called by SagaEventListener when payment.cancelled event is received.
     */
    @Transactional
    public void processPaymentCancelled(PaymentCancelledEvent event) {
        SagaInstance saga = findBySagaId(event.getSagaId());
        SagaPayload payload = fromJson(saga.getPayload());

        recordStep(saga, SagaStepName.PAYMENT_CANCELLED, SagaStepStatus.COMPENSATED, null);
        sagaInstanceRepository.save(saga);
        log.info("Saga [{}] → payment cancelled, releasing stock", saga.getId());

        kafkaTemplate.send(KafkaTopics.STOCK_RELEASE_REQUEST, saga.getId().toString(),
                Map.of("sagaId", saga.getId().toString(),
                       "orderId", payload.getOrderId().toString(),
                       "reservationId", payload.getReservationId().toString(),
                       "reason", event.getReason()));
    }

    // ─── Queries ──────────────────────────────────────────────────────────────

    @Transactional(readOnly = true)
    public SagaInstance findById(UUID sagaId) {
        return sagaInstanceRepository.findById(sagaId)
                .orElseThrow(() -> new EntityNotFoundException("Saga not found: " + sagaId));
    }

    @Transactional(readOnly = true)
    public List<SagaStep> findStepsBySagaId(UUID sagaId) {
        return sagaStepRepository.findBySagaInstanceIdOrderByCreatedAtAsc(sagaId);
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private SagaInstance findBySagaId(UUID sagaId) {
        return sagaInstanceRepository.findById(sagaId)
                .orElseThrow(() -> new EntityNotFoundException("Saga not found: " + sagaId));
    }

    private void recordStep(SagaInstance saga, SagaStepName stepName,
                             SagaStepStatus status, String errorMessage) {
        SagaStep step = SagaStep.builder()
                .sagaInstance(saga)
                .stepName(stepName)
                .status(status)
                .errorMessage(errorMessage)
                .build();
        sagaStepRepository.save(step);
    }

    private void cancelOrder(UUID sagaId, UUID orderId, String reason) {
        OrderCancelledEvent event = OrderCancelledEvent.builder()
                .eventId(UUID.randomUUID())
                .sagaId(sagaId)
                .orderId(orderId)
                .reason(reason)
                .build();
        event.init();
        kafkaTemplate.send(KafkaTopics.ORDER_CANCELLED, orderId.toString(), event);

        SagaInstance saga = findBySagaId(sagaId);
        recordStep(saga, SagaStepName.ORDER_CANCELLED, SagaStepStatus.COMPENSATED, reason);
        saga.setStatus(SagaStatus.FAILED);
        sagaInstanceRepository.save(saga);
        log.info("Saga [{}] FAILED — order cancelled", sagaId);
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize saga payload", e);
        }
    }

    private SagaPayload fromJson(String json) {
        try {
            return objectMapper.readValue(json, SagaPayload.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize saga payload", e);
        }
    }
}
