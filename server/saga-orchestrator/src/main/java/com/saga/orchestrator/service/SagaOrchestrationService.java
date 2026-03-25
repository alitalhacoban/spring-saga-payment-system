package com.saga.orchestrator.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.saga.order.grpc.CreateOrderResponse;
import com.saga.orchestrator.entity.SagaInstance;
import com.saga.orchestrator.entity.SagaStep;
import com.saga.orchestrator.entity.enums.SagaStatus;
import com.saga.orchestrator.entity.enums.SagaStepName;
import com.saga.orchestrator.entity.enums.SagaStepStatus;
import com.saga.orchestrator.grpc.OrderServiceClient;
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
import java.util.UUID;

/**
 * Core saga state machine.
 *
 * Forward flow:
 *   startSaga()
 *     → gRPC → order-service.createOrder()
 *     → Kafka → stock.reserve.request
 *     → [SagaEventListener.onStockReserved()]
 *     → Kafka → payment.process.request
 *     → [SagaEventListener.onPaymentProcessed()]
 *     → Kafka → order.confirmed
 *     → status = COMPLETED
 *
 * Compensation flow (stock reservation failure):
 *   [SagaEventListener.onStockReservationFailed()]
 *     → Kafka → order.cancelled
 *     → status = FAILED
 *
 * Compensation flow (payment failure):
 *   [SagaEventListener.onPaymentFailed()]
 *     → Kafka → stock.release.request
 *     → [SagaEventListener.onStockReleased()]
 *     → Kafka → order.cancelled
 *     → status = FAILED
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SagaOrchestrationService {

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaStepRepository sagaStepRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final OrderServiceClient orderServiceClient;

    // ─── Saga Start ───────────────────────────────────────────────────────────

    /**
     * Entry point — triggered by gRPC startSaga call.
     *
     * 1. Create order via gRPC (sync)
     * 2. Persist SagaInstance with full context
     * 3. Publish StockReserveCommand → inventory-service
     */
    @Transactional
    public SagaInstance startSaga(String userId, String productId,
                                  Integer quantity, BigDecimal amount, String currency) {

        // Step 1: Create order synchronously via gRPC
        CreateOrderResponse orderResponse = orderServiceClient.createOrder(
                userId, productId, quantity, amount);
        UUID orderId = UUID.fromString(orderResponse.getOrderId());
        log.info("Order created via gRPC: orderId={}", orderId);

        // Step 2: Persist SagaInstance
        SagaPayload payload = SagaPayload.builder()
                .userId(userId)
                .productId(productId)
                .quantity(quantity)
                .amount(amount)
                .currency(currency)
                .orderId(orderId)
                .build();

        SagaInstance saga = SagaInstance.builder()
                .orderId(orderId.toString())
                .currentStep(SagaStepName.ORDER_CREATED)
                .status(SagaStatus.STARTED)
                .payload(toJson(payload))
                .build();
        saga = sagaInstanceRepository.save(saga);

        recordStep(saga, SagaStepName.ORDER_CREATED, SagaStepStatus.COMPLETED, null);
        log.info("Saga started: id={}, orderId={}", saga.getId(), orderId);

        // Step 3: Publish stock reserve command
        publishStockReserveCommand(saga.getId(), payload);

        return saga;
    }

    // ─── Forward Flow ─────────────────────────────────────────────────────────

    /**
     * Stock reserved → trigger payment processing.
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
        log.info("Saga [{}] STOCK_RESERVED → processing payment", saga.getId());

        publishPaymentProcessCommand(saga.getId(), payload);
    }

    /**
     * Payment processed → confirm order, saga COMPLETED.
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
        log.info("Saga [{}] COMPLETED ✓", saga.getId());

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

    // ─── Compensation ─────────────────────────────────────────────────────────

    /**
     * Stock reservation failed → cancel order immediately (nothing to roll back yet).
     */
    @Transactional
    public void processStockReservationFailed(StockReservationFailedEvent event) {
        SagaInstance saga = findBySagaId(event.getSagaId());
        SagaPayload payload = fromJson(saga.getPayload());

        recordStep(saga, SagaStepName.STOCK_RESERVED, SagaStepStatus.FAILED, event.getReason());
        saga.setStatus(SagaStatus.COMPENSATING);
        sagaInstanceRepository.save(saga);
        log.warn("Saga [{}] stock reservation FAILED → cancelling order", saga.getId());

        publishOrderCancelledAndFail(saga, payload.getOrderId(), event.getReason());
    }

    /**
     * Payment failed → release stock (compensation step 1).
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
        log.warn("Saga [{}] payment FAILED → releasing stock", saga.getId());

        StockReleaseCommand cmd = StockReleaseCommand.builder()
                .eventId(UUID.randomUUID())
                .sagaId(saga.getId())
                .orderId(payload.getOrderId())
                .reservationId(payload.getReservationId())
                .reason(event.getReason())
                .build();
        cmd.init();
        kafkaTemplate.send(KafkaTopics.STOCK_RELEASE_REQUEST,
                payload.getOrderId().toString(), cmd);
    }

    /**
     * Stock released → cancel order (compensation step 2).
     */
    @Transactional
    public void processStockReleased(StockReleasedEvent event) {
        SagaInstance saga = findBySagaId(event.getSagaId());
        SagaPayload payload = fromJson(saga.getPayload());

        recordStep(saga, SagaStepName.STOCK_RELEASED, SagaStepStatus.COMPENSATED, null);
        sagaInstanceRepository.save(saga);
        log.info("Saga [{}] stock released → cancelling order", saga.getId());

        publishOrderCancelledAndFail(saga, payload.getOrderId(), "Payment failed");
    }

    /**
     * Payment cancelled → release stock (compensation).
     */
    @Transactional
    public void processPaymentCancelled(PaymentCancelledEvent event) {
        SagaInstance saga = findBySagaId(event.getSagaId());
        SagaPayload payload = fromJson(saga.getPayload());

        recordStep(saga, SagaStepName.PAYMENT_CANCELLED, SagaStepStatus.COMPENSATED, null);
        sagaInstanceRepository.save(saga);
        log.info("Saga [{}] payment cancelled → releasing stock", saga.getId());

        StockReleaseCommand cmd = StockReleaseCommand.builder()
                .eventId(UUID.randomUUID())
                .sagaId(saga.getId())
                .orderId(payload.getOrderId())
                .reservationId(payload.getReservationId())
                .reason(event.getReason())
                .build();
        cmd.init();
        kafkaTemplate.send(KafkaTopics.STOCK_RELEASE_REQUEST,
                payload.getOrderId().toString(), cmd);
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

    // ─── Private Helpers ──────────────────────────────────────────────────────

    private void publishStockReserveCommand(UUID sagaId, SagaPayload payload) {
        StockReserveCommand cmd = StockReserveCommand.builder()
                .eventId(UUID.randomUUID())
                .sagaId(sagaId)
                .orderId(payload.getOrderId())
                .productId(UUID.fromString(payload.getProductId()))
                .quantity(payload.getQuantity())
                .build();
        cmd.init();
        kafkaTemplate.send(KafkaTopics.STOCK_RESERVE_REQUEST,
                payload.getOrderId().toString(), cmd);
        log.info("Saga [{}] → published StockReserveCommand", sagaId);
    }

    private void publishPaymentProcessCommand(UUID sagaId, SagaPayload payload) {
        PaymentProcessCommand cmd = PaymentProcessCommand.builder()
                .eventId(UUID.randomUUID())
                .sagaId(sagaId)
                .orderId(payload.getOrderId())
                .userId(payload.getUserId())
                .amount(payload.getAmount())
                .currency(payload.getCurrency())
                .build();
        cmd.init();
        kafkaTemplate.send(KafkaTopics.PAYMENT_PROCESS_REQUEST,
                payload.getOrderId().toString(), cmd);
        log.info("Saga [{}] → published PaymentProcessCommand", sagaId);
    }

    private void publishOrderCancelledAndFail(SagaInstance saga, UUID orderId, String reason) {
        OrderCancelledEvent event = OrderCancelledEvent.builder()
                .eventId(UUID.randomUUID())
                .sagaId(saga.getId())
                .orderId(orderId)
                .reason(reason)
                .build();
        event.init();
        kafkaTemplate.send(KafkaTopics.ORDER_CANCELLED, orderId.toString(), event);

        recordStep(saga, SagaStepName.ORDER_CANCELLED, SagaStepStatus.COMPENSATED, reason);
        saga.setCurrentStep(SagaStepName.ORDER_CANCELLED);
        saga.setStatus(SagaStatus.FAILED);
        sagaInstanceRepository.save(saga);
        log.info("Saga [{}] FAILED — order cancelled, reason={}", saga.getId(), reason);
    }

    private SagaInstance findBySagaId(UUID sagaId) {
        return sagaInstanceRepository.findById(sagaId)
                .orElseThrow(() -> new EntityNotFoundException("Saga not found: " + sagaId));
    }

    private void recordStep(SagaInstance saga, SagaStepName stepName,
                             SagaStepStatus status, String errorMessage) {
        sagaStepRepository.save(SagaStep.builder()
                .sagaInstance(saga)
                .stepName(stepName)
                .status(status)
                .errorMessage(errorMessage)
                .build());
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
