package com.saga.payment.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.saga.order.grpc.GetOrderResponse;
import com.saga.payment.entity.OutboxEvent;
import com.saga.payment.entity.Payment;
import com.saga.payment.entity.enums.OutboxStatus;
import com.saga.payment.entity.enums.PaymentStatus;
import com.saga.payment.grpc.OrderServiceClient;
import com.saga.payment.repository.OutboxEventRepository;
import com.saga.payment.repository.PaymentRepository;
import com.saga.shared.event.BaseEvent;
import com.saga.shared.event.PaymentCancelledEvent;
import com.saga.shared.event.PaymentFailedEvent;
import com.saga.shared.event.PaymentProcessedEvent;
import com.saga.shared.kafka.KafkaTopics;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final OrderServiceClient orderServiceClient;

    @Transactional
    public Payment processPayment(UUID sagaId, String orderId, String userId,
                                  BigDecimal amount, String currency, String idempotencyKey) {
        if (idempotencyKey != null) {
            var existing = paymentRepository.findByIdempotencyKey(idempotencyKey);
            if (existing.isPresent()) {
                log.info("Payment processing skipped — idempotent replay: idempotencyKey={}, paymentId={}",
                        idempotencyKey, existing.get().getId());
                return existing.get();
            }
        }

        // Validate the order via gRPC before charging — ensures the order is still PENDING
        GetOrderResponse orderDetails = orderServiceClient.getOrder(orderId);
        log.info("Order validated via gRPC: orderId={}, status={}, userId={}, totalAmount={}",
                orderDetails.getOrderId(), orderDetails.getStatus(),
                orderDetails.getUserId(), orderDetails.getTotalAmount());
        if (!"PENDING".equals(orderDetails.getStatus())) {
            throw new IllegalStateException(
                    "Cannot process payment: order " + orderId + " is not PENDING (status=" + orderDetails.getStatus() + ")");
        }

        Payment payment = Payment.builder()
                .orderId(orderId)
                .userId(userId)
                .amount(amount)
                .currency(currency)
                .status(PaymentStatus.PENDING)
                .idempotencyKey(idempotencyKey)
                .build();
        try {
            payment = paymentRepository.save(payment);
        } catch (DataIntegrityViolationException e) {
            Payment raced = paymentRepository.findByIdempotencyKey(idempotencyKey)
                    .orElseThrow(() -> e);
            log.info("Payment processing skipped — idempotent replay (race): idempotencyKey={}, paymentId={}",
                    idempotencyKey, raced.getId());
            return raced;
        }
        log.info("Payment initiated: id={}, orderId={}, amount={} {}", payment.getId(), orderId, amount, currency);

        // TODO: integrate with real payment gateway
        boolean success = isPaymentApproved(amount);

        if (success) {
            payment.setStatus(PaymentStatus.SUCCESS);
            payment = paymentRepository.save(payment);
            log.info("Payment successful: id={}", payment.getId());

            PaymentProcessedEvent event = PaymentProcessedEvent.builder()
                    .eventId(UUID.randomUUID())
                    .sagaId(sagaId)
                    .orderId(UUID.fromString(orderId))
                    .paymentId(payment.getId())
                    .amount(amount)
                    .currency(currency)
                    .build();
            event.init();
            outboxEventRepository.save(toOutboxEvent("Payment", payment.getId().toString(),
                    KafkaTopics.PAYMENT_PROCESSED, orderId, event));
        } else {
            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Payment declined by gateway");
            payment = paymentRepository.save(payment);
            log.warn("Payment failed: id={}", payment.getId());

            PaymentFailedEvent event = PaymentFailedEvent.builder()
                    .eventId(UUID.randomUUID())
                    .sagaId(sagaId)
                    .orderId(UUID.fromString(orderId))
                    .paymentId(payment.getId())
                    .amount(amount)
                    .reason("Payment declined by gateway")
                    .build();
            event.init();
            outboxEventRepository.save(toOutboxEvent("Payment", payment.getId().toString(),
                    KafkaTopics.PAYMENT_FAILED, orderId, event));
        }
        return payment;
    }

    @Transactional
    public Payment cancelPayment(UUID sagaId, UUID paymentId, String reason) {
        Payment payment = findById(paymentId);
        payment.setStatus(PaymentStatus.CANCELLED);
        payment.setFailureReason(reason);
        payment = paymentRepository.save(payment);
        log.info("Payment cancelled: id={}, reason={}", paymentId, reason);

        PaymentCancelledEvent event = PaymentCancelledEvent.builder()
                .eventId(UUID.randomUUID())
                .sagaId(sagaId)
                .orderId(UUID.fromString(payment.getOrderId()))
                .paymentId(payment.getId())
                .reason(reason)
                .build();
        event.init();
        outboxEventRepository.save(toOutboxEvent("Payment", payment.getId().toString(),
                KafkaTopics.PAYMENT_CANCELLED, payment.getOrderId(), event));
        return payment;
    }

    @Transactional(readOnly = true)
    public Payment findById(UUID paymentId) {
        return paymentRepository.findById(paymentId)
                .orElseThrow(() -> new EntityNotFoundException("Payment not found: " + paymentId));
    }

    @Transactional(readOnly = true)
    public Payment findByOrderId(String orderId) {
        return paymentRepository.findByOrderId(orderId)
                .orElseThrow(() -> new EntityNotFoundException("Payment not found for order: " + orderId));
    }

    // Simulates payment gateway approval logic
    private boolean isPaymentApproved(BigDecimal amount) {
        return amount.compareTo(BigDecimal.valueOf(10_000)) < 0;
    }

    private OutboxEvent toOutboxEvent(String aggregateType, String aggregateId,
                                      String topic, String kafkaKey, BaseEvent event) {
        try {
            return OutboxEvent.builder()
                    .aggregateType(aggregateType)
                    .aggregateId(aggregateId)
                    .eventType(event.getClass().getName())
                    .topic(topic)
                    .kafkaKey(kafkaKey)
                    .payload(objectMapper.writeValueAsString(event))
                    .status(OutboxStatus.PENDING)
                    .build();
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize event for outbox: " + event.getClass(), e);
        }
    }
}
