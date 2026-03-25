package com.saga.order.listener;

import com.saga.order.service.OrderService;
import com.saga.shared.event.OrderCancelledEvent;
import com.saga.shared.event.OrderConfirmedEvent;
import com.saga.shared.kafka.KafkaTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Listens to order lifecycle events from saga-orchestrator.
 *
 *   order.confirmed → confirmOrder   (saga completed successfully)
 *   order.cancelled → cancelOrder    (saga compensation)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventListener {

    private final OrderService orderService;

    @KafkaListener(topics = KafkaTopics.ORDER_CONFIRMED,
                   groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderConfirmed(OrderConfirmedEvent event) {
        log.info("← order.confirmed: sagaId={}, orderId={}",
                event.getSagaId(), event.getOrderId());
        orderService.confirmOrder(event.getOrderId());
    }

    @KafkaListener(topics = KafkaTopics.ORDER_CANCELLED,
                   groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderCancelled(OrderCancelledEvent event) {
        log.warn("← order.cancelled: sagaId={}, orderId={}, reason={}",
                event.getSagaId(), event.getOrderId(), event.getReason());
        orderService.cancelOrder(event.getOrderId(), event.getReason());
    }
}
