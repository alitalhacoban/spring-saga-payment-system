package com.saga.shared.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.UUID;

/**
 * Published by saga-orchestrator when all saga steps complete successfully.
 * Consumed by order-service to mark the order as CONFIRMED.
 */
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class OrderConfirmedEvent extends BaseEvent {

    private UUID orderId;
    private UUID paymentId;
    private UUID reservationId;
}
