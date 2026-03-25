package com.saga.shared.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.UUID;

/**
 * Published by saga-orchestrator during compensation.
 * Consumed by order-service to mark the order as CANCELLED.
 */
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCancelledEvent extends BaseEvent {

    private UUID orderId;
    private String reason;
}
