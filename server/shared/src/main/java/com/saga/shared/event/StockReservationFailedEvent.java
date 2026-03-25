package com.saga.shared.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.UUID;

/**
 * Published by inventory-service when stock reservation fails (e.g. insufficient stock).
 * Consumed by saga-orchestrator to trigger compensation (cancel the order).
 */
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class StockReservationFailedEvent extends BaseEvent {

    private UUID orderId;
    private UUID productId;
    private Integer requestedQuantity;
    private Integer availableQuantity;
    private String reason;
}
