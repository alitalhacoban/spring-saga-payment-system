package com.saga.shared.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.UUID;

/**
 * Command published by saga-orchestrator → consumed by inventory-service.
 * Instructs inventory-service to reserve stock for an order.
 */
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class StockReserveCommand extends BaseEvent {

    private UUID orderId;
    private UUID productId;
    private Integer quantity;
}
