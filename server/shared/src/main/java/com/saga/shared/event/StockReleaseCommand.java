package com.saga.shared.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.UUID;

/**
 * Command published by saga-orchestrator → consumed by inventory-service (compensation).
 * Instructs inventory-service to release a previously made stock reservation.
 */
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class StockReleaseCommand extends BaseEvent {

    private UUID orderId;
    private UUID reservationId;
    private String reason;
}
