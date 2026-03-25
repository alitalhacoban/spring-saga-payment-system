package com.saga.shared.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.UUID;

/**
 * Published by payment-service after cancelling a payment during compensation.
 * Consumed by saga-orchestrator to confirm the compensation step is done.
 */
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentCancelledEvent extends BaseEvent {

    private UUID orderId;
    private UUID paymentId;
    private String reason;
}
