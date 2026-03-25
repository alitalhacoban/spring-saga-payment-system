package com.saga.shared.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Command published by saga-orchestrator → consumed by payment-service.
 * Instructs payment-service to process the payment for an order.
 */
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentProcessCommand extends BaseEvent {

    private UUID orderId;
    private String userId;
    private BigDecimal amount;
    private String currency;
}
