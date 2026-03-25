package com.saga.shared.kafka;

/**
 * Central registry of all Kafka topic names used across Saga services.
 *
 * Naming convention:  {domain}.{event}
 *
 * Flow:
 *   order-service  ──► order.created          ──► saga-orchestrator
 *   orchestrator   ──► stock.reserve.request  ──► inventory-service
 *   inventory      ──► stock.reserved         ──► saga-orchestrator
 *   inventory      ──► stock.reservation.failed ► saga-orchestrator
 *   orchestrator   ──► payment.process.request ──► payment-service
 *   payment        ──► payment.processed       ──► saga-orchestrator
 *   payment        ──► payment.failed          ──► saga-orchestrator
 *   orchestrator   ──► order.confirmed         ──► order-service
 *
 * Compensation flow:
 *   orchestrator   ──► stock.release.request  ──► inventory-service
 *   inventory      ──► stock.released         ──► saga-orchestrator
 *   orchestrator   ──► payment.cancel.request ──► payment-service
 *   payment        ──► payment.cancelled      ──► saga-orchestrator
 *   orchestrator   ──► order.cancelled        ──► order-service
 */
public final class KafkaTopics {

    private KafkaTopics() {}

    // ─── Order ───────────────────────────────────────────────────────────────
    public static final String ORDER_CREATED   = "order.created";
    public static final String ORDER_CONFIRMED = "order.confirmed";
    public static final String ORDER_CANCELLED = "order.cancelled";

    // ─── Stock ───────────────────────────────────────────────────────────────
    public static final String STOCK_RESERVE_REQUEST    = "stock.reserve.request";
    public static final String STOCK_RESERVED           = "stock.reserved";
    public static final String STOCK_RESERVATION_FAILED = "stock.reservation.failed";
    public static final String STOCK_RELEASE_REQUEST    = "stock.release.request";
    public static final String STOCK_RELEASED           = "stock.released";

    // ─── Payment ─────────────────────────────────────────────────────────────
    public static final String PAYMENT_PROCESS_REQUEST = "payment.process.request";
    public static final String PAYMENT_PROCESSED       = "payment.processed";
    public static final String PAYMENT_FAILED          = "payment.failed";
    public static final String PAYMENT_CANCEL_REQUEST  = "payment.cancel.request";
    public static final String PAYMENT_CANCELLED       = "payment.cancelled";
}
