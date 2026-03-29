package com.saga.orchestrator.controller;

import com.saga.orchestrator.dto.SagaStatusResponse;
import com.saga.orchestrator.dto.StartOrderRequest;
import com.saga.orchestrator.dto.StartOrderResponse;
import com.saga.orchestrator.entity.SagaInstance;
import com.saga.orchestrator.entity.SagaStep;
import com.saga.orchestrator.service.SagaOrchestrationService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
public class SagaController {

    private final SagaOrchestrationService orchestrationService;

    /**
     * POST /api/orders
     *
     * Triggers a new saga. Internally:
     *   1. Creates the order via gRPC → order-service
     *   2. Publishes StockReserveCommand → inventory-service
     *   3. Kafka drives the rest asynchronously
     *
     * Returns the sagaId which can be polled via GET /api/sagas/{sagaId}
     */
    @PostMapping("/api/orders")
    public ResponseEntity<StartOrderResponse> startOrder(@Valid @RequestBody StartOrderRequest request) {
        log.info("POST /api/orders: userId={}, productId={}, qty={}, amount={} {}",
                request.userId(), request.productId(), request.quantity(),
                request.amount(), request.currency());

        SagaInstance saga = orchestrationService.startSaga(
                request.userId(),
                request.productId(),
                request.quantity(),
                request.amount(),
                request.currency()
        );

        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(new StartOrderResponse(
                        saga.getId(),
                        saga.getOrderId(),
                        saga.getStatus().name(),
                        "Saga started. Poll GET /api/sagas/" + saga.getId() + " for status."
                ));
    }

    /**
     * Returns 400 Bad Request when saga cannot be started due to business rule violations
     * (e.g. insufficient stock detected by gRPC pre-check on inventory-service).
     */
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, String>> handleIllegalState(IllegalStateException ex) {
        log.warn("Saga start rejected: {}", ex.getMessage());
        return ResponseEntity.badRequest().body(Map.of("error", ex.getMessage()));
    }

    /**
     * GET /api/sagas/{sagaId}
     *
     * Returns the current state of a saga including all recorded steps.
     * Useful for polling after POST /api/orders.
     */
    @GetMapping("/api/sagas/{sagaId}")
    public ResponseEntity<SagaStatusResponse> getSagaStatus(@PathVariable UUID sagaId) {
        log.info("GET /api/sagas/{}", sagaId);

        SagaInstance saga = orchestrationService.findById(sagaId);
        List<SagaStep> steps = orchestrationService.findStepsBySagaId(sagaId);

        return ResponseEntity.ok(SagaStatusResponse.from(saga, steps));
    }
}
