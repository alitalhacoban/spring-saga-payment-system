package com.saga.orchestrator.grpc;

import com.saga.order.grpc.CreateOrderRequest;
import com.saga.order.grpc.CreateOrderResponse;
import com.saga.order.grpc.OrderServiceGrpc;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Slf4j
@Component
public class OrderServiceClient {

    // Retries only transient gRPC failures (UNAVAILABLE, DEADLINE_EXCEEDED). NOTE: createOrder
    // is a non-idempotent write RPC — if the server processed the request but the response was
    // lost, a retry could create a duplicate order. Proper mitigation needs order-service's
    // CreateOrder to accept its own idempotency key (out of scope here).
    private static final String RETRYABLE_GRPC_STATUS =
            "#root instanceof T(io.grpc.StatusRuntimeException) && (" +
                    "#root.status.code.name() == 'UNAVAILABLE' || " +
                    "#root.status.code.name() == 'DEADLINE_EXCEEDED')";

    @GrpcClient("order-service")
    private OrderServiceGrpc.OrderServiceBlockingStub orderServiceStub;

    @Retryable(exceptionExpression = RETRYABLE_GRPC_STATUS,
               maxAttempts = 3, backoff = @Backoff(delay = 200, multiplier = 2))
    public CreateOrderResponse createOrder(String userId, String productId,
                                           Integer quantity, BigDecimal price) {
        log.info("gRPC → order-service.createOrder: userId={}, productId={}", userId, productId);
        return orderServiceStub.createOrder(
                CreateOrderRequest.newBuilder()
                        .setUserId(userId)
                        .setProductId(productId)
                        .setQuantity(quantity)
                        .setPrice(price.doubleValue())
                        .build()
        );
    }
}
