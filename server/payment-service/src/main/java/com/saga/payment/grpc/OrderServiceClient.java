package com.saga.payment.grpc;

import com.saga.order.grpc.GetOrderRequest;
import com.saga.order.grpc.GetOrderResponse;
import com.saga.order.grpc.OrderServiceGrpc;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Component;

/**
 * Wraps the gRPC blocking stub for order-service.
 * Used by PaymentService to validate the order is still PENDING
 * before attempting to charge the customer.
 */
@Slf4j
@Component
public class OrderServiceClient {

    @GrpcClient("order-service")
    private OrderServiceGrpc.OrderServiceBlockingStub orderServiceStub;

    public GetOrderResponse getOrder(String orderId) {
        log.info("gRPC → order-service.getOrder: orderId={}", orderId);
        return orderServiceStub.getOrder(
                GetOrderRequest.newBuilder()
                        .setOrderId(orderId)
                        .build()
        );
    }
}
