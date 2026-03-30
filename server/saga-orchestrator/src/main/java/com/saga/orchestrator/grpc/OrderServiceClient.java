package com.saga.orchestrator.grpc;

import com.saga.order.grpc.CreateOrderRequest;
import com.saga.order.grpc.CreateOrderResponse;
import com.saga.order.grpc.OrderServiceGrpc;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Slf4j
@Component
public class OrderServiceClient {

    @GrpcClient("order-service")
    private OrderServiceGrpc.OrderServiceBlockingStub orderServiceStub;

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
