package com.saga.payment.grpc;

import com.saga.order.grpc.GetOrderRequest;
import com.saga.order.grpc.GetOrderResponse;
import com.saga.order.grpc.OrderServiceGrpc;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Component;

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
