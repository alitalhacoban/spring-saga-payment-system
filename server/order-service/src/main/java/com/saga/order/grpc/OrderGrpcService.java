package com.saga.order.grpc;

import com.saga.order.entity.Order;
import com.saga.order.service.OrderService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class OrderGrpcService extends OrderServiceGrpc.OrderServiceImplBase {

    private final OrderService orderService;

    @Override
    public void createOrder(CreateOrderRequest request,
                            StreamObserver<CreateOrderResponse> responseObserver) {
        log.info("gRPC createOrder: userId={}, productId={}, quantity={}, price={}",
                request.getUserId(), request.getProductId(),
                request.getQuantity(), request.getPrice());
        try {
            Order order = orderService.createOrder(
                    request.getUserId(),
                    request.getProductId(),
                    request.getQuantity(),
                    BigDecimal.valueOf(request.getPrice())
            );

            CreateOrderResponse response = CreateOrderResponse.newBuilder()
                    .setOrderId(order.getId().toString())
                    .setStatus(order.getStatus().name())
                    .setMessage("Order created successfully")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("createOrder failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException()
            );
        }
    }

    @Override
    public void updateOrderStatus(UpdateOrderStatusRequest request,
                                  StreamObserver<UpdateOrderStatusResponse> responseObserver) {
        log.info("gRPC updateOrderStatus: orderId={}, status={}",
                request.getOrderId(), request.getStatus());
        try {
            Order order = switch (request.getStatus()) {
                case "CONFIRMED" -> orderService.confirmOrder(UUID.fromString(request.getOrderId()));
                case "CANCELLED" -> orderService.cancelOrder(UUID.fromString(request.getOrderId()), "Manual cancellation");
                default -> throw new IllegalArgumentException("Unknown status: " + request.getStatus());
            };

            UpdateOrderStatusResponse response = UpdateOrderStatusResponse.newBuilder()
                    .setOrderId(order.getId().toString())
                    .setStatus(order.getStatus().name())
                    .setMessage("Order status updated successfully")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("updateOrderStatus failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException()
            );
        }
    }
}
