package com.saga.order.grpc;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class OrderGrpcService extends OrderServiceGrpc.OrderServiceImplBase {

    // TODO: inject OrderService when implemented
    // private final OrderService orderService;

    @Override
    public void createOrder(CreateOrderRequest request,
                            StreamObserver<CreateOrderResponse> responseObserver) {
        log.info("gRPC createOrder called: userId={}, productId={}, quantity={}, price={}",
                request.getUserId(), request.getProductId(),
                request.getQuantity(), request.getPrice());
        try {
            // TODO: delegate to orderService.create(request)
            CreateOrderResponse response = CreateOrderResponse.newBuilder()
                    .setOrderId("mock-order-id")
                    .setStatus("PENDING")
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
        log.info("gRPC updateOrderStatus called: orderId={}, status={}",
                request.getOrderId(), request.getStatus());
        try {
            // TODO: delegate to orderService.updateStatus(request)
            UpdateOrderStatusResponse response = UpdateOrderStatusResponse.newBuilder()
                    .setOrderId(request.getOrderId())
                    .setStatus(request.getStatus())
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
