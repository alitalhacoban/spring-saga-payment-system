package com.saga.inventory.grpc;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class InventoryGrpcService extends InventoryServiceGrpc.InventoryServiceImplBase {

    // TODO: inject InventoryService when implemented
    // private final InventoryService inventoryService;

    @Override
    public void reserveStock(ReserveStockRequest request,
                             StreamObserver<ReserveStockResponse> responseObserver) {
        log.info("gRPC reserveStock called: orderId={}, productId={}, quantity={}",
                request.getOrderId(), request.getProductId(), request.getQuantity());
        try {
            // TODO: delegate to inventoryService.reserve(request)
            ReserveStockResponse response = ReserveStockResponse.newBuilder()
                    .setReservationId("mock-reservation-id")
                    .setStatus("RESERVED")
                    .setMessage("Stock reserved successfully")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("reserveStock failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException()
            );
        }
    }

    @Override
    public void releaseStock(ReleaseStockRequest request,
                             StreamObserver<ReleaseStockResponse> responseObserver) {
        log.info("gRPC releaseStock called: reservationId={}, reason={}",
                request.getReservationId(), request.getReason());
        try {
            // TODO: delegate to inventoryService.release(request)
            ReleaseStockResponse response = ReleaseStockResponse.newBuilder()
                    .setReservationId(request.getReservationId())
                    .setStatus("RELEASED")
                    .setMessage("Stock released successfully")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("releaseStock failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException()
            );
        }
    }
}
