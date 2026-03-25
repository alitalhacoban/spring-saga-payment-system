package com.saga.inventory.grpc;

import com.saga.inventory.entity.StockReservation;
import com.saga.inventory.service.InventoryService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

import java.util.UUID;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class InventoryGrpcService extends InventoryServiceGrpc.InventoryServiceImplBase {

    private final InventoryService inventoryService;

    @Override
    public void reserveStock(ReserveStockRequest request,
                             StreamObserver<ReserveStockResponse> responseObserver) {
        log.info("gRPC reserveStock: orderId={}, productId={}, quantity={}",
                request.getOrderId(), request.getProductId(), request.getQuantity());
        try {
            StockReservation reservation = inventoryService.reserveStock(
                    null, // sagaId unknown in direct gRPC calls
                    request.getOrderId(),
                    UUID.fromString(request.getProductId()),
                    request.getQuantity()
            );

            ReserveStockResponse response = ReserveStockResponse.newBuilder()
                    .setReservationId(reservation.getId().toString())
                    .setStatus(reservation.getStatus().name())
                    .setMessage("Stock reserved successfully")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (IllegalStateException e) {
            // Insufficient stock — surface as gRPC FAILED_PRECONDITION
            log.warn("reserveStock precondition failed: {}", e.getMessage());
            responseObserver.onError(
                    Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException()
            );
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
        log.info("gRPC releaseStock: reservationId={}, reason={}",
                request.getReservationId(), request.getReason());
        try {
            StockReservation reservation = inventoryService.releaseStock(
                    null, // sagaId unknown in direct gRPC calls
                    UUID.fromString(request.getReservationId()),
                    request.getReason()
            );

            ReleaseStockResponse response = ReleaseStockResponse.newBuilder()
                    .setReservationId(reservation.getId().toString())
                    .setStatus(reservation.getStatus().name())
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
