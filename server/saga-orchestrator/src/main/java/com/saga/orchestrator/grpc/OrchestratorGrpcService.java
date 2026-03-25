package com.saga.orchestrator.grpc;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class OrchestratorGrpcService extends OrchestratorServiceGrpc.OrchestratorServiceImplBase {

    // TODO: inject SagaOrchestrationService when implemented
    // private final SagaOrchestrationService orchestrationService;

    @Override
    public void startSaga(StartSagaRequest request,
                          StreamObserver<StartSagaResponse> responseObserver) {
        log.info("gRPC startSaga called: userId={}, productId={}, quantity={}, amount={} {}",
                request.getUserId(), request.getProductId(),
                request.getQuantity(), request.getAmount(), request.getCurrency());
        try {
            // TODO: delegate to orchestrationService.start(request)
            StartSagaResponse response = StartSagaResponse.newBuilder()
                    .setSagaId("mock-saga-id")
                    .setOrderId("mock-order-id")
                    .setStatus("STARTED")
                    .setMessage("Saga started successfully")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("startSaga failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException()
            );
        }
    }

    @Override
    public void getSagaStatus(SagaStatusRequest request,
                              StreamObserver<SagaStatusResponse> responseObserver) {
        log.info("gRPC getSagaStatus called: sagaId={}", request.getSagaId());
        try {
            // TODO: delegate to orchestrationService.getStatus(request)
            SagaStatusResponse response = SagaStatusResponse.newBuilder()
                    .setSagaId(request.getSagaId())
                    .setOrderId("mock-order-id")
                    .setCurrentStep("ORDER_CREATED")
                    .setStatus("STARTED")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("getSagaStatus failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException()
            );
        }
    }
}
