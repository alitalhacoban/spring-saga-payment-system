package com.saga.payment.grpc;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class PaymentGrpcService extends PaymentServiceGrpc.PaymentServiceImplBase {

    // TODO: inject PaymentService when implemented
    // private final PaymentService paymentService;

    @Override
    public void processPayment(PaymentRequest request,
                               StreamObserver<PaymentResponse> responseObserver) {
        log.info("gRPC processPayment called: orderId={}, amount={} {}",
                request.getOrderId(), request.getAmount(), request.getCurrency());
        try {
            // TODO: delegate to paymentService.process(request)
            PaymentResponse response = PaymentResponse.newBuilder()
                    .setPaymentId("mock-payment-id")
                    .setStatus("SUCCESS")
                    .setMessage("Payment processed successfully")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("processPayment failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException()
            );
        }
    }

    @Override
    public void cancelPayment(CancelPaymentRequest request,
                              StreamObserver<CancelPaymentResponse> responseObserver) {
        log.info("gRPC cancelPayment called: paymentId={}, reason={}",
                request.getPaymentId(), request.getReason());
        try {
            // TODO: delegate to paymentService.cancel(request)
            CancelPaymentResponse response = CancelPaymentResponse.newBuilder()
                    .setPaymentId(request.getPaymentId())
                    .setStatus("CANCELLED")
                    .setMessage("Payment cancelled successfully")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("cancelPayment failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException()
            );
        }
    }
}
