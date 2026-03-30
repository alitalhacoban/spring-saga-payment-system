package com.saga.orchestrator.grpc;

import com.saga.inventory.grpc.CheckStockRequest;
import com.saga.inventory.grpc.CheckStockResponse;
import com.saga.inventory.grpc.GetProductRequest;
import com.saga.inventory.grpc.GetProductResponse;
import com.saga.inventory.grpc.InventoryServiceGrpc;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class InventoryServiceClient {

    @GrpcClient("inventory-service")
    private InventoryServiceGrpc.InventoryServiceBlockingStub inventoryServiceStub;

    // Advisory check only — does not reserve. The actual reservation uses pessimistic locking.
    public CheckStockResponse checkStock(String productId, int quantity) {
        log.info("gRPC → inventory-service.checkStock: productId={}, quantity={}", productId, quantity);
        return inventoryServiceStub.checkStock(
                CheckStockRequest.newBuilder()
                        .setProductId(productId)
                        .setQuantity(quantity)
                        .build()
        );
    }

    public GetProductResponse getProduct(String productId) {
        log.info("gRPC → inventory-service.getProduct: productId={}", productId);
        return inventoryServiceStub.getProduct(
                GetProductRequest.newBuilder()
                        .setProductId(productId)
                        .build()
        );
    }
}
