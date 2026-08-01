package com.saga.orchestrator.repository;

import com.saga.orchestrator.entity.SagaInstance;
import com.saga.orchestrator.entity.enums.SagaStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface SagaInstanceRepository extends JpaRepository<SagaInstance, UUID> {

    Optional<SagaInstance> findByOrderId(String orderId);

    Optional<SagaInstance> findByIdempotencyKey(String idempotencyKey);

    List<SagaInstance> findByStatus(SagaStatus status);
}
