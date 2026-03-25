package com.saga.inventory.repository;

import com.saga.inventory.entity.StockReservation;
import com.saga.inventory.entity.enums.ReservationStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface StockReservationRepository extends JpaRepository<StockReservation, UUID> {

    Optional<StockReservation> findByOrderId(String orderId);

    List<StockReservation> findByStatus(ReservationStatus status);
}
