package com.ticketflow.reservation.repository;

import com.ticketflow.reservation.entity.Reservation;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface ReservationRepository extends JpaRepository<Reservation, Long> {

    Optional<Reservation> findByUserIdAndIdempotencyKey(Long userId, String idempotencyKey);

    Optional<Reservation> findByEventIdAndUserId(Long eventId, Long userId);

    List<Reservation> findAllByUserIdOrderByIdDesc(Long userId);
}
