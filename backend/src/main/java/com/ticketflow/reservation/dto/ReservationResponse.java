package com.ticketflow.reservation.dto;

import com.ticketflow.reservation.entity.Reservation;
import com.ticketflow.reservation.entity.ReservationStatus;

import java.time.Instant;

/**
 * 예약 응답. {@code status=QUEUED}는 접수 완료일 뿐 좌석 확보가 아니다.
 */
public record ReservationResponse(
        Long reservationId,
        Long eventId,
        ReservationStatus status,
        String failureReason,
        String statusUrl,
        Instant createdAt,
        Instant updatedAt
) {

    public static ReservationResponse from(Reservation reservation) {
        return new ReservationResponse(
                reservation.getId(),
                reservation.getEventId(),
                reservation.getStatus(),
                reservation.getFailureReason(),
                statusUrl(reservation.getId()),
                reservation.getCreatedAt(),
                reservation.getUpdatedAt());
    }

    public static ReservationResponse queued(long reservationId, long eventId, Instant now) {
        return new ReservationResponse(
                reservationId, eventId, ReservationStatus.QUEUED, null, statusUrl(reservationId), now, now);
    }

    public static String statusUrl(long reservationId) {
        return "/api/v1/reservations/" + reservationId;
    }
}
