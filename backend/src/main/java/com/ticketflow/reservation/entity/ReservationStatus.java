package com.ticketflow.reservation.entity;

/**
 * 예약 상태 (PRD §9.3). QUEUED 외에는 모두 최종 상태다.
 */
public enum ReservationStatus {
    QUEUED,
    CONFIRMED,
    SOLD_OUT,
    FAILED;

    public boolean isFinal() {
        return this != QUEUED;
    }
}
