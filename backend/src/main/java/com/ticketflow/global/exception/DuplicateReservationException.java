package com.ticketflow.global.exception;

/**
 * 같은 사용자가 같은 공연에 이미 예약을 접수한 경우. 응답에 기존 예약 ID를 담는다 (PRD Q2).
 */
public class DuplicateReservationException extends BusinessException {

    private final long reservationId;

    public DuplicateReservationException(long reservationId) {
        super(ErrorCode.RESERVATION_ALREADY_EXISTS);
        this.reservationId = reservationId;
    }

    public long reservationId() {
        return reservationId;
    }
}
