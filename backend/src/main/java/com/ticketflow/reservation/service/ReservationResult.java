package com.ticketflow.reservation.service;

import com.ticketflow.reservation.dto.ReservationResponse;

/**
 * 접수 결과.
 *
 * @param created   true면 새로 접수(비동기 202 / 동기 201), false면 같은 요청의 재전송(200, PRD Q1)
 * @param processed true면 응답 시점에 이미 최종 상태 (동기 비교 모드)
 */
public record ReservationResult(ReservationResponse reservation, boolean created, boolean processed) {

    public static ReservationResult accepted(ReservationResponse reservation) {
        return new ReservationResult(reservation, true, false);
    }

    public static ReservationResult processedNow(ReservationResponse reservation) {
        return new ReservationResult(reservation, true, true);
    }

    public static ReservationResult replay(ReservationResponse reservation) {
        return new ReservationResult(reservation, false, false);
    }
}
