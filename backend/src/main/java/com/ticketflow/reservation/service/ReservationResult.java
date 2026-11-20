package com.ticketflow.reservation.service;

import com.ticketflow.reservation.dto.ReservationResponse;

/**
 * 접수 결과. {@code created=true}면 새로 접수(202), false면 같은 요청의 재전송(200, PRD Q1).
 */
public record ReservationResult(ReservationResponse reservation, boolean created) {
}
