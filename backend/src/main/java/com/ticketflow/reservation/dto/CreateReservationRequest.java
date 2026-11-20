package com.ticketflow.reservation.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record CreateReservationRequest(
        @Schema(example = "1")
        @NotNull @Positive
        Long eventId
) {

    /**
     * 멱등성 비교용 정규화 문자열. 필드가 늘어나면 여기에 같은 순서로 추가한다.
     */
    public String canonical() {
        return "POST /api/v1/reservations|eventId=" + eventId;
    }
}
