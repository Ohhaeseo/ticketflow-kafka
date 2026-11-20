package com.ticketflow.global.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.ticketflow.global.exception.ErrorCode;

import java.util.List;

/**
 * 공통 오류 응답 형식 (PRD §11.4). {@code reservationId}는 기존 예약과 충돌했을 때만 담는다.
 */
public record ErrorResponse(
        String code,
        String message,
        List<FieldErrorDetail> errors,
        @JsonInclude(JsonInclude.Include.NON_NULL) Long reservationId
) {

    public record FieldErrorDetail(String field, String reason) {
    }

    public static ErrorResponse of(ErrorCode errorCode, String message) {
        return new ErrorResponse(errorCode.name(), message, List.of(), null);
    }

    public static ErrorResponse of(ErrorCode errorCode, List<FieldErrorDetail> errors) {
        return new ErrorResponse(errorCode.name(), errorCode.defaultMessage(), errors, null);
    }

    public static ErrorResponse withReservation(ErrorCode errorCode, String message, long reservationId) {
        return new ErrorResponse(errorCode.name(), message, List.of(), reservationId);
    }
}
