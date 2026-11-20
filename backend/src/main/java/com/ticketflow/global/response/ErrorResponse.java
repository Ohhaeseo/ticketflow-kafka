package com.ticketflow.global.response;

import com.ticketflow.global.exception.ErrorCode;

import java.util.List;

/**
 * 공통 오류 응답 형식 (PRD §11.4).
 */
public record ErrorResponse(String code, String message, List<FieldErrorDetail> errors) {

    public record FieldErrorDetail(String field, String reason) {
    }

    public static ErrorResponse of(ErrorCode errorCode, String message) {
        return new ErrorResponse(errorCode.name(), message, List.of());
    }

    public static ErrorResponse of(ErrorCode errorCode, List<FieldErrorDetail> errors) {
        return new ErrorResponse(errorCode.name(), errorCode.defaultMessage(), errors);
    }
}
