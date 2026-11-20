package com.ticketflow.global.exception;

import org.springframework.http.HttpStatus;

public enum ErrorCode {

    INVALID_REQUEST(HttpStatus.BAD_REQUEST, "요청 형식이 올바르지 않습니다."),
    INVALID_SALES_PERIOD(HttpStatus.BAD_REQUEST, "판매 시작 시각은 판매 종료 시각보다 빨라야 합니다."),
    INVALID_IDEMPOTENCY_KEY(HttpStatus.BAD_REQUEST,
            "Idempotency-Key는 1~100자의 영문, 숫자, '.', '_', ':', '-'만 쓸 수 있습니다."),

    UNAUTHENTICATED(HttpStatus.UNAUTHORIZED, "사용자를 식별할 수 없습니다."),
    FORBIDDEN(HttpStatus.FORBIDDEN, "관리자만 사용할 수 있습니다."),

    EVENT_NOT_FOUND(HttpStatus.NOT_FOUND, "공연을 찾을 수 없습니다."),
    RESERVATION_NOT_FOUND(HttpStatus.NOT_FOUND, "예약을 찾을 수 없습니다."),

    IDEMPOTENCY_KEY_REUSED(HttpStatus.CONFLICT, "같은 Idempotency-Key로 다른 내용의 요청이 이미 접수되었습니다."),
    RESERVATION_ALREADY_EXISTS(HttpStatus.CONFLICT, "이미 해당 공연에 접수된 예약이 있습니다."),

    SALES_NOT_STARTED(HttpStatus.UNPROCESSABLE_CONTENT, "아직 판매가 시작되지 않았습니다."),
    SALES_ENDED(HttpStatus.UNPROCESSABLE_CONTENT, "판매가 종료되었습니다."),

    RATE_LIMITED(HttpStatus.TOO_MANY_REQUESTS, "예약 시도가 너무 잦습니다. 잠시 후 다시 시도하세요.");

    private final HttpStatus status;
    private final String defaultMessage;

    ErrorCode(HttpStatus status, String defaultMessage) {
        this.status = status;
        this.defaultMessage = defaultMessage;
    }

    public HttpStatus status() {
        return status;
    }

    public String defaultMessage() {
        return defaultMessage;
    }
}
