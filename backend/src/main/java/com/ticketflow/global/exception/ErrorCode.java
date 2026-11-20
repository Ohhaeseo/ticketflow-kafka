package com.ticketflow.global.exception;

import org.springframework.http.HttpStatus;

public enum ErrorCode {

    INVALID_REQUEST(HttpStatus.BAD_REQUEST, "요청 형식이 올바르지 않습니다."),
    INVALID_SALES_PERIOD(HttpStatus.BAD_REQUEST, "판매 시작 시각은 판매 종료 시각보다 빨라야 합니다."),
    EVENT_NOT_FOUND(HttpStatus.NOT_FOUND, "공연을 찾을 수 없습니다.");

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
