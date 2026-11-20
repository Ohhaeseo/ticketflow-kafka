package com.ticketflow.global.exception;

/**
 * 업무 규칙 위반을 나타내는 예외. {@link ErrorCode}가 HTTP 상태와 응답 코드를 결정한다.
 */
public class BusinessException extends RuntimeException {

    private final ErrorCode errorCode;

    public BusinessException(ErrorCode errorCode) {
        this(errorCode, errorCode.defaultMessage());
    }

    public BusinessException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public ErrorCode errorCode() {
        return errorCode;
    }
}
