package com.ticketflow.messaging.consumer;

/**
 * 다시 시도해도 결과가 같은 메시지 오류 (스키마 불일치, 필수 값 누락, 대상 예약 없음).
 * 재시도 없이 바로 DLT로 보낸다 (PRD FR-CSM-03).
 */
public class NonRetryableMessageException extends RuntimeException {

    public NonRetryableMessageException(String message) {
        super(message);
    }
}
