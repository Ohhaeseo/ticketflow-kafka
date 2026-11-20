package com.ticketflow.reservation.service;

/**
 * Consumer가 메시지 하나를 처리한 결과.
 */
public enum ProcessingOutcome {
    /** 재고 확보, CONFIRMED로 전이 */
    CONFIRMED,
    /** 재고 없음, SOLD_OUT으로 전이 (정상 업무 결과, 재시도하지 않음) */
    SOLD_OUT,
    /** 판매 기간 종료 후 처리됨, FAILED(SALES_CLOSED)로 전이 */
    FAILED,
    /** 이미 최종 상태인 예약 (중복 메시지). 아무것도 바꾸지 않음 */
    SKIPPED_ALREADY_PROCESSED,
    /** 예약이 없음. 아무것도 바꾸지 않음 */
    SKIPPED_NOT_FOUND
}
