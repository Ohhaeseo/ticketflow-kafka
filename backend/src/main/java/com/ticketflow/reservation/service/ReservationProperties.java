package com.ticketflow.reservation.service;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 예약 처리 설정 ({@code ticketflow.reservation.*}).
 *
 * @param processingMode {@code async}(기본): 접수만 하고 Outbox → Kafka → Consumer가 확정한다.
 *                       {@code sync}: 벤치마크 비교용. 요청 트랜잭션 안에서 바로 재고를 줄이고 확정한다 (PRD FR-BMK-01).
 */
@ConfigurationProperties(prefix = "ticketflow.reservation")
public record ReservationProperties(ProcessingMode processingMode) {

    public enum ProcessingMode {
        ASYNC,
        SYNC
    }

    public boolean synchronous() {
        return processingMode == ProcessingMode.SYNC;
    }
}
