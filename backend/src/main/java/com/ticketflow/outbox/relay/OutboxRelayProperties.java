package com.ticketflow.outbox.relay;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Outbox Relay 설정 ({@code ticketflow.outbox.relay.*}).
 *
 * @param enabled       false면 Relay 빈을 만들지 않는다 (장애 실험용)
 * @param fixedDelayMs  폴링 간격. 직전 실행이 끝난 뒤부터 센다
 * @param batchSize     한 번에 읽어 발행할 최대 건수
 * @param sendTimeoutMs 브로커 ack를 기다리는 최대 시간
 */
@ConfigurationProperties(prefix = "ticketflow.outbox.relay")
public record OutboxRelayProperties(boolean enabled, long fixedDelayMs, int batchSize, long sendTimeoutMs) {
}
