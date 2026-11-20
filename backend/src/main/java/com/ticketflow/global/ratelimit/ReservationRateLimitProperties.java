package com.ticketflow.global.ratelimit;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * 예약 시도 빈도 제한 설정 ({@code ticketflow.rate-limit.reservation.*}).
 *
 * @param enabled          false면 검사하지 않는다 (부하 테스트에서 끌 수 있음)
 * @param maxRequests      window 동안 허용하는 시도 횟수
 * @param window           첫 시도부터 세는 시간 창
 * @param failOpenCooldown Redis 오류 뒤 Redis 호출을 건너뛰고 바로 허용하는 시간
 */
@ConfigurationProperties(prefix = "ticketflow.rate-limit.reservation")
public record ReservationRateLimitProperties(boolean enabled, int maxRequests, Duration window,
                                             Duration failOpenCooldown) {
}
