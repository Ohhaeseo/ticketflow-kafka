package com.ticketflow.global.ratelimit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

/**
 * 사용자별 예약 시도 빈도 제한 (PRD FR-RL-01·02).
 * <p>
 * Redis 키 {@code rl:reservation:{userId}}를 INCR하고, 첫 시도에서 만료 시간(window)을 건다.
 * 첫 시도부터 window 동안 maxRequests회를 넘으면 {@link RateLimitExceededException}(429)을 던진다.
 * <p>
 * <b>fail-open:</b> 빈도 제한은 부하 완화 수단일 뿐 정확성 장치가 아니다. Redis가 죽거나 느리면 요청을 허용하고,
 * 오류 뒤 {@code failOpenCooldown} 동안은 Redis를 부르지 않아 요청마다 타임아웃을 기다리지 않게 한다.
 */
@Component
public class ReservationRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(ReservationRateLimiter.class);

    // INCR 후 첫 시도면 만료를 건다. 반환: {현재 횟수, 남은 ms}
    private static final RedisScript<List> SCRIPT = new DefaultRedisScript<>("""
            local count = redis.call('INCR', KEYS[1])
            if count == 1 then
              redis.call('PEXPIRE', KEYS[1], ARGV[1])
            end
            return {count, redis.call('PTTL', KEYS[1])}
            """, List.class);

    private final StringRedisTemplate redisTemplate;
    private final ReservationRateLimitProperties properties;
    private final Clock clock;
    private final Counter rejected;
    private final Counter redisFailures;
    private final Counter failOpenAllowed;
    private volatile Instant skipRedisUntil = Instant.MIN;

    public ReservationRateLimiter(StringRedisTemplate redisTemplate, ReservationRateLimitProperties properties,
                                  MeterRegistry meterRegistry, Clock clock) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.clock = clock;
        this.rejected = Counter.builder("ticketflow.ratelimit.rejected")
                .description("예약 시도 빈도 제한으로 거부된 요청").register(meterRegistry);
        this.redisFailures = Counter.builder("ticketflow.ratelimit.redis.failures")
                .description("빈도 제한 중 Redis 호출 실패").register(meterRegistry);
        this.failOpenAllowed = Counter.builder("ticketflow.ratelimit.fail.open")
                .description("Redis 장애로 검사 없이 허용한 요청").register(meterRegistry);
    }

    /**
     * 예약 시도 1회를 센다. 한도를 넘으면 예외를 던진다.
     */
    public void checkReservationAttempt(long userId) {
        if (!properties.enabled()) {
            return;
        }
        Instant now = clock.instant();
        if (now.isBefore(skipRedisUntil)) {
            failOpenAllowed.increment();
            return;
        }

        List<?> result;
        try {
            result = redisTemplate.execute(SCRIPT, List.of("rl:reservation:" + userId),
                    String.valueOf(properties.window().toMillis()));
        } catch (RuntimeException e) {
            redisFailures.increment();
            failOpenAllowed.increment();
            skipRedisUntil = now.plus(properties.failOpenCooldown());
            log.warn("Rate limiter unavailable, allowing requests for {} (fail-open): {}",
                    properties.failOpenCooldown(), e.toString());
            return;
        }
        if (result == null || result.size() < 2) {
            return;
        }
        long count = ((Number) result.get(0)).longValue();
        long ttlMillis = ((Number) result.get(1)).longValue();
        if (count > properties.maxRequests()) {
            rejected.increment();
            throw new RateLimitExceededException(Math.max(1, (ttlMillis + 999) / 1000));
        }
    }
}
