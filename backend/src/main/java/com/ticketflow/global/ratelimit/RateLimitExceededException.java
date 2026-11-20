package com.ticketflow.global.ratelimit;

import com.ticketflow.global.exception.BusinessException;
import com.ticketflow.global.exception.ErrorCode;

/**
 * 예약 시도 빈도 제한 초과. 응답에 {@code Retry-After}(초)를 담는다.
 */
public class RateLimitExceededException extends BusinessException {

    private final long retryAfterSeconds;

    public RateLimitExceededException(long retryAfterSeconds) {
        super(ErrorCode.RATE_LIMITED);
        this.retryAfterSeconds = retryAfterSeconds;
    }

    public long retryAfterSeconds() {
        return retryAfterSeconds;
    }
}
