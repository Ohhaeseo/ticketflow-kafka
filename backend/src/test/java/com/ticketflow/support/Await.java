package com.ticketflow.support;

import java.time.Duration;
import java.util.function.BooleanSupplier;

/**
 * 비동기 처리(Relay·Consumer) 결과를 기다리는 단순 폴링 헬퍼.
 */
public final class Await {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);

    private Await() {
    }

    public static void until(String description, Duration timeout, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out after " + timeout + " waiting for: " + description);
            }
            try {
                Thread.sleep(POLL_INTERVAL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while waiting for: " + description, e);
            }
        }
    }
}
