package com.ticketflow.outbox.entity;

import java.time.Instant;
import java.util.UUID;

/**
 * outbox_events 행. {@code publishedAt == null}이면 아직 Kafka로 발행하지 않은 이벤트다.
 */
public record OutboxEvent(
        long id,
        UUID messageId,
        long reservationId,
        String eventType,
        String messageKey,
        String payload,
        Instant createdAt,
        Instant publishedAt
) {
}
