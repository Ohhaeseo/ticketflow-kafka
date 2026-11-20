package com.ticketflow.messaging.dto;

import java.time.Instant;
import java.util.UUID;

/**
 * {@code reservation.requested.v1} 메시지 (PRD §10.2, docs/event-contract.md).
 */
public record ReservationRequestedMessage(
        UUID messageId,
        int schemaVersion,
        long reservationId,
        long eventId,
        Instant occurredAt
) {

    public static final String EVENT_TYPE = "ReservationRequested";
    public static final int SCHEMA_VERSION = 1;

    public static ReservationRequestedMessage create(long reservationId, long eventId, Instant occurredAt) {
        return new ReservationRequestedMessage(UUID.randomUUID(), SCHEMA_VERSION, reservationId, eventId, occurredAt);
    }

    /** Kafka 메시지 key. 같은 공연의 이벤트는 같은 파티션으로 간다. */
    public String key() {
        return String.valueOf(eventId);
    }
}
