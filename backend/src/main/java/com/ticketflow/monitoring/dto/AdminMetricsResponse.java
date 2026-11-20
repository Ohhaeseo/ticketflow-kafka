package com.ticketflow.monitoring.dto;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * GET /api/v1/admin/metrics 응답 (PRD FR-OPS-01).
 */
public record AdminMetricsResponse(
        Instant generatedAt,
        Map<String, Long> reservationsByStatus,
        Outbox outbox,
        ConsumerLag consumerLag,
        DeadLetters deadLetters,
        List<EventStock> recentEvents
) {

    public record Outbox(long unpublished, long oldestUnpublishedAgeMs) {
    }

    /** total이 -1이면 Kafka에서 계산하지 못한 것이다. */
    public record ConsumerLag(String groupId, String topic, long total, Map<Integer, Long> byPartition) {
    }

    public record DeadLetters(long total, long last24h) {
    }

    public record EventStock(long eventId, String title, int totalTickets, int remainingTickets,
                             long confirmed, long soldOut, long failed, long queued, boolean invariantOk) {
    }
}
