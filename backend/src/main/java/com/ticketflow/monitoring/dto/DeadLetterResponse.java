package com.ticketflow.monitoring.dto;

import java.time.Instant;

/**
 * GET /api/v1/admin/dlt 항목 (PRD FR-OPS-02).
 */
public record DeadLetterResponse(
        long id,
        String originalTopic,
        int originalPartition,
        long originalOffset,
        String messageKey,
        String payload,
        String exceptionCauseClass,
        String exceptionMessage,
        Instant receivedAt
) {
}
