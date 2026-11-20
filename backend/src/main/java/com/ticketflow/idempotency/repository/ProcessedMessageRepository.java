package com.ticketflow.idempotency.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Consumer 처리 이력 (processed_messages). 같은 message_id를 두 번 처리하지 않기 위한 기록이다.
 */
@Repository
public class ProcessedMessageRepository {

    private final JdbcClient jdbcClient;

    public ProcessedMessageRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcClient = JdbcClient.create(jdbcTemplate);
    }

    public boolean exists(UUID messageId) {
        return jdbcClient.sql("SELECT EXISTS (SELECT 1 FROM processed_messages WHERE message_id = :messageId)")
                .param("messageId", messageId)
                .query(Boolean.class)
                .single();
    }

    /**
     * 처리 결과를 기록한다. 같은 message_id가 이미 있으면 PK 위반으로 예외가 나고 트랜잭션 전체가 롤백된다
     * (호출 쪽이 예약 행을 잠근 뒤 {@link #exists}로 먼저 확인하므로 정상 흐름에서는 일어나지 않는다).
     */
    public void record(UUID messageId, long reservationId, String outcome, Instant processedAt) {
        jdbcClient.sql("""
                        INSERT INTO processed_messages (message_id, reservation_id, outcome, processed_at)
                        VALUES (:messageId, :reservationId, :outcome, :processedAt)
                        """)
                .param("messageId", messageId)
                .param("reservationId", reservationId)
                .param("outcome", outcome)
                .param("processedAt", OffsetDateTime.ofInstant(processedAt, ZoneOffset.UTC))
                .update();
    }
}
