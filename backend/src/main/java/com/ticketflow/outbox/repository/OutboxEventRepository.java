package com.ticketflow.outbox.repository;

import com.ticketflow.outbox.entity.OutboxEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Repository
public class OutboxEventRepository {

    private static final RowMapper<OutboxEvent> ROW_MAPPER = (rs, rowNum) -> new OutboxEvent(
            rs.getLong("id"),
            rs.getObject("message_id", UUID.class),
            rs.getLong("reservation_id"),
            rs.getString("event_type"),
            rs.getString("message_key"),
            rs.getString("payload"),
            toInstant(rs.getObject("created_at", OffsetDateTime.class)),
            toInstant(rs.getObject("published_at", OffsetDateTime.class)));

    private final JdbcClient jdbcClient;

    public OutboxEventRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcClient = JdbcClient.create(jdbcTemplate);
    }

    /** 호출한 쪽의 트랜잭션 안에서 기록된다 (예약 저장과 함께 커밋·롤백). */
    public void append(UUID messageId, long reservationId, String eventType, String messageKey,
                       String payload, Instant createdAt) {
        jdbcClient.sql("""
                        INSERT INTO outbox_events (message_id, reservation_id, event_type, message_key, payload, created_at)
                        VALUES (:messageId, :reservationId, :eventType, :messageKey, :payload, :createdAt)
                        """)
                .param("messageId", messageId)
                .param("reservationId", reservationId)
                .param("eventType", eventType)
                .param("messageKey", messageKey)
                .param("payload", payload)
                .param("createdAt", OffsetDateTime.ofInstant(createdAt, ZoneOffset.UTC))
                .update();
    }

    public List<OutboxEvent> findUnpublished(int limit) {
        return jdbcClient.sql("""
                        SELECT id, message_id, reservation_id, event_type, message_key, payload, created_at, published_at
                          FROM outbox_events
                         WHERE published_at IS NULL
                         ORDER BY id
                         LIMIT :limit
                        """)
                .param("limit", limit)
                .query(ROW_MAPPER)
                .list();
    }

    public int markPublished(Collection<Long> ids, Instant publishedAt) {
        if (ids.isEmpty()) {
            return 0;
        }
        return jdbcClient.sql("""
                        UPDATE outbox_events
                           SET published_at = :publishedAt
                         WHERE id IN (:ids)
                           AND published_at IS NULL
                        """)
                .param("publishedAt", OffsetDateTime.ofInstant(publishedAt, ZoneOffset.UTC))
                .param("ids", ids)
                .update();
    }

    public long countUnpublished() {
        return jdbcClient.sql("SELECT count(*) FROM outbox_events WHERE published_at IS NULL")
                .query(Long.class)
                .single();
    }

    public List<OutboxEvent> findByReservationId(long reservationId) {
        return jdbcClient.sql("""
                        SELECT id, message_id, reservation_id, event_type, message_key, payload, created_at, published_at
                          FROM outbox_events
                         WHERE reservation_id = :reservationId
                         ORDER BY id
                        """)
                .param("reservationId", reservationId)
                .query(ROW_MAPPER)
                .list();
    }

    private static Instant toInstant(OffsetDateTime value) {
        return value == null ? null : value.toInstant();
    }
}
