package com.ticketflow.reservation.repository;

import com.ticketflow.reservation.entity.ReservationStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

/**
 * 예약 쓰기 전용 SQL. 동시 요청 경쟁은 애플리케이션 락이 아니라 DB UNIQUE 제약으로 판정한다.
 */
@Repository
public class ReservationCommandRepository {

    private final JdbcClient jdbcClient;

    public ReservationCommandRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcClient = JdbcClient.create(jdbcTemplate);
    }

    /**
     * QUEUED 예약을 저장한다. {@code (user_id, idempotency_key)} 또는 {@code (event_id, user_id)}가 이미 있으면
     * 아무것도 저장하지 않고 빈 값을 반환한다. 경쟁 중인 다른 트랜잭션이 있으면 그 트랜잭션이 끝날 때까지 기다린 뒤 판정한다.
     *
     * @return 새로 저장한 예약 ID. 충돌이면 empty
     */
    public Optional<Long> insertQueuedIfAbsent(long eventId, long userId, String idempotencyKey,
                                               String requestHash, Instant now) {
        OffsetDateTime timestamp = OffsetDateTime.ofInstant(now, ZoneOffset.UTC);
        return jdbcClient.sql("""
                        INSERT INTO reservations
                            (event_id, user_id, idempotency_key, request_hash, status, created_at, updated_at)
                        VALUES (:eventId, :userId, :idempotencyKey, :requestHash, 'QUEUED', :now, :now)
                        ON CONFLICT DO NOTHING
                        RETURNING id
                        """)
                .param("eventId", eventId)
                .param("userId", userId)
                .param("idempotencyKey", idempotencyKey)
                .param("requestHash", requestHash)
                .param("now", timestamp)
                .query(Long.class)
                .optional();
    }

    /**
     * 처리할 예약 행을 잠근다. 같은 예약의 중복 메시지가 동시에 처리되어도 한 트랜잭션씩 순서대로 진행된다.
     */
    public Optional<LockedReservation> lockForProcessing(long reservationId) {
        return jdbcClient.sql("SELECT id, event_id, status FROM reservations WHERE id = :id FOR UPDATE")
                .param("id", reservationId)
                .query((rs, rowNum) -> new LockedReservation(
                        rs.getLong("id"),
                        rs.getLong("event_id"),
                        ReservationStatus.valueOf(rs.getString("status"))))
                .optional();
    }

    // 최종 상태 전이(WHERE status = 'QUEUED')는 ReservationOutcomeRepository의 한 문장 안에서 한다.

    public record LockedReservation(long id, long eventId, ReservationStatus status) {
    }
}
