package com.ticketflow.reservation.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * 잠근 QUEUED 예약 하나의 최종 결과를 <b>SQL 한 문장(왕복 1번)</b>으로 정하고 반영한다.
 * <p>
 * 이전에는 재고 감소 → (실패 시) 재고 상태 조회 → 예약 상태 변경 → 처리 이력 기록을 따로 실행했다 (왕복 3~4번).
 * 지금은 PostgreSQL data-modifying CTE로 한 번에 실행한다. 규칙은 그대로다.
 * <ul>
 *   <li>조건부 재고 감소({@code remaining_tickets > 0}, 판매 기간 안)가 1행이면 CONFIRMED.</li>
 *   <li>0행이고, 재고가 0이거나 판매 기간 안이면 SOLD_OUT. 판매 기간 안인데 감소가 실패했다면 다른 트랜잭션이 마지막 재고를 가져간 것이다.</li>
 *   <li>그 외(판매 기간 밖, 재고 남음)는 FAILED + SALES_CLOSED (PRD Q3).</li>
 * </ul>
 * 판정에 쓰는 {@code events} 조회는 이 문장의 스냅샷을 쓴다. 같은 트랜잭션에서 앞서 줄인 재고(배치 리스너)도 보인다.
 * 한 문장이므로 어느 부분이 실패해도 재고·상태·이력이 함께 롤백된다 (FR-INV-03).
 */
@Repository
public class ReservationOutcomeRepository {

    private static final String DECIDE_AND_APPLY = """
            WITH stock AS (
                UPDATE events
                   SET remaining_tickets = remaining_tickets - 1
                 WHERE id = :eventId
                   AND remaining_tickets > 0
                   AND sales_start_at <= CURRENT_TIMESTAMP
                   AND sales_end_at   >  CURRENT_TIMESTAMP
                RETURNING id
            ),
            decision AS (
                SELECT CASE
                         WHEN EXISTS (SELECT 1 FROM stock) THEN 'CONFIRMED'
                         WHEN e.remaining_tickets = 0
                           OR (e.sales_start_at <= CURRENT_TIMESTAMP AND e.sales_end_at > CURRENT_TIMESTAMP) THEN 'SOLD_OUT'
                         ELSE 'FAILED'
                       END AS outcome
                  FROM events e
                 WHERE e.id = :eventId
            ),
            updated AS (
                UPDATE reservations r
                   SET status = d.outcome,
                       failure_reason = CASE WHEN d.outcome = 'FAILED' THEN 'SALES_CLOSED' END,
                       updated_at = :now
                  FROM decision d
                 WHERE r.id = :reservationId
                   AND r.status = 'QUEUED'
                RETURNING r.id
            ),
            recorded AS (
                INSERT INTO processed_messages (message_id, reservation_id, outcome, processed_at)
                SELECT :messageId, :reservationId, d.outcome, :now
                  FROM decision d
                RETURNING message_id
            )
            SELECT d.outcome,
                   (SELECT count(*) FROM updated)  AS updated_rows,
                   (SELECT count(*) FROM recorded) AS recorded_rows
              FROM decision d
            """;

    private final JdbcClient jdbcClient;

    public ReservationOutcomeRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcClient = JdbcClient.create(jdbcTemplate);
    }

    /**
     * 호출 전에 예약 행을 잠그고 QUEUED·미처리임을 확인해야 한다.
     *
     * @return 결과 이름(CONFIRMED/SOLD_OUT/FAILED)과 반영된 행 수. 공연이 없으면 empty
     */
    public java.util.Optional<Applied> decideAndApply(long reservationId, long eventId, UUID messageId, Instant now) {
        return jdbcClient.sql(DECIDE_AND_APPLY)
                .param("eventId", eventId)
                .param("reservationId", reservationId)
                .param("messageId", messageId)
                .param("now", OffsetDateTime.ofInstant(now, ZoneOffset.UTC))
                .query((rs, rowNum) -> new Applied(
                        rs.getString("outcome"), rs.getInt("updated_rows"), rs.getInt("recorded_rows")))
                .optional();
    }

    public record Applied(String outcome, int updatedRows, int recordedRows) {
    }
}
