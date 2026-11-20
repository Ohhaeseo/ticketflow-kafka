package com.ticketflow.support;

import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 재고 불변식 검증 (PRD §12.3, NFR-ACC-01·02).
 */
public final class StockInvariant {

    private StockInvariant() {
    }

    public record Snapshot(int total, int remaining, long confirmed, long soldOut, long failed, long queued) {
    }

    public static Snapshot snapshot(JdbcTemplate jdbcTemplate, long eventId) {
        Map<String, Object> row = jdbcTemplate.queryForMap("""
                SELECT e.total_tickets,
                       e.remaining_tickets,
                       count(r.id) FILTER (WHERE r.status = 'CONFIRMED') AS confirmed,
                       count(r.id) FILTER (WHERE r.status = 'SOLD_OUT')  AS sold_out,
                       count(r.id) FILTER (WHERE r.status = 'FAILED')    AS failed,
                       count(r.id) FILTER (WHERE r.status = 'QUEUED')    AS queued
                  FROM events e
                  LEFT JOIN reservations r ON r.event_id = e.id
                 WHERE e.id = ?
                 GROUP BY e.id
                """, eventId);
        return new Snapshot(
                ((Number) row.get("total_tickets")).intValue(),
                ((Number) row.get("remaining_tickets")).intValue(),
                ((Number) row.get("confirmed")).longValue(),
                ((Number) row.get("sold_out")).longValue(),
                ((Number) row.get("failed")).longValue(),
                ((Number) row.get("queued")).longValue());
    }

    /** 처리가 모두 끝난 뒤의 불변식: 음수 재고 없음, 초과 확정 없음, 확정 + 잔여 = 총 수량, QUEUED 없음. */
    public static Snapshot assertHolds(JdbcTemplate jdbcTemplate, long eventId) {
        Snapshot s = snapshot(jdbcTemplate, eventId);
        assertThat(s.remaining()).as("remaining_tickets >= 0").isGreaterThanOrEqualTo(0);
        assertThat(s.confirmed()).as("CONFIRMED <= total_tickets").isLessThanOrEqualTo(s.total());
        assertThat(s.confirmed() + s.remaining()).as("CONFIRMED + remaining = total").isEqualTo(s.total());
        assertThat(s.queued()).as("no QUEUED left").isZero();
        return s;
    }
}
