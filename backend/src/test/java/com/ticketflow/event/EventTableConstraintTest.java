package com.ticketflow.event;

import com.ticketflow.support.IntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 애플리케이션 검증을 우회해도 DB CHECK 제약이 잘못된 재고를 막는지 확인한다 (PRD FR-INV-02).
 */
@IntegrationTest
class EventTableConstraintTest {

    private static final Instant START = Instant.parse("2026-11-23T10:00:00Z");
    private static final Instant END = START.plus(30, ChronoUnit.DAYS);

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("정상 범위의 재고는 저장된다")
    void validRowIsAccepted() {
        assertThat(insert(100, 100, START, END)).isEqualTo(1);
    }

    @Test
    @DisplayName("잔여 수량이 총 수량보다 크면 거부된다")
    void remainingGreaterThanTotalIsRejected() {
        assertThatThrownBy(() -> insert(100, 101, START, END))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("ck_events_remaining_tickets");
    }

    @Test
    @DisplayName("음수 재고는 거부된다")
    void negativeRemainingIsRejected() {
        assertThatThrownBy(() -> insert(100, -1, START, END))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("ck_events_remaining_tickets");
    }

    @Test
    @DisplayName("재고 0 미만으로 감소시키는 UPDATE는 거부된다")
    void decrementBelowZeroIsRejected() {
        insert(1, 0, START, END);
        Long id = jdbcTemplate.queryForObject("SELECT max(id) FROM events", Long.class);

        assertThatThrownBy(() -> jdbcTemplate.update(
                "UPDATE events SET remaining_tickets = remaining_tickets - 1 WHERE id = ?", id))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    @DisplayName("판매 시작이 종료보다 늦으면 거부된다")
    void invertedSalesPeriodIsRejected() {
        assertThatThrownBy(() -> insert(100, 100, END, START))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("ck_events_sales_period");
    }

    private int insert(int total, int remaining, Instant salesStartAt, Instant salesEndAt) {
        return jdbcTemplate.update("""
                        INSERT INTO events (title, total_tickets, remaining_tickets, sales_start_at, sales_end_at)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                "constraint-test", total, remaining,
                OffsetDateTime.ofInstant(salesStartAt, ZoneOffset.UTC),
                OffsetDateTime.ofInstant(salesEndAt, ZoneOffset.UTC));
    }
}
