package com.ticketflow.event.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * 재고 변경 SQL. 재고는 이 조건부 UPDATE로만 줄인다 (PRD FR-INV-01). 읽고 계산한 뒤 쓰는 방식은 쓰지 않는다.
 */
@Repository
public class EventStockRepository {

    private final JdbcClient jdbcClient;

    public EventStockRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcClient = JdbcClient.create(jdbcTemplate);
    }

    /**
     * 재고가 남아 있고 판매 기간 안이면 1장 줄인다.
     *
     * @return 1이면 재고 확보 성공, 0이면 매진 또는 판매 기간 밖
     */
    public int decrementIfAvailable(long eventId) {
        return jdbcClient.sql("""
                        UPDATE events
                           SET remaining_tickets = remaining_tickets - 1
                         WHERE id = :eventId
                           AND remaining_tickets > 0
                           AND sales_start_at <= CURRENT_TIMESTAMP
                           AND sales_end_at   >  CURRENT_TIMESTAMP
                        """)
                .param("eventId", eventId)
                .update();
    }

    /** 재고 감소가 0행이었을 때 원인을 가리기 위한 조회. */
    public StockState findStockState(long eventId) {
        return jdbcClient.sql("""
                        SELECT remaining_tickets,
                               (sales_start_at <= CURRENT_TIMESTAMP AND sales_end_at > CURRENT_TIMESTAMP) AS on_sale
                          FROM events
                         WHERE id = :eventId
                        """)
                .param("eventId", eventId)
                .query((rs, rowNum) -> new StockState(rs.getInt("remaining_tickets"), rs.getBoolean("on_sale")))
                .single();
    }

    public record StockState(int remainingTickets, boolean onSale) {
    }
}
