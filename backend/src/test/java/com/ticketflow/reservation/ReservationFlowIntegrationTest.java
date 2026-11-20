package com.ticketflow.reservation;

import com.ticketflow.event.service.EventService;
import com.ticketflow.support.Await;
import com.ticketflow.support.IntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.ticketflow.support.ReservationRequests.openEvent;
import static com.ticketflow.support.ReservationRequests.reserve;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 3주차 마일스톤: 접수(202) → Outbox → Relay → Kafka → Consumer → 최종 상태.
 */
@IntegrationTest
class ReservationFlowIntegrationTest {

    @Autowired
    WebApplicationContext context;

    @Autowired
    EventService eventService;

    @Autowired
    JdbcTemplate jdbcTemplate;

    MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    @DisplayName("티켓 2장에 3명이 접수하면 Kafka를 거쳐 CONFIRMED 2건, SOLD_OUT 1건, 잔여 0이 된다")
    void acceptedReservationsAreProcessedThroughKafka() throws Exception {
        long eventId = openEvent(eventService, 2);
        for (long userId = 301; userId <= 303; userId++) {
            mockMvc.perform(reserve(userId, "flow-" + UUID.randomUUID(), eventId))
                    .andExpect(status().isAccepted());
        }

        Await.until("all reservations reach a final state", Duration.ofSeconds(20), () ->
                countByStatus(eventId).getOrDefault("QUEUED", 0L) == 0 && countByStatus(eventId).size() > 0);

        assertThat(countByStatus(eventId)).containsExactlyInAnyOrderEntriesOf(Map.of(
                "CONFIRMED", 2L,
                "SOLD_OUT", 1L));
        assertThat(jdbcTemplate.queryForObject(
                "SELECT remaining_tickets FROM events WHERE id = ?", Integer.class, eventId)).isZero();
        assertThat(jdbcTemplate.queryForObject("""
                        SELECT count(*) FROM outbox_events o
                          JOIN reservations r ON r.id = o.reservation_id
                         WHERE r.event_id = ? AND o.published_at IS NULL
                        """, Long.class, eventId)).isZero();
    }

    private Map<String, Long> countByStatus(long eventId) {
        return jdbcTemplate.queryForList(
                        "SELECT status, count(*) AS cnt FROM reservations WHERE event_id = ? GROUP BY status", eventId)
                .stream()
                .collect(Collectors.toMap(row -> (String) row.get("status"), row -> ((Number) row.get("cnt")).longValue()));
    }
}
