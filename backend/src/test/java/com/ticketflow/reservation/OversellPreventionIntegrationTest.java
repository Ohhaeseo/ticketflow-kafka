package com.ticketflow.reservation;

import com.ticketflow.event.service.EventService;
import com.ticketflow.support.Await;
import com.ticketflow.support.IntegrationTest;
import com.ticketflow.support.StockInvariant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.ticketflow.support.ReservationRequests.openEvent;
import static com.ticketflow.support.ReservationRequests.reserve;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * 4주차 마일스톤 "100장 초과 확정 없음": 전체 경로(API → Outbox → Kafka → Consumer)에 동시 요청을 넣는다.
 * 부하 테스트(LOAD-01, 1,000명)는 5주차에 k6로 별도 측정한다.
 */
@IntegrationTest
class OversellPreventionIntegrationTest {

    private static final int TICKETS = 100;
    private static final int USERS = 150;
    private static final int CLIENT_THREADS = 8;
    private static final long FIRST_USER_ID = 601;

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
    @DisplayName("티켓 100장에 150명이 동시에 접수하면 최종 CONFIRMED는 정확히 100건, SOLD_OUT 50건")
    void exactly100ConfirmedOutOf150() throws Exception {
        long eventId = openEvent(eventService, TICKETS);

        List<Integer> statuses = reserveConcurrently(eventId);
        assertThat(statuses).hasSize(USERS).containsOnly(202);

        Await.until("all reservations processed", Duration.ofSeconds(60), () ->
                StockInvariant.snapshot(jdbcTemplate, eventId).queued() == 0);

        StockInvariant.Snapshot snapshot = StockInvariant.assertHolds(jdbcTemplate, eventId);
        assertThat(snapshot.confirmed()).isEqualTo(TICKETS);
        assertThat(snapshot.soldOut()).isEqualTo(USERS - TICKETS);
        assertThat(snapshot.remaining()).isZero();
        assertThat(jdbcTemplate.queryForObject("""
                        SELECT count(*) FROM processed_messages p
                          JOIN reservations r ON r.id = p.reservation_id
                         WHERE r.event_id = ?
                        """, Integer.class, eventId))
                .as("one processing record per reservation")
                .isEqualTo(USERS);
    }

    private List<Integer> reserveConcurrently(long eventId) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(CLIENT_THREADS);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<Integer>> futures = new ArrayList<>();
            for (int i = 0; i < USERS; i++) {
                long userId = FIRST_USER_ID + i;
                futures.add(executor.submit(() -> {
                    start.await();
                    return mockMvc.perform(reserve(userId, "oversell-" + UUID.randomUUID(), eventId))
                            .andReturn().getResponse().getStatus();
                }));
            }
            start.countDown();
            List<Integer> statuses = new ArrayList<>();
            for (Future<Integer> future : futures) {
                statuses.add(future.get(60, TimeUnit.SECONDS));
            }
            return statuses;
        } finally {
            executor.shutdownNow();
        }
    }
}
