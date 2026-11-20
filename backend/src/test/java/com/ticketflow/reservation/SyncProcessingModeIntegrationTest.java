package com.ticketflow.reservation;

import com.jayway.jsonpath.JsonPath;
import com.ticketflow.event.service.EventService;
import com.ticketflow.support.IntegrationTest;
import com.ticketflow.support.StockInvariant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.ticketflow.support.ReservationRequests.openEvent;
import static com.ticketflow.support.ReservationRequests.reserve;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * FR-BMK-01 동기 처리 비교 모드. 별도 Spring 컨텍스트(와 컨테이너)로 실행된다.
 */
@IntegrationTest
@TestPropertySource(properties = "ticketflow.reservation.processing-mode=sync")
class SyncProcessingModeIntegrationTest {

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
    @DisplayName("동기 모드는 201과 최종 상태를 바로 반환하고 Outbox를 쓰지 않는다")
    void respondsWithFinalStatus() throws Exception {
        long eventId = openEvent(eventService, 2);

        mockMvc.perform(reserve(311, key(), eventId))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("CONFIRMED"));
        mockMvc.perform(reserve(312, key(), eventId))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("CONFIRMED"));
        String third = mockMvc.perform(reserve(313, key(), eventId))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("SOLD_OUT"))
                .andReturn().getResponse().getContentAsString();

        assertThat(JsonPath.<Integer>read(third, "$.reservationId")).isPositive();
        assertThat(jdbcTemplate.queryForObject("""
                        SELECT count(*) FROM outbox_events o JOIN reservations r ON r.id = o.reservation_id
                         WHERE r.event_id = ?
                        """, Integer.class, eventId)).isZero();
        StockInvariant.assertHolds(jdbcTemplate, eventId);
    }

    @Test
    @DisplayName("동기 모드에서도 같은 키 재요청은 200, 동시 접수는 재고만큼만 확정된다")
    void replayAndConcurrency() throws Exception {
        long eventId = openEvent(eventService, 10);
        String key = key();
        mockMvc.perform(reserve(321, key, eventId)).andExpect(status().isCreated());
        mockMvc.perform(reserve(321, key, eventId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CONFIRMED"));

        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Future<Integer>> futures = new ArrayList<>();
            for (long userId = 322; userId < 352; userId++) {
                long uid = userId;
                futures.add(executor.submit(() ->
                        mockMvc.perform(reserve(uid, key(), eventId)).andReturn().getResponse().getStatus()));
            }
            for (Future<Integer> future : futures) {
                assertThat(future.get(30, TimeUnit.SECONDS)).isEqualTo(201);
            }
        } finally {
            executor.shutdownNow();
        }

        StockInvariant.Snapshot snapshot = StockInvariant.assertHolds(jdbcTemplate, eventId);
        assertThat(snapshot.confirmed()).isEqualTo(10);
        assertThat(snapshot.soldOut()).isEqualTo(21);
    }

    private static String key() {
        return "sync-" + UUID.randomUUID();
    }
}
