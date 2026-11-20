package com.ticketflow.reservation;

import com.ticketflow.event.service.EventService;
import com.ticketflow.support.IntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import static com.ticketflow.support.ReservationRequests.openEvent;
import static com.ticketflow.support.ReservationRequests.reserve;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * CON-01 동일 요청 중복 (PRD §12.1): 같은 사용자·공연·멱등성 키 요청 10회 → 예약 1건.
 */
@IntegrationTest
class ReservationIdempotencyTest {

    private static final int REQUESTS = 10;

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
    @DisplayName("CON-01 같은 요청을 10회 순차로 보내면 202 1회, 200 9회, 예약 1건")
    void sequentialDuplicates() throws Exception {
        long eventId = openEvent(eventService, 100);
        String key = "con01-seq-" + UUID.randomUUID();

        List<Integer> statuses = new ArrayList<>();
        for (int i = 0; i < REQUESTS; i++) {
            statuses.add(mockMvc.perform(reserve(101, key, eventId)).andReturn().getResponse().getStatus());
        }

        assertThat(statuses.getFirst()).isEqualTo(202);
        assertThat(statuses.subList(1, REQUESTS)).containsOnly(200);
        assertThat(countByUserAndKey(101, key)).isEqualTo(1);
    }

    @Test
    @DisplayName("CON-01 같은 요청 10개를 동시에 보내도 202는 1회, 나머지는 200, 예약 1건")
    void concurrentDuplicates() throws Exception {
        long eventId = openEvent(eventService, 100);
        String key = "con01-par-" + UUID.randomUUID();

        Map<Integer, Long> statusCounts = sendConcurrently(i -> reserve(102, key, eventId));

        assertThat(statusCounts).containsOnlyKeys(202, 200);
        assertThat(statusCounts.get(202)).isEqualTo(1);
        assertThat(statusCounts.get(200)).isEqualTo(REQUESTS - 1);
        assertThat(countByUserAndKey(102, key)).isEqualTo(1);
    }

    @Test
    @DisplayName("같은 사용자가 같은 공연에 서로 다른 키로 동시에 10개를 보내도 예약 1건, 나머지는 409")
    void concurrentDifferentKeysSameEvent() throws Exception {
        long eventId = openEvent(eventService, 100);
        String prefix = "con01-keys-" + UUID.randomUUID() + "-";

        Map<Integer, Long> statusCounts = sendConcurrently(i -> reserve(103, prefix + i, eventId));

        assertThat(statusCounts).containsOnlyKeys(202, 409);
        assertThat(statusCounts.get(202)).isEqualTo(1);
        assertThat(statusCounts.get(409)).isEqualTo(REQUESTS - 1);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM reservations WHERE event_id = ? AND user_id = ?",
                Integer.class, eventId, 103)).isEqualTo(1);
    }

    private Map<Integer, Long> sendConcurrently(IntFunction<MockHttpServletRequestBuilder> request) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(REQUESTS);
        CountDownLatch ready = new CountDownLatch(REQUESTS);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<Integer>> futures = new ArrayList<>();
            for (int i = 0; i < REQUESTS; i++) {
                MockHttpServletRequestBuilder builder = request.apply(i);
                Callable<Integer> call = () -> {
                    ready.countDown();
                    start.await();
                    MvcResult result = mockMvc.perform(builder).andReturn();
                    return result.getResponse().getStatus();
                };
                futures.add(executor.submit(call));
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            List<Integer> statuses = new ArrayList<>();
            for (Future<Integer> future : futures) {
                statuses.add(future.get(30, TimeUnit.SECONDS));
            }
            return statuses.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        } finally {
            executor.shutdownNow();
        }
    }

    private int countByUserAndKey(long userId, String key) {
        return jdbcTemplate.queryForObject(
                "SELECT count(*) FROM reservations WHERE user_id = ? AND idempotency_key = ?",
                Integer.class, userId, key);
    }
}
