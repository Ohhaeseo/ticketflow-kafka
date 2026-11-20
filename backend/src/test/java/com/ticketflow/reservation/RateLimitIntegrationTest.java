package com.ticketflow.reservation;

import com.ticketflow.event.service.EventService;
import com.ticketflow.support.IntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.ticketflow.support.ReservationRequests.openEvent;
import static com.ticketflow.support.ReservationRequests.reserve;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * SEC-01 과도한 요청 (PRD §12.1): 같은 사용자의 반복 예약 시도는 Redis 빈도 제한으로 429가 된다.
 * 기본 설정은 첫 시도부터 10초 동안 20회.
 */
@IntegrationTest
class RateLimitIntegrationTest {

    private static final int LIMIT = 20;

    @Autowired
    WebApplicationContext context;

    @Autowired
    EventService eventService;

    MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    @DisplayName("SEC-01 같은 사용자가 22번 시도하면 21번째부터 429와 Retry-After를 받는다")
    void repeatedAttemptsAreRateLimited() throws Exception {
        long eventId = openEvent(eventService, 100);

        List<MockHttpServletResponse> responses = new ArrayList<>();
        for (int i = 0; i < LIMIT + 2; i++) {
            responses.add(mockMvc.perform(reserve(990, "rl-" + UUID.randomUUID(), eventId)).andReturn().getResponse());
        }

        Map<Integer, Long> byStatus = responses.stream()
                .collect(Collectors.groupingBy(MockHttpServletResponse::getStatus, Collectors.counting()));
        // 1번째: 접수, 2~20번째: 같은 공연 중복(409, 실패한 시도도 센다), 21~22번째: 빈도 제한
        assertThat(byStatus).containsOnly(Map.entry(202, 1L), Map.entry(409, (long) LIMIT - 1), Map.entry(429, 2L));

        MockHttpServletResponse limited = responses.getLast();
        assertThat(limited.getContentAsString()).contains("RATE_LIMITED");
        assertThat(Integer.parseInt(limited.getHeader("Retry-After"))).isBetween(1, 10);
    }

    @Test
    @DisplayName("다른 사용자는 영향을 받지 않는다")
    void otherUsersAreNotAffected() throws Exception {
        long eventId = openEvent(eventService, 100);
        for (int i = 0; i < LIMIT + 1; i++) {
            mockMvc.perform(reserve(992, "rl-" + UUID.randomUUID(), eventId));
        }
        mockMvc.perform(reserve(992, "rl-" + UUID.randomUUID(), eventId)).andExpect(status().isTooManyRequests());

        mockMvc.perform(reserve(993, "rl-" + UUID.randomUUID(), eventId))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("QUEUED"));
    }

    @SuppressWarnings("unused")
    private static <T> Map<T, Long> count(List<T> values) {
        return values.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    }
}
