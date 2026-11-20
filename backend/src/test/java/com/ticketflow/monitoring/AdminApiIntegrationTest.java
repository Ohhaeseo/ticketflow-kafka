package com.ticketflow.monitoring;

import com.ticketflow.event.service.EventService;
import com.ticketflow.messaging.config.TicketFlowKafkaProperties;
import com.ticketflow.support.Await;
import com.ticketflow.support.IntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.ticketflow.support.ReservationRequests.ROLE_HEADER;
import static com.ticketflow.support.ReservationRequests.USER_HEADER;
import static com.ticketflow.support.ReservationRequests.openEvent;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * FR-OPS-01·02 관리자 지표·DLT API, FR-OPS-03 Prometheus 지표.
 */
@IntegrationTest
class AdminApiIntegrationTest {

    @Autowired
    WebApplicationContext context;

    @Autowired
    EventService eventService;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    TicketFlowKafkaProperties kafkaProperties;

    MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    @DisplayName("관리자가 아니면 403, 사용자 헤더가 없으면 401")
    void adminOnly() throws Exception {
        mockMvc.perform(get("/api/v1/admin/metrics").header(USER_HEADER, 1))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.code").value("FORBIDDEN"));
        mockMvc.perform(get("/api/v1/admin/dlt"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    @DisplayName("운영 지표: 상태별 예약 수, Outbox, Consumer lag, 공연별 재고 불변식")
    void metrics() throws Exception {
        long eventId = openEvent(eventService, 7);

        mockMvc.perform(admin(get("/api/v1/admin/metrics")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.reservationsByStatus.QUEUED").isNumber())
                .andExpect(jsonPath("$.reservationsByStatus.CONFIRMED").isNumber())
                .andExpect(jsonPath("$.outbox.unpublished", greaterThanOrEqualTo(0)))
                .andExpect(jsonPath("$.consumerLag.groupId").value("ticketflow-reservation"))
                .andExpect(jsonPath("$.consumerLag.total", greaterThanOrEqualTo(0)))
                .andExpect(jsonPath("$.recentEvents[*].eventId", hasItem((int) eventId)))
                .andExpect(jsonPath("$.recentEvents[?(@.eventId == %d)].invariantOk".formatted(eventId)).value(true));
    }

    @Test
    @DisplayName("DLT 조회: 방금 DLT로 간 메시지가 원인과 함께 보인다")
    void deadLetters() throws Exception {
        String payload = "{broken " + UUID.randomUUID();
        kafkaTemplate.send(kafkaProperties.reservationRequestedTopic(), "admin-dlt", payload).get(10, TimeUnit.SECONDS);

        Await.until("dead letter visible via admin API", Duration.ofSeconds(20), () -> {
            try {
                return mockMvc.perform(admin(get("/api/v1/admin/dlt").param("limit", "50")))
                        .andReturn().getResponse().getContentAsString().contains(payload);
            } catch (Exception e) {
                return false;
            }
        });
        mockMvc.perform(admin(get("/api/v1/admin/dlt").param("limit", "50")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.payload == '%s')].exceptionCauseClass".formatted(payload))
                        .value(hasItem(org.hamcrest.Matchers.startsWith("tools.jackson"))));
    }

    @Test
    @DisplayName("Prometheus 엔드포인트에 운영 지표가 노출된다")
    void prometheusMetrics() throws Exception {
        String body = mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        org.assertj.core.api.Assertions.assertThat(body).contains(
                "ticketflow_outbox_unpublished",
                "ticketflow_outbox_oldest_unpublished_age_seconds",
                "ticketflow_consumer_lag",
                "ticketflow_dead_letters",
                "ticketflow_reservation_processing_seconds_bucket",
                "ticketflow_ratelimit_rejected_total",
                "hikaricp_connections_pending");
    }

    private static MockHttpServletRequestBuilder admin(MockHttpServletRequestBuilder request) {
        return request.header(USER_HEADER, 1).header(ROLE_HEADER, "ADMIN");
    }
}
