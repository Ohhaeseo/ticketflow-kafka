package com.ticketflow.outbox;

import com.jayway.jsonpath.JsonPath;
import com.ticketflow.event.service.EventService;
import com.ticketflow.outbox.entity.OutboxEvent;
import com.ticketflow.outbox.repository.OutboxEventRepository;
import com.ticketflow.support.Await;
import com.ticketflow.support.IntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static com.ticketflow.support.ReservationRequests.openEvent;
import static com.ticketflow.support.ReservationRequests.reserve;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * FR-OBX-01·02: 예약과 Outbox는 같은 트랜잭션에 기록되고, Relay가 발행 후 published_at을 채운다.
 */
@IntegrationTest
class OutboxIntegrationTest {

    @Autowired
    WebApplicationContext context;

    @Autowired
    EventService eventService;

    @Autowired
    OutboxEventRepository outboxEventRepository;

    MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    @DisplayName("예약을 접수하면 Outbox에 이벤트 1건이 PRD 계약대로 기록된다")
    void reservationWritesOneOutboxEvent() throws Exception {
        long eventId = openEvent(eventService, 100);
        long reservationId = reserveAndGetId(401, "outbox-" + UUID.randomUUID(), eventId);

        List<OutboxEvent> events = outboxEventRepository.findByReservationId(reservationId);

        assertThat(events).hasSize(1);
        OutboxEvent event = events.getFirst();
        assertThat(event.eventType()).isEqualTo("ReservationRequested");
        assertThat(event.messageKey()).isEqualTo(String.valueOf(eventId));
        assertThat(JsonPath.<String>read(event.payload(), "$.messageId")).isEqualTo(event.messageId().toString());
        assertThat(JsonPath.<Integer>read(event.payload(), "$.schemaVersion")).isEqualTo(1);
        assertThat(JsonPath.<Integer>read(event.payload(), "$.reservationId")).isEqualTo((int) reservationId);
        assertThat(JsonPath.<Integer>read(event.payload(), "$.eventId")).isEqualTo((int) eventId);
        assertThat(JsonPath.<String>read(event.payload(), "$.occurredAt")).isNotBlank();
    }

    @Test
    @DisplayName("재요청(200)과 중복 예약(409)은 Outbox 이벤트를 추가하지 않는다")
    void replayAndConflictDoNotWriteOutbox() throws Exception {
        long eventId = openEvent(eventService, 100);
        String key = "outbox-" + UUID.randomUUID();
        long reservationId = reserveAndGetId(402, key, eventId);

        mockMvc.perform(reserve(402, key, eventId)).andExpect(status().isOk());
        mockMvc.perform(reserve(402, "outbox-other-" + UUID.randomUUID(), eventId)).andExpect(status().isConflict());

        assertThat(outboxEventRepository.findByReservationId(reservationId)).hasSize(1);
    }

    @Test
    @DisplayName("Relay가 이벤트를 Kafka로 발행하면 published_at이 채워진다")
    void relayMarksEventPublished() throws Exception {
        long eventId = openEvent(eventService, 100);
        long reservationId = reserveAndGetId(403, "outbox-" + UUID.randomUUID(), eventId);

        Await.until("outbox event published", Duration.ofSeconds(15), () ->
                outboxEventRepository.findByReservationId(reservationId).getFirst().publishedAt() != null);
    }

    private long reserveAndGetId(long userId, String key, long eventId) throws Exception {
        String body = mockMvc.perform(reserve(userId, key, eventId))
                .andExpect(status().isAccepted())
                .andReturn().getResponse().getContentAsString();
        return JsonPath.<Integer>read(body, "$.reservationId").longValue();
    }
}
