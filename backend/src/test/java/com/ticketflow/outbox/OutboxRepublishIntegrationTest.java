package com.ticketflow.outbox;

import com.jayway.jsonpath.JsonPath;
import com.ticketflow.event.service.EventService;
import com.ticketflow.messaging.config.TicketFlowKafkaProperties;
import com.ticketflow.outbox.repository.OutboxEventRepository;
import com.ticketflow.support.Await;
import com.ticketflow.support.IntegrationTest;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.ConsumerFactory;
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
 * MSG-03 Outbox 재발행 (PRD §12.1): Relay가 발행 후 published_at을 갱신하기 전에 죽은 상황을
 * published_at을 되돌려 재현한다. 같은 message_id가 Kafka에 두 번 실리지만 재고는 한 번만 줄어든다.
 */
@IntegrationTest
class OutboxRepublishIntegrationTest {

    @Autowired
    WebApplicationContext context;

    @Autowired
    EventService eventService;

    @Autowired
    OutboxEventRepository outboxEventRepository;

    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @Autowired
    TicketFlowKafkaProperties kafkaProperties;

    @Autowired
    JdbcTemplate jdbcTemplate;

    MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    @DisplayName("MSG-03 published_at 갱신 전 장애로 같은 메시지가 재발행돼도 재고는 한 번만 줄어든다")
    void republishedMessageIsAppliedOnce() throws Exception {
        long eventId = openEvent(eventService, 10);
        long target = reserveAndAwaitFinal(961, eventId);
        UUID targetMessageId = outboxEventRepository.findByReservationId(target).getFirst().messageId();
        assertThat(remaining(eventId)).isEqualTo(9);

        // Relay가 Kafka에 보낸 뒤 published_at을 쓰기 전에 죽은 상태로 되돌린다
        jdbcTemplate.update("UPDATE outbox_events SET published_at = NULL WHERE reservation_id = ?", target);
        Await.until("relay republished the same message", Duration.ofSeconds(15), () ->
                outboxEventRepository.findByReservationId(target).getFirst().publishedAt() != null);

        // 같은 파티션(key=eventId)에 재발행 메시지 뒤로 들어갈 sentinel. 이게 처리되면 재발행분도 처리된 것이다.
        long sentinel = reserveAndAwaitFinal(962, eventId);
        UUID sentinelMessageId = outboxEventRepository.findByReservationId(sentinel).getFirst().messageId();

        assertThat(countOnTopic(targetMessageId, sentinelMessageId)).as("same messageId on Kafka twice").isEqualTo(2);
        assertThat(remaining(eventId)).as("target once + sentinel once").isEqualTo(8);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM processed_messages WHERE reservation_id = ?", Integer.class, target))
                .isEqualTo(1);
    }

    private long reserveAndAwaitFinal(long userId, long eventId) throws Exception {
        String body = mockMvc.perform(reserve(userId, "republish-" + UUID.randomUUID(), eventId))
                .andExpect(status().isAccepted())
                .andReturn().getResponse().getContentAsString();
        long reservationId = JsonPath.<Integer>read(body, "$.reservationId").longValue();
        Await.until("reservation " + reservationId + " processed", Duration.ofSeconds(15), () ->
                !"QUEUED".equals(jdbcTemplate.queryForObject(
                        "SELECT status FROM reservations WHERE id = ?", String.class, reservationId)));
        return reservationId;
    }

    /** 토픽을 처음부터 읽어 targetMessageId가 실린 횟수를 센다. sentinel이 보이면 멈춘다. */
    private int countOnTopic(UUID targetMessageId, UUID sentinelMessageId) {
        int count = 0;
        boolean sentinelSeen = false;
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        try (Consumer<String, String> consumer =
                     consumerFactory.createConsumer("msg03-verify-" + UUID.randomUUID(), "verify")) {
            consumer.subscribe(List.of(kafkaProperties.reservationRequestedTopic()));
            while (!sentinelSeen && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (record.value().contains(targetMessageId.toString())) {
                        count++;
                    }
                    if (record.value().contains(sentinelMessageId.toString())) {
                        sentinelSeen = true;
                    }
                }
            }
        }
        assertThat(sentinelSeen).as("sentinel message found on topic").isTrue();
        return count;
    }

    private int remaining(long eventId) {
        return jdbcTemplate.queryForObject("SELECT remaining_tickets FROM events WHERE id = ?", Integer.class, eventId);
    }
}
