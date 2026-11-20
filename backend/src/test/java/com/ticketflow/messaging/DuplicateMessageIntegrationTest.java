package com.ticketflow.messaging;

import com.ticketflow.event.service.EventService;
import com.ticketflow.messaging.config.TicketFlowKafkaProperties;
import com.ticketflow.messaging.dto.ReservationRequestedMessage;
import com.ticketflow.reservation.repository.ReservationCommandRepository;
import com.ticketflow.support.Await;
import com.ticketflow.support.IntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.ticketflow.support.ReservationRequests.openEvent;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * MSG-01 중복 Kafka 메시지 (PRD §12.1): 같은 messageId가 실제 Kafka를 통해 두 번 와도 재고는 한 번만 줄어든다.
 * Outbox 재발행·Kafka 재전달과 같은 상황을 직접 메시지를 보내 재현한다.
 */
@IntegrationTest
class DuplicateMessageIntegrationTest {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    TicketFlowKafkaProperties kafkaProperties;

    @Autowired
    ReservationCommandRepository reservationCommandRepository;

    @Autowired
    EventService eventService;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    JsonMapper jsonMapper;

    @Test
    @DisplayName("MSG-01 같은 messageId를 Kafka로 두 번 보내도 재고는 한 번만 줄고 처리 이력은 1건이다")
    void duplicateMessageThroughKafka() throws Exception {
        long eventId = openEvent(eventService, 10);
        long target = queued(eventId, 901);
        long sentinel = queued(eventId, 902);
        ReservationRequestedMessage message = ReservationRequestedMessage.create(target, eventId, Instant.now());

        send(message);
        send(message);
        // 같은 key(eventId)라 같은 파티션에 순서대로 쌓인다. 마지막에 보낸 sentinel이 처리되면 앞의 두 건도 처리된 것이다.
        send(ReservationRequestedMessage.create(sentinel, eventId, Instant.now()));

        Await.until("sentinel reservation processed", Duration.ofSeconds(20), () ->
                !"QUEUED".equals(status(sentinel)));

        assertThat(status(target)).isEqualTo("CONFIRMED");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT remaining_tickets FROM events WHERE id = ?", Integer.class, eventId))
                .as("two confirmations (target + sentinel), duplicate ignored")
                .isEqualTo(8);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM processed_messages WHERE message_id = ?", Integer.class, message.messageId()))
                .isEqualTo(1);
    }

    private void send(ReservationRequestedMessage message) throws Exception {
        kafkaTemplate.send(kafkaProperties.reservationRequestedTopic(), message.key(),
                jsonMapper.writeValueAsString(message)).get(10, TimeUnit.SECONDS);
    }

    private long queued(long eventId, long userId) {
        return reservationCommandRepository.insertQueuedIfAbsent(
                eventId, userId, "msg01-" + UUID.randomUUID(), "0".repeat(64), Instant.now()).orElseThrow();
    }

    private String status(long reservationId) {
        return jdbcTemplate.queryForObject("SELECT status FROM reservations WHERE id = ?", String.class, reservationId);
    }
}
