package com.ticketflow.messaging;

import com.ticketflow.event.service.EventService;
import com.ticketflow.messaging.config.TicketFlowKafkaProperties;
import com.ticketflow.messaging.dto.ReservationRequestedMessage;
import com.ticketflow.reservation.repository.ReservationCommandRepository;
import com.ticketflow.support.Await;
import com.ticketflow.support.IntegrationTest;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.ticketflow.support.ReservationRequests.openEvent;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * MSG-04 실패 메시지 (PRD §12.1)와 FR-CSM-03 재시도 정책.
 * 영구 오류는 재시도 없이 DLT로 가고 dead_letter_messages에 감사 이력이 남는다. 일시 오류는 재시도 후 성공한다.
 */
@IntegrationTest
class DeadLetterIntegrationTest {

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
    @DisplayName("MSG-04 JSON이 깨진 메시지는 DLT로 이동하고 원본·예외 정보가 감사 이력에 남는다")
    void malformedJsonGoesToDeadLetter() throws Exception {
        String payload = "{not-json " + UUID.randomUUID();

        Map<String, Object> row = awaitDeadLetter(send("dlt-test", payload));

        assertThat(row).containsEntry("payload", payload).containsEntry("message_key", "dlt-test");
        assertThat((String) row.get("exception_cause_class")).startsWith("tools.jackson");
    }

    @Test
    @DisplayName("MSG-04 지원하지 않는 schemaVersion은 재시도 없이 DLT로 이동한다")
    void unsupportedSchemaVersionGoesToDeadLetter() throws Exception {
        String payload = """
                {"messageId":"%s","schemaVersion":99,"reservationId":1,"eventId":1,"occurredAt":"2026-11-20T00:00:00Z"}"""
                .formatted(UUID.randomUUID());

        Map<String, Object> row = awaitDeadLetter(send("1", payload));

        assertThat((String) row.get("exception_cause_class")).endsWith("NonRetryableMessageException");
        assertThat((String) row.get("exception_message")).contains("schemaVersion 99");
    }

    @Test
    @DisplayName("MSG-04 존재하지 않는 예약의 메시지는 DLT로 이동한다")
    void unknownReservationGoesToDeadLetter() throws Exception {
        ReservationRequestedMessage message = ReservationRequestedMessage.create(999_999_999L, 1L, Instant.now());

        Map<String, Object> row = awaitDeadLetter(send(message.key(), jsonMapper.writeValueAsString(message)));

        assertThat((String) row.get("exception_message")).contains("999999999 does not exist");
    }

    @Test
    @DisplayName("FR-CSM-03 일시 오류는 백오프 재시도 후 성공하고 DLT로 가지 않는다 (재고는 한 번만 감소)")
    void transientFailureIsRetried() throws Exception {
        long eventId = openEvent(eventService, 5);
        long reservationId = reservationCommandRepository.insertQueuedIfAbsent(
                eventId, 298, "retry-" + UUID.randomUUID(), "0".repeat(64), Instant.now()).orElseThrow();
        // user 298 예약의 상태 변경을 처음 2번만 실패시킨다. 시퀀스는 롤백되지 않아 시도 횟수를 셀 수 있다.
        jdbcTemplate.execute("CREATE SEQUENCE test_retry_seq");
        jdbcTemplate.execute("""
                CREATE OR REPLACE FUNCTION test_fail_twice_user_298() RETURNS trigger AS $$
                BEGIN
                    IF NEW.user_id = 298 AND NEW.status <> 'QUEUED' THEN
                        IF nextval('test_retry_seq') <= 2 THEN
                            RAISE EXCEPTION 'injected transient failure';
                        END IF;
                    END IF;
                    RETURN NEW;
                END $$ LANGUAGE plpgsql
                """);
        jdbcTemplate.execute("""
                CREATE TRIGGER test_fail_twice_user_298 BEFORE UPDATE ON reservations
                FOR EACH ROW EXECUTE FUNCTION test_fail_twice_user_298()
                """);
        try {
            RecordMetadata sent = send(String.valueOf(eventId), jsonMapper.writeValueAsString(
                    ReservationRequestedMessage.create(reservationId, eventId, Instant.now())));

            Await.until("reservation confirmed after retries", Duration.ofSeconds(20), () ->
                    "CONFIRMED".equals(jdbcTemplate.queryForObject(
                            "SELECT status FROM reservations WHERE id = ?", String.class, reservationId)));

            assertThat(jdbcTemplate.queryForObject("SELECT last_value FROM test_retry_seq", Long.class))
                    .as("2 failed attempts + 1 successful attempt").isEqualTo(3);
            assertThat(jdbcTemplate.queryForObject(
                    "SELECT remaining_tickets FROM events WHERE id = ?", Integer.class, eventId)).isEqualTo(4);
            assertThat(deadLetters(sent)).isEmpty();
        } finally {
            jdbcTemplate.execute("DROP TRIGGER IF EXISTS test_fail_twice_user_298 ON reservations");
            jdbcTemplate.execute("DROP FUNCTION IF EXISTS test_fail_twice_user_298()");
            jdbcTemplate.execute("DROP SEQUENCE IF EXISTS test_retry_seq");
        }
    }

    private RecordMetadata send(String key, String payload) throws Exception {
        return kafkaTemplate.send(kafkaProperties.reservationRequestedTopic(), key, payload)
                .get(10, TimeUnit.SECONDS).getRecordMetadata();
    }

    private Map<String, Object> awaitDeadLetter(RecordMetadata sent) {
        Await.until("dead letter recorded", Duration.ofSeconds(20), () -> !deadLetters(sent).isEmpty());
        return deadLetters(sent).getFirst();
    }

    private List<Map<String, Object>> deadLetters(RecordMetadata sent) {
        return jdbcTemplate.queryForList("""
                SELECT * FROM dead_letter_messages
                 WHERE original_topic = ? AND original_partition = ? AND original_offset = ?
                """, sent.topic(), sent.partition(), sent.offset());
    }
}
