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
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.ticketflow.support.ReservationRequests.openEvent;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * 배치 리스너의 실패 격리: 배치 안의 문제 레코드 하나 때문에 나머지 레코드가 막히거나 DLT로 가지 않는다.
 */
@IntegrationTest
class BatchListenerIsolationTest {

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
    @DisplayName("배치 중간의 깨진 메시지만 DLT로 가고 앞뒤 메시지는 정상 처리된다")
    void invalidRecordInTheMiddleIsIsolated() throws Exception {
        long eventId = openEvent(eventService, 10);
        long before = queued(eventId, 941);
        long after = queued(eventId, 942);
        String key = String.valueOf(eventId);
        String invalid = """
                {"messageId":"%s","schemaVersion":99,"reservationId":%d,"eventId":%d,"occurredAt":"2026-11-20T00:00:00Z"}"""
                .formatted(UUID.randomUUID(), before, eventId);

        // 같은 key로 연달아 보내 한 파티션에 순서대로 쌓는다 (대개 한 배치로 함께 poll된다)
        List<CompletableFuture<SendResult<String, String>>> sends = List.of(
                send(key, message(before, eventId)),
                send(key, invalid),
                send(key, message(after, eventId)));
        RecordMetadata invalidMetadata = sends.get(1).get(10, TimeUnit.SECONDS).getRecordMetadata();
        sends.get(2).get(10, TimeUnit.SECONDS);

        Await.until("valid records processed", Duration.ofSeconds(20), () ->
                "CONFIRMED".equals(status(before)) && "CONFIRMED".equals(status(after)));
        Await.until("invalid record in DLT", Duration.ofSeconds(20), () -> deadLetterExists(invalidMetadata));

        assertThat(remaining(eventId)).isEqualTo(8);
    }

    @Test
    @DisplayName("한 레코드의 DB 처리가 계속 실패하면 그 레코드만 재시도 후 DLT로 가고 나머지는 확정된다")
    void persistentlyFailingRecordIsIsolated() throws Exception {
        long eventId = openEvent(eventService, 10);
        long ok1 = queued(eventId, 296);
        long bad = queued(eventId, 297);
        long ok2 = queued(eventId, 295);
        jdbcTemplate.execute("""
                CREATE OR REPLACE FUNCTION test_always_fail_user_297() RETURNS trigger AS $$
                BEGIN
                    IF NEW.user_id = 297 AND NEW.status <> 'QUEUED' THEN
                        RAISE EXCEPTION 'injected persistent failure';
                    END IF;
                    RETURN NEW;
                END $$ LANGUAGE plpgsql
                """);
        jdbcTemplate.execute("""
                CREATE TRIGGER test_always_fail_user_297 BEFORE UPDATE ON reservations
                FOR EACH ROW EXECUTE FUNCTION test_always_fail_user_297()
                """);
        try {
            String key = String.valueOf(eventId);
            send(key, message(ok1, eventId));
            RecordMetadata badMetadata = send(key, message(bad, eventId)).get(10, TimeUnit.SECONDS).getRecordMetadata();
            send(key, message(ok2, eventId)).get(10, TimeUnit.SECONDS);

            Await.until("healthy records confirmed", Duration.ofSeconds(30), () ->
                    "CONFIRMED".equals(status(ok1)) && "CONFIRMED".equals(status(ok2)));
            Await.until("failing record moved to DLT after retries", Duration.ofSeconds(30), () ->
                    deadLetterExists(badMetadata));

            assertThat(status(bad)).as("rolled back every time").isEqualTo("QUEUED");
            assertThat(remaining(eventId)).isEqualTo(8);
            Map<String, Object> deadLetter = jdbcTemplate.queryForMap("""
                    SELECT exception_message FROM dead_letter_messages
                     WHERE original_topic = ? AND original_partition = ? AND original_offset = ?
                    """, badMetadata.topic(), badMetadata.partition(), badMetadata.offset());
            assertThat((String) deadLetter.get("exception_message")).contains("injected persistent failure");
        } finally {
            jdbcTemplate.execute("DROP TRIGGER IF EXISTS test_always_fail_user_297 ON reservations");
            jdbcTemplate.execute("DROP FUNCTION IF EXISTS test_always_fail_user_297()");
        }
    }

    private CompletableFuture<SendResult<String, String>> send(String key, String payload) {
        return kafkaTemplate.send(kafkaProperties.reservationRequestedTopic(), key, payload);
    }

    private String message(long reservationId, long eventId) {
        return jsonMapper.writeValueAsString(ReservationRequestedMessage.create(reservationId, eventId, Instant.now()));
    }

    private long queued(long eventId, long userId) {
        return reservationCommandRepository.insertQueuedIfAbsent(
                eventId, userId, "batch-" + UUID.randomUUID(), "0".repeat(64), Instant.now()).orElseThrow();
    }

    private String status(long reservationId) {
        return jdbcTemplate.queryForObject("SELECT status FROM reservations WHERE id = ?", String.class, reservationId);
    }

    private int remaining(long eventId) {
        return jdbcTemplate.queryForObject("SELECT remaining_tickets FROM events WHERE id = ?", Integer.class, eventId);
    }

    private boolean deadLetterExists(RecordMetadata metadata) {
        return jdbcTemplate.queryForObject("""
                SELECT count(*) FROM dead_letter_messages
                 WHERE original_topic = ? AND original_partition = ? AND original_offset = ?
                """, Integer.class, metadata.topic(), metadata.partition(), metadata.offset()) > 0;
    }
}
