package com.ticketflow.reservation;

import com.ticketflow.event.service.EventService;
import com.ticketflow.messaging.dto.ReservationRequestedMessage;
import com.ticketflow.reservation.repository.ReservationCommandRepository;
import com.ticketflow.reservation.service.ProcessingOutcome;
import com.ticketflow.reservation.service.ReservationProcessor;
import com.ticketflow.support.IntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.ticketflow.support.ReservationRequests.openEvent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Consumer 처리 로직을 Kafka 없이 직접 호출해 검증한다.
 * 예약은 Outbox 없이 직접 저장하므로 백그라운드 Consumer가 끼어들지 않는다.
 */
@IntegrationTest
class ReservationProcessorTest {

    @Autowired
    ReservationProcessor processor;

    @Autowired
    ReservationCommandRepository reservationCommandRepository;

    @Autowired
    EventService eventService;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("재고가 있으면 CONFIRMED로 바꾸고 재고를 1 줄이며 처리 이력을 남긴다")
    void confirmsAndDecrementsStock() {
        long eventId = openEvent(eventService, 2);
        long reservationId = queued(eventId, 201);
        ReservationRequestedMessage message = message(reservationId, eventId);

        assertThat(processor.process(message)).isEqualTo(ProcessingOutcome.CONFIRMED);

        assertThat(status(reservationId)).isEqualTo("CONFIRMED");
        assertThat(remaining(eventId)).isEqualTo(1);
        assertThat(processedOutcome(message.messageId())).isEqualTo("CONFIRMED");
    }

    @Test
    @DisplayName("MSG-01 같은 messageId를 두 번 처리하면 두 번째는 건너뛰고 재고는 한 번만 줄어든다")
    void duplicateMessageIdIsSkipped() {
        long eventId = openEvent(eventService, 5);
        long reservationId = queued(eventId, 202);
        ReservationRequestedMessage message = message(reservationId, eventId);

        assertThat(processor.process(message)).isEqualTo(ProcessingOutcome.CONFIRMED);
        assertThat(processor.process(message)).isEqualTo(ProcessingOutcome.SKIPPED_DUPLICATE_MESSAGE);

        assertThat(remaining(eventId)).isEqualTo(4);
        assertThat(processedCount(reservationId)).isEqualTo(1);
    }

    @Test
    @DisplayName("다른 messageId로 같은 예약이 다시 오면 상태는 그대로 두고 이력만 남긴다")
    void sameReservationDifferentMessageIdIsSkipped() {
        long eventId = openEvent(eventService, 5);
        long reservationId = queued(eventId, 206);

        assertThat(processor.process(message(reservationId, eventId))).isEqualTo(ProcessingOutcome.CONFIRMED);
        ReservationRequestedMessage second = message(reservationId, eventId);
        assertThat(processor.process(second)).isEqualTo(ProcessingOutcome.SKIPPED_ALREADY_PROCESSED);

        assertThat(remaining(eventId)).isEqualTo(4);
        assertThat(processedOutcome(second.messageId())).isEqualTo("SKIPPED_ALREADY_PROCESSED");
    }

    @Test
    @DisplayName("재고가 없으면 SOLD_OUT으로 바꾸고 재고는 0에서 더 줄지 않는다")
    void soldOutWhenNoStock() {
        long eventId = openEvent(eventService, 1);
        long first = queued(eventId, 203);
        long second = queued(eventId, 204);

        assertThat(processor.process(message(first, eventId))).isEqualTo(ProcessingOutcome.CONFIRMED);
        assertThat(processor.process(message(second, eventId))).isEqualTo(ProcessingOutcome.SOLD_OUT);

        assertThat(status(second)).isEqualTo("SOLD_OUT");
        assertThat(remaining(eventId)).isZero();
    }

    @Test
    @DisplayName("판매 종료 후 처리되면 FAILED(SALES_CLOSED)이고 재고는 그대로다 (PRD Q3)")
    void failedWhenSalesClosedBeforeProcessing() {
        long eventId = openEvent(eventService, 10);
        long reservationId = queued(eventId, 205);
        jdbcTemplate.update("UPDATE events SET sales_end_at = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = ?",
                eventId);

        assertThat(processor.process(message(reservationId, eventId))).isEqualTo(ProcessingOutcome.FAILED);

        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT status, failure_reason FROM reservations WHERE id = ?", reservationId);
        assertThat(row).containsEntry("status", "FAILED").containsEntry("failure_reason", "SALES_CLOSED");
        assertThat(remaining(eventId)).isEqualTo(10);
    }

    @Test
    @DisplayName("FR-INV-03 재고를 줄인 뒤 상태 변경이 실패하면 재고·상태·처리 이력이 함께 롤백된다")
    void rollsBackStockWhenStatusUpdateFails() {
        long eventId = openEvent(eventService, 3);
        long reservationId = queued(eventId, 299);
        ReservationRequestedMessage message = message(reservationId, eventId);
        // user 299의 예약 상태 변경만 실패하게 만드는 트리거. 재고 감소는 이미 실행된 뒤에 실패한다.
        jdbcTemplate.execute("""
                CREATE OR REPLACE FUNCTION test_fail_user_299() RETURNS trigger AS $$
                BEGIN
                    IF NEW.user_id = 299 AND NEW.status <> 'QUEUED' THEN
                        RAISE EXCEPTION 'injected failure for rollback test';
                    END IF;
                    RETURN NEW;
                END $$ LANGUAGE plpgsql
                """);
        jdbcTemplate.execute("""
                CREATE TRIGGER test_fail_user_299 BEFORE UPDATE ON reservations
                FOR EACH ROW EXECUTE FUNCTION test_fail_user_299()
                """);
        try {
            assertThatThrownBy(() -> processor.process(message))
                    .isInstanceOf(DataAccessException.class)
                    .hasMessageContaining("injected failure");
        } finally {
            jdbcTemplate.execute("DROP TRIGGER IF EXISTS test_fail_user_299 ON reservations");
            jdbcTemplate.execute("DROP FUNCTION IF EXISTS test_fail_user_299()");
        }

        assertThat(remaining(eventId)).as("stock restored").isEqualTo(3);
        assertThat(status(reservationId)).isEqualTo("QUEUED");
        assertThat(processedCount(reservationId)).isZero();

        // 트리거를 없앤 뒤 같은 메시지를 다시 처리하면 정상 확정된다 (Kafka 재전달 상황)
        assertThat(processor.process(message)).isEqualTo(ProcessingOutcome.CONFIRMED);
        assertThat(remaining(eventId)).isEqualTo(2);
    }

    @Test
    @DisplayName("한 트랜잭션(배치)에서 앞의 재고 감소가 뒤 판정에 보인다: 티켓 1장에 2건 → CONFIRMED, SOLD_OUT")
    void batchSeesEarlierDecrementsInSameTransaction() {
        long eventId = openEvent(eventService, 1);
        long first = queued(eventId, 209);
        long second = queued(eventId, 210);

        assertThat(processor.processBatch(List.of(message(first, eventId), message(second, eventId))))
                .containsExactly(ProcessingOutcome.CONFIRMED, ProcessingOutcome.SOLD_OUT);

        assertThat(remaining(eventId)).isZero();
        assertThat(status(second)).isEqualTo("SOLD_OUT");
    }

    @Test
    @DisplayName("재고 0이면서 판매도 끝났으면 SOLD_OUT (PRD Q3: 재고가 0이면 판매 기간과 관계없이 SOLD_OUT)")
    void soldOutTakesPrecedenceOverSalesClosed() {
        long eventId = openEvent(eventService, 1);
        long first = queued(eventId, 207);
        long second = queued(eventId, 208);
        assertThat(processor.process(message(first, eventId))).isEqualTo(ProcessingOutcome.CONFIRMED);
        jdbcTemplate.update("UPDATE events SET sales_end_at = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = ?",
                eventId);

        assertThat(processor.process(message(second, eventId))).isEqualTo(ProcessingOutcome.SOLD_OUT);

        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT status, failure_reason FROM reservations WHERE id = ?", second);
        assertThat(row).containsEntry("status", "SOLD_OUT").containsEntry("failure_reason", null);
    }

    @Test
    @DisplayName("없는 예약의 메시지는 아무것도 바꾸지 않는다")
    void unknownReservationIsSkipped() {
        assertThat(processor.process(message(999_999_999L, 1L))).isEqualTo(ProcessingOutcome.SKIPPED_NOT_FOUND);
    }

    private long queued(long eventId, long userId) {
        return reservationCommandRepository.insertQueuedIfAbsent(
                eventId, userId, "processor-" + UUID.randomUUID(), "0".repeat(64), Instant.now()).orElseThrow();
    }

    private static ReservationRequestedMessage message(long reservationId, long eventId) {
        return new ReservationRequestedMessage(UUID.randomUUID(), 1, reservationId, eventId, Instant.now());
    }

    private String status(long reservationId) {
        return jdbcTemplate.queryForObject("SELECT status FROM reservations WHERE id = ?", String.class, reservationId);
    }

    private int remaining(long eventId) {
        return jdbcTemplate.queryForObject("SELECT remaining_tickets FROM events WHERE id = ?", Integer.class, eventId);
    }

    private String processedOutcome(UUID messageId) {
        return jdbcTemplate.queryForObject(
                "SELECT outcome FROM processed_messages WHERE message_id = ?", String.class, messageId);
    }

    private int processedCount(long reservationId) {
        return jdbcTemplate.queryForObject(
                "SELECT count(*) FROM processed_messages WHERE reservation_id = ?", Integer.class, reservationId);
    }
}
