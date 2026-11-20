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
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static com.ticketflow.support.ReservationRequests.openEvent;
import static org.assertj.core.api.Assertions.assertThat;

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
    @DisplayName("재고가 있으면 CONFIRMED로 바꾸고 재고를 1 줄인다")
    void confirmsAndDecrementsStock() {
        long eventId = openEvent(eventService, 2);
        long reservationId = queued(eventId, 201);

        assertThat(processor.process(message(reservationId, eventId))).isEqualTo(ProcessingOutcome.CONFIRMED);

        assertThat(status(reservationId)).isEqualTo("CONFIRMED");
        assertThat(remaining(eventId)).isEqualTo(1);
    }

    @Test
    @DisplayName("같은 메시지를 두 번 처리해도 재고는 한 번만 줄어든다")
    void duplicateMessageIsSkipped() {
        long eventId = openEvent(eventService, 5);
        long reservationId = queued(eventId, 202);
        ReservationRequestedMessage message = message(reservationId, eventId);

        assertThat(processor.process(message)).isEqualTo(ProcessingOutcome.CONFIRMED);
        assertThat(processor.process(message)).isEqualTo(ProcessingOutcome.SKIPPED_ALREADY_PROCESSED);

        assertThat(remaining(eventId)).isEqualTo(4);
        assertThat(status(reservationId)).isEqualTo("CONFIRMED");
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
}
