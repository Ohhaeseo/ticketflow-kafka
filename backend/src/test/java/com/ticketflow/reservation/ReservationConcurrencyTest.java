package com.ticketflow.reservation;

import com.ticketflow.event.service.EventService;
import com.ticketflow.messaging.dto.ReservationRequestedMessage;
import com.ticketflow.reservation.repository.ReservationCommandRepository;
import com.ticketflow.reservation.service.ProcessingOutcome;
import com.ticketflow.reservation.service.ReservationProcessor;
import com.ticketflow.support.IntegrationTest;
import com.ticketflow.support.StockInvariant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
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
import java.util.stream.Collectors;

import static com.ticketflow.support.ReservationRequests.openEvent;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * CON-02 병렬 Consumer (PRD §12.1). 같은 공연의 메시지는 Kafka key 때문에 한 스레드가 순서대로 처리하므로
 * (PRD §10.3), DB 동시성은 처리 로직을 여러 스레드에서 동시에 호출해 검증한다.
 * 스레드 수는 커넥션 풀(10)보다 작게 둬서 백그라운드 Relay·Consumer가 커넥션을 얻을 수 있게 한다.
 */
@IntegrationTest
class ReservationConcurrencyTest {

    private static final int THREADS = 8;

    @Autowired
    ReservationProcessor processor;

    @Autowired
    ReservationCommandRepository reservationCommandRepository;

    @Autowired
    EventService eventService;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("CON-02 재고 1장에 서로 다른 예약 8건을 동시에 처리하면 확정 1건, 매진 7건, 음수 재고 없음")
    void lastTicketIsConfirmedOnlyOnce() throws Exception {
        long eventId = openEvent(eventService, 1);
        List<ReservationRequestedMessage> messages = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            long reservationId = queued(eventId, 801 + i);
            messages.add(new ReservationRequestedMessage(UUID.randomUUID(), 1, reservationId, eventId, Instant.now()));
        }

        Map<ProcessingOutcome, Long> outcomes = processConcurrently(messages);

        assertThat(outcomes).containsOnly(
                Map.entry(ProcessingOutcome.CONFIRMED, 1L),
                Map.entry(ProcessingOutcome.SOLD_OUT, (long) THREADS - 1));
        StockInvariant.Snapshot snapshot = StockInvariant.assertHolds(jdbcTemplate, eventId);
        assertThat(snapshot.remaining()).isZero();
        assertThat(snapshot.confirmed()).isEqualTo(1);
    }

    @Test
    @DisplayName("같은 예약에 서로 다른 messageId 8개가 동시에 와도 재고는 한 번만 줄어든다")
    void sameReservationDifferentMessagesConcurrently() throws Exception {
        long eventId = openEvent(eventService, 10);
        long reservationId = queued(eventId, 811);
        List<ReservationRequestedMessage> messages = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            messages.add(new ReservationRequestedMessage(UUID.randomUUID(), 1, reservationId, eventId, Instant.now()));
        }

        Map<ProcessingOutcome, Long> outcomes = processConcurrently(messages);

        assertThat(outcomes).containsOnly(
                Map.entry(ProcessingOutcome.CONFIRMED, 1L),
                Map.entry(ProcessingOutcome.SKIPPED_ALREADY_PROCESSED, (long) THREADS - 1));
        assertThat(remaining(eventId)).isEqualTo(9);
    }

    @Test
    @DisplayName("MSG-01 같은 메시지 8개가 동시에 와도 처리는 1번, 나머지는 중복으로 건너뛴다")
    void sameMessageConcurrently() throws Exception {
        long eventId = openEvent(eventService, 10);
        long reservationId = queued(eventId, 812);
        ReservationRequestedMessage message =
                new ReservationRequestedMessage(UUID.randomUUID(), 1, reservationId, eventId, Instant.now());
        List<ReservationRequestedMessage> messages = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            messages.add(message);
        }

        Map<ProcessingOutcome, Long> outcomes = processConcurrently(messages);

        assertThat(outcomes).containsOnly(
                Map.entry(ProcessingOutcome.CONFIRMED, 1L),
                Map.entry(ProcessingOutcome.SKIPPED_DUPLICATE_MESSAGE, (long) THREADS - 1));
        assertThat(remaining(eventId)).isEqualTo(9);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM processed_messages WHERE reservation_id = ?", Integer.class, reservationId))
                .isEqualTo(1);
    }

    private Map<ProcessingOutcome, Long> processConcurrently(List<ReservationRequestedMessage> messages)
            throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(messages.size());
        CountDownLatch ready = new CountDownLatch(messages.size());
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<ProcessingOutcome>> futures = new ArrayList<>();
            for (ReservationRequestedMessage message : messages) {
                Callable<ProcessingOutcome> call = () -> {
                    ready.countDown();
                    start.await();
                    return processor.process(message);
                };
                futures.add(executor.submit(call));
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            List<ProcessingOutcome> outcomes = new ArrayList<>();
            for (Future<ProcessingOutcome> future : futures) {
                outcomes.add(future.get(30, TimeUnit.SECONDS));
            }
            return outcomes.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        } finally {
            executor.shutdownNow();
        }
    }

    private long queued(long eventId, long userId) {
        return reservationCommandRepository.insertQueuedIfAbsent(
                eventId, userId, "con02-" + UUID.randomUUID(), "0".repeat(64), Instant.now()).orElseThrow();
    }

    private int remaining(long eventId) {
        return jdbcTemplate.queryForObject("SELECT remaining_tickets FROM events WHERE id = ?", Integer.class, eventId);
    }
}
