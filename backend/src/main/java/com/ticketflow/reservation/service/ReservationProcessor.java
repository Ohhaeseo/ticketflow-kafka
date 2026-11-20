package com.ticketflow.reservation.service;

import com.ticketflow.event.repository.EventStockRepository;
import com.ticketflow.event.repository.EventStockRepository.StockState;
import com.ticketflow.idempotency.repository.ProcessedMessageRepository;
import com.ticketflow.messaging.dto.ReservationRequestedMessage;
import com.ticketflow.reservation.entity.ReservationStatus;
import com.ticketflow.reservation.repository.ReservationCommandRepository;
import com.ticketflow.reservation.repository.ReservationCommandRepository.LockedReservation;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 예약 접수 이벤트를 처리해 최종 상태를 정한다 (PRD §8.3, FR-CSM-01, FR-INV-01).
 * <p>
 * 하나의 DB 트랜잭션 안에서 아래 순서로 실행한다.
 * <ol>
 *   <li>예약 행 잠금 ({@code FOR UPDATE}). 같은 예약에 대한 메시지는 여기서 한 줄로 선다.</li>
 *   <li>같은 {@code message_id}를 이미 처리했으면 건너뜀 (processed_messages).</li>
 *   <li>예약이 QUEUED가 아니면 건너뜀 (다른 message_id로 온 중복).</li>
 *   <li>조건부 재고 감소 → CONFIRMED / SOLD_OUT / FAILED 전이.</li>
 *   <li>processed_messages에 결과 기록.</li>
 * </ol>
 * 중간에 실패하면 재고, 상태, 처리 이력이 함께 롤백되고 오프셋도 커밋되지 않는다 (FR-INV-03).
 */
@Service
public class ReservationProcessor {

    public static final String SALES_CLOSED = "SALES_CLOSED";

    private static final Logger log = LoggerFactory.getLogger(ReservationProcessor.class);

    private final ReservationCommandRepository reservationCommandRepository;
    private final EventStockRepository eventStockRepository;
    private final ProcessedMessageRepository processedMessageRepository;
    private final Clock clock;
    private final MeterRegistry meterRegistry;
    private final Timer processingTimer;

    public ReservationProcessor(ReservationCommandRepository reservationCommandRepository,
                                EventStockRepository eventStockRepository,
                                ProcessedMessageRepository processedMessageRepository,
                                Clock clock,
                                MeterRegistry meterRegistry) {
        this.reservationCommandRepository = reservationCommandRepository;
        this.eventStockRepository = eventStockRepository;
        this.processedMessageRepository = processedMessageRepository;
        this.clock = clock;
        this.meterRegistry = meterRegistry;
        this.processingTimer = Timer.builder("ticketflow.reservation.processing")
                .description("예약 메시지 하나를 처리하는 시간 (커밋 제외)")
                .publishPercentileHistogram()
                .register(meterRegistry);
    }

    /** 메시지 하나를 자기 트랜잭션에서 처리한다. */
    @Transactional
    public ProcessingOutcome process(ReservationRequestedMessage message) {
        return processInCurrentTransaction(message);
    }

    /**
     * 메시지 여러 개를 <b>하나의 트랜잭션</b>에서 순서대로 처리한다 (배치 리스너용).
     * 커밋·fsync·재고 행 잠금 획득이 메시지마다가 아니라 배치마다 한 번이 된다.
     * 메시지별 처리 규칙(행 잠금, 중복 확인, 조건부 재고 감소)은 {@link #process}와 같다.
     * 같은 배치에 같은 message_id가 두 번 있어도 앞의 처리 이력이 같은 트랜잭션 안에서 보이므로 한 번만 적용된다.
     * 하나라도 실패하면 배치 전체가 롤백된다.
     */
    @Transactional
    public List<ProcessingOutcome> processBatch(List<ReservationRequestedMessage> messages) {
        List<ProcessingOutcome> outcomes = new ArrayList<>(messages.size());
        for (ReservationRequestedMessage message : messages) {
            outcomes.add(processInCurrentTransaction(message));
        }
        return outcomes;
    }

    private ProcessingOutcome processInCurrentTransaction(ReservationRequestedMessage message) {
        Timer.Sample sample = Timer.start(meterRegistry);
        ProcessingOutcome outcome = processInTransaction(message);
        sample.stop(processingTimer);
        countAfterCommit(outcome);
        return outcome;
    }

    /** 롤백된 처리를 세지 않도록 커밋된 뒤에만 결과 카운터를 올린다. */
    private void countAfterCommit(ProcessingOutcome outcome) {
        Runnable increment = () -> meterRegistry.counter("ticketflow.reservations.processed",
                "outcome", outcome.name()).increment();
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    increment.run();
                }
            });
        } else {
            increment.run();
        }
    }

    private ProcessingOutcome processInTransaction(ReservationRequestedMessage message) {
        Optional<LockedReservation> locked = reservationCommandRepository.lockForProcessing(message.reservationId());
        if (locked.isEmpty()) {
            log.warn("Reservation not found: reservationId={} messageId={}", message.reservationId(), message.messageId());
            return ProcessingOutcome.SKIPPED_NOT_FOUND;
        }
        LockedReservation reservation = locked.get();

        if (processedMessageRepository.exists(message.messageId())) {
            log.info("Duplicate message skipped: messageId={} reservationId={}", message.messageId(), reservation.id());
            return ProcessingOutcome.SKIPPED_DUPLICATE_MESSAGE;
        }

        Instant now = clock.instant().truncatedTo(ChronoUnit.MICROS);
        ProcessingOutcome outcome = decide(reservation, message, now);
        processedMessageRepository.record(message.messageId(), reservation.id(), outcome.name(), now);
        return outcome;
    }

    private ProcessingOutcome decide(LockedReservation reservation, ReservationRequestedMessage message, Instant now) {
        if (reservation.status().isFinal()) {
            log.info("Reservation already final: reservationId={} status={} messageId={}",
                    reservation.id(), reservation.status(), message.messageId());
            return ProcessingOutcome.SKIPPED_ALREADY_PROCESSED;
        }
        if (reservation.eventId() != message.eventId()) {
            log.warn("Message eventId {} differs from reservation eventId {}; using the reservation's",
                    message.eventId(), reservation.eventId());
        }

        if (eventStockRepository.decrementIfAvailable(reservation.eventId()) == 1) {
            complete(reservation.id(), ReservationStatus.CONFIRMED, null, now);
            return ProcessingOutcome.CONFIRMED;
        }

        StockState stock = eventStockRepository.findStockState(reservation.eventId());
        if (stock.remainingTickets() == 0) {
            complete(reservation.id(), ReservationStatus.SOLD_OUT, null, now);
            return ProcessingOutcome.SOLD_OUT;
        }
        if (!stock.onSale()) {
            // PRD Q3: 판매 기간 안에 접수됐지만 종료 후 처리된 예약
            complete(reservation.id(), ReservationStatus.FAILED, SALES_CLOSED, now);
            return ProcessingOutcome.FAILED;
        }
        // 재고가 있고 판매 중인데 감소가 실패한 경우: 일시적 이상으로 보고 예외로 재시도하게 한다
        throw new IllegalStateException("Stock decrement failed although stock is available: eventId="
                + reservation.eventId());
    }

    private void complete(long reservationId, ReservationStatus status, String failureReason, Instant now) {
        int updated = reservationCommandRepository.completeIfQueued(reservationId, status, failureReason, now);
        if (updated != 1) {
            // 행을 잠근 상태라 일어나면 안 된다. 예외로 재고 감소까지 롤백한다.
            throw new IllegalStateException("Reservation " + reservationId + " was not QUEUED while locked");
        }
    }
}
