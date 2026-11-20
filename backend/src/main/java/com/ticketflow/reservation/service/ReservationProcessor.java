package com.ticketflow.reservation.service;

import com.ticketflow.idempotency.repository.ProcessedMessageRepository;
import com.ticketflow.messaging.dto.ReservationRequestedMessage;
import com.ticketflow.reservation.repository.ReservationCommandRepository;
import com.ticketflow.reservation.repository.ReservationCommandRepository.LockedReservation;
import com.ticketflow.reservation.repository.ReservationOutcomeRepository;
import com.ticketflow.reservation.repository.ReservationOutcomeRepository.Applied;
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
 * 하나의 DB 트랜잭션 안에서 아래 순서로 실행한다 (메시지당 SQL 왕복 3번).
 * <ol>
 *   <li>예약 행 잠금 ({@code FOR UPDATE}). 같은 예약에 대한 메시지는 여기서 한 줄로 선다.</li>
 *   <li>같은 {@code message_id}를 이미 처리했으면 건너뜀 (processed_messages). 잠금을 얻은 뒤 새로 조회한다.</li>
 *   <li>예약이 QUEUED가 아니면 이력만 남기고 건너뜀 (다른 message_id로 온 중복).</li>
 *   <li>{@link ReservationOutcomeRepository} 한 문장: 조건부 재고 감소 → CONFIRMED / SOLD_OUT / FAILED 판정 →
 *       상태 변경 → processed_messages 기록.</li>
 * </ol>
 * 중간에 실패하면 재고, 상태, 처리 이력이 함께 롤백되고 오프셋도 커밋되지 않는다 (FR-INV-03).
 */
@Service
public class ReservationProcessor {

    public static final String SALES_CLOSED = "SALES_CLOSED";

    private static final Logger log = LoggerFactory.getLogger(ReservationProcessor.class);

    private final ReservationCommandRepository reservationCommandRepository;
    private final ReservationOutcomeRepository reservationOutcomeRepository;
    private final ProcessedMessageRepository processedMessageRepository;
    private final Clock clock;
    private final MeterRegistry meterRegistry;
    private final Timer processingTimer;

    public ReservationProcessor(ReservationCommandRepository reservationCommandRepository,
                                ReservationOutcomeRepository reservationOutcomeRepository,
                                ProcessedMessageRepository processedMessageRepository,
                                Clock clock,
                                MeterRegistry meterRegistry) {
        this.reservationCommandRepository = reservationCommandRepository;
        this.reservationOutcomeRepository = reservationOutcomeRepository;
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
        if (reservation.status().isFinal()) {
            log.info("Reservation already final: reservationId={} status={} messageId={}",
                    reservation.id(), reservation.status(), message.messageId());
            processedMessageRepository.record(message.messageId(), reservation.id(),
                    ProcessingOutcome.SKIPPED_ALREADY_PROCESSED.name(), now);
            return ProcessingOutcome.SKIPPED_ALREADY_PROCESSED;
        }
        if (reservation.eventId() != message.eventId()) {
            log.warn("Message eventId {} differs from reservation eventId {}; using the reservation's",
                    message.eventId(), reservation.eventId());
        }

        // 재고 감소 → 결과 판정 → 상태 변경 → 처리 이력을 SQL 한 문장으로 (왕복 1번)
        Applied applied = reservationOutcomeRepository
                .decideAndApply(reservation.id(), reservation.eventId(), message.messageId(), now)
                .orElseThrow(() -> new IllegalStateException("Event " + reservation.eventId() + " not found"));
        if (applied.updatedRows() != 1 || applied.recordedRows() != 1) {
            // 행을 잠근 상태라 일어나면 안 된다. 예외로 재고 감소까지 롤백한다.
            throw new IllegalStateException("Reservation " + reservation.id() + " was not applied while locked: "
                    + applied);
        }
        return ProcessingOutcome.valueOf(applied.outcome());
    }
}
