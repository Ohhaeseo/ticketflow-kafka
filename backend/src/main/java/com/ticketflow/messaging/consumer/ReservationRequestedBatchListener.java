package com.ticketflow.messaging.consumer;

import com.ticketflow.messaging.dto.ReservationRequestedMessage;
import com.ticketflow.reservation.service.ProcessingOutcome;
import com.ticketflow.reservation.service.ReservationProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

import static com.ticketflow.messaging.consumer.ReservationRequestedListener.LISTENER_ID;

/**
 * {@code reservation.requested.v1} <b>배치</b> Consumer (기본값, 처리량 개선).
 * <p>
 * poll로 받은 레코드 묶음을 <b>하나의 DB 트랜잭션</b>으로 처리하고, 오프셋도 배치마다 한 번 커밋한다.
 * 메시지별 정확성 규칙(예약 행 잠금 → processed_messages 확인 → 조건부 재고 감소)은 레코드 방식과 같다.
 * <p>
 * 실패 격리:
 * <ol>
 *   <li>파싱·검증 실패 레코드가 있으면 그 앞까지만 처리·커밋하고 {@link BatchListenerFailedException}(index)를 던진다.
 *       오류 처리기가 앞쪽 오프셋을 커밋하고, 실패 레코드는 DLT로 보내고, 나머지는 다시 전달한다.</li>
 *   <li>배치 트랜잭션이 실패하면(일시 오류, 특정 레코드 문제) 한 건씩 각자의 트랜잭션으로 다시 처리해
 *       실패한 레코드만 골라 {@link BatchListenerFailedException}으로 알린다. 성공한 앞 레코드는 이미 커밋돼 있다.</li>
 *   <li>없는 예약은 처리 후 해당 index로 알려 DLT로 보낸다. 뒤 레코드가 다시 전달돼도 processed_messages로 건너뛴다.</li>
 * </ol>
 */
@Component
@ConditionalOnBooleanProperty(name = "ticketflow.consumer.batch-enabled", matchIfMissing = true)
public class ReservationRequestedBatchListener {

    private static final Logger log = LoggerFactory.getLogger(ReservationRequestedBatchListener.class);

    private final ReservationProcessor reservationProcessor;
    private final ReservationMessageParser parser;

    public ReservationRequestedBatchListener(ReservationProcessor reservationProcessor, ReservationMessageParser parser) {
        this.reservationProcessor = reservationProcessor;
        this.parser = parser;
    }

    @KafkaListener(id = LISTENER_ID,
            topics = "${ticketflow.kafka.reservation-requested-topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "reservationBatchListenerContainerFactory")
    public void onBatch(List<ConsumerRecord<String, String>> records) {
        List<ReservationRequestedMessage> messages = new ArrayList<>(records.size());
        RuntimeException invalid = null;
        for (ConsumerRecord<String, String> record : records) {
            try {
                messages.add(parser.parse(record.value()));
            } catch (RuntimeException e) {
                invalid = e;
                break;
            }
        }

        processValidPrefix(messages);

        if (invalid != null) {
            int index = messages.size();
            throw new BatchListenerFailedException("Invalid message at index " + index, invalid, index);
        }
        log.debug("Processed batch of {} records", records.size());
    }

    private void processValidPrefix(List<ReservationRequestedMessage> messages) {
        if (messages.isEmpty()) {
            return;
        }
        List<ProcessingOutcome> outcomes;
        try {
            outcomes = reservationProcessor.processBatch(messages);
        } catch (RuntimeException batchFailure) {
            log.warn("Batch of {} failed ({}); retrying one by one to isolate the failing record",
                    messages.size(), batchFailure.toString());
            processOneByOne(messages);
            return;
        }
        failOnMissingReservation(messages, outcomes);
    }

    private void processOneByOne(List<ReservationRequestedMessage> messages) {
        for (int i = 0; i < messages.size(); i++) {
            ProcessingOutcome outcome;
            try {
                outcome = reservationProcessor.process(messages.get(i));
            } catch (RuntimeException e) {
                throw new BatchListenerFailedException("Processing failed at index " + i, e, i);
            }
            if (outcome == ProcessingOutcome.SKIPPED_NOT_FOUND) {
                throw notFound(messages.get(i), i);
            }
        }
    }

    private static void failOnMissingReservation(List<ReservationRequestedMessage> messages,
                                                 List<ProcessingOutcome> outcomes) {
        for (int i = 0; i < outcomes.size(); i++) {
            if (outcomes.get(i) == ProcessingOutcome.SKIPPED_NOT_FOUND) {
                throw notFound(messages.get(i), i);
            }
        }
    }

    private static BatchListenerFailedException notFound(ReservationRequestedMessage message, int index) {
        return new BatchListenerFailedException("Reservation not found at index " + index,
                new NonRetryableMessageException("Reservation " + message.reservationId() + " does not exist"), index);
    }
}
