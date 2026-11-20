package com.ticketflow.messaging.consumer;

import com.ticketflow.messaging.dto.ReservationRequestedMessage;
import com.ticketflow.reservation.service.ProcessingOutcome;
import com.ticketflow.reservation.service.ReservationProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * {@code reservation.requested.v1} <b>레코드 단위</b> Consumer (3~7주차 방식).
 * {@code ticketflow.consumer.batch-enabled=false}일 때만 쓴다. 기본은 {@link ReservationRequestedBatchListener}.
 * <p>
 * 메시지 하나 = DB 트랜잭션 하나. 오프셋은 {@code spring.kafka.listener.ack-mode}(record)에 따라 커밋된다.
 * 오류 처리는 {@code KafkaErrorHandlingConfig} 참고.
 */
@Component
@ConditionalOnBooleanProperty(name = "ticketflow.consumer.batch-enabled", havingValue = false)
public class ReservationRequestedListener {

    /** 리스너 컨테이너 id. 레코드·배치 리스너가 같은 id를 쓴다 (장애 실험에서 멈추거나 다시 시작할 때 사용). */
    public static final String LISTENER_ID = "reservation-requested";

    private static final Logger log = LoggerFactory.getLogger(ReservationRequestedListener.class);

    private final ReservationProcessor reservationProcessor;
    private final ReservationMessageParser parser;

    public ReservationRequestedListener(ReservationProcessor reservationProcessor, ReservationMessageParser parser) {
        this.reservationProcessor = reservationProcessor;
        this.parser = parser;
    }

    // id를 지정하면 Spring Kafka는 기본적으로 id를 group.id로 쓴다. groupId를 명시해 설정값을 따르게 한다.
    @KafkaListener(id = LISTENER_ID,
            topics = "${ticketflow.kafka.reservation-requested-topic}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(ConsumerRecord<String, String> record) {
        ReservationRequestedMessage message = parser.parse(record.value());
        ProcessingOutcome outcome = reservationProcessor.process(message);
        if (outcome == ProcessingOutcome.SKIPPED_NOT_FOUND) {
            // Outbox는 예약과 같은 트랜잭션에서 기록되므로 정상이라면 일어나지 않는다. 운영자 확인이 필요하다.
            throw new NonRetryableMessageException("Reservation " + message.reservationId() + " does not exist");
        }
        log.debug("Processed reservationId={} outcome={} partition={} offset={}",
                message.reservationId(), outcome, record.partition(), record.offset());
    }
}
