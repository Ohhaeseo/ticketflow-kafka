package com.ticketflow.messaging.consumer;

import com.ticketflow.messaging.dto.ReservationRequestedMessage;
import com.ticketflow.reservation.service.ProcessingOutcome;
import com.ticketflow.reservation.service.ReservationProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * {@code reservation.requested.v1} Consumer.
 * <p>
 * 오프셋은 이 메서드가 정상 반환한 뒤(=DB 트랜잭션 커밋 후) 레코드 단위로 커밋된다
 * ({@code spring.kafka.listener.ack-mode: record}). 커밋 전에 죽으면 같은 메시지를 다시 받고,
 * {@link ReservationProcessor}가 멱등하게 처리한다.
 * <p>
 * 오류 처리 ({@code KafkaErrorHandlingConfig}): 일시 오류는 같은 파티션 안에서 지수 백오프로 재시도하고,
 * {@link NonRetryableMessageException}·JSON 파싱 오류는 재시도 없이 DLT로 보낸다.
 */
@Component
public class ReservationRequestedListener {

    /** 리스너 컨테이너 id. 장애 실험에서 컨테이너를 멈추거나 다시 시작할 때 쓴다. */
    public static final String LISTENER_ID = "reservation-requested";

    private static final Logger log = LoggerFactory.getLogger(ReservationRequestedListener.class);

    private final ReservationProcessor reservationProcessor;
    private final JsonMapper jsonMapper;

    public ReservationRequestedListener(ReservationProcessor reservationProcessor, JsonMapper jsonMapper) {
        this.reservationProcessor = reservationProcessor;
        this.jsonMapper = jsonMapper;
    }

    // id를 지정하면 Spring Kafka는 기본적으로 id를 group.id로 쓴다. groupId를 명시해 설정값을 따르게 한다.
    @KafkaListener(id = LISTENER_ID,
            topics = "${ticketflow.kafka.reservation-requested-topic}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(ConsumerRecord<String, String> record) {
        ReservationRequestedMessage message = parse(record.value());
        ProcessingOutcome outcome = reservationProcessor.process(message);
        if (outcome == ProcessingOutcome.SKIPPED_NOT_FOUND) {
            // Outbox는 예약과 같은 트랜잭션에서 기록되므로 정상이라면 일어나지 않는다. 운영자 확인이 필요하다.
            throw new NonRetryableMessageException("Reservation " + message.reservationId() + " does not exist");
        }
        log.debug("Processed reservationId={} outcome={} partition={} offset={}",
                message.reservationId(), outcome, record.partition(), record.offset());
    }

    private ReservationRequestedMessage parse(String payload) {
        // JSON 형식 오류(JacksonException)는 그대로 던진다. 오류 처리기가 재시도 없이 DLT로 보낸다.
        ReservationRequestedMessage message = jsonMapper.readValue(payload, ReservationRequestedMessage.class);
        if (message.schemaVersion() != ReservationRequestedMessage.SCHEMA_VERSION) {
            throw new NonRetryableMessageException("Unsupported schemaVersion " + message.schemaVersion());
        }
        if (message.messageId() == null || message.reservationId() <= 0 || message.eventId() <= 0) {
            throw new NonRetryableMessageException("Missing required fields: " + payload);
        }
        return message;
    }
}
