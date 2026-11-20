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
 * 재시도·DLT 정책은 6주차에 추가한다 (현재는 Spring Kafka 기본 오류 처리).
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
        ReservationRequestedMessage message = jsonMapper.readValue(record.value(), ReservationRequestedMessage.class);
        ProcessingOutcome outcome = reservationProcessor.process(message);
        log.debug("Processed reservationId={} outcome={} partition={} offset={}",
                message.reservationId(), outcome, record.partition(), record.offset());
    }
}
