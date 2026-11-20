package com.ticketflow.messaging.producer;

import com.ticketflow.messaging.dto.ReservationRequestedMessage;
import com.ticketflow.outbox.repository.OutboxEventRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;

/**
 * 예약 이벤트를 Kafka에 직접 보내지 않고 Outbox에 기록한다. 실제 발행은 {@code OutboxRelay}가 한다.
 * 반드시 예약 저장과 같은 트랜잭션에서 호출해야 한다 (PRD FR-OBX-01).
 */
@Component
public class ReservationEventOutbox {

    private final OutboxEventRepository outboxEventRepository;
    private final JsonMapper jsonMapper;

    public ReservationEventOutbox(OutboxEventRepository outboxEventRepository, JsonMapper jsonMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.jsonMapper = jsonMapper;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public ReservationRequestedMessage reservationRequested(long reservationId, long eventId, Instant occurredAt) {
        ReservationRequestedMessage message = ReservationRequestedMessage.create(reservationId, eventId, occurredAt);
        outboxEventRepository.append(
                message.messageId(),
                reservationId,
                ReservationRequestedMessage.EVENT_TYPE,
                message.key(),
                jsonMapper.writeValueAsString(message),
                occurredAt);
        return message;
    }
}
