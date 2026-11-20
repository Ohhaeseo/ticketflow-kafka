package com.ticketflow.messaging.consumer;

import com.ticketflow.messaging.dto.ReservationRequestedMessage;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * {@code reservation.requested.v1} 메시지 파싱과 검증. 레코드·배치 리스너가 같이 쓴다.
 * JSON 형식 오류는 {@code JacksonException}, 계약 위반은 {@link NonRetryableMessageException}을 던진다.
 * 둘 다 재시도 없이 DLT로 간다.
 */
@Component
public class ReservationMessageParser {

    private final JsonMapper jsonMapper;

    public ReservationMessageParser(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    public ReservationRequestedMessage parse(String payload) {
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
