-- Transactional Outbox. 예약 저장과 같은 트랜잭션에서 기록하고 Relay가 Kafka로 발행한다. PRD §6.3
CREATE TABLE outbox_events (
    id             BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- 저장 시 생성하고 재발행해도 같은 값을 쓴다. Consumer 중복 판별 키 (PRD 부록 A2)
    message_id     UUID         NOT NULL,
    reservation_id BIGINT       NOT NULL,
    event_type     VARCHAR(100) NOT NULL,
    -- Kafka 메시지 key (예약 접수 이벤트는 eventId)
    message_key    VARCHAR(100) NOT NULL,
    payload        TEXT         NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL,
    published_at   TIMESTAMPTZ,

    CONSTRAINT uk_outbox_events_message_id UNIQUE (message_id),
    CONSTRAINT fk_outbox_events_reservation FOREIGN KEY (reservation_id) REFERENCES reservations (id)
);

-- Relay는 미발행 행만 id 순으로 읽는다. 발행된 행은 인덱스에서 빠지므로 테이블이 커져도 조회 비용이 늘지 않는다.
CREATE INDEX idx_outbox_events_unpublished ON outbox_events (id) WHERE published_at IS NULL;
