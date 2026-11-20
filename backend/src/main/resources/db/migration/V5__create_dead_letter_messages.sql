-- DLT로 이동한 메시지의 감사 이력 (PRD FR-CSM-03, MSG-04).
-- DLT 토픽에도 원본이 남지만, 운영자가 SQL·API로 원인을 바로 볼 수 있도록 DB에 한 번 더 기록한다.
CREATE TABLE dead_letter_messages (
    id                    BIGINT        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    original_topic        VARCHAR(200)  NOT NULL,
    original_partition    INTEGER       NOT NULL,
    original_offset       BIGINT        NOT NULL,
    message_key           VARCHAR(200),
    payload               TEXT,
    exception_class       VARCHAR(300),
    exception_cause_class VARCHAR(300),
    exception_message     TEXT,
    dlt_partition         INTEGER       NOT NULL,
    dlt_offset            BIGINT        NOT NULL,
    received_at           TIMESTAMPTZ   NOT NULL,

    -- 같은 원본 레코드는 한 번만 기록한다 (DLT 감사 리스너가 재전달을 받아도 중복 없음)
    CONSTRAINT uk_dead_letter_messages_origin UNIQUE (original_topic, original_partition, original_offset)
);

CREATE INDEX idx_dead_letter_messages_received_at ON dead_letter_messages (received_at);
