-- Consumer가 처리한 메시지 이력. 같은 message_id는 한 번만 처리하고, 처리 결과를 감사용으로 남긴다. PRD §9.2, FR-CSM-01
CREATE TABLE processed_messages (
    message_id     UUID        PRIMARY KEY,
    reservation_id BIGINT      NOT NULL,
    outcome        VARCHAR(30) NOT NULL, -- CONFIRMED / SOLD_OUT / FAILED / SKIPPED_ALREADY_PROCESSED
    processed_at   TIMESTAMPTZ NOT NULL,

    CONSTRAINT fk_processed_messages_reservation FOREIGN KEY (reservation_id) REFERENCES reservations (id)
);

CREATE INDEX idx_processed_messages_reservation ON processed_messages (reservation_id);
