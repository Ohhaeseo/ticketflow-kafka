-- 공연(행사) 테이블. PRD §9.2
-- remaining_tickets는 조건부 UPDATE로만 감소시킨다 (FR-INV-01). CHECK 제약은 마지막 방어선이다 (FR-INV-02).
CREATE TABLE events (
    id                BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    title             VARCHAR(200) NOT NULL,
    total_tickets     INTEGER      NOT NULL,
    remaining_tickets INTEGER      NOT NULL,
    sales_start_at    TIMESTAMPTZ  NOT NULL,
    sales_end_at      TIMESTAMPTZ  NOT NULL,

    CONSTRAINT ck_events_total_tickets     CHECK (total_tickets >= 0),
    CONSTRAINT ck_events_remaining_tickets CHECK (remaining_tickets BETWEEN 0 AND total_tickets),
    CONSTRAINT ck_events_sales_period      CHECK (sales_start_at < sales_end_at)
);
