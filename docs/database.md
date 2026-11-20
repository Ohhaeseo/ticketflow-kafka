# 데이터베이스

> 요구사항 기준: [PRD §9 데이터 모델](../prd.md#9-데이터-모델). 이 문서는 **실제 마이그레이션 결과**를 기록합니다.

## 마이그레이션 이력

| 버전 | 파일 | 내용 | 주차 |
| --- | --- | --- | --- |
| V1 | `V1__create_events.sql` | `events` 테이블, CHECK 제약 3개 | 1주 ✅ |
| | | `users`, `reservations` | 2주 |
| | | `outbox_events`, `processed_messages` | 3~4주 |

- 마이그레이션 위치: `backend/src/main/resources/db/migration/`
- JPA는 `ddl-auto: validate`로 엔티티와 스키마가 맞는지만 확인한다. 스키마 변경은 Flyway로만 한다.
- 이미 적용된 마이그레이션 파일은 수정하지 않고 새 버전을 추가한다.

## 테이블 정의

### `events`

| 컬럼 | 타입 | 제약 |
| --- | --- | --- |
| `id` | `BIGINT` | PK, `GENERATED ALWAYS AS IDENTITY` |
| `title` | `VARCHAR(200)` | NOT NULL |
| `total_tickets` | `INTEGER` | NOT NULL |
| `remaining_tickets` | `INTEGER` | NOT NULL |
| `sales_start_at` | `TIMESTAMPTZ` | NOT NULL |
| `sales_end_at` | `TIMESTAMPTZ` | NOT NULL |

| 제약 이름 | 조건 | 목적 |
| --- | --- | --- |
| `ck_events_total_tickets` | `total_tickets >= 0` | 음수 총 수량 방지 |
| `ck_events_remaining_tickets` | `remaining_tickets BETWEEN 0 AND total_tickets` | **초과 판매의 마지막 방어선** (FR-INV-02) |
| `ck_events_sales_period` | `sales_start_at < sales_end_at` | 잘못된 판매 기간 방지 |

검증 테스트: `backend/src/test/java/com/ticketflow/event/EventTableConstraintTest.java`
애플리케이션 검증을 우회해 SQL을 직접 실행해도 위 제약이 잘못된 값을 거부하는지 확인한다. 재고 0에서 `remaining_tickets - 1`을 실행하는 경우도 포함한다.

## 재고 감소 쿼리

<!-- 4주차: 실제 사용하는 조건부 UPDATE, 영향 행 수에 따른 분기, 실행 계획(EXPLAIN) -->

## 상태 전이

<!-- 2주차: PRD §9.3 기준. 상태 변경 SQL의 WHERE status = 'QUEUED' 조건 -->

## 불변식 검증 쿼리

<!-- 5주차: PRD §12.3 SQL과 실행 결과 예시 -->

## 직접 접속

```powershell
docker exec -it ticketflow-postgres psql -U ticketflow -d ticketflow
```
