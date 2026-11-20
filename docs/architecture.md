# 아키텍처

> 요구사항 기준: [PRD §8 시스템 아키텍처](../prd.md#8-시스템-아키텍처). 이 문서는 **구현된 결과**를 주차별로 기록합니다.
> 설계 결정의 이유는 [ADR-0001](adr/0001-why-kafka.md)에 있습니다.

## 현재 구현 상태 (4주차 완료)

```mermaid
flowchart LR
  Client[Client / Swagger UI] -->|POST /reservations| API[Reservation API]
  API -->|TX: reservations QUEUED + outbox_events| PG[(PostgreSQL 18.7)]
  RELAY[Outbox Relay<br/>100ms 폴링] -->|미발행 조회 · published_at 갱신| PG
  RELAY -->|key=eventId| K[(Kafka 4.3.2<br/>reservation.requested.v1)]
  K --> C[Consumer x3<br/>ReservationProcessor]
  C -->|TX: 행 잠금 → 조건부 재고 감소 → 상태 전이| PG
  Client -->|GET /reservations/id| API
```

| 구성 요소 | 상태 | 비고 |
| --- | --- | --- |
| 공연 등록·조회 API | ✅ | 등록은 `local` 프로필 전용 |
| PostgreSQL + Flyway | ✅ | `V1__create_events.sql`, CHECK 제약 3개 |
| Kafka KRaft 단일 노드 | ✅ | 자동 토픽 생성 꺼짐, 토픽은 애플리케이션이 생성 |
| 예약 접수·조회 API | ✅ | `202` + `QUEUED`, 멱등성 키, `ON CONFLICT DO NOTHING`으로 동시 요청 판정 |
| 테스트용 사용자 식별 | ✅ | `X-Test-User-Id` 헤더, `local` 전용 (PRD Q4) |
| Transactional Outbox | ✅ 3주차 | 예약과 같은 트랜잭션에 기록, `message_id`는 저장 시 생성 |
| Outbox Relay | ✅ 3주차 | 100ms 폴링, 배치 200건, ack 후 `published_at` 갱신 |
| Kafka Consumer | ✅ 3주차 | 동시성 3, 레코드 단위 오프셋 커밋, group `ticketflow-reservation` |
| 원자적 재고 감소 | ✅ 3주차 | 예약 행 잠금 + 조건부 UPDATE. 중복 메시지는 상태 확인으로 건너뜀 |
| `processed_messages` 중복 이력 | ✅ 4주차 | 같은 `message_id`는 건너뜀, 처리 결과(`outcome`) 기록 (MSG-01) |
| 동시성·불변식 검증 | ✅ 4주차 | CON-02, 롤백(FR-INV-03), 100장/150명 동시 접수 → 정확히 100건 확정 |
| 재시도 · DLT | ⏳ 6주차 | 현재는 Spring Kafka 기본 오류 처리 |
| Redis 빈도 제한 | ⏳ 7주차 | |

## 목표 구성도

```mermaid
flowchart TD
  User[합성 사용자 / k6] --> API[Spring Boot Reservation API]
  API --> PG[(PostgreSQL)]
  API -. 요청 빈도 제한 .-> REDIS[(Redis)]
  PG -->|예약 QUEUED + Outbox 같은 트랜잭션| OUTBOX[(outbox_events)]
  OUTBOX --> RELAY[Outbox Relay / Poller]
  RELAY --> KAFKA[(Kafka: reservation.requested.v1)]
  KAFKA --> CONSUMER[Reservation Consumer]
  CONSUMER -->|중복 검사 + 조건부 재고 감소| PG
  CONSUMER -->|일시 오류 재시도 / 영구 오류| DLT[(reservation.requested.v1-dlt)]
  User -->|예약 상태 조회| API
```

## 컴포넌트와 프로세스 배치

- 현재는 **단일 Spring Boot 프로세스**에 모든 컴포넌트가 들어 있다.
- 3주차 이후 필요하면 `api`(접수·조회) / `worker`(Relay·Consumer) 프로필로 같은 코드를 별도 프로세스로 띄운다.

## 로컬 실행 환경

| 서비스 | 이미지 | 호스트 주소 | 비고 |
| --- | --- | --- | --- |
| PostgreSQL | `postgres:18.7-alpine` | `127.0.0.1:15432` | 로컬에 설치된 PostgreSQL(5432)과 충돌을 피하려고 15432 사용 |
| Kafka | `apache/kafka:4.3.2` | `127.0.0.1:9092` | broker + controller 겸용, `auto.create.topics.enable=false` |
| Spring Boot | — | `localhost:8080` | 기본 프로필 `local` |

Testcontainers도 같은 이미지 태그를 쓴다 (`backend/src/test/java/com/ticketflow/support/TestcontainersConfiguration.java`).

## 예약 처리 흐름 (3주차 ✅)

```mermaid
sequenceDiagram
  autonumber
  participant C as Client
  participant API as ReservationService
  participant DB as PostgreSQL
  participant R as OutboxRelay
  participant K as Kafka
  participant W as Listener → ReservationProcessor

  C->>API: POST /reservations
  rect rgba(100,150,255,0.12)
  note over API,DB: 트랜잭션 A
  API->>DB: INSERT reservations ... ON CONFLICT DO NOTHING
  API->>DB: INSERT outbox_events (message_id 생성)
  end
  API-->>C: 202 QUEUED
  R->>DB: SELECT 미발행 ORDER BY id LIMIT 200
  R->>K: send (key=eventId) × n
  K-->>R: ack
  R->>DB: UPDATE published_at (ack 받은 행만)
  K->>W: record
  rect rgba(100,200,120,0.12)
  note over W,DB: 트랜잭션 B
  W->>DB: SELECT reservation FOR UPDATE
  W->>DB: processed_messages에 message_id 있으면 종료
  W->>DB: UPDATE events 조건부 감소
  W->>DB: UPDATE reservations 상태 전이 (WHERE status='QUEUED')
  W->>DB: INSERT processed_messages (outcome)
  end
  W->>K: offset commit (트랜잭션 B 커밋 후)
```

| 장애 지점 | 결과 |
| --- | --- |
| 트랜잭션 A 실패 | 예약도 Outbox도 남지 않음. 클라이언트는 오류를 받고 다시 시도 |
| Relay가 발행 후 `published_at` 갱신 전 중단 | 다음 실행에서 같은 `message_id`로 재발행 → Consumer가 `processed_messages`를 보고 건너뜀 |
| 트랜잭션 B 실패 | 재고·상태·처리 이력 모두 롤백, 오프셋 미커밋 → 메시지 재수신 후 다시 처리 |
| 트랜잭션 B 커밋 후 오프셋 커밋 전 중단 | 재시작 후 같은 메시지 재수신 → `processed_messages`를 보고 건너뜀 |

3주차 수동 검증: consumer group을 바꿔 메시지 4건을 처음부터 다시 읽게 했을 때, 재고와 확정 건수가 바뀌지 않았다.

## 장애 시나리오별 동작

| 장애 | 동작 | 근거 실험 |
| --- | --- | --- |
| Kafka 일시 중단 | | `experiments/outbox-republish.md` |
| Consumer 강제 종료 | | `experiments/consumer-crash.md` |
| 중복 메시지 | | `experiments/duplicate-event.md` |
| 영구 오류 메시지 | | MSG-04 테스트 |

## 설정값

| 항목 | 값 | 이유 |
| --- | --- | --- |
| 토픽 파티션 수 | 3 | PRD Q6 제안값. 행사 하나의 처리량은 key 특성상 늘지 않음 |
| 토픽 복제 계수 | 1 | 로컬 단일 브로커 |
| Producer `acks` / idempotence | `all` / `true` | 브로커 측 중복 위험 완화 |
| Consumer 자동 커밋 | 꺼짐 | 오프셋 커밋은 Spring 컨테이너가 처리 후 수행 |
| Listener ack-mode | `record` | 레코드 처리(DB 커밋)가 끝날 때마다 커밋 |
| Outbox 폴링 주기 / 배치 크기 | 100ms / 200건 | 로컬 기준 접수 → 발행 지연 약 200ms 이내. 5주차 부하 테스트로 조정 |
| Consumer 동시성 | 3 | 파티션 수와 같음 |
| 재시도 횟수 / 간격 | | 6주차 |
