# 이벤트 계약

> 요구사항 기준: [PRD §10 이벤트 계약](../prd.md#10-이벤트-계약-kafka). 스키마가 바뀌면 이 문서를 먼저 고칩니다.

## 토픽

| 토픽 | 파티션 | 복제 | Key | 보존 기간 | 용도 |
| --- | --- | --- | --- | --- | --- |
| `reservation.requested.v1` | 3 | 1 | `eventId` | 브로커 기본값 (7일) | 예약 접수 이벤트 |
| `reservation.requested.v1-dlt` | 3 | 1 | 원본과 같음 | 브로커 기본값 (7일) | 영구 오류 격리 |

- 토픽은 애플리케이션 기동 시 `KafkaTopicConfig`의 `NewTopic` 빈으로 생성된다. 이미 있으면 그대로 둔다.
- 브로커는 `auto.create.topics.enable=false`다. 정의하지 않은 토픽으로 보내면 `UNKNOWN_TOPIC_OR_PARTITION` 오류가 난다.
- 토픽 이름과 파티션 수는 `application.yml`의 `ticketflow.kafka.*`에서 바꾼다.

## 메시지 스키마 — `reservation.requested.v1` (3주차 ✅)

구현: `messaging/dto/ReservationRequestedMessage`. Outbox `payload`에 JSON 문자열로 저장되고, Relay가 그대로 value로 보낸다.

```json
{"messageId":"e22c669f-4f64-4c45-8552-4e8061c2e555","schemaVersion":1,"reservationId":3,"eventId":3,"occurredAt":"2026-11-20T13:23:38.280278Z"}
```

| 필드 | 타입 | 필수 | 설명 |
| --- | --- | --- | --- |
| `messageId` | UUID | Y | Outbox 저장 시 생성, 재발행해도 같은 값 |
| `schemaVersion` | int | Y | `1` |
| `reservationId` | long | Y | |
| `eventId` | long | Y | 메시지 key와 같음 |
| `occurredAt` | ISO-8601 UTC | Y | 접수 시각 |

## 재시도와 DLT (6주차 ✅)

구현: `messaging/config/KafkaErrorHandlingConfig`

| 오류 | 예 | 처리 |
| --- | --- | --- |
| 일시 오류 | DB 연결 끊김, 잠금 타임아웃, 예상하지 못한 예외 | 같은 파티션 안에서 **200ms → 400ms → 800ms** 간격으로 3번 재시도. 그래도 실패하면 DLT |
| 영구 오류 | JSON 파싱 실패(`JacksonException`), `schemaVersion` 불일치, 필수 값 누락, 대상 예약 없음 (`NonRetryableMessageException`) | 재시도 없이 바로 DLT |
| 업무 결과 | `SOLD_OUT`, `FAILED(SALES_CLOSED)` | 오류가 아니다. 정상 처리하고 오프셋 커밋 |

- 재시도는 **blocking** 방식이다. 재시도하는 동안 같은 파티션의 뒤 메시지는 기다리므로 파티션 안의 순서가 유지된다. 대가로 영구 오류가 아닌 독성 메시지 하나가 파티션을 최대 약 1.4초 붙잡는다. `@RetryableTopic`(non-blocking)은 순서를 보존하지 않아 쓰지 않았다 (PRD §10.3).
- DLT 파티션은 원본과 같은 번호다 (`reservation.requested.v1-dlt`도 파티션 3개).

### DLT 헤더와 감사 이력

`DeadLetterPublishingRecoverer`가 원본 레코드에 아래 헤더를 붙여 DLT로 보낸다. `DeadLetterAuditListener`(group `ticketflow-dlt-audit`)가 이를 읽어 `dead_letter_messages` 테이블에 기록한다.

| 헤더 | 기록 컬럼 |
| --- | --- |
| `kafka_dlt-original-topic` / `-partition` / `-offset` | `original_topic`, `original_partition`, `original_offset` (UNIQUE) |
| `kafka_dlt-exception-fqcn` | `exception_class` (보통 `ListenerExecutionFailedException`) |
| `kafka_dlt-exception-stacktrace`의 가장 깊은 `Caused by:` | `exception_cause_class`, `exception_message` (근본 원인, 예: `tools.jackson...`, `NonRetryableMessageException`, `PSQLException`) |
| `kafka_dlt-exception-cause-fqcn` / `-message` | 스택 트레이스가 없을 때의 대체값 |

배치 리스너에서는 예외가 `ListenerExecutionFailedException → BatchListenerFailedException → 실제 원인`으로 감싸진다. `-cause-fqcn` 헤더에는 바로 아래 한 단계(`BatchListenerFailedException`)만 담긴다. 그래서 스택 트레이스에서 근본 원인을 찾아 기록한다.

감사 리스너는 전용 컨테이너 팩토리를 쓴다. 기록에 실패하면 재시도 후 로그만 남기고 DLT로 다시 보내지 않는다 (무한 루프 방지).

```sql
-- 운영자 확인용
SELECT received_at, original_partition, original_offset, exception_cause_class, exception_message, payload
  FROM dead_letter_messages ORDER BY received_at DESC LIMIT 20;
```

### 배치 리스너 (기본값, 11-21)

`ticketflow.consumer.batch-enabled=true`이면 `ReservationRequestedBatchListener`가 poll 묶음(최대 `max.poll.records`=500)을 **하나의 DB 트랜잭션**으로 처리하고, 오프셋은 배치마다 한 번 커밋한다 (`AckMode.BATCH`).

| 상황 | 처리 |
| --- | --- |
| 배치 안에 파싱·검증 실패 레코드 | 그 앞까지 처리·커밋 → `BatchListenerFailedException(index)` → 오류 처리기가 앞쪽 오프셋 커밋, 실패 레코드는 (재시도 없이) DLT, 나머지는 다시 전달 |
| 배치 트랜잭션 실패 (일시 오류, 특정 레코드의 DB 오류) | 한 건씩 각자의 트랜잭션으로 다시 처리 → 실패한 레코드만 `BatchListenerFailedException`으로 알림 → 그 레코드만 백오프 재시도 후 DLT |
| 없는 예약 | 배치 커밋 후 해당 index를 DLT로. 뒤 레코드는 다시 전달돼도 `processed_messages`로 건너뜀 |
| 같은 배치에 같은 `message_id` 두 번 | 앞 처리 이력이 같은 트랜잭션 안에서 보이므로 한 번만 적용 |

`false`로 두면 레코드 리스너(`ReservationRequestedListener`, 3~7주차 방식)로 돌아간다. 처리량 비교는 [benchmark-report.md](benchmark-report.md#consumer-처리량-개선-배치-리스너-2026-11-2021) 참고.

### Consumer 그룹 멤버십

| 설정 | 값 | 이유 |
| --- | --- | --- |
| `group.instance.id` | `${KAFKA_GROUP_INSTANCE_ID:ticketflow-local-1}` (+ 컨테이너가 `-0/-1/-2` 붙임) | 정적 멤버십. 강제 종료 후 재시작해도 리밸런스 없이 파티션을 바로 돌려받는다. 없으면 `session.timeout.ms`(45초) 동안 처리가 멈춘다 ([experiments/consumer-crash.md](../experiments/consumer-crash.md)) |

## Producer / Consumer 설정

| 구분 | 설정 | 값 |
| --- | --- | --- |
| Producer | `acks` | `all` |
| Producer | `enable.idempotence` | `true` |
| Producer | key / value 직렬화 | `String` / `String` (value는 JSON 문자열) |
| Consumer | `group.id` | `ticketflow-reservation` |
| Consumer | `auto.offset.reset` | `earliest` |
| Consumer | `enable.auto.commit` | `false` |
| Listener | 종류 / `ack-mode` | 배치 리스너 / `batch` — 배치 처리(DB 커밋)가 끝난 뒤 오프셋 한 번 커밋. 레코드 모드(`batch-enabled=false`)는 `record` |
| Listener | `concurrency` | `3` (파티션 수와 같음) |

- 애플리케이션의 kafka-clients는 4.2.2, 브로커는 4.3.2다.
- `@KafkaListener`에 `id`를 주면 Spring Kafka는 기본적으로 그 id를 group id로 쓴다. 그래서 `groupId`를 명시했다 (3주차에 실제로 겪은 문제).
- 일시 오류와 영구 오류의 분류, 재시도, DLT 이동은 아래 [재시도와 DLT](#재시도와-dlt-6주차-) 참고.

## 발행·소비 보장

| 단계 | 보장 | 구현 |
| --- | --- | --- |
| 예약 → Outbox | 예약이 커밋되면 이벤트도 반드시 남는다 | 같은 트랜잭션 (`ReservationEventOutbox`, `Propagation.MANDATORY`) |
| Outbox → Kafka | 최소 한 번 발행 | 브로커 ack 후에만 `published_at` 갱신. 갱신 전 장애 시 같은 `messageId`로 재발행 |
| Kafka → DB | 업무 결과는 한 번만 적용 | 예약 행 `FOR UPDATE` 잠금 → `processed_messages`에 같은 `messageId`가 있으면 건너뜀 → `QUEUED`일 때만 조건부 재고 감소 → 처리 이력 기록. 커밋 후 오프셋 커밋 |

## 스키마 변경 규칙

- 필드 추가처럼 호환되는 변경은 같은 토픽에서 `schemaVersion`을 올린다.
- 호환되지 않는 변경은 새 토픽(`reservation.requested.v2`)으로 분리한다.

## CLI로 확인

```powershell
# 토픽 목록과 설정
docker exec ticketflow-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic reservation.requested.v1

# 메시지 확인 (Kafka 4.x는 --property 대신 --formatter-property 사용)
docker exec ticketflow-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic reservation.requested.v1 --from-beginning --formatter-property print.key=true
```
