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

## DLT 헤더

<!-- 6주차: DLT로 보낼 때 남기는 원본 토픽·파티션·오프셋·예외 정보 -->

## Producer / Consumer 설정

| 구분 | 설정 | 값 |
| --- | --- | --- |
| Producer | `acks` | `all` |
| Producer | `enable.idempotence` | `true` |
| Producer | key / value 직렬화 | `String` / `String` (value는 JSON 문자열) |
| Consumer | `group.id` | `ticketflow-reservation` |
| Consumer | `auto.offset.reset` | `earliest` |
| Consumer | `enable.auto.commit` | `false` |
| Listener | `ack-mode` | `record` — 레코드 처리(DB 커밋)가 끝난 뒤 오프셋 커밋 |
| Listener | `concurrency` | `3` (파티션 수와 같음) |

- 애플리케이션의 kafka-clients는 4.2.2, 브로커는 4.3.2다.
- `@KafkaListener`에 `id`를 주면 Spring Kafka는 기본적으로 그 id를 group id로 쓴다. 그래서 `groupId`를 명시했다 (3주차에 실제로 겪은 문제).
- 일시 오류와 영구 오류의 분류, 재시도 횟수, DLT 이동은 6주차에 정한다. 현재는 Spring Kafka 기본 오류 처리(재시도 후 건너뜀)다.

## 발행·소비 보장

| 단계 | 보장 | 구현 |
| --- | --- | --- |
| 예약 → Outbox | 예약이 커밋되면 이벤트도 반드시 남는다 | 같은 트랜잭션 (`ReservationEventOutbox`, `Propagation.MANDATORY`) |
| Outbox → Kafka | 최소 한 번 발행 | 브로커 ack 후에만 `published_at` 갱신. 갱신 전 장애 시 같은 `messageId`로 재발행 |
| Kafka → DB | 업무 결과는 한 번만 적용 | 예약 행 `FOR UPDATE` 잠금 → `QUEUED`일 때만 처리 → 조건부 재고 감소. 커밋 후 오프셋 커밋 |

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
