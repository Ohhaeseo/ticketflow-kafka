# 실험: 중복 Kafka 메시지 (MSG-01)

> PRD §12.1 MSG-01 · 4주차 · **결과: 통과** (2026-11-20)

## 목적

같은 `messageId`를 가진 메시지를 여러 번 소비해도 재고가 한 번만 줄어드는지 확인한다.
Outbox Relay가 발행 후 `published_at`을 갱신하기 전에 죽거나(MSG-03), Consumer가 DB 커밋 후 오프셋 커밋 전에 죽으면 같은 메시지가 다시 온다. 이 실험은 그 상황을 Kafka CLI로 직접 재현한다.

## 조건

| 항목 | 값 |
| --- | --- |
| PC | Intel Core Ultra 7 258V (8C/8T), RAM 32GB, Windows 11 + WSL2 |
| Docker 리소스 | 8 CPUs, 약 15.4GiB |
| 이미지 | `apache/kafka:4.3.2`, `postgres:18.7-alpine` |
| 애플리케이션 | Spring Boot 4.1.3, `local` 프로필, Consumer 동시성 3, ack-mode `record` |
| 코드 | 4주차 커밋 (`feat(idempotency): processed_messages로 중복 소비 방지 …`) |
| 행사 총 수량 / 시작 잔여 | 5 / 5 |
| 중복 주입 방법 | 처리가 끝난 예약의 Outbox `payload`를 `kafka-console-producer`로 같은 key와 함께 2번 더 전송 |

## 절차

1. 앱을 띄우고 5장짜리 공연(id 4)을 등록한다.
2. 사용자 701로 예약을 접수한다 → 예약 7이 `CONFIRMED`, 잔여 4가 된다.
3. `outbox_events`에서 예약 7의 `message_key`와 `payload`를 읽는다 (`message_id = 2003d67c-…`).
4. 같은 key·payload를 Kafka에 2번 더 보낸다.

```bash
printf '%s\t%s\n%s\t%s\n' "$KEY" "$PAYLOAD" "$KEY" "$PAYLOAD" | docker exec -i ticketflow-kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic reservation.requested.v1 --reader-property parse.key=true --reader-property "key.separator=<TAB>"
```

5. 애플리케이션 로그, 재고, 처리 이력, consumer lag를 확인한다.

## 기대 결과

- 재고는 4에서 더 줄지 않는다.
- `processed_messages`에는 해당 `message_id`가 1건만 있다.
- 두 메시지 모두 오류 없이 건너뛰고 오프셋이 커밋된다 (lag 0).

## 실측 결과

| 지표 | 값 |
| --- | --- |
| 중복 전송 후 잔여 재고 | **4** (변화 없음) |
| 예약 7 상태 | `CONFIRMED` (변화 없음) |
| `processed_messages` (예약 7) | 1건, `outcome = CONFIRMED` |
| 애플리케이션 로그 | `Duplicate message skipped: messageId=2003d67c-… reservationId=7` 2회 (전송 후 약 10ms 간격) |
| consumer lag | 모든 파티션 0 |

## 결론

같은 `messageId`가 두 번 더 왔지만 재고와 상태는 바뀌지 않았고, 처리 이력도 늘지 않았다. Consumer는 두 메시지를 오류 없이 건너뛰고 오프셋을 커밋했다.

방어는 두 겹이다.

1. `processed_messages`에 같은 `message_id`가 있으면 건너뛴다 (이번 실험에서 동작한 경로).
2. `message_id`가 달라도 예약이 이미 최종 상태면 재고를 건드리지 않는다 (`SKIPPED_ALREADY_PROCESSED`).

두 검사 모두 예약 행을 `FOR UPDATE`로 잠근 뒤에 하므로, 같은 메시지가 동시에 처리되어도 결과가 같다.

## 자동 테스트

| 테스트 | 내용 |
| --- | --- |
| `messaging/DuplicateMessageIntegrationTest` | 실제 Kafka로 같은 메시지 2회 전송 → 재고 1회 감소, 처리 이력 1건 |
| `reservation/ReservationProcessorTest` | 같은 메시지 2회 직접 처리 → 두 번째 `SKIPPED_DUPLICATE_MESSAGE` |
| `reservation/ReservationConcurrencyTest` | 같은 메시지 8개 동시 처리 → 1번만 처리 |
