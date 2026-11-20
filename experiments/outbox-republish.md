# 실험: Outbox 재발행 (MSG-03)

> PRD §12.1 MSG-03 · 6주차 · DoD-3 · **결과: 통과** (2026-11-20)

## 목적

1. Relay가 Kafka 발행에 성공한 뒤 `published_at`을 갱신하기 전에 죽으면 같은 메시지가 다시 발행된다. 이때도 업무 결과가 한 번만 적용되는지 확인한다.
2. Kafka 브로커가 멈춘 동안에도 예약 접수가 되고, 브로커가 돌아오면 쌓인 이벤트가 모두 발행·처리되는지 확인한다.

## 조건

| 항목 | 값 |
| --- | --- |
| PC | Intel Core Ultra 7 258V (8C/8T), RAM 32GB, Windows 11 + WSL2, Docker Desktop 8 CPUs |
| 이미지 | `apache/kafka:4.3.2`, `postgres:18.7-alpine` |
| Relay | 폴링 100ms, 배치 200건, ack 대기 5초 |

## 실험 1 — 발행 후 `published_at` 갱신 전 장애

### 1-a. 자동 테스트로 재현 (`outbox/OutboxRepublishIntegrationTest`)

1. 예약을 접수하고 확정될 때까지 기다린다 (잔여 9/10).
2. 그 예약의 Outbox 행을 `published_at = NULL`로 되돌린다. "Kafka에는 보냈지만 표시를 못 한" 상태다.
3. Relay가 같은 `message_id`로 다시 발행한다.
4. 같은 파티션에 sentinel 예약을 하나 더 넣고, 그것이 처리될 때까지 기다린다.
5. 토픽을 처음부터 읽어 같은 `message_id`가 몇 번 실렸는지 센다.

| 확인 | 결과 |
| --- | --- |
| Kafka에 같은 `message_id` | **2번** |
| 잔여 재고 | 8 (target 1회 + sentinel 1회) |
| target 예약의 처리 이력 | 1건 |

### 1-b. 실제 프로세스 강제 종료에서 발생 ([consumer-crash.md](consumer-crash.md) 시나리오 A)

LOAD-01 처리 도중 JVM을 `taskkill /F`로 죽였을 때 Outbox 미발행이 181건 남아 있었다. 그중 일부는 이미 Kafka에 보낸 상태였고, 재시작 후 재발행됐다.

| 확인 | 결과 |
| --- | --- |
| 재시작 후 `Duplicate message skipped` 로그 | 102건 |
| 두 번 반영된 예약 | 0 |
| CONFIRMED | 정확히 100 |

## 실험 2 — Kafka 브로커 중단

### 절차

1. 앱을 띄우고 20장짜리 공연을 등록한다.
2. `docker stop ticketflow-kafka`로 브로커를 멈춘다.
3. 사용자 30명이 예약한다.
4. 12초 기다린 뒤 상태를 확인한다.
5. `docker start ticketflow-kafka`로 브로커를 다시 띄우고, 모든 예약이 처리될 때까지 기다린다.

### 결과

| 확인 | 결과 |
| --- | --- |
| 브로커가 멈춘 동안의 예약 요청 | **30건 모두 202** (합계 1.1초). 접수는 DB만 쓰므로 Kafka 장애의 영향을 받지 않음 |
| 브로커가 멈춘 동안의 상태 | 예약 30건 `QUEUED`, Outbox 미발행 30건 |
| 브로커 다운 시간 | 14초 |
| 브로커 재시작 후 처리 완료까지 | 4.5초 |
| 최종 상태 | CONFIRMED 20, SOLD_OUT 10, 잔여 0, 처리 이력 30, 미발행 0 |
| Kafka에 실린 메시지 | 30건 (중복 없음) |

브로커가 멈춘 동안 Relay는 실패 로그를 남기지 않았다. Kafka Producer가 메타데이터를 다시 얻을 때까지 `send()` 안에서 기다렸고(`max.block.ms` 기본 60초), 14초 만에 브로커가 돌아와 그대로 전송이 끝났기 때문이다.

## 한계와 남은 위험

- 브로커가 `max.block.ms`(60초)보다 오래 멈추면 `send()`가 예외를 던지고, Relay는 다음 실행(100ms 뒤)에서 같은 행을 다시 보낸다. 그러면 Kafka에 같은 메시지가 여러 번 실릴 수 있다. Consumer가 걸러내므로 결과는 맞지만, 실패가 이어질 때 Relay가 발행 간격을 늘리는 처리(backoff)는 아직 없다.
- Relay가 멈춘 동안 Outbox 미발행 건수와 가장 오래 기다린 시간(M7)을 바로 볼 수 있는 지표는 7주차에 추가한다.

## 결론

같은 메시지가 재발행돼도(자동 재현, 실제 장애 모두) 재고는 한 번만 줄었다. Kafka 브로커가 멈춰도 예약 접수는 계속 됐고, 브로커가 돌아오자 쌓인 이벤트가 유실 없이 처리됐다. Transactional Outbox가 "DB 커밋 후 Kafka 발행 실패" 문제를 막는다는 것을 확인했다 (DoD-3).
