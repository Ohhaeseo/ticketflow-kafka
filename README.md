<div align="center">

# 🎫 TicketFlow Kafka

**대규모 트래픽 티켓 예약 서버**<br>
Kafka 이벤트 기반 비동기 예약 · 동시성 제어 · 장애 대응 · 성능 분석

<sub>A Kafka-powered ticket reservation backend with transactional outbox, idempotent consumers, and concurrency testing.</sub>

<br>

![Java](https://img.shields.io/badge/Java_21-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)
![Spring Boot](https://img.shields.io/badge/Spring_Boot_4.1-6DB33F?style=for-the-badge&logo=springboot&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-FF4438?style=for-the-badge&logo=redis&logoColor=white)

[![CI](https://github.com/Ohhaeseo/ticketflow-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/Ohhaeseo/ticketflow-kafka/actions/workflows/ci.yml)
![Status](https://img.shields.io/badge/status-8_weeks_done-brightgreen?style=flat-square)
![Tests](https://img.shields.io/badge/tests-62_passed-success?style=flat-square)
![Period](https://img.shields.io/badge/period-8_weeks-blue?style=flat-square)
![Cost](https://img.shields.io/badge/cost-%E2%82%A90-success?style=flat-square)

[📋 PRD](prd.md) · [🏗️ 아키텍처](docs/architecture.md) · [🗄️ DB 설계](docs/database.md) · [📨 이벤트 계약](docs/event-contract.md) · [📊 벤치마크](docs/benchmark-report.md) · [🧯 장애 실험](experiments/consumer-crash.md) · [📈 모니터링](docs/monitoring.md)

</div>

<br>

## 🏁 결과 한눈에 보기

| 검증 | 결과 |
| --- | --- |
| 티켓 100장에 고유 사용자 1,000명 동시 예약 (k6) | **CONFIRMED 정확히 100건, 초과 판매 0** — 동기·비동기 모두 |
| 같은 요청 10개 동시 전송 · 같은 Kafka 메시지 재전달 | 예약 1건, 재고 1회 감소 |
| 부하 중 프로세스 강제 종료 4회 · Kafka 브로커 14초 중단 | **유실 0, 중복 반영 0** |
| 같은 조건 동기 vs 비동기 (50~500 RPS) | 비동기는 접수 p95 55ms(500 RPS)로 버티지만, 공연 하나의 확정 처리량은 두 방식 모두 약 220건/s |
| 자동 테스트 | Testcontainers 통합 테스트 62개, GitHub Actions에서 실행 |

### ✅ 완료 기준 (PRD DoD)

| # | 기준 | 상태 | 근거 |
| --- | --- | :---: | --- |
| 1 | 100장에 1,000명 → 최종 CONFIRMED 정확히 100 | ✅ | [벤치마크 LOAD-01](docs/benchmark-report.md#결과--load-01-1000명--100장), `OversellPreventionIntegrationTest` |
| 2 | 중복 요청·중복 메시지·Consumer 재시작에서 초과 판매·중복 확정 0 | ✅ | [중복 메시지](experiments/duplicate-event.md), [강제 종료](experiments/consumer-crash.md), CON-01·MSG-01·MSG-02 테스트 |
| 3 | DB 커밋–Kafka 발행 사이 장애를 Outbox로 재현·복구 | ✅ | [Outbox 재발행·Kafka 중단](experiments/outbox-republish.md) |
| 4 | 부하 단계별 접수 p95·확정 p95·lag·오류율 실측 그래프 | ✅ | [벤치마크 그래프](docs/benchmark-report.md#그래프), [Grafana 대시보드](docs/monitoring.md) |
| 5 | Testcontainers 통합 테스트와 GitHub Actions 통과 | ✅ | [CI](.github/workflows/ci.yml) |
| 6 | README에 Kafka 도입 이유, 보장 범위, 장애 대응 | ✅ | [왜 Kafka인가](#-왜-kafka인가), [보장 범위](#보장하는-것과-보장하지-않는-것), [장애 대응](#-장애-대응) |
| 7 | 동기 vs Kafka 비동기를 같은 조건으로 비교 (Kafka가 항상 빠르다고 주장하지 않음) | ✅ | [벤치마크 분석](docs/benchmark-report.md#분석) |

<br>

## 📖 프로젝트 소개

인기 공연의 티켓이 소량 남은 상황에서 예약 요청이 한꺼번에 몰리는 문제를 다룹니다.
API는 요청을 **먼저 접수**하고, 확정은 Kafka를 통해 **비동기로 처리**합니다. 이 과정에서 중복 예약, 초과 판매, 메시지 중복 처리, Consumer 장애가 생겨도 결과가 정확한지 검증합니다.

<div align="center">

| 🎟️ 잔여 티켓 | 👥 동시 예약 시도 | 🎯 목표 |
| :---: | :---: | :---: |
| **100장** | **1,000명** | 초과 판매 **0** · 중복 확정 **0** |

</div>

### 해결할 문제

| 문제 | 원인 | 해결 수단 |
| --- | --- | --- |
| 초과 판매 | 동시 요청이 같은 재고를 동시에 읽고 감소 | DB 조건부 `UPDATE` + `CHECK` 제약 |
| 중복 예약 | 클라이언트 재시도, 같은 요청 반복 전송 | `Idempotency-Key` + `UNIQUE` 제약 |
| DB·메시지 불일치 | DB 커밋 후 Kafka 발행 전 장애 | Transactional Outbox |
| 중복 처리 | at-least-once 전달, Outbox 재발행 | `processed_messages` + 상태 조건부 갱신 |
| 처리 유실·정체 | Consumer 장애, 처리 불가 메시지 | 재시도 정책 + Dead Letter Topic |

<br>

## 💡 핵심 설계 원칙

> [!IMPORTANT]
> **Kafka를 도입했다고 초과 판매가 자동으로 막히지는 않습니다.**
> 정확성의 최종 근거는 PostgreSQL의 원자적 조건부 갱신과 트랜잭션입니다.

| 구성 요소 | 맡는 일 | 맡지 않는 일 |
| --- | --- | --- |
| **PostgreSQL** | 재고·예약 정확성의 최종 근거 (트랜잭션, 제약 조건) | — |
| **Kafka** | 대량 요청의 비동기 완충, 이벤트 전달, Consumer 확장·장애 복구 | 정확성 보장 |
| **Redis** | 사용자별 요청 빈도 제한 | 재고·예약의 원본 데이터 |

- 메시지 전달은 항상 **at-least-once**로 가정하고, 모든 Consumer 처리를 멱등하게 만듭니다.
- **`202 Accepted`는 좌석 확보가 아니라 접수 완료**를 뜻합니다.
- 엄격한 선착순(공정 대기열)은 범위 밖입니다. HTTP 도착 순서와 Kafka 처리 순서는 다를 수 있습니다.

### 🤔 왜 Kafka인가

요청이 몰리는 순간 **접수와 확정을 분리**하기 위해서입니다 ([ADR-0001](docs/adr/0001-why-kafka.md)).

- **접수 API를 처리 능력과 떼어 놓는다.** 재고 행 하나를 갱신하는 처리량에는 한계가 있습니다(실측 약 220건/s). 동기 방식에서는 그 이상의 요청이 HTTP 연결을 붙잡고 기다리다 밀립니다. 비동기 방식에서는 접수(INSERT 2건)만 하고 나머지는 Kafka가 받아 둡니다. 500 RPS에서 접수 p95는 동기 5,008ms, 비동기 55ms였습니다.
- **장애를 견딘다.** Kafka 브로커가 멈춰도 접수는 계속되고(Outbox), Consumer가 죽어도 메시지는 남아 재처리됩니다.
- **대신 감수하는 것:** 결과를 바로 알 수 없고(polling), 처리 대기열(lag)이 쌓이면 확정이 늦어집니다. 1,000명 스파이크에서는 동기 방식이 최종 결과를 더 빨리 냈습니다. 모든 처리는 멱등해야 하고, 운영할 구성 요소가 늘어납니다.

<br>

## 🏗️ 아키텍처

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

### 예약 처리 흐름

```mermaid
sequenceDiagram
  autonumber
  participant C as Client (k6)
  participant API as Reservation API
  participant DB as PostgreSQL
  participant R as Outbox Relay
  participant K as Kafka
  participant W as Consumer

  C->>API: POST /reservations (eventId, Idempotency-Key)
  API->>API: 검증 (판매 기간, 중복 정책, 빈도 제한)
  API->>DB: TX { INSERT reservations(QUEUED), INSERT outbox_events(message_id) }
  API-->>C: 202 Accepted (reservationId, statusUrl)
  R->>DB: SELECT 미발행 outbox (id 순, 배치)
  R->>K: publish reservation.requested.v1 (key=eventId)
  K-->>R: ack (acks=all)
  R->>DB: UPDATE published_at
  K->>W: consume
  W->>DB: TX { 중복 확인 → 상태 확인 → 조건부 UPDATE → CONFIRMED/SOLD_OUT → processed_messages }
  W->>K: offset commit (DB 커밋 이후)
  C->>API: GET /reservations/{id}
  API-->>C: CONFIRMED | SOLD_OUT | FAILED | QUEUED
```

### 예약 상태

```mermaid
stateDiagram-v2
  [*] --> QUEUED: 접수 (202)
  QUEUED --> CONFIRMED: 재고 확보
  QUEUED --> SOLD_OUT: 재고 없음
  QUEUED --> FAILED: 복구 불가 실패
  CONFIRMED --> [*]
  SOLD_OUT --> [*]
  FAILED --> [*]
```

`SOLD_OUT`은 실패가 아니라 정상적인 업무 결과이므로 재시도하지 않습니다.

<br>

## 🛠️ 기술 스택

| 영역 | 기술 | 용도 |
| --- | --- | --- |
| **Language** | ![Java](https://img.shields.io/badge/Java_21-ED8B00?style=flat-square&logo=openjdk&logoColor=white) | 백엔드 언어 |
| **Framework** | ![Spring Boot](https://img.shields.io/badge/Spring_Boot_4.1.3-6DB33F?style=flat-square&logo=springboot&logoColor=white) ![Spring Data JPA](https://img.shields.io/badge/Spring_Data_JPA-6DB33F?style=flat-square&logo=spring&logoColor=white) ![Spring Kafka](https://img.shields.io/badge/Spring_for_Apache_Kafka-6DB33F?style=flat-square&logo=spring&logoColor=white) | 트랜잭션, 비즈니스 로직, 이벤트 생산·소비 |
| **Messaging** | ![Apache Kafka](https://img.shields.io/badge/Apache_Kafka_4.3_%28KRaft%29-231F20?style=flat-square&logo=apachekafka&logoColor=white) | 비동기 완충, 재시도, DLT |
| **Database** | ![PostgreSQL](https://img.shields.io/badge/PostgreSQL_18-4169E1?style=flat-square&logo=postgresql&logoColor=white) ![Flyway](https://img.shields.io/badge/Flyway-CC0200?style=flat-square&logo=flyway&logoColor=white) | 원자적 재고 갱신, 스키마 마이그레이션 |
| **Cache** | ![Redis](https://img.shields.io/badge/Redis-FF4438?style=flat-square&logo=redis&logoColor=white) | 요청 빈도 제한 |
| **Infra** | ![Docker](https://img.shields.io/badge/Docker_Compose-2496ED?style=flat-square&logo=docker&logoColor=white) ![WSL2](https://img.shields.io/badge/WSL2-0078D4?style=flat-square&logo=linux&logoColor=white) | 로컬 단일 실행 환경 |
| **Test** | ![JUnit5](https://img.shields.io/badge/JUnit_5-25A162?style=flat-square&logo=junit5&logoColor=white) ![Testcontainers](https://img.shields.io/badge/Testcontainers-291A3F?style=flat-square) | 실제 Kafka·PostgreSQL 통합 테스트 |
| **Performance** | ![k6](https://img.shields.io/badge/k6-7D64FF?style=flat-square&logo=k6&logoColor=white) ![Micrometer](https://img.shields.io/badge/Actuator_·_Micrometer-6DB33F?style=flat-square&logo=spring&logoColor=white) | 부하 생성, p95·오류율·lag 측정 |
| **Monitoring** | ![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=flat-square&logo=prometheus&logoColor=white) ![Grafana](https://img.shields.io/badge/Grafana-F46800?style=flat-square&logo=grafana&logoColor=white) | 지표 수집·시각화 |
| **Docs · CI** | ![Swagger](https://img.shields.io/badge/Swagger-85EA2D?style=flat-square&logo=swagger&logoColor=black) ![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=flat-square&logo=githubactions&logoColor=white) | API 문서, 자동 테스트 |

<br>

## ✨ 주요 기능

<table>
<tr>
<td valign="top" width="50%">

**P0 — 핵심**

- [x] 공연 등록·조회
- [x] 예약 접수 API (`202` + `reservationId`)
- [x] 멱등성 키 기반 중복 요청 방어
- [x] Transactional Outbox 발행
- [x] Kafka Consumer 예약 처리
- [x] 원자적 재고 감소
- [x] 예약 상태 조회 (polling)

</td>
<td valign="top" width="50%">

**P1 — 안정성·운영**

- [x] Consumer 장애 복구 (재시도·DLT)
- [x] Redis 요청 빈도 제한 (`429`)
- [x] 운영 지표 (확정 지연, 오류율, lag)
- [x] 동기 처리 비교 모드 (벤치마크용)

**P2 — 후순위**

- [x] DLT 조회 API
- [x] Prometheus + Grafana 대시보드

</td>
</tr>
</table>

<br>

## 🛡️ 정확성 보장 전략

한 가지 장치에 기대지 않고, 위협마다 방어를 겹칩니다.

| 위협 | 방어 위치 | 구현 |
| --- | --- | --- |
| 초과 판매 | DB | 조건부 `UPDATE` 한 문장 + `CHECK (remaining_tickets BETWEEN 0 AND total_tickets)` |
| 같은 요청 반복 | API · DB | `Idempotency-Key` + `UNIQUE(user_id, idempotency_key)` |
| 같은 공연 중복 예약 | DB | `UNIQUE(event_id, user_id)` |
| 이벤트 유실 | DB | 예약과 Outbox를 같은 트랜잭션에 기록 |
| 메시지 중복 소비 | Consumer · DB | 예약 행 `FOR UPDATE` + `processed_messages` PK + `WHERE status = 'QUEUED'` 조건부 상태 변경 |
| 처리 불가 메시지 | Kafka | 일시 오류는 재시도, 영구 오류는 DLT로 격리 |
| 과도한 요청 | Redis | 사용자별 빈도 제한 |

```sql
-- 재고 감소는 이 한 문장으로만 한다 (읽고 계산한 뒤 쓰는 방식 금지)
UPDATE events
   SET remaining_tickets = remaining_tickets - 1
 WHERE id = :eventId
   AND remaining_tickets > 0
   AND sales_start_at <= CURRENT_TIMESTAMP
   AND sales_end_at   >  CURRENT_TIMESTAMP;
-- 1행 → CONFIRMED / 0행 → SOLD_OUT 또는 판매 기간 외
```

### 보장하는 것과 보장하지 않는 것

| ✅ 보장 | ❌ 보장하지 않음 |
| --- | --- |
| 확정 수 ≤ 총 수량, 잔여 재고 ≥ 0 | 엄격한 HTTP 도착 순서대로의 선착순 |
| 같은 예약 요청의 중복 확정 0 | `202` 응답 시점의 좌석 확보 |
| API 성공 후 이벤트 유실 없음 | 메시지의 정확히 한 번 전달 (업무 결과만 1회 적용) |
| Consumer 재시작 후 처리 재개 | 단일 노드 Kafka의 고가용성 |
| 처리 불가 메시지의 DLT 격리와 이력 | 로컬 측정 수치의 운영 환경 재현 |

<br>

## 🧪 검증 시나리오

| ID | 시나리오 | 합격 기준 |
| --- | --- | --- |
| `LOAD-01` | 티켓 100장에 고유 사용자 1,000명 집중 예약 | `CONFIRMED = 100`, 초과 판매 0 |
| `CON-01` | 같은 사용자·공연·멱등성 키로 10회 요청 | 예약 1건만 생성 |
| `CON-02` | 재고 1장에 동시 처리 | 확정 1건, 음수 재고 0 |
| `MSG-01` | 같은 `messageId` 2회 소비 | 재고 1회만 감소 |
| `MSG-02` | 처리 전 / 중 / 커밋 직후 Consumer 강제 종료 | 재시작 후 유실·중복 확정 없음 |
| `MSG-03` | Kafka 발행 후 `published_at` 갱신 전 Relay 중단 | 중복 메시지에도 정상 결과 |
| `MSG-04` | 재시도할 수 없는 스키마 오류 메시지 | DLT 이동, 감사 이력 유지 |
| `PERF-01` | 50 → 100 → 300 → 500 RPS 단계적 증가 | p95·오류율·lag 측정 및 분석 |
| `SEC-01` | 같은 사용자의 반복 예약 요청 | 빈도 제한·중복 억제 동작 |

**측정 지표:** 접수 p95 · 확정 p95 · RPS와 오류율(5xx / 429 / 409 분리) · 재고 불변식 · 중복 확정 건수 · Consumer lag · Outbox 미발행 건수

> [!TIP]
> '가상 사용자 1,000명'과 '초당 1,000요청'은 다른 조건입니다. 모든 측정 결과에는 VU 수, 요청 도착률, 실행 시간, PC 사양을 함께 기록합니다.

<br>

## 📊 벤치마크 요약

> 로컬 단일 PC(Core Ultra 7 258V, 32GB, Docker Desktop) 측정값입니다. 조건과 한계는 [벤치마크 보고서](docs/benchmark-report.md)에 있습니다.

**LOAD-01 — 티켓 100장, 고유 사용자 1,000명 동시 예약**

| 방식 | CONFIRMED | 초과 판매 | 접수 p95 | 1,000건 결과 확정까지 |
| --- | ---: | :---: | ---: | ---: |
| 동기 DB 처리 | **100** | 0 | 3,333ms | 약 3.6초 |
| Kafka 비동기 | **100** | 0 | 1,384ms | 약 7.6초 |

**PERF-01 — 도착률별 접수 p95 / 결과 확정 p95**

| RPS | 동기 접수 (= 확정) | 비동기 접수 | 비동기 확정 |
| ---: | ---: | ---: | ---: |
| 50 | 17ms | 9ms | 139ms |
| 100 | 14ms | 9ms | 142ms |
| 300 | 3,353ms (포화, 일부 요청 미발사) | 18ms | 9,999ms |
| 500 | 5,008ms (포화) | 55ms | 28,394ms |

- 공연 하나의 처리량 상한은 **두 방식 모두 초당 약 220건**입니다. 같은 재고 행을 순서대로 갱신해야 하기 때문이며, Kafka를 써도 늘지 않습니다.
- 비동기 방식은 그 이상의 요청을 **접수 지연 없이 받아 두고** 확정을 뒤로 미룹니다. 동기 방식은 같은 대기를 HTTP 요청이 떠안습니다.
- 스파이크에서는 동기 방식이 최종 결과를 더 빨리 냈습니다. **Kafka 방식이 항상 빠른 것은 아닙니다.**

<br>

## 🧯 장애 대응

| 장애 | 시스템 동작 | 실측 결과 | 운영자가 할 일 |
| --- | --- | --- | --- |
| **애플리케이션 강제 종료** | 커밋 전 접수는 롤백되고 클라이언트는 오류를 받는다. 커밋된 접수는 재시작 후 처리. 재전달·재발행 메시지는 `processed_messages`로 건너뜀 | 부하 중 `taskkill /F` 4회: 유실 0, 중복 반영 0, 재시작 후 3.9~9초에 처리 완료 | 재시작만 하면 된다. 클라이언트는 같은 `Idempotency-Key`로 재시도 |
| **Kafka 브로커 중단** | 접수는 계속 `202` (DB만 씀). 이벤트는 Outbox에 쌓였다가 브로커 복구 후 발행 | 14초 중단 중 30건 접수 → 복구 4.5초 후 전부 처리, 중복 0 | 브로커 복구. Outbox 미발행 건수 확인 |
| **처리 중 일시 오류** (DB 연결 등) | 같은 파티션 안에서 200·400·800ms 백오프로 3번 재시도. 실패마다 재고·상태 롤백 | 2번 실패 후 성공, 재고 1회만 감소 | 없음. 계속 실패하면 DLT로 이동 |
| **처리할 수 없는 메시지** (깨진 JSON, 스키마 불일치, 없는 예약) | 재시도 없이 DLT로 이동, `dead_letter_messages`에 원본·예외 기록 | 3가지 모두 DLT 이동, 감사 이력 남음 | `GET /api/v1/admin/dlt`로 원인 확인 후 조치 |
| **Redis 중단** | 빈도 제한만 꺼지고(fail-open) 예약은 계속 받는다. 첫 실패 후 5초 동안은 Redis를 부르지 않음. 헬스는 `UP` 유지 | 중단 중 6건 모두 `202` (첫 요청 0.5초, 이후 15~23ms), 복구 후 제한 재개 | Redis 복구. Grafana의 fail-open 패널 확인 |
| **같은 메시지 재전달** | 예약 행 잠금 → `processed_messages` 확인 → 건너뜀 | 재고·이력 변화 없음 | 없음 |

**실험에서 발견해 고친 문제:** 강제 종료된 Consumer는 그룹 탈퇴 요청을 보내지 못해서, 재시작한 앱이 `session.timeout.ms`(45초) 동안 파티션을 받지 못했습니다. Consumer 정적 멤버십(`group.instance.id`)을 설정해 재시작 즉시 파티션을 돌려받게 했습니다 (재시작 후 처리 완료 41.3초 → 3.9~9초).

자세한 기록: [Consumer 강제 종료](experiments/consumer-crash.md) · [Outbox 재발행·Kafka 중단](experiments/outbox-republish.md) · [중복 메시지](experiments/duplicate-event.md)

<br>

## 📁 프로젝트 구조

```text
ticketflow-kafka
├── prd.md                         # 제품 요구사항 문서
├── README.md
├── .env.example                   # 환경 변수 예시 (.env는 커밋 금지)
│
├── backend/                       # Spring Boot 단일 코드베이스
│   ├── build.gradle               # Spring Boot 4.1.3 · Java 21 · Gradle 9.9 wrapper
│   └── src
│       ├── main
│       │   ├── java/com/ticketflow
│       │   │   ├── event/         # 공연 등록·조회
│       │   │   ├── reservation/   # 예약 접수·조회, 상태 전이
│       │   │   ├── user/          # 테스트용 합성 사용자
│       │   │   ├── idempotency/   # 멱등성 키 검증
│       │   │   ├── outbox/        # Outbox 엔티티, Relay
│       │   │   ├── messaging/     # Kafka Producer·Consumer, 재시도·DLT
│       │   │   ├── monitoring/    # 관리자 지표 API
│       │   │   └── global/        # 공통 설정, 예외, 응답 형식, 테스트용 인증
│       │   └── resources
│       │       ├── db/migration/  # Flyway 마이그레이션
│       │       └── db/seed/       # local 전용 시드 (합성 사용자 1~1000)
│       └── test/java/com/ticketflow
│           ├── reservation/       # CON-01, SEC-01
│           ├── messaging/         # CON-02, MSG-01, MSG-04
│           ├── outbox/            # Outbox 기록·발행
│           ├── event/
│           └── support/           # Testcontainers 공통 설정
│
├── infrastructure/
│   ├── compose.yml                # PostgreSQL · Kafka(KRaft) · Redis (+ monitoring 프로필)
│   └── monitoring/                # Prometheus 설정, Grafana 프로비저닝·대시보드
│
├── load-tests/                    # k6 부하 테스트 (Docker 이미지로 실행)
│   ├── reservation-spike.js       # LOAD-01
│   ├── reservation-ramp.js        # PERF-01
│   ├── sql/result.sql             # 확정 지연·불변식 집계
│   └── run.py                     # 실행기 (k6 + 풀 샘플링 + 집계)
│
├── experiments/                   # 장애 재현 실험 기록
│   ├── duplicate-event.md         # MSG-01
│   ├── consumer-crash.md          # MSG-02 (프로세스 강제 종료)
│   ├── outbox-republish.md        # MSG-03 (재발행, Kafka 중단)
│   └── crash_test.py              # 강제 종료 실험 자동화
│
├── docs/
│   ├── architecture.md
│   ├── database.md
│   ├── event-contract.md
│   ├── api-spec.md
│   ├── benchmark-report.md        # 동기 vs 비동기 비교
│   └── adr/                       # 아키텍처 결정 기록
│
└── .github/workflows/             # CI
```

API, Outbox Relay, Consumer는 **하나의 Spring Boot 코드베이스**에서 패키지로 나눕니다. 확장이 필요하면 `api` / `worker` 프로필로 같은 코드를 별도 프로세스로 실행합니다.

<br>

## 🗓️ 로드맵

| 주차 | 기간 | 목표 | 마일스톤 | 상태 |
| :---: | :---: | --- | --- | :---: |
| 0 | ~ 11/20 | 기획·설계 | PRD, 폴더 구조 | ✅ |
| 1 | 11/23 ~ 11/29 | Spring Boot, PostgreSQL·Kafka Compose, 공연 API | 헬스 체크, DB 연결, 토픽 송수신 | ✅ |
| 2 | 11/30 ~ 12/06 | 예약 테이블, 접수 API, 멱등성 키 | `202` + `QUEUED` | ✅ |
| 3 | 12/07 ~ 12/13 | Outbox 트랜잭션, Relay, Kafka Consumer | 접수 → 이벤트 → 예약 완료 | ✅ |
| 4 | 12/14 ~ 12/20 | 원자적 재고 감소, 중복 소비 방지 | 100장 초과 확정 없음 | ✅ |
| 5 | 12/21 ~ 12/27 | k6 1,000명 시뮬레이션, 병목 분석 | 재고 불변식 + 성능 보고 | ✅ |
| 6 | 12/28 ~ 01/03 | Consumer 중단·재시작, 중복 발행, DLT | 장애 복구 테스트 | ✅ |
| 7 | 01/04 ~ 01/10 | Redis 빈도 제한, 지표·모니터링 | 운영 관측 지표 | ✅ |
| 8 | 01/11 ~ 01/17 | 통합 테스트, CI, 시연 영상 | 재현 가능한 포트폴리오 | ✅ (시연 영상은 [대본](docs/demo.md)까지) |

<br>

## 🚀 시작하기

**사전 요구사항:** Java 21, Docker Desktop (WSL2 백엔드). k6는 5주차 부하 테스트부터 필요합니다.

### 1. 환경 변수 준비

```powershell
Copy-Item .env.example .env
```

`.env`의 `POSTGRES_PASSWORD`를 원하는 값으로 바꿉니다. Compose와 Spring Boot가 모두 이 파일을 읽습니다.

### 2. 인프라 실행

```powershell
docker compose -f infrastructure/compose.yml --env-file .env up -d --wait
```

| 서비스 | 이미지 | 호스트 주소 | 비고 |
| --- | --- | --- | --- |
| PostgreSQL | `postgres:18.7-alpine` | `127.0.0.1:15432` | PC에 설치된 PostgreSQL(5432)과 겹치지 않게 15432 사용 |
| Kafka (KRaft) | `apache/kafka:4.3.2` | `127.0.0.1:9092` | 단일 노드, 토픽 자동 생성 꺼짐 |
| Redis | `redis:8.8.4-alpine` | `127.0.0.1:16379` | 예약 시도 빈도 제한 전용 |
| Prometheus *(선택)* | `prom/prometheus:v3.15.1` | `127.0.0.1:9090` | `--profile monitoring` |
| Grafana *(선택)* | `grafana/grafana:13.2.6` | `127.0.0.1:3000` | `--profile monitoring`, 대시보드 자동 등록 |

모니터링까지 띄우려면 `--profile monitoring`을 붙입니다. 지표 설명은 [docs/monitoring.md](docs/monitoring.md)에 있습니다.

```powershell
docker compose -f infrastructure/compose.yml --env-file .env --profile monitoring up -d --wait
```

### 3. 애플리케이션 실행

```powershell
cd backend
.\gradlew.bat bootRun
```

| 확인 | 주소 |
| --- | --- |
| 헬스 체크 | http://localhost:8080/actuator/health |
| Swagger UI | http://localhost:8080/swagger-ui/index.html |
| Grafana 대시보드 | http://localhost:3000 (`--profile monitoring`) |
| 운영 지표 API | `GET /api/v1/admin/metrics` (헤더 `X-Test-User-Role: ADMIN`) |

기동하면 Flyway가 스키마를 만들고, Kafka에 `reservation.requested.v1`과 `-dlt` 토픽(파티션 3)이 생성됩니다. Outbox Relay(100ms 폴링)와 Consumer(동시성 3)도 같은 프로세스에서 함께 뜹니다.

### 4. 100장 공연 등록하기

```powershell
Invoke-RestMethod -Method Post http://localhost:8080/api/v1/events -ContentType 'application/json' -Body '{"title":"Open Concert","totalTickets":100,"salesStartAt":"2026-01-01T00:00:00Z","salesEndAt":"2027-12-31T23:59:59Z"}'
```

```powershell
Invoke-RestMethod http://localhost:8080/api/v1/events/1
```

공연 등록은 `local` 프로필(기본값)에서만 열립니다.

### 5. 예약 접수하고 상태 조회하기

`local` 프로필에는 테스트용 합성 사용자 1~1000이 들어 있고, `X-Test-User-Id` 헤더로 사용자를 고릅니다. 이 헤더는 테스트 전용이며 실제 인증이 아닙니다.

```powershell
Invoke-WebRequest -UseBasicParsing -Method Post http://localhost:8080/api/v1/reservations -Headers @{ 'X-Test-User-Id' = '42'; 'Idempotency-Key' = 'demo-user42-concert1-attempt1' } -ContentType 'application/json' -Body '{"eventId":1}'
```

처음에는 `202`가 나옵니다. 같은 명령을 다시 실행하면 새 예약 없이 `200`과 기존 예약이 나옵니다.

```powershell
Invoke-RestMethod http://localhost:8080/api/v1/users/me/reservations -Headers @{ 'X-Test-User-Id' = '42' }
```

> `202`는 좌석 확정이 아니라 접수 완료입니다. 보통 1초 안에 Consumer가 처리해 `CONFIRMED`(재고 있음) 또는 `SOLD_OUT`(재고 없음)으로 바뀝니다.

Kafka로 오간 메시지를 직접 보려면:

```powershell
docker exec ticketflow-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic reservation.requested.v1 --from-beginning --timeout-ms 5000 --formatter-property print.key=true
```

### 6. 테스트

```powershell
cd backend
.\gradlew.bat test
```

Testcontainers가 Compose와 같은 이미지로 컨테이너를 따로 띄우므로, Docker만 실행 중이면 됩니다.

### 7. 정리

```powershell
docker compose -f infrastructure/compose.yml --env-file .env down
```

데이터까지 지우려면 `down -v`를 씁니다.

<br>

## 📚 문서

| 문서 | 내용 |
| --- | --- |
| [prd.md](prd.md) | 목표, 기능·비기능 요구사항, 검증 계획, 미결 사항 |
| [docs/architecture.md](docs/architecture.md) | 구현된 아키텍처와 장애 시나리오별 동작 |
| [docs/database.md](docs/database.md) | 스키마, 제약 조건, 재고 감소 쿼리 |
| [docs/event-contract.md](docs/event-contract.md) | Kafka 토픽·메시지 스키마 |
| [docs/api-spec.md](docs/api-spec.md) | REST API 명세 |
| [docs/benchmark-report.md](docs/benchmark-report.md) | 동기 vs 비동기 벤치마크 결과 |
| [docs/monitoring.md](docs/monitoring.md) | 운영 지표, Grafana 대시보드, 장애 신호별 확인 방법 |
| [docs/demo.md](docs/demo.md) | 시연 대본 (8~10분) |
| [docs/portfolio-summary.md](docs/portfolio-summary.md) | 이력서 문구, 면접에서 이야기할 것 |
| [docs/adr/](docs/adr/) | 아키텍처 결정 기록 (ADR) |
| [experiments/](experiments/) | 장애 재현 실험 기록 |

<br>

## 🚫 범위 밖 (Non-goals)

실제 결제 · 외부 공연 API 연동 · 실서비스 대상 트래픽 · 대규모 MSA · 실제 개인정보 수집 · Kubernetes 운영 · 엄격한 선착순 대기열 · 예약 취소·만료 (2차 버전)

<br>

<div align="center">
<sub>모든 성능 수치는 로컬 단일 PC에서 측정하며, 측정 환경과 조건을 함께 기록합니다.</sub>
</div>
