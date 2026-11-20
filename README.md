# TicketFlow Kafka

A Kafka-powered ticket reservation backend with transactional outbox, idempotent consumers, and concurrency testing.

잔여 티켓이 적은 공연에 대량 예약 요청이 몰리는 상황을 **Spring Boot + Kafka + PostgreSQL + Redis**로 처리하고, 중복 예약·초과 판매·이벤트 중복 처리·Consumer 장애를 검증하는 서버입니다.

> **현재 상태:** 착수 전 (2026-11-20). 문서와 폴더 구조만 있습니다. 1주차(2026-11-23~)에 Spring Boot 프로젝트와 Compose를 작성합니다.

## 핵심 설계 원칙

- **Kafka를 도입했다고 초과 판매가 자동으로 막히지는 않습니다.** 정확성의 근거는 PostgreSQL의 조건부 `UPDATE`, 트랜잭션, 제약 조건입니다.
- Kafka는 대량 요청의 비동기 완충, 이벤트 전달, Consumer 확장·장애 복구를 맡습니다.
- 메시지 전달은 at-least-once로 가정하고, 모든 Consumer 처리를 멱등하게 구현합니다.
- **202 Accepted는 좌석 확보가 아니라 접수 완료**입니다.

## 문서

| 문서 | 내용 |
| --- | --- |
| [prd.md](prd.md) | 제품 요구사항 (목표, 기능·비기능 요구사항, 테스트, 마일스톤) |
| [docs/architecture.md](docs/architecture.md) | 아키텍처와 처리 흐름 |
| [docs/database.md](docs/database.md) | 스키마, 제약, 상태 전이 |
| [docs/event-contract.md](docs/event-contract.md) | Kafka 토픽·메시지 계약 |
| [docs/api-spec.md](docs/api-spec.md) | REST API 명세 |
| [docs/benchmark-report.md](docs/benchmark-report.md) | 동기 vs 비동기 벤치마크 결과 |
| [docs/adr/](docs/adr/) | 아키텍처 결정 기록 |
| [experiments/](experiments/) | 장애 재현 실험 기록 |

## 폴더 구조

```text
backend/          Spring Boot 단일 코드베이스 (API · Outbox Relay · Consumer)
infrastructure/   Docker Compose (PostgreSQL, Kafka KRaft, Redis), 모니터링 설정
load-tests/       k6 부하 테스트 스크립트
experiments/      장애 재현 실험 기록
docs/             설계 문서, ADR, 벤치마크 보고서
.github/workflows CI
```

## 보장하는 것 / 보장하지 않는 것

| 보장 | 보장하지 않음 |
| --- | --- |
| 확정 수 ≤ 총 수량, 잔여 재고 ≥ 0 | 엄격한 HTTP 도착 순서대로의 선착순 |
| 같은 예약 요청의 중복 확정 0 | 202 응답 시점의 좌석 확보 |
| API 성공 후 이벤트 유실 없음 (Outbox) | 메시지의 정확히 한 번 전달 (업무 결과만 1회 적용) |
| Consumer 재시작 후 처리 재개 | 단일 노드 Kafka의 고가용성 |

## 실행 방법

> 1주차 작업 후 작성합니다. (사전 준비: Java 21, Docker Desktop + WSL2)

```powershell
java -version
docker compose version
```

## 장애 대응

> 6주차 실험(`experiments/`) 결과를 바탕으로 작성합니다.

## 라이선스

TBD
