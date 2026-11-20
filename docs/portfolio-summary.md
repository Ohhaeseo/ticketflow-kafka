# 포트폴리오 요약

> 실측 수치는 로컬 단일 PC(Core Ultra 7 258V, 32GB, Docker Desktop) 기준입니다. 이력서·면접에 쓸 때도 이 조건을 함께 말합니다.

## 한 줄 소개

Spring Boot·Kafka·PostgreSQL 기반 비동기 티켓 예약 서버. Transactional Outbox와 멱등 Consumer로 1,000명 동시 예약에서 초과 판매·중복 확정 0을 검증하고, 동기 방식과의 성능 차이와 장애 복구를 실측했습니다.

## 이력서 문구 (예시)

**TicketFlow Kafka — 대규모 트래픽 티켓 예약 서버** (개인 프로젝트, Java 21 · Spring Boot 4 · Kafka · PostgreSQL · Redis)

- 원자적 조건부 UPDATE, Transactional Outbox, `processed_messages` 기반 멱등 Consumer로 **티켓 100장에 1,000명이 동시에 예약해도 CONFIRMED 정확히 100건, 초과 판매 0**을 k6로 검증
- 부하 중 프로세스 강제 종료 4회와 Kafka 브로커 중단을 재현해 **유실 0, 중복 반영 0** 확인. 재시작 후 45초간 처리가 멈추는 원인(Consumer 세션 타임아웃)을 찾아 정적 멤버십으로 **복구 시간을 41초에서 4~9초로 단축**
- 동기 처리와 Kafka 비동기 처리를 같은 조건(50~500 RPS)으로 비교: 비동기는 500 RPS에서 접수 p95 55ms(동기 5,008ms)로 버티지만, 공연 하나의 확정 처리량은 두 방식 모두 약 220건/s로 같다는 점을 수치로 정리
- Consumer를 배치 리스너(poll 묶음 = DB 트랜잭션 1개, 문제 레코드만 DLT로 격리)로 바꿔 같은 조건 짝 비교에서 **처리 속도 +26~31%, 확정 p95 약 절반**
- 재시도(지수 백오프)·DLT 감사 이력·Redis 빈도 제한(fail-open)·Prometheus/Grafana 대시보드 구성. Testcontainers 기반 테스트 67개를 GitHub Actions로 실행

## 면접에서 이야기할 것

### "Kafka를 왜 썼나요?"

- 초과 판매를 막으려고 쓴 것이 아닙니다. 정확성은 DB 조건부 UPDATE와 제약 조건이 책임집니다.
- Kafka는 **접수와 확정을 분리**하려고 썼습니다. 처리 능력(공연당 약 220건/s)을 넘는 요청을 API가 느려지지 않게 받아 두고, 장애가 나도 메시지가 남아 다시 처리됩니다.
- 대가도 측정했습니다. 스파이크에서는 동기 방식이 최종 결과를 더 빨리 냈습니다 (동기 3.6초, 비동기 7.6초).

### "중복 처리는 어떻게 막았나요?"

세 겹으로 막았습니다.

1. **API:** `Idempotency-Key` + `UNIQUE(user_id, idempotency_key)`. 저장은 `INSERT ... ON CONFLICT DO NOTHING`이라, 동시에 10개가 와도 DB가 하나만 통과시킵니다.
2. **Consumer:** 예약 행을 `FOR UPDATE`로 잠근 뒤 `processed_messages`에서 같은 `message_id`를 확인합니다.
3. **상태 전이:** `WHERE status = 'QUEUED'` 조건으로만 바꿉니다.

`message_id`는 Outbox에 저장할 때 만들어서, 재발행해도 같은 값이 나가게 했습니다 (기획 단계에서 찾은 빈틈).

### "장애 실험에서 무엇을 배웠나요?"

- 강제 종료된 Consumer는 그룹 탈퇴 요청을 보내지 못합니다. 그래서 브로커가 `session.timeout.ms`(45초) 동안 파티션을 묶어 둡니다. 정확성에는 문제가 없었지만 **복구 시간**이 문제였고, 정적 멤버십으로 해결했습니다.
- 실제 강제 종료에서 Outbox 재발행(102건)과 오프셋 미커밋 재전달(1건)이 모두 일어났고, 전부 걸러졌습니다.

### "병목은 어디였나요?"

- **스파이크(1,000명 동시):** DB 커넥션 풀(10)이 먼저 막혔습니다 (대기 190).
- **지속 부하:** 공연 하나의 재고 행 직렬화가 막았습니다. key가 `eventId`라 한 파티션을 Consumer 스레드 하나가 처리하고, 레코드마다 오프셋을 동기 커밋합니다.
- **개선:** 배치 리스너로 커밋·fsync·오프셋 커밋을 배치마다 한 번으로 줄였습니다. 같은 조건 짝 비교에서 처리 속도 +26~31%였습니다. 이때 실패 격리가 핵심이었습니다. 문제 레코드 하나 때문에 배치 전체가 DLT로 가지 않도록, 배치 트랜잭션이 실패하면 한 건씩 다시 처리해 실패 레코드만 골라냅니다.
- **측정에서 배운 것:** 같은 PC의 다른 프로세스가 CPU를 절반쯤 쓰면서 같은 방식도 2배까지 흔들렸습니다. 그래서 백그라운드 부하를 회차마다 기록하고 같은 조건의 짝으로만 개선폭을 말했습니다.
- **다음 개선 후보:** 메시지당 SQL 왕복 줄이기, 커넥션 풀 조정, 재고 분할.

### "테스트는 어떻게 했나요?"

- 모든 통합 테스트는 Testcontainers로 실제 Kafka·PostgreSQL·Redis를 띄워 돌립니다. 컨텍스트와 컨테이너를 공유해서 67개가 약 2분에 끝납니다.
- 롤백은 DB 트리거로 실패를 주입해 검증했습니다. 일시 오류 재시도는 롤백되지 않는 시퀀스로 시도 횟수를 셌습니다. Mock 없이 실제 DB 동작을 확인했습니다.

## 숫자 출처

| 수치 | 문서 |
| --- | --- |
| LOAD-01, PERF-01, 동기 vs 비동기 | [benchmark-report.md](benchmark-report.md) |
| 강제 종료 4회, 45초 → 4~9초 | [experiments/consumer-crash.md](../experiments/consumer-crash.md) |
| Kafka 14초 중단, 재발행 | [experiments/outbox-republish.md](../experiments/outbox-republish.md) |
| 중복 메시지 | [experiments/duplicate-event.md](../experiments/duplicate-event.md) |
