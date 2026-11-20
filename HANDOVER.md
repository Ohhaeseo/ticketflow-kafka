# 인수인계 (2026-11-20 23:58)

## 한 줄 요약

8주 로드맵(DoD 1~7)은 끝나서 GitHub에 올라가 있다. 그 뒤 **Consumer 처리량 개선(배치 리스너)** 을 진행했고, 구현·테스트·짧은 측정까지 마쳤다. **로컬 커밋만 있고 push하지 않았다.**

## 저장소 상태

| 항목 | 상태 |
| --- | --- |
| GitHub `main` | `1ecae03` (8주차 최종 정리)까지. CI 통과 |
| 로컬에만 있는 커밋 | `e76b964` perf(consumer): 배치 리스너 / 이 문서와 측정 요약 커밋 |
| 커밋 안 된 것 | 없음 (`.claude/launch.json`, `load-tests/results/`는 Git 제외 대상) |
| 컨테이너 | 모두 내림 (`docker compose ... down`). 데이터 볼륨 `ticketflow_postgres-data`, `ticketflow_kafka-data`는 남아 있음 |

## 마지막 작업: Consumer 처리량 개선 (진행 중)

### 한 것 (`e76b964`)

- **배치 리스너** `messaging/consumer/ReservationRequestedBatchListener`가 기본값이 됐다 (`ticketflow.consumer.batch-enabled=true`).
  - poll 묶음을 `ReservationProcessor.processBatch()` 하나의 DB 트랜잭션으로 처리하고, 오프셋은 배치마다 커밋한다 (`AckMode.BATCH`).
  - 메시지별 정확성 규칙(예약 행 `FOR UPDATE` → `processed_messages` 확인 → 조건부 재고 감소)은 그대로다.
- **실패 격리**
  - 파싱·검증 실패: 그 앞까지 처리·커밋한 뒤 `BatchListenerFailedException(index)` → 실패 레코드만 DLT로 간다.
  - 배치 트랜잭션 실패: 한 건씩 각자의 트랜잭션으로 다시 처리해 실패한 레코드만 골라낸다.
- **처리 결과 카운터**는 커밋된 뒤에만 올린다 (`afterCommit`). 롤백 후 재처리할 때 중복으로 집계되지 않는다.
- **DLT 감사 이력**에 스택 트레이스의 가장 깊은 `Caused by:`를 기록한다. 배치 방식에서는 원인 예외가 `BatchListenerFailedException`에 감싸여 실제 원인이 가려졌기 때문이다.
- 오류 처리기의 "Seek to current after exception" 로그를 ERROR에서 WARN으로 낮췄다 (배치 재시도의 정상 동작).
- `ticketflow.consumer.batch-enabled=false`로 두면 기존 레코드 리스너로 돌아간다 (비교용).
- **테스트 67개 통과** (새로 5개): `BatchListenerIsolationTest` 2개, `RecordListenerModeIntegrationTest`, `DeadLetterAuditListenerTest` 2개.
  - ⚠️ 로그 수준을 바꾼 뒤에는 컴파일만 확인했고 전체 테스트는 다시 돌리지 않았다.

### 측정 (짧게 1회, 앱 기동 직후, 500 RPS × 10초)

| 지표 | 레코드 방식 (기존) | 배치 방식 |
| --- | ---: | ---: |
| 처리 속도 | 237건/s | **298건/s** (+26%) |
| 확정 p95 | 15,521ms | **8,803ms** (-43%) |
| 부하 종료 후 처리 완료까지 | 13.2s | 9.2s |
| 접수 / 확정 / 불변식 | 4,340 / 4,340 / ✅ | 4,361 / 4,361 / ✅ |

원본: `docs/benchmark/2026-11-20/consumer-record-500.txt`, `consumer-batch-500.txt`

- 워밍업 없이 1회씩만 쟀다. 경향만 보는 수치다.
- 개선폭이 기대보다 작다. 메시지마다 SQL 5번 왕복(잠금, 중복 확인, 재고 감소, 상태 변경, 이력 기록)하는 비용이 다음 병목으로 보인다.

### 남은 일 (다음에 할 순서)

1. 전체 테스트 다시 실행: `cd backend; .\gradlew.bat test` (로그 수준 변경 후 확인)
2. 제대로 다시 측정: 워밍업 후 LOAD-01과 PERF-01(300·500 RPS)을 세 방식으로 측정한다.
   - 기존 (`batch-enabled=false`)
   - A: 레코드 처리 + 오프셋만 배치 커밋 (`batch-enabled=false --spring.kafka.listener.ack-mode=batch`)
   - B: 배치 리스너 (기본)
3. 문서 반영: `docs/benchmark-report.md`(개선 결과 섹션), `docs/event-contract.md`·`docs/architecture.md`(ack-mode batch, 배치 리스너, 실패 격리), `backend/README.md`(테스트 67개), README 배지·요약
4. push 후 CI 확인
5. 다음 개선 후보: 메시지당 SQL 왕복 줄이기 (`processed_messages` 확인을 `INSERT ... ON CONFLICT`로 합치기, 재고 감소와 상태 변경을 CTE 한 문장으로 묶기 등)

## 환경 주의 사항

- **8080 포트를 다른 프로젝트 앱이 쓰고 있다.** `com.securelab.SecureLabApplication`(23:51에 시작)이다. 이 프로젝트 앱이 8080에서 뜨지 못해, 측정은 `--server.port=8090/8091`로 했다. 이어서 할 때 8080이 비어 있지 않으면 아래처럼 포트를 바꿔 실행한다.

```powershell
cd backend; .\gradlew.bat bootRun --args='--server.port=8090'
```

```powershell
python load-tests/run.py stage --rate 500 --duration 20 --label <label> --app-url http://localhost:8090 --k6-base-url http://host.docker.internal:8090
```

- 6379(securelab Redis), 5432(로컬 PostgreSQL 서비스)도 다른 쪽이 쓰고 있어서, 이 프로젝트는 16379, 15432를 쓴다.
- 커밋 메시지에 `Co-Authored-By` 줄을 넣지 않는다 (사용자 요청).

## 다시 시작하는 법

```powershell
docker compose -f infrastructure/compose.yml --env-file .env up -d --wait
```

```powershell
git log --oneline -3; git status
```

push가 필요하면 `git push` (현재 원격보다 커밋 2개 앞섬).
