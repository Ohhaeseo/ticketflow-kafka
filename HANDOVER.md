# 인수인계 (2026-11-21 갱신)

## 한 줄 요약

8주 로드맵(DoD 1~7), **Consumer 처리량 개선(배치 리스너)**, **메시지당 SQL 왕복 줄이기(5~6 → 3)** 까지 끝났다. 배치 리스너가 기본값이며, 같은 조건 짝 비교에서 처리 속도 +26~31%였다. SQL 왕복 줄이기는 정확성 테스트를 통과했지만 로컬에서는 처리량 개선을 확인하지 못했다. 측정에서 다음 병목은 API와 Consumer의 자원 경쟁으로 나타났다.

## 저장소 상태

| 항목 | 상태 |
| --- | --- |
| GitHub `main` | 최신까지 push. CI(테스트 67개) 통과 |
| 컨테이너 | 모두 내림. 데이터 볼륨 `ticketflow_postgres-data`, `ticketflow_kafka-data`는 남아 있음 |

## 끝난 것 (11-20 ~ 11-21)

- **배치 리스너** `ReservationRequestedBatchListener` (기본값 `ticketflow.consumer.batch-enabled=true`)
  - poll 묶음을 DB 트랜잭션 하나로 처리하고, 오프셋은 배치마다 커밋한다.
  - 실패 격리: 파싱 실패는 그 앞까지 커밋한 뒤 실패 레코드만 DLT로 보낸다. 배치 트랜잭션이 실패하면 한 건씩 다시 처리해 실패 레코드만 골라낸다.
  - 처리 결과 카운터는 커밋 후에만 올린다. DLT 감사 이력에는 근본 원인을 기록한다.
- **CI 통과** (`003de75`, 67/67)
- **측정 완료** ([benchmark-report.md](docs/benchmark-report.md#consumer-처리량-개선-배치-리스너-2026-11-2021))
  - 짝 비교 1 (11-20): 237 → 298건/s (+26%)
  - 짝 비교 2 (11-21, PC CPU 82~89%): 198 → 259건/s (+31%), 확정 p95 13.1s → 6.4s
  - 모든 회차 중앙값: 기존 129, A(오프셋만 배치 커밋) 220, B(배치 리스너) 248건/s
  - 원본: `docs/benchmark/2026-11-21/`
- 문서 반영: benchmark-report, event-contract(배치 리스너·DLT 원인), architecture, backend/README, README, portfolio-summary

## 남은 일 (선택)

1. **조용한 PC에서 재측정:** 측정 중 `llama-server`가 CPU를 3~4코어씩 간헐적으로 써서 수치가 2배까지 흔들렸다. 백그라운드 프로세스를 끈 뒤 방식마다 3회 이상 반복해 중앙값으로 비교한다.
   - 실행 도구: `load-tests/run.py` (회차별 백그라운드 부하 기록 방법은 benchmark-report 참고)
2. ~~메시지당 SQL 왕복 줄이기~~ → **11-21 완료.** 메시지당 5~6번에서 3번으로 줄였다 (재고 감소·판정·상태·이력을 CTE 한 문장으로, `ReservationOutcomeRepository`). 테스트 69개 통과. 로컬 측정에서는 처리량 개선이 잡음 안에 있었다 ([보고서](docs/benchmark-report.md#메시지당-sql-왕복-줄이기-2026-11-21)).
3. **API와 Consumer의 자원 경쟁 줄이기 (다음 병목):** 같은 Consumer가 부하 종료 후에는 부하 중보다 약 2배 빠르다 (380~700 대 220~310건/s). 후보는 다음과 같다.
   - Consumer 전용 커넥션 풀
   - `api`/`worker` 프로필로 프로세스 분리 (PRD §8.2)
   - Hikari 풀 크기 조정
4. 재고 분할 (공연 하나의 직렬화 해소). MVP 범위 밖.

## 환경 주의 사항

- 8080 포트는 다른 프로젝트 앱(`com.securelab.SecureLabApplication`)이 쓰는 경우가 있다. 이때는 `--server.port=8090`으로 띄우고, `load-tests/run.py`에 `--app-url`, `--k6-base-url`을 준다.
- 6379(securelab Redis), 5432(로컬 PostgreSQL 서비스)는 다른 쪽이 쓰고 있어서, 이 프로젝트는 16379, 15432를 쓴다.
- `llama-server`가 돌고 있으면 성능 수치가 크게 흔들린다. 측정 전에 작업 관리자에서 확인한다.
- 커밋 메시지에 `Co-Authored-By` 줄을 넣지 않는다 (사용자 요청).

## 다시 시작하는 법

```powershell
docker compose -f infrastructure/compose.yml --env-file .env up -d --wait
```

```powershell
cd backend; .\gradlew.bat bootRun
```
