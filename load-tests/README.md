# load-tests

k6 부하 테스트 스크립트와 실행기입니다. 결과 분석은 [docs/benchmark-report.md](../docs/benchmark-report.md)에 있습니다.

## 구성

| 파일 | 내용 |
| --- | --- |
| `reservation-spike.js` | **LOAD-01**: 공연 1개(기본 100장)에 고유 사용자 1,000명이 동시에 1회 예약 |
| `reservation-ramp.js` | **PERF-01**: 한 단계(RATE)를 고정 도착률로 측정. 50/100/300/500 RPS를 각각 실행 |
| `lib/common.js` | 공연 생성, 예약 요청, 응답 코드별 카운터, 요약 출력 |
| `sql/result.sql` | 확정 지연(M2), 재고 불변식, Outbox 발행 지연(M7), 처리 속도 집계 |
| `run.py` | k6 실행 + 커넥션 풀 샘플링 + 처리 완료 대기 + SQL 집계 (Python 표준 라이브러리만 사용) |
| `results/` | 실행 결과 (Git 제외). 보고서에 쓴 요약은 `docs/benchmark/`에 복사 |

## 전제

- Docker Desktop이 실행 중이고 인프라가 떠 있어야 합니다: `docker compose -f infrastructure/compose.yml --env-file .env up -d --wait`
- 애플리케이션이 `local` 프로필로 `localhost:8080`에서 실행 중이어야 합니다. 합성 사용자 1~1000이 시드되어 있어야 하므로 `local` 프로필이 필요합니다.
- k6는 설치하지 않아도 됩니다. `grafana/k6:2.3.0` 이미지를 씁니다. 컨테이너에서는 `host.docker.internal:8080`으로 앱에 접속합니다.
- Python 3.10 이상.

## 실행

```powershell
# 워밍업 (JIT·커넥션 준비)
python load-tests/run.py stage --rate 50 --duration 10 --label warmup-async

# LOAD-01
python load-tests/run.py spike --users 1000 --tickets 100 --label load01-async

# PERF-01 (단계별)
python load-tests/run.py stage --rate 50  --duration 20 --label perf-async-50
python load-tests/run.py stage --rate 100 --duration 20 --label perf-async-100
python load-tests/run.py stage --rate 300 --duration 20 --label perf-async-300
python load-tests/run.py stage --rate 500 --duration 20 --label perf-async-500
```

동기 방식과 비교하려면 애플리케이션을 `--args='--ticketflow.reservation.processing-mode=sync'`로 다시 띄우고 `--label`만 바꿔 같은 명령을 실행합니다.

## 측정 규칙 (PRD §12.2)

- '가상 사용자 1,000명'(LOAD-01)과 '초당 N요청'(PERF-01)은 다른 조건입니다. 결과에 executor, VU 수, 도착률, 실행 시간을 함께 적습니다.
- 실행마다 새 공연을 만들어 데이터를 분리합니다. 공연 title이 `LOAD-01 <label>` 또는 `PERF-01 <label>`이라 SQL 집계가 실행 단위로 나뉩니다.
- 결과에 PC 사양과 Docker 리소스 설정을 함께 기록합니다.
- 부하 테스트는 소유한 로컬 환경에서만 실행합니다 (NFR-SEC-04).
