# 시연 대본 (약 8~10분)

> PRD 8주차 "시연 영상"용 대본입니다. 화면 녹화(예: Windows `Win + Alt + R` 또는 OBS)를 켜고 순서대로 따라 하면 됩니다.
> 모든 명령은 저장소 루트의 PowerShell에서 실행합니다.

## 0. 준비 (녹화 전)

```powershell
docker compose -f infrastructure/compose.yml --env-file .env --profile monitoring up -d --wait
```

```powershell
cd backend; .\gradlew.bat bootRun
```

- 새 PowerShell 창을 하나 더 열어 저장소 루트로 이동해 둡니다.
- 브라우저 탭 3개를 열어 둡니다.
  - Swagger: http://localhost:8080/swagger-ui/index.html
  - Grafana: http://localhost:3000 (TicketFlow 대시보드, 시간 범위 "Last 5 minutes")
  - GitHub 저장소의 Actions 탭
- JIT 워밍업을 위해 한 번 미리 실행해 둡니다.

```powershell
python load-tests/run.py stage --rate 50 --duration 10 --label demo-warmup
```

## 1. 문제 소개 (30초) — README 화면

- "티켓 100장에 1,000명이 동시에 몰리는 상황입니다. 목표는 초과 판매 0, 중복 확정 0입니다."
- 핵심 원칙 강조: **"Kafka가 초과 판매를 막아 주지 않습니다. 정확성은 DB의 조건부 UPDATE가 보장합니다."**
- 아키텍처 다이어그램으로 접수 → Outbox → Relay → Kafka → Consumer 흐름을 짚습니다.

## 2. API 흐름 (1분 30초) — Swagger

1. `POST /api/v1/events`: `totalTickets: 3`인 공연을 등록합니다.
2. `POST /api/v1/reservations`: `X-Test-User-Id: 1`, `Idempotency-Key: demo-1`로 보냅니다 → **202 QUEUED**.
   - "202는 좌석 확보가 아니라 접수입니다."
3. 같은 요청을 다시 보냅니다 → **200**, 같은 `reservationId`. "네트워크 재시도에도 예약은 하나입니다."
4. `GET /api/v1/reservations/{id}` → **CONFIRMED**. Consumer가 이미 처리했습니다.
5. 같은 공연에 키를 바꿔 보냅니다 → **409 RESERVATION_ALREADY_EXISTS**.

## 3. 1,000명 / 100장 (2분) — 터미널 + Grafana

```powershell
python load-tests/run.py spike --users 1000 --tickets 100 --label demo-load01
```

- Grafana를 보여 줍니다: 접수율이 치솟고, Consumer lag가 쌓였다가 줄어듭니다.
- 결과에서 짚을 것:
  - `accepted=1000`, `error_rate_pct=0.0`
  - `confirmed = 100`, `sold_out = 900`, `invariant_ok = t`
  - "확정은 정확히 100건입니다. 1,000명이 동시에 와도 초과 판매는 없습니다."

## 4. 장애 시연 (3분)

### 4-1. Kafka 브로커를 멈춰도 접수는 계속된다

```powershell
docker stop ticketflow-kafka
```

- Swagger에서 예약을 2~3건 보냅니다 → 여전히 **202**.
- Grafana "Outbox 미발행" 숫자가 올라가는 것을 보여 줍니다.

```powershell
docker start ticketflow-kafka
```

- 몇 초 뒤 Outbox 미발행이 0이 되고 예약이 CONFIRMED로 바뀝니다.
- "DB 커밋과 Kafka 발행 사이의 장애를 Transactional Outbox가 막습니다."

### 4-2. 처리 중 프로세스 강제 종료 (선택, 앱을 스크립트가 직접 띄움)

`bootRun` 창을 `Ctrl+C`로 멈춘 뒤 실행합니다.

```powershell
python experiments/crash_test.py spike --users 1000 --tickets 100 --kill-at 3.5 --label demo-crash
```

- 결과의 `applied_twice | 0`, `never_applied | 0`, `confirmed 100`을 짚습니다.
- "처리 도중 JVM이 강제로 죽어도 재시작 후 유실도 중복도 없습니다."
- 한 줄 덧붙이기: "실험 중 재시작 후 45초 멈춤을 발견하고 Kafka 정적 멤버십으로 고쳤습니다."

### 4-3. 깨진 메시지는 DLT로

```powershell
"{not-json" | docker exec -i ticketflow-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic reservation.requested.v1
```

- Swagger에서 `GET /api/v1/admin/dlt`를 호출합니다 (`X-Test-User-Id: 1`, `X-Test-User-Role: ADMIN`).
- 원본과 예외 클래스가 기록된 것을 보여 줍니다. Grafana의 "DLT 누적"도 1 올라갑니다.

## 5. 동기 vs 비동기 (1분) — docs/benchmark-report.md

- 그래프 3개를 보여 줍니다.
- "비동기는 접수가 빠르지만, 공연 하나의 처리량은 두 방식 모두 약 220건/s입니다. Kafka는 대기를 HTTP 대신 lag로 옮길 뿐이고, 스파이크에서는 동기가 결과를 더 빨리 냈습니다. 그래서 Kafka가 항상 빠르다고 말하지 않습니다."

## 6. 마무리 (30초) — GitHub

- Actions 탭에서 CI가 초록색인 것을 보여 줍니다. Testcontainers 테스트 69개가 실제 Kafka·PostgreSQL·Redis로 돕니다.
- README의 DoD 체크리스트를 보여 주며 마칩니다.

## 녹화 후 정리

```powershell
docker compose -f infrastructure/compose.yml --env-file .env --profile monitoring down
```
