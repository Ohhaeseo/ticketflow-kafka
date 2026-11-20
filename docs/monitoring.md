# 운영 지표와 모니터링

> 요구사항 기준: [PRD §13 측정 지표](../prd.md#13-측정-지표), FR-OBX-04, FR-OPS-01~03. 7주차 구현.

## 한눈에 보기

| 무엇을 | 어디서 |
| --- | --- |
| 실시간 대시보드 | Grafana `http://localhost:3000` (로그인 없이 보기 가능, 로컬 전용) |
| 원시 지표 | `http://localhost:8080/actuator/prometheus` → Prometheus `http://localhost:9090` |
| 현재 상태 스냅샷 (JSON) | `GET /api/v1/admin/metrics` (관리자) |
| DLT 메시지와 원인 | `GET /api/v1/admin/dlt?limit=20` (관리자) |

```powershell
# 모니터링까지 띄우기
docker compose -f infrastructure/compose.yml --env-file .env --profile monitoring up -d --wait
```

## 지표

| 지표 (Prometheus 이름) | 종류 | 의미 | PRD |
| --- | --- | --- | --- |
| `http_server_requests_seconds_bucket{uri="/api/v1/reservations",method="POST"}` | histogram | 접수 API 지연 분위수, 응답 코드별 건수 | M1, M3 |
| `ticketflow_reservations_accepted_total{mode}` | counter | 새로 접수된 예약 (async/sync) | M3 |
| `ticketflow_reservations_processed_total{outcome}` | counter | Consumer 처리 결과 (CONFIRMED, SOLD_OUT, FAILED, SKIPPED_…) | M5 |
| `ticketflow_reservation_processing_seconds_bucket` | histogram | 메시지 하나를 처리하는 시간 (커밋 제외) | M2 보조 |
| `ticketflow_consumer_lag` | gauge | 예약 Consumer group lag 합계. AdminClient로 직접 계산, -1이면 실패 | M6 |
| `ticketflow_outbox_unpublished` | gauge | 미발행 Outbox 건수 | M7 |
| `ticketflow_outbox_oldest_unpublished_age_seconds` | gauge | 가장 오래 기다린 미발행 이벤트의 대기 시간 | M7 |
| `ticketflow_dead_letters` | gauge | DLT 감사 이력 누적 건수 | MSG-04 |
| `ticketflow_ratelimit_rejected_total` | counter | 빈도 제한으로 429를 받은 요청 | SEC-01 |
| `ticketflow_ratelimit_fail_open_total` | counter | Redis 장애로 검사 없이 허용한 요청 | FR-RL-02 |
| `ticketflow_ratelimit_redis_failures_total` | counter | 빈도 제한 중 Redis 호출 실패 | FR-RL-02 |
| `hikaricp_connections_active` / `_pending` | gauge | DB 커넥션 풀 사용 / 대기 | 5주차 병목 |

- gauge(lag, Outbox, DLT)는 `ticketflow.monitoring.refresh-ms`(5초)마다 미리 계산한다. Prometheus가 수집할 때마다 DB·Kafka를 조회하지 않는다.
- 재고 불변식은 공연별로 `GET /api/v1/admin/metrics`의 `recentEvents[].invariantOk`에서 본다.

## Grafana 대시보드

`infrastructure/monitoring/grafana/dashboards/ticketflow.json` (프로비저닝으로 자동 등록, 홈 대시보드)

| 패널 | 보는 법 |
| --- | --- |
| Consumer lag / Outbox 미발행 / DLT 누적 / 빈도 제한 거부 | 상단 숫자. 빨강·주황이면 확인 |
| 예약 접수 / 처리 결과 (건/s) | 접수 선이 처리 선보다 위에 있는 동안 lag가 쌓인다 |
| Consumer lag · Outbox 미발행 | lag만 늘면 Consumer 처리 한계, Outbox만 늘면 Kafka 연결 문제 |
| 접수 API 지연 (p50/p95/p99) | 커넥션 풀 대기와 함께 본다 |
| 메시지 처리 시간 · Outbox 최고 대기 | 처리 시간이 늘면 DB 잠금 경합을 의심한다 |
| HTTP 응답 코드 | 202 / 409 / 429 / 5xx 분리 (M3) |
| DB 커넥션 풀 | pending이 쌓이면 풀이 병목 |
| Redis 빈도 제한 상태 | fail-open이 0이 아니면 Redis 장애 |

### 7주차 확인 (400 RPS × 30초)

대시보드에서 아래를 확인했다.
- 접수는 초당 약 380건인데 처리(CONFIRMED)는 초당 약 220건에서 막힌다.
- 그 차이만큼 Consumer lag가 최대 약 8,000까지 쌓였다가, 부하가 끝난 뒤 줄어든다.
- 5주차 벤치마크에서 숫자로 본 처리량 한계가 시계열로 그대로 보인다.

## 장애 신호와 대응

| 신호 | 가능한 원인 | 확인 |
| --- | --- | --- |
| Outbox 미발행이 계속 증가, lag는 0 | Kafka 브로커 장애·연결 문제 | `docker ps`, Relay 로그(`Failed to publish outbox`) |
| lag가 계속 증가, 처리율이 0 | Consumer 멈춤, 파티션 미할당 | 앱 로그 `partitions assigned`, `GET /admin/metrics`의 `consumerLag.byPartition` |
| lag가 증가, 처리율은 일정 | 처리 능력 한계 (핫 파티션) | [벤치마크 보고서](benchmark-report.md) |
| DLT 누적 증가 | 깨진 메시지, 스키마 불일치, 없는 예약 | `GET /api/v1/admin/dlt` |
| fail-open 증가 | Redis 장애. 예약은 계속 받지만 빈도 제한이 꺼진 상태 | `docker ps`, Redis 복구 |
| 커넥션 풀 pending 증가 + 접수 지연 증가 | 스파이크로 DB 커넥션 부족 | 풀 크기·Tomcat 스레드 검토 |

## 헬스 체크와 Redis

`management.health.redis.enabled: false`로 Redis를 `/actuator/health`에서 뺐다. Redis는 빈도 제한에만 쓰고 장애 시 fail-open으로 동작하므로, Redis 장애 때문에 앱 전체가 DOWN(재시작 대상)이 되면 안 된다. 대신 Redis 상태는 fail-open 지표로 본다.
