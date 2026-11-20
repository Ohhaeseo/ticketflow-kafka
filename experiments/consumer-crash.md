# 실험: Consumer 중단·재시작 (MSG-02)

> PRD §12.1 MSG-02 · 6주차 · **결과: 통과** (2026-11-20) · 원본 요약: [`results/2026-11-20/`](results/2026-11-20/)

## 목적

부하를 처리하는 도중 애플리케이션 프로세스가 **강제 종료**돼도, 재시작 후 접수된 예약이 **유실 없이, 한 번씩만** 처리되는지 확인한다.
처리 전 / 처리 중 / DB 커밋 직후(오프셋 커밋 전) 중단을 모두 포함하도록, 처리가 계속 진행 중인 시점에 JVM을 죽인다.

## 조건

| 항목 | 값 |
| --- | --- |
| PC | Intel Core Ultra 7 258V (8C/8T), RAM 32GB, Windows 11 + WSL2, Docker Desktop 8 CPUs |
| 이미지 | `apache/kafka:4.3.2`, `postgres:18.7-alpine`, `grafana/k6:2.3.0` |
| 애플리케이션 | Spring Boot 4.1.3, Consumer 동시성 3, ack-mode `record`, Relay 100ms |
| 중단 방법 | `taskkill /F` (TerminateProcess, SIGKILL과 같음). 종료 훅·커넥션 정리 없이 즉시 죽는다 |
| 도구 | [`experiments/crash_test.py`](crash_test.py): 앱 실행 → k6 부하 → 지정 시점에 JVM 강제 종료 → 재시작 → 처리 완료 대기 → 검증 |

## 시나리오

| # | 실행 | 부하 | 죽인 시점 | 그때의 상태 |
| --- | --- | --- | --- | --- |
| A | `crash-spike` (정적 멤버십 없음) | LOAD-01 1,000명 / 100장 | 접수 완료 직후 | Consumer 처리 대기 966건, Outbox 미발행 181건 |
| B | `crash-spike-static` | LOAD-01 1,000명 / 100장 | 접수 도중 | 접수 692건 중 처리 대기 587건 |
| C | `crash-midload` | PERF-01 300 RPS × 10초 | 부하 도중 | API·Relay·Consumer 모두 처리 중. 대기 1,736건, 미발행 58건 |
| D | `crash-afterload` | PERF-01 300 RPS × 10초 | 부하 종료 직후 | 처리 대기 642건 |

## 검증 기준

- `still_queued = 0`: 재시작 후 모든 접수 건이 최종 상태가 됨 (유실 없음)
- `applied_twice = 0`: 같은 예약에 재고 반영(CONFIRMED/SOLD_OUT/FAILED)이 두 번 기록되지 않음 (중복 없음)
- `never_applied = 0`: 반영 기록이 없는 예약 없음
- 재고 불변식: `CONFIRMED + remaining = total`, 초과 확정 없음

## 결과

| # | 접수 성공 | 죽은 뒤 실패한 요청 | 재시작→정상 | 재시작 후 처리 완료까지 | 건너뛴 중복 | 두 번 반영 | 미반영 | 불변식 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :---: |
| A | 1,000 | 0 | 10.3s | **41.3s** | 102 | **0** | **0** | ✅ (CONFIRMED 100) |
| B | 692 | 308 | 20.7s | 5.1s | 0 | **0** | **0** | ✅ (CONFIRMED 100) |
| C | 2,202 | 798 | 10.3s | 9.0s | 1 | **0** | **0** | ✅ |
| D | 3,001 | 0 | 10.2s | 3.9s | 0 | **0** | **0** | ✅ |

- **네 번 모두 유실·중복 반영 0건.** LOAD-01 두 번(A, B)도 CONFIRMED가 정확히 100건이었다.
- "죽은 뒤 실패한 요청"은 k6가 연결 오류를 받은 요청이다. DB에도 남지 않았다 (접수 트랜잭션이 커밋 전에 끊겨 롤백됨).
- A의 중복 102건: Relay가 Kafka로 보냈지만 `published_at`을 쓰기 전에 죽은 메시지가 재시작 후 재발행된 것이다 (MSG-03이 실제 장애에서 발생). 모두 `processed_messages`로 걸러졌다.
- C의 중복 1건: DB 커밋 후 오프셋 커밋 전에 죽은 레코드가 재전달된 것이다 (MSG-02의 "커밋 직후" 경우).

## 발견한 문제: 강제 종료 후 45초 동안 처리가 멈춤

A에서 재시작한 앱은 6.5초 만에 떴지만 **파티션이 35초 뒤에야 할당됐다.**

```text
23:00:38.474 Started TicketFlowApplication in 6.553 seconds
23:01:13.777 ticketflow-reservation: partitions assigned: [reservation.requested.v1-0]
```

강제 종료된 Consumer는 그룹을 떠난다는 요청(LeaveGroup)을 보내지 못한다. 브로커는 `session.timeout.ms`(기본 45초)가 지날 때까지 죽은 멤버에게 파티션을 묶어 두고, 새 Consumer는 그동안 기다린다.

**조치:** Consumer에 정적 멤버십(`group.instance.id`)을 설정했다 (`application.yml`). 재시작한 Consumer가 같은 instance id로 들어오면 브로커가 같은 멤버로 보고 리밸런스 없이 바로 파티션을 돌려준다. 리스너 컨테이너가 Consumer마다 `-0`, `-1`, `-2`를 붙이므로 동시성 3에서도 id가 겹치지 않는다.

**효과:** B·C·D에서 재시작 후 처리 완료까지 3.9~9.0초 (A는 41.3초). 자동 테스트(`ConsumerRestartIntegrationTest`) 실행 시간도 짧아졌다.

**주의:** 애플리케이션을 여러 인스턴스로 띄우면 인스턴스마다 `KAFKA_GROUP_INSTANCE_ID`를 다르게 줘야 한다. 같은 값을 쓰면 서로를 밀어낸다(fenced).

## 결론

프로세스가 어느 시점에 죽어도 접수된 예약은 재시작 후 모두 한 번씩만 처리됐다. 근거는 세 겹이다.

1. Outbox: 접수가 커밋됐으면 이벤트도 남아 있다 → 유실 없음.
2. 오프셋은 DB 커밋 후에만 커밋된다 → 처리 도중 죽으면 다시 받는다.
3. `processed_messages` + 예약 행 잠금 → 다시 받은 메시지는 건너뛴다 → 중복 반영 없음.

장애 복구 **시간**은 Consumer 그룹 설정에 크게 좌우됐다. 정적 멤버십 없이는 강제 종료 후 최대 45초 동안 확정이 멈춘다.

## 자동 테스트

| 테스트 | 내용 |
| --- | --- |
| `messaging/ConsumerRestartIntegrationTest` | 리스너 컨테이너를 멈춘 사이 8건 접수 → 재시작 후 확정 5, 매진 3, 처리 이력 8 |
| `messaging/DuplicateMessageIntegrationTest` | 재전달(같은 messageId 2회) → 재고 1회 감소 |
| `reservation/ReservationProcessorTest` | 처리 중 실패 → 재고·상태·이력 롤백 후 재처리 시 정상 확정 |

## 재현

```powershell
# 인프라만 띄운 상태에서 (앱은 스크립트가 직접 띄운다)
python experiments/crash_test.py spike --users 1000 --tickets 100 --kill-at 3.5 --label crash-spike-static
python experiments/crash_test.py stage --rate 300 --duration 10 --kill-at 7  --label crash-midload
python experiments/crash_test.py stage --rate 300 --duration 10 --kill-at 14 --label crash-afterload
```

`--kill-at`은 k6 컨테이너를 띄운 시점부터 센다. 컨테이너 시작 시간 때문에 실제로 죽인 시점은 1~2초 늦다 (결과 파일의 `kill_at`).
