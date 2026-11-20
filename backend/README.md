# backend

Spring Boot 단일 코드베이스입니다. API, Outbox Relay, Consumer를 패키지로 나누고, 필요하면 `api`/`worker` 프로필로 별도 프로세스로 실행합니다.

## 버전

| 항목 | 버전 |
| --- | --- |
| Java | 21 (Gradle toolchain) |
| Spring Boot | 4.1.3 (Spring Initializr로 생성) |
| Gradle | 9.9.2 (wrapper) |
| spring-kafka / kafka-clients | 4.1.3 / 4.2.2 |
| Hibernate ORM | 7.4 |
| Flyway | 12.4 |
| springdoc-openapi | 3.2.0 (Spring Boot 4.1.x 기준 빌드) |
| Testcontainers | 2.x (`testcontainers-kafka`, `testcontainers-postgresql`) |

## 실행

저장소 루트에서 인프라를 먼저 띄웁니다 (루트 README 참고).

```powershell
cd backend
.\gradlew.bat bootRun
```

- 기본 프로필은 `local`입니다. 공연 등록 API, Swagger UI, 테스트용 헤더 인증(`X-Test-User-Id`), 합성 사용자 시드가 켜집니다. 다른 프로필에서는 모두 꺼집니다.
- DB·Kafka 접속 정보는 루트의 `.env`에서 읽습니다 (`spring.config.import: optional:file:../.env[.properties]`).
- Compose 없이 Testcontainers로 인프라까지 띄우려면 `.\gradlew.bat bootTestRun`을 실행합니다.
- 벤치마크용 동기 처리 모드: `.\gradlew.bat bootRun --args='--ticketflow.reservation.processing-mode=sync'` (접수 응답 201 + 최종 상태, Outbox·Kafka 미사용)

## 테스트

```powershell
.\gradlew.bat test
```

Docker만 실행 중이면 됩니다. Testcontainers가 Compose와 같은 이미지 태그(`apache/kafka:4.3.2`, `postgres:18.7-alpine`)로 컨테이너를 띄우고, 모든 통합 테스트가 하나의 Spring 컨텍스트와 컨테이너를 공유합니다 (`@IntegrationTest`).

| 테스트 | 검증 내용 |
| --- | --- |
| `TicketFlowApplicationTests` | 헬스 체크 UP (DB 포함), OpenAPI 문서 생성 |
| `event/EventApiIntegrationTest` | 100장 공연 등록·조회, 목록, 404, 필드 검증 400, 판매 기간 오류, 잘못된 JSON |
| `event/EventTableConstraintTest` | DB CHECK 제약: 초과 재고, 음수 재고, 0 이하로 감소, 판매 기간 역전 |
| `messaging/KafkaIntegrationTest` | 기동 시 예약 토픽·DLT 생성(파티션 3), Producer → Consumer 송수신 |
| `reservation/ReservationApiIntegrationTest` | 202 접수, 재요청 200(Q1), 409 두 종류(Q2), 400·401·404·422, 본인·관리자 조회, 내 예약 목록 |
| `reservation/ReservationIdempotencyTest` | **CON-01**: 같은 요청 10회 순차·동시 → 예약 1건. 같은 공연에 다른 키 10개 동시 → 1건 + 409 9건 |
| `reservation/ReservationProcessorTest` | Consumer 처리 로직 직접 호출: CONFIRMED·재고 감소, 같은 messageId 2회(MSG-01), 다른 messageId 중복, SOLD_OUT, FAILED(SALES_CLOSED), **트리거로 실패 주입 → 재고·상태·이력 롤백**(FR-INV-03), 없는 예약 |
| `reservation/ReservationConcurrencyTest` | **CON-02**: 재고 1장에 예약 8건 동시 처리 → 확정 1. 같은 예약에 다른 messageId 8개 동시, 같은 메시지 8개 동시 → 재고 1회 감소 |
| `reservation/OversellPreventionIntegrationTest` | **100장 / 150명 동시 접수** → API·Outbox·Kafka·Consumer 전체 경로에서 CONFIRMED 정확히 100, SOLD_OUT 50, 불변식 성립 |
| `messaging/DuplicateMessageIntegrationTest` | **MSG-01**: 같은 messageId를 실제 Kafka로 2번 전송 → 재고 1회 감소, 처리 이력 1건 |
| `reservation/ReservationFlowIntegrationTest` | **E2E**: 티켓 2장·3명 → Outbox → Kafka → Consumer → CONFIRMED 2, SOLD_OUT 1, 잔여 0, 미발행 0 |
| `outbox/OutboxIntegrationTest` | 접수 시 Outbox 1건(계약 필드), 재요청·409는 Outbox 없음, Relay 발행 후 `published_at` |
| `reservation/SyncProcessingModeIntegrationTest` | **FR-BMK-01** 동기 비교 모드: 201 + 최종 상태, Outbox 없음, 재요청 200, 동시 접수 시 재고만큼만 확정 (별도 컨텍스트) |
| `messaging/DeadLetterIntegrationTest` | **MSG-04**: 깨진 JSON·schemaVersion 99·없는 예약 → DLT + 감사 이력. **FR-CSM-03**: 트리거로 2번 실패 주입 → 백오프 재시도 후 성공, DLT 없음 |
| `messaging/ConsumerRestartIntegrationTest` | **MSG-02**: 리스너 컨테이너를 멈춘 사이 8건 접수 → 재시작 후 확정 5, 매진 3 |
| `outbox/OutboxRepublishIntegrationTest` | **MSG-03**: `published_at`을 되돌려 재발행 → Kafka에 같은 messageId 2번, 재고는 1번 감소 |

총 54개. 동시성·중복 테스트는 반복 실행으로 결과가 흔들리지 않는 것을 확인했습니다. 재고 불변식 검사는 `support/StockInvariant`를 함께 씁니다.

Relay와 Consumer는 테스트 중에도 백그라운드에서 돈다. 처리 로직만 따로 검증할 때는 Outbox 없이 예약을 직접 저장해(`ReservationCommandRepository`) Consumer가 끼어들지 않게 한다.

## 패키지 구조

| 패키지 | 책임 | 상태 |
| --- | --- | --- |
| `event` | 공연 등록·조회, 재고 조회 | ✅ 1주차 |
| `reservation` | 예약 접수·조회 API, 메시지 처리(`ReservationProcessor`), 동기 비교 모드 | 접수·조회 ✅ 2주차, 처리·상태 전이 ✅ 3주차 |
| `idempotency` | 멱등성 키 검증, 요청 해시(SHA-256), Consumer 처리 이력(`processed_messages`) | ✅ 2·4주차 |
| `user` | 테스트용 합성 사용자 (local 시드 1~1000) | ✅ 2주차 |
| `outbox` | Outbox 행·저장소, Relay(Poller) | ✅ 3주차 |
| `messaging` | Kafka 토픽·오류 처리 설정, 메시지 DTO, Outbox 기록(`producer`), Listener(`consumer`), DLT 감사(`dlt`) | ✅ 3·6주차 |
| `monitoring` | 관리자 지표 API, 커스텀 Micrometer 지표 | 7주차 |
| `global` | 공통 설정(OpenAPI, Clock), 예외 처리, 오류 응답 형식, 테스트용 헤더 인증(`auth`) | ✅ 1~2주차 |

## 테스트 구조

| 폴더 | 대상 테스트 (PRD §12.1) |
| --- | --- |
| `test/.../support` | Testcontainers 공통 설정, `@IntegrationTest` |
| `test/.../event` | 공연 API, DB 제약 |
| `test/.../messaging` | Kafka 연결·group id, MSG-01, MSG-02, MSG-04, 재시도 ✅ |
| `test/.../reservation` | 예약 API, CON-01, CON-02, 처리 로직·롤백, E2E, 초과 판매 방지 ✅ · SEC-01 (7주차) |
| `test/.../outbox` | Outbox 기록·발행, MSG-03 재발행 ✅ |

## Flyway 마이그레이션

`src/main/resources/db/migration/`에 `V2__...sql`처럼 버전 순서대로 추가합니다. 이미 적용된 마이그레이션 파일은 수정하지 않습니다. JPA는 `ddl-auto: validate`로 스키마와 엔티티가 맞는지만 확인합니다.
