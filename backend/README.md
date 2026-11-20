# backend

Spring Boot 단일 코드베이스입니다. API, Outbox Relay, Consumer를 패키지로 나누고, 필요하면 `api`/`worker` 프로필로 별도 프로세스로 실행합니다.

## 1주차 월요일(11/23): 프로젝트 생성

빌드 파일(`build.gradle`, `settings.gradle`, `gradlew.bat`, `gradle/wrapper/`)과 `TicketFlowApplication.java`는 아직 없습니다.
[Spring Initializr](https://start.spring.io)로 아래 설정을 생성한 뒤 이 폴더에 풀어 넣으세요. 이미 만들어 둔 패키지 폴더와 합쳐집니다.

| 항목 | 값 |
| --- | --- |
| Project | Gradle - Groovy |
| Language | Java |
| Spring Boot | 4.1.x (생성 시점의 최신 패치) |
| Group | `com` |
| Artifact | `ticketflow` |
| Name | `TicketFlow` → 메인 클래스 `TicketFlowApplication` |
| Package name | `com.ticketflow` |
| Java | 21 |

**Dependencies:** Spring Web, Validation, Spring Data JPA, PostgreSQL Driver, Flyway Migration, Spring for Apache Kafka, Spring Data Redis, Spring Boot Actuator, Testcontainers

생성 후 할 일:

- `application.properties`를 `application.yml`로 바꾸고 `local` 프로필을 만듭니다.
- springdoc-openapi는 Spring Boot 4.x와 호환되는 버전을 확인한 뒤 추가합니다.
- 패키지 폴더의 `.gitkeep`은 실제 파일이 생기면 지웁니다.

## 패키지 구조

| 패키지 | 책임 |
| --- | --- |
| `event` | 행사 등록·조회, 재고 조회 |
| `reservation` | 예약 접수·조회 API, 상태 전이, 동기 비교 모드 |
| `idempotency` | 멱등성 키 검증, 요청 해시 비교 |
| `outbox` | Outbox 엔티티, Relay(Poller) |
| `messaging` | Kafka Producer/Consumer 설정, 메시지 DTO, 재시도·DLT 설정 |
| `monitoring` | 관리자 지표 API, 커스텀 Micrometer 지표 |
| `global` | 공통 설정, 예외 처리, 응답 형식 |

## 테스트 구조

| 폴더 | 대상 테스트 (PRD §12.1) |
| --- | --- |
| `test/.../reservation` | CON-01, SEC-01 |
| `test/.../messaging` | CON-02, MSG-01, MSG-04 |
| `test/.../outbox` | Outbox 기록·발행, MSG-03 보조 |
| `test/.../event` | 행사 API |
| `test/.../support` | Testcontainers 공통 설정, 테스트 데이터 픽스처 |

## Flyway 마이그레이션

`src/main/resources/db/migration/`에 `V1__create_events.sql`처럼 버전 순서대로 추가합니다. 이미 적용된 마이그레이션 파일은 수정하지 않습니다.
