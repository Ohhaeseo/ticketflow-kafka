# API 명세

> 요구사항 기준: [PRD §11 API 명세](../prd.md#11-api-명세). 실행 중인 서버의 Swagger와 내용을 맞춥니다.

- 기본 경로: `/api/v1`
- Swagger UI: `http://localhost:8080/swagger-ui/index.html` (`local` 프로필에서만)
- OpenAPI JSON: `http://localhost:8080/v3/api-docs` (`local` 프로필에서만)

## 사용자 식별 (MVP, PRD Q4)

> [!WARNING]
> 아래 방식은 **테스트 전용**입니다. 헤더 값을 그대로 믿으므로 실제 인증이 아닙니다. 공개 배포 전에 실제 인증·인가로 바꿔야 합니다 (NFR-SEC-03).

| 헤더 | 값 | 설명 |
| --- | --- | --- |
| `X-Test-User-Id` | 양의 정수 | 요청한 사용자. `local` 프로필에는 합성 사용자 1~1000이 시드되어 있다 |
| `X-Test-User-Role` | `ADMIN` (선택) | 다른 사용자의 예약도 조회할 수 있다 |

- `ticketflow.test-auth.enabled=true`일 때만 동작합니다. `local` 프로필이 켜고, 기본값은 꺼짐입니다.
- 꺼져 있거나 헤더가 없거나 형식이 틀리면 `401 UNAUTHENTICATED`입니다.
- 시드에 없는 사용자로 예약을 접수하면 `401 UNAUTHENTICATED`입니다.

## 공연 (Events) — 1주차 ✅

### `POST /events` — 공연 등록 (local 프로필 전용)

`ticketflow.admin.local-endpoints-enabled=true`일 때만 등록된다. `local` 프로필이 이 값을 켠다.

```http
POST /api/v1/events
Content-Type: application/json

{
  "title": "Open Concert",
  "totalTickets": 100,
  "salesStartAt": "2026-01-01T00:00:00Z",
  "salesEndAt": "2027-12-31T23:59:59Z"
}
```

```http
HTTP/1.1 201 Created
Location: /api/v1/events/1

{
  "id": 1,
  "title": "Open Concert",
  "totalTickets": 100,
  "remainingTickets": 100,
  "salesStartAt": "2026-01-01T00:00:00Z",
  "salesEndAt": "2027-12-31T23:59:59Z"
}
```

| 필드 | 규칙 |
| --- | --- |
| `title` | 필수, 공백 불가, 최대 200자 |
| `totalTickets` | 필수, 1 이상 100,000 이하 |
| `salesStartAt`, `salesEndAt` | 필수, ISO-8601, `salesStartAt < salesEndAt` |

### `GET /events` — 공연 목록

`id` 오름차순 배열을 반환한다. 항목 형식은 등록 응답과 같다.

### `GET /events/{eventId}` — 공연 상세

남은 재고(`remainingTickets`)를 포함한다. 없으면 `404 EVENT_NOT_FOUND`.

## 예약 (Reservations) — 2주차 ✅

### `POST /reservations` — 예약 접수

```http
POST /api/v1/reservations
Content-Type: application/json
X-Test-User-Id: 42
Idempotency-Key: demo-user42-concert1-attempt1

{ "eventId": 1 }
```

```http
HTTP/1.1 202 Accepted
Location: /api/v1/reservations/1

{
  "reservationId": 1,
  "eventId": 1,
  "status": "QUEUED",
  "failureReason": null,
  "statusUrl": "/api/v1/reservations/1",
  "createdAt": "2026-11-20T13:13:27.528774Z",
  "updatedAt": "2026-11-20T13:13:27.528774Z"
}
```

> **`202`는 좌석 확정이 아니라 접수 완료입니다.** 최종 결과(`CONFIRMED`/`SOLD_OUT`)는 `statusUrl`을 조회해서 확인합니다. 2주차 현재는 Consumer가 없어서 `QUEUED`에 머뭅니다 (3~4주차 구현).

**처리 순서**

1. `Idempotency-Key` 형식을 검증한다.
2. 같은 사용자·같은 키의 예약이 있으면 본문 해시를 비교해 **200 + 기존 예약** 또는 **409 `IDEMPOTENCY_KEY_REUSED`**를 반환한다. 판매 기간이 끝난 뒤에 재전송해도 기존 예약을 돌려준다.
3. 사용자(시드 존재), 공연 존재, 판매 기간을 검증한다.
4. `INSERT ... ON CONFLICT DO NOTHING RETURNING id`로 `QUEUED` 예약을 저장한다. 동시 요청이 경쟁하면 DB의 UNIQUE 제약이 한 건만 통과시킨다.
5. 충돌로 저장되지 않았으면 먼저 저장된 예약을 다시 읽어 2번 규칙(200/409)을 적용하고, 같은 공연의 다른 키라면 **409 `RESERVATION_ALREADY_EXISTS` + `reservationId`**를 반환한다.

| 응답 | 상황 |
| --- | --- |
| `202` | 최초 접수 |
| `200` | 같은 사용자·같은 키·같은 본문 재요청 (Q1) |
| `400 INVALID_REQUEST` | `eventId` 누락, JSON 오류, `Idempotency-Key` 헤더 누락 |
| `400 INVALID_IDEMPOTENCY_KEY` | 키가 1~100자의 `[A-Za-z0-9._:-]`가 아님 |
| `429 RATE_LIMITED` | 사용자별 시도 한도 초과 (업무 검증보다 먼저 검사) |
| `401 UNAUTHENTICATED` | 사용자 식별 실패, 미등록 사용자 |
| `404 EVENT_NOT_FOUND` | 공연 없음 |
| `409 IDEMPOTENCY_KEY_REUSED` | 같은 키로 다른 본문(다른 `eventId`) |
| `409 RESERVATION_ALREADY_EXISTS` | 같은 공연에 이미 예약 있음 (Q2). 응답에 기존 `reservationId` 포함 |
| `422 SALES_NOT_STARTED` / `SALES_ENDED` | 판매 기간 밖 |

### `GET /reservations/{reservationId}` — 예약 상태 조회

본인 예약만 조회할 수 있다. 다른 사용자의 예약은 존재 여부를 드러내지 않도록 `404 RESERVATION_NOT_FOUND`로 응답한다. `X-Test-User-Role: ADMIN`이면 모든 예약을 조회할 수 있다.

### `GET /users/me/reservations` — 내 예약 목록

본인 예약을 최근 접수 순(`reservationId` 내림차순)으로 반환한다.

## 관리자 — 7주차 ✅

헤더 `X-Test-User-Role: ADMIN`이 있어야 합니다. 없으면 `403 FORBIDDEN`입니다 (사용자 헤더 자체가 없으면 401).

### `GET /admin/metrics` — 운영 지표 (FR-OPS-01)

```json
{
  "generatedAt": "2026-11-20T14:22:31.112Z",
  "reservationsByStatus": { "QUEUED": 0, "CONFIRMED": 39408, "SOLD_OUT": 3303, "FAILED": 0 },
  "outbox": { "unpublished": 0, "oldestUnpublishedAgeMs": 0 },
  "consumerLag": { "groupId": "ticketflow-reservation", "topic": "reservation.requested.v1", "total": 0, "byPartition": { "0": 0, "1": 0, "2": 0 } },
  "deadLetters": { "total": 0, "last24h": 0 },
  "recentEvents": [
    { "eventId": 72, "title": "Open Concert", "totalTickets": 100, "remainingTickets": 0,
      "confirmed": 100, "soldOut": 900, "failed": 0, "queued": 0, "invariantOk": true }
  ]
}
```

- `consumerLag.total`이 `-1`이면 Kafka에서 계산하지 못한 것입니다 (브로커 장애 등).
- `recentEvents`는 최근 공연 20개이며, `invariantOk`는 PRD §12.3 재고 불변식 결과입니다.

### `GET /admin/dlt?limit=20` — DLT 메시지 (FR-OPS-02)

최근에 DLT로 이동한 메시지를 원본 위치·내용·실패 원인과 함께 반환합니다. `limit`은 1~200입니다.

```json
[
  { "id": 3, "originalTopic": "reservation.requested.v1", "originalPartition": 1, "originalOffset": 52,
    "messageKey": "dlt-test", "payload": "{not-json ...", "exceptionCauseClass": "tools.jackson.core.exc.StreamReadException",
    "exceptionMessage": "Unexpected character ...", "receivedAt": "2026-11-20T14:02:11.501Z" }
]
```

## 요청 빈도 제한 (7주차 ✅)

`POST /reservations`는 사용자별로 **첫 시도부터 10초 동안 20회**까지 허용합니다 (`ticketflow.rate-limit.reservation.*`). 실패한 시도(409 등)도 셉니다.

```http
HTTP/1.1 429 Too Many Requests
Retry-After: 9

{ "code": "RATE_LIMITED", "message": "예약 시도가 너무 잦습니다. 잠시 후 다시 시도하세요.", "errors": [] }
```

Redis에 연결할 수 없으면 제한 없이 허용합니다 (fail-open, PRD Q5).

## 오류 응답

```json
{
  "code": "RESERVATION_ALREADY_EXISTS",
  "message": "이미 해당 공연에 접수된 예약이 있습니다.",
  "errors": [],
  "reservationId": 1
}
```

- `errors`는 필드 검증 실패일 때만 내용이 있고, 그 외에는 빈 배열이다.
- `reservationId`는 `RESERVATION_ALREADY_EXISTS`일 때만 포함된다.

| HTTP | code | 상황 |
| --- | --- | --- |
| 400 | `INVALID_REQUEST` | 필드 검증 실패, JSON 형식 오류, 필수 헤더 누락, 경로 변수 타입 오류 |
| 400 | `INVALID_SALES_PERIOD` | 공연 등록 시 판매 시작 시각이 종료 시각보다 늦거나 같음 |
| 400 | `INVALID_IDEMPOTENCY_KEY` | 멱등성 키 형식 오류 |
| 401 | `UNAUTHENTICATED` | 사용자 식별 실패 |
| 403 | `FORBIDDEN` | 관리자 API를 관리자가 아닌 사용자가 호출 |
| 404 | `EVENT_NOT_FOUND` | 공연 없음 |
| 404 | `RESERVATION_NOT_FOUND` | 예약 없음 또는 다른 사용자의 예약 |
| 409 | `IDEMPOTENCY_KEY_REUSED` | 같은 키·다른 본문 |
| 409 | `RESERVATION_ALREADY_EXISTS` | 같은 공연 중복 예약 |
| 422 | `SALES_NOT_STARTED` | 판매 시작 전 |
| 422 | `SALES_ENDED` | 판매 종료 후 |
| 429 | `RATE_LIMITED` | 사용자별 예약 시도 한도 초과. `Retry-After` 헤더(초) |
