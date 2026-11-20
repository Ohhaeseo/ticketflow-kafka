# API 명세

> 요구사항 기준: [PRD §11 API 명세](../prd.md#11-api-명세). 실행 중인 서버의 Swagger와 내용을 맞춥니다.

- 기본 경로: `/api/v1`
- Swagger UI: `http://localhost:8080/swagger-ui/index.html`
- OpenAPI JSON: `http://localhost:8080/v3/api-docs`

## 사용자 식별 (MVP)

<!-- 2주차: PRD Q4 결정 내용. local 프로필 전용임을 명시 -->

## 공연 (Events) — 1주차 ✅

### `POST /events` — 공연 등록 (local 프로필 전용)

`ticketflow.admin.local-endpoints-enabled=true`일 때만 등록된다. `local` 프로필이 이 값을 켠다.

```http
POST /api/v1/events
Content-Type: application/json

{
  "title": "Popular Concert",
  "totalTickets": 100,
  "salesStartAt": "2026-11-23T10:00:00Z",
  "salesEndAt": "2026-12-31T23:59:59Z"
}
```

```http
HTTP/1.1 201 Created
Location: /api/v1/events/1

{
  "id": 1,
  "title": "Popular Concert",
  "totalTickets": 100,
  "remainingTickets": 100,
  "salesStartAt": "2026-11-23T10:00:00Z",
  "salesEndAt": "2026-12-31T23:59:59Z"
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

## 예약 (Reservations) — 2주차 예정

### `POST /reservations`

<!-- 요청 헤더(Idempotency-Key), 본문, 202/200/409/422/429 응답 예시 -->

### `GET /reservations/{reservationId}`

### `GET /users/me/reservations`

## 관리자 — 7주차 예정

### `GET /admin/metrics`

### `GET /admin/dlt`

## 오류 응답

```json
{
  "code": "INVALID_REQUEST",
  "message": "요청 형식이 올바르지 않습니다.",
  "errors": [
    { "field": "totalTickets", "reason": "0보다 커야 합니다" }
  ]
}
```

`errors`는 필드 검증 실패일 때만 내용이 있고, 그 외에는 빈 배열이다.

| HTTP | code | 상황 |
| --- | --- | --- |
| 400 | `INVALID_REQUEST` | 필드 검증 실패, JSON 형식 오류, 경로 변수 타입 오류 |
| 400 | `INVALID_SALES_PERIOD` | 판매 시작 시각이 종료 시각보다 늦거나 같음 |
| 404 | `EVENT_NOT_FOUND` | 공연 없음 |
