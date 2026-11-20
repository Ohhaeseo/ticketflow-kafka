# API 명세

> 요구사항 기준: [PRD §11 API 명세](../prd.md#11-api-명세). 구현 후 Swagger(OpenAPI)와 내용을 맞춥니다.

- 기본 경로: `/api/v1`
- Swagger UI: <!-- 1주차 금요일(11/27) 이후 경로 기록 -->

## 사용자 식별 (MVP)

<!-- PRD Q4 결정 내용. local 프로필 전용임을 명시 -->

## 엔드포인트

### `POST /events`

### `GET /events`

### `GET /events/{eventId}`

### `POST /reservations`

<!-- 요청 헤더(Idempotency-Key), 본문, 202/200/409/422/429 응답 예시 -->

### `GET /reservations/{reservationId}`

### `GET /users/me/reservations`

### `GET /admin/metrics`

### `GET /admin/dlt`

## 오류 코드

| HTTP | code | 상황 |
| --- | --- | --- |
| | | |
