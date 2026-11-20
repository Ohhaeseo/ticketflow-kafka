# 이벤트 계약

> 요구사항 기준: [PRD §10 이벤트 계약](../prd.md#10-이벤트-계약-kafka). 스키마가 바뀌면 이 문서를 먼저 고칩니다.

## 토픽

| 토픽 | 파티션 | Key | 보존 기간 | 용도 |
| --- | --- | --- | --- | --- |
| `reservation.requested.v1` | | `eventId` | | 예약 접수 이벤트 |
| `reservation.requested.v1-dlt` | | 원본과 같음 | | 영구 오류 격리 |

## 메시지 스키마 — `reservation.requested.v1`

| 필드 | 타입 | 필수 | 설명 |
| --- | --- | --- | --- |
| `messageId` | UUID | Y | Outbox 저장 시 생성, 재발행해도 같은 값 |
| `schemaVersion` | int | Y | `1` |
| `reservationId` | long | Y | |
| `eventId` | long | Y | 메시지 key와 같음 |
| `occurredAt` | ISO-8601 UTC | Y | 접수 시각 |

## DLT 헤더

<!-- DLT로 보낼 때 남기는 원본 토픽·파티션·오프셋·예외 정보 -->

## Producer / Consumer 설정

<!-- acks, idempotence, 재시도, 오프셋 커밋 방식, 오류 분류(일시/영구) -->

## 스키마 변경 규칙

- 필드 추가처럼 호환되는 변경은 같은 토픽에서 `schemaVersion`을 올립니다.
- 호환되지 않는 변경은 새 토픽(`reservation.requested.v2`)으로 분리합니다.
