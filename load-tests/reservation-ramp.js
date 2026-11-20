// PERF-01 부하 증가 (PRD §12.1) — 5주차 작성, 7주차 재측정
//
// 조건
//   - 요청 도착률을 50 → 100 → 300 → 500 RPS 단계로 증가
//   - 처음에는 100 VU부터 시작해 로컬 PC 자원 한계를 기록
//   - 동기 처리 모드(FR-BMK-01)와 Kafka 비동기 모드를 같은 스크립트·데이터 조건으로 실행
//
// 측정 (PRD §13)
//   - M1 접수 p95, M2 확정 p95, M3 RPS·오류율(5xx / 429 / 409 분리), M6 Consumer lag
//
// 결과는 docs/benchmark-report.md에 정리 (원본 출력은 load-tests/results/, Git 제외)
