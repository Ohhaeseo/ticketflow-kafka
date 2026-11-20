// 공통 헬퍼: 공연 생성, 예약 요청, 결과 지표, 요약 출력
import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

export const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:8080';
export const RUN_LABEL = __ENV.RUN_LABEL || `run-${Date.now()}`;
export const TITLE = __ENV.TITLE || RUN_LABEL;
export const SEED_USERS = 1000; // local 프로필 시드 사용자 1~1000

const JSON_HEADERS = { 'Content-Type': 'application/json' };

// 접수 API 지연 (PRD M1). http_req_duration에는 setup 요청도 섞이므로 따로 잰다.
export const reserveDuration = new Trend('reserve_duration', true);
export const reserveAccepted = new Counter('reserve_accepted');   // 202 (async) / 201 (sync)
export const reserveConflict = new Counter('reserve_409');
export const reserveRateLimited = new Counter('reserve_429');
export const reserveClientError = new Counter('reserve_4xx_other');
export const reserveServerError = new Counter('reserve_5xx');
export const reserveNetworkError = new Counter('reserve_network_error');

/** 지금 판매 중인 공연을 만든다 (local 프로필 전용 API). */
export function createEvent(totalTickets) {
  const now = Date.now();
  const res = http.post(`${BASE_URL}/api/v1/events`, JSON.stringify({
    title: TITLE,
    totalTickets,
    salesStartAt: new Date(now - 3600 * 1000).toISOString(),
    salesEndAt: new Date(now + 24 * 3600 * 1000).toISOString(),
  }), { headers: JSON_HEADERS, tags: { name: 'setup' } });
  if (!check(res, { 'event created': (r) => r.status === 201 })) {
    throw new Error(`event creation failed: ${res.status} ${res.body}`);
  }
  return res.json('id');
}

export function reserve(userId, eventId, idempotencyKey) {
  const res = http.post(`${BASE_URL}/api/v1/reservations`, JSON.stringify({ eventId }), {
    headers: {
      ...JSON_HEADERS,
      'X-Test-User-Id': String(userId),
      'Idempotency-Key': idempotencyKey,
    },
    tags: { name: 'reserve' },
  });
  reserveDuration.add(res.timings.duration);
  if (res.status === 202 || res.status === 201) reserveAccepted.add(1);
  else if (res.status === 409) reserveConflict.add(1);
  else if (res.status === 429) reserveRateLimited.add(1);
  else if (res.status >= 500) reserveServerError.add(1);
  else if (res.status === 0) reserveNetworkError.add(1);
  else reserveClientError.add(1);
  check(res, { 'reserve accepted (201/202)': (r) => r.status === 202 || r.status === 201 });
  return res;
}

function metric(data, name, stat) {
  const m = data.metrics[name];
  if (!m) return 0;
  const v = m.values[stat];
  return v === undefined ? 0 : v;
}

/** k6 결과를 JSON 파일과 짧은 텍스트로 남긴다. 표준 출력 형식은 load-tests/run.py가 읽는다. */
export function summary(data) {
  const fmt = (v) => (Math.round(v * 10) / 10).toFixed(1);
  // Trend 요약에는 count가 없으므로 응답 분류 카운터를 더해 요청 수를 구한다
  const sent = ['reserve_accepted', 'reserve_409', 'reserve_429', 'reserve_4xx_other', 'reserve_5xx',
    'reserve_network_error'].reduce((sum, name) => sum + metric(data, name, 'count'), 0);
  const failed = sent - metric(data, 'reserve_accepted', 'count');
  const lines = [
    `RUN_LABEL=${RUN_LABEL}`,
    `TITLE=${TITLE}`,
    `EVENT_IDS=${JSON.stringify(data.setup_data ? data.setup_data.eventIds : [])}`,
    `reserve_requests=${sent}`,
    `reserve_rps=${fmt(sent / (data.state.testRunDurationMs / 1000))}`,
    `test_duration_s=${fmt(data.state.testRunDurationMs / 1000)}`,
    `reserve_p50_ms=${fmt(metric(data, 'reserve_duration', 'med'))}`,
    `reserve_p95_ms=${fmt(metric(data, 'reserve_duration', 'p(95)'))}`,
    `reserve_p99_ms=${fmt(metric(data, 'reserve_duration', 'p(99)'))}`,
    `reserve_max_ms=${fmt(metric(data, 'reserve_duration', 'max'))}`,
    `accepted=${metric(data, 'reserve_accepted', 'count')}`,
    `status_409=${metric(data, 'reserve_409', 'count')}`,
    `status_429=${metric(data, 'reserve_429', 'count')}`,
    `status_4xx_other=${metric(data, 'reserve_4xx_other', 'count')}`,
    `status_5xx=${metric(data, 'reserve_5xx', 'count')}`,
    `network_errors=${metric(data, 'reserve_network_error', 'count')}`,
    `error_rate_pct=${fmt(sent > 0 ? (failed / sent) * 100 : 0)}`,
    `dropped_iterations=${metric(data, 'dropped_iterations', 'count')}`,
    `max_vus=${metric(data, 'vus_max', 'max')}`,
  ];
  return {
    stdout: `\n=== k6 summary ===\n${lines.join('\n')}\n`,
    [`/scripts/results/${RUN_LABEL}.json`]: JSON.stringify(data, null, 2),
  };
}
