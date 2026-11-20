// PERF-01 부하 증가 (PRD §12.1) — 한 번 실행에 한 단계(RATE)를 고정 도착률로 측정한다
//
// 50 → 100 → 300 → 500 RPS를 각각 따로 실행해 단계마다 데이터(공연)를 분리한다.
//   python load-tests/run.py stage --rate 50  --duration 20 --label perf-async-50
//   python load-tests/run.py stage --rate 100 --duration 20 --label perf-async-100 ...
//
// 사용자는 시드 1~1000을 돌려 쓰고, 1,000건마다 새 공연으로 넘어가 (공연, 사용자) 중복이 없게 한다.
// 공연마다 티켓 1,000장이라 모든 요청이 CONFIRMED가 된다 (매진이 아니라 처리량을 본다).
import exec from 'k6/execution';
import { createEvent, reserve, summary, RUN_LABEL, SEED_USERS } from './lib/common.js';

const RATE = Number(__ENV.RATE || 50);
const DURATION_S = Number(__ENV.DURATION_S || 20);
const TICKETS_PER_EVENT = SEED_USERS;

export const options = {
  scenarios: {
    stage: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: `${DURATION_S}s`,
      preAllocatedVUs: Math.min(Math.max(RATE, 50), 300),
      maxVUs: 1000,
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  const events = Math.ceil((RATE * DURATION_S) / SEED_USERS) + 1;
  const eventIds = [];
  for (let i = 0; i < events; i++) {
    eventIds.push(createEvent(TICKETS_PER_EVENT));
  }
  return { eventIds };
}

export default function (data) {
  const i = exec.scenario.iterationInTest;
  const userId = 1 + (i % SEED_USERS);
  const eventId = data.eventIds[Math.floor(i / SEED_USERS) % data.eventIds.length];
  reserve(userId, eventId, `${RUN_LABEL}-e${eventId}-u${userId}`);
}

export function handleSummary(data) {
  return summary(data);
}
