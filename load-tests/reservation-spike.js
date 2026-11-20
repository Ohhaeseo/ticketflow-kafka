// LOAD-01 집중 예약 (PRD §12.1)
//
// 조건: 티켓 TICKETS장(기본 100)인 공연 1개에 고유 사용자 USERS명(기본 1,000)이 동시에 1회씩 예약한다.
//       VU 1개 = 사용자 1명 = 요청 1회 (per-vu-iterations). '초당 1,000요청'과는 다른 조건이다.
// 합격: 처리 완료 후 CONFIRMED = TICKETS, 초과 판매 0 (load-tests/sql/result.sql이 검사)
//
// 실행: python load-tests/run.py spike --label load01-async   (README 참고)
import exec from 'k6/execution';
import { createEvent, reserve, summary, RUN_LABEL, SEED_USERS } from './lib/common.js';

const USERS = Number(__ENV.USERS || 1000);
const TICKETS = Number(__ENV.TICKETS || 100);

if (USERS > SEED_USERS) {
  throw new Error(`USERS must be <= ${SEED_USERS} (seeded users)`);
}

export const options = {
  scenarios: {
    spike: {
      executor: 'per-vu-iterations',
      vus: USERS,
      iterations: 1,
      maxDuration: '2m',
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  return { eventIds: [createEvent(TICKETS)] };
}

export default function (data) {
  const userId = exec.vu.idInTest; // 1..USERS
  const eventId = data.eventIds[0];
  reserve(userId, eventId, `${RUN_LABEL}-e${eventId}-u${userId}`);
}

export function handleSummary(data) {
  return summary(data);
}
