// k6 load against the gateway's HTTP endpoint (not xk6-kafka: the gateway, its acks=all publish and
// the 202 contract are part of what is being measured).
//   k6 run -e RATE=100 -e DURATION=60s -e RUN=step100 load/payments.js
// Open model (constant-arrival-rate): the offered load stays fixed even when the system slows down,
// so saturation shows up as latency and lag instead of being hidden by fewer requests.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const RATE = Number(__ENV.RATE || 50);
const RUN = __ENV.RUN || `run${Date.now()}`;
const URL = __ENV.URL || 'http://localhost:8080';
const CCY = ['GBP', 'EUR', 'USD']; // ACC-n has currency CCY[n % 3], same as the seed migration

const accepted = new Counter('payments_accepted');

export const options = {
  discardResponseBodies: false,
  scenarios: {
    load: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: Math.max(20, RATE),
      maxVUs: Math.max(100, RATE * 4),
    },
  },
  thresholds: { http_req_failed: ['rate<0.01'] },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

const acc = (n) => `ACC-${String(n).padStart(6, '0')}`;
const rnd = (lo, hi) => lo + Math.floor(Math.random() * (hi - lo + 1));

export default function () {
  const a = rnd(1, 1000);
  let b = rnd(1, 1000);
  while (b === a || b % 3 !== a % 3) b = rnd(1, 1000);
  const amount = Math.max(0.01, Math.min(25000, Math.round(Math.exp(3.5 + 1.2 * gauss()) * 100) / 100));
  const res = http.post(`${URL}/payments`,
    JSON.stringify({ fromAccount: acc(a), toAccount: acc(b), amount, currency: CCY[a % 3] }),
    {
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `k6-${RUN}-${__VU}-${__ITER}` },
      tags: { name: 'POST /payments' },
      timeout: '15s',
    });
  if (check(res, { '202': (r) => r.status === 202 })) accepted.add(1);
}

function gauss() { // Box-Muller
  return Math.sqrt(-2 * Math.log(1 - Math.random())) * Math.cos(2 * Math.PI * Math.random());
}
