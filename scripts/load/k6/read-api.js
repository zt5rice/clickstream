import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  scenarios: {
    reads: {
      executor: "ramping-vus",
      startVUs: 1,
      stages: [
        { duration: "20s", target: 10 },
        { duration: "30s", target: 20 },
        { duration: "20s", target: 0 },
      ],
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<500"],
  },
};

const base = __ENV.BASE_URL || "http://localhost:8000";
const endpoints = [
  "/api/v1/summary",
  "/api/v1/top/pages?limit=5",
  "/api/v1/events/recent?limit=10",
];

export default function () {
  const path = endpoints[Math.floor(Math.random() * endpoints.length)];
  // The API rate-limits per client IP (P2-05, default 60 req/min). Simulate
  // different clients per VU so the load test measures the API, not the limiter.
  const fakeClientIp = `10.${__VU}.${Math.floor(Math.random() * 200) + 1}.1`;
  const res = http.get(base + path, {
    headers: { "X-Forwarded-For": fakeClientIp },
  });
  check(res, { "status 200": (r) => r.status === 200 });
  sleep(0.1);
}
