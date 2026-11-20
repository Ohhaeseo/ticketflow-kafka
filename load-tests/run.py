"""TicketFlow 부하 테스트 실행기 (표준 라이브러리만 사용).

1. k6를 Docker 이미지로 실행한다 (PC에 k6 설치 불필요).
2. 실행 중 Hikari 커넥션 풀 지표를 샘플링한다 (/actuator/metrics).
3. 비동기 처리가 끝날 때까지(QUEUED = 0) 기다린다.
4. load-tests/sql/result.sql로 확정 지연·불변식·Outbox·처리량을 집계한다.
5. 결과를 load-tests/results/<label>.txt에 저장한다 (Git 제외).

사용:
  python load-tests/run.py spike --label load01-async
  python load-tests/run.py stage --rate 100 --duration 20 --label perf-async-100

전제: 인프라(compose)와 애플리케이션(local 프로필)이 실행 중이어야 한다.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

K6_IMAGE = "grafana/k6:2.3.0"
POSTGRES_CONTAINER = "ticketflow-postgres"
ROOT = Path(__file__).resolve().parent.parent
LOAD_TESTS = ROOT / "load-tests"
RESULTS = LOAD_TESTS / "results"


def read_env() -> dict[str, str]:
    env: dict[str, str] = {}
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                env[key.strip()] = value.strip()
    return env


class PoolSampler(threading.Thread):
    """Hikari active/pending 커넥션 수의 최댓값을 기록한다."""

    def __init__(self, app_url: str, interval_s: float = 0.25) -> None:
        super().__init__(daemon=True)
        self.app_url = app_url
        self.interval_s = interval_s
        self.stop_event = threading.Event()
        self.max_active = 0.0
        self.max_pending = 0.0
        self.samples = 0

    def _read(self, name: str) -> float:
        with urllib.request.urlopen(f"{self.app_url}/actuator/metrics/{name}", timeout=2) as resp:
            body = json.load(resp)
        return float(body["measurements"][0]["value"])

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.max_active = max(self.max_active, self._read("hikaricp.connections.active"))
                self.max_pending = max(self.max_pending, self._read("hikaricp.connections.pending"))
                self.samples += 1
            except Exception:  # noqa: BLE001 - 샘플 실패는 무시하고 계속한다
                pass
            self.stop_event.wait(self.interval_s)


def psql(env: dict[str, str], sql: str, variables: dict[str, str] | None = None, tuples_only: bool = True) -> str:
    cmd = ["docker", "exec", "-i", POSTGRES_CONTAINER, "psql",
           "-U", env.get("POSTGRES_USER", "ticketflow"), "-d", env.get("POSTGRES_DB", "ticketflow")]
    if tuples_only:
        cmd += ["-tA"]
    for key, value in (variables or {}).items():
        cmd += ["-v", f"{key}={value}"]
    return subprocess.run(cmd, input=sql, capture_output=True, text=True, encoding="utf-8", check=True).stdout


def wait_until_processed(env: dict[str, str], title: str, timeout_s: float) -> float:
    """QUEUED가 0이 될 때까지 기다리고, k6 종료 후 걸린 시간을 반환한다."""
    start = time.monotonic()
    query = ("SELECT count(*) FROM reservations r JOIN events e ON e.id = r.event_id "
             "WHERE e.title = :'title' AND r.status = 'QUEUED';")
    while time.monotonic() - start < timeout_s:
        if int(psql(env, query, {"title": title}).strip() or 0) == 0:
            return time.monotonic() - start
        time.sleep(0.5)
    raise TimeoutError(f"reservations still QUEUED after {timeout_s}s for title={title}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("scenario", choices=["spike", "stage"])
    parser.add_argument("--label", required=True, help="결과 파일 이름과 공연 title에 쓰인다")
    parser.add_argument("--users", type=int, default=1000, help="spike: 동시 사용자 수")
    parser.add_argument("--tickets", type=int, default=100, help="spike: 티켓 수")
    parser.add_argument("--rate", type=int, default=50, help="stage: 초당 요청 수")
    parser.add_argument("--duration", type=int, default=20, help="stage: 실행 시간(초)")
    parser.add_argument("--app-url", default="http://localhost:8080", help="호스트에서 본 애플리케이션 주소")
    parser.add_argument("--k6-base-url", default="http://host.docker.internal:8080", help="k6 컨테이너에서 본 주소")
    parser.add_argument("--wait-timeout", type=float, default=300)
    args = parser.parse_args()

    env = read_env()
    RESULTS.mkdir(exist_ok=True)
    prefix = "LOAD-01" if args.scenario == "spike" else "PERF-01"
    title = f"{prefix} {args.label}"
    script = "reservation-spike.js" if args.scenario == "spike" else "reservation-ramp.js"

    k6_env = {"BASE_URL": args.k6_base_url, "RUN_LABEL": args.label, "TITLE": title,
              "USERS": str(args.users), "TICKETS": str(args.tickets),
              "RATE": str(args.rate), "DURATION_S": str(args.duration)}
    cmd = ["docker", "run", "--rm", "-v", f"{LOAD_TESTS}:/scripts"]
    for key, value in k6_env.items():
        cmd += ["-e", f"{key}={value}"]
    cmd += [K6_IMAGE, "run", "--quiet", f"/scripts/{script}"]

    sampler = PoolSampler(args.app_url)
    sampler.start()
    started = time.monotonic()
    k6 = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
    k6_seconds = time.monotonic() - started
    sampler.stop_event.set()
    sampler.join()

    k6_out = k6.stdout[k6.stdout.find("=== k6 summary ==="):] if "=== k6 summary ===" in k6.stdout else k6.stdout
    if k6.returncode not in (0, 99):  # 99 = threshold 실패 (결과는 그대로 기록)
        print(k6.stdout, k6.stderr, sep="\n", file=sys.stderr)
        return k6.returncode

    drain_s = wait_until_processed(env, title, args.wait_timeout)
    sql_out = psql(env, (LOAD_TESTS / "sql" / "result.sql").read_text(encoding="utf-8"),
                   {"title": title}, tuples_only=False)

    report = "\n".join([
        f"# {title}",
        f"scenario={args.scenario} users={args.users} tickets={args.tickets} rate={args.rate} duration={args.duration}s",
        f"k6_wall_s={k6_seconds:.1f} drain_after_k6_s={drain_s:.1f}",
        f"hikari_max_active={sampler.max_active:.0f} hikari_max_pending={sampler.max_pending:.0f} (samples={sampler.samples})",
        k6_out.strip(),
        sql_out.strip(),
        "",
    ])
    (RESULTS / f"{args.label}.txt").write_text(report, encoding="utf-8")
    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
