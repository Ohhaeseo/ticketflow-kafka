"""MSG-02 / MSG-03 프로세스 강제 종료 실험 (Windows 전용, 표준 라이브러리만 사용).

부하를 거는 중에 애플리케이션 JVM을 강제 종료(taskkill /F, SIGKILL과 같음)한 뒤 다시 띄우고,
모든 접수 건이 유실·중복 없이 처리되는지 확인한다.

  python experiments/crash_test.py spike --kill-at 3.5 --label crash-spike
  python experiments/crash_test.py stage --rate 300 --duration 10 --kill-at 5 --label crash-midload

전제: 인프라(compose)가 실행 중이고, 8080 포트에 애플리케이션이 떠 있지 않아야 한다 (스크립트가 직접 띄운다).
"""
from __future__ import annotations

import argparse
import importlib.util
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BACKEND = ROOT / "backend"
LOGS = ROOT / "load-tests" / "results"

_spec = importlib.util.spec_from_file_location("loadrun", ROOT / "load-tests" / "run.py")
loadrun = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(loadrun)

CHECK_SQL = """
\\pset footer off
\\echo '--- exactly-once check'
SELECT count(DISTINCT r.id)                                                        AS reservations,
       count(DISTINCT r.id) FILTER (WHERE r.status = 'QUEUED')                     AS still_queued,
       count(p.message_id)                                                         AS processing_records,
       count(p.message_id) FILTER (WHERE p.outcome LIKE 'SKIPPED%')                AS skipped_records,
       (SELECT count(*) FROM (
            SELECT p2.reservation_id FROM processed_messages p2
              JOIN reservations r2 ON r2.id = p2.reservation_id
              JOIN events e2 ON e2.id = r2.event_id
             WHERE e2.title = :'title' AND p2.outcome NOT LIKE 'SKIPPED%'
             GROUP BY p2.reservation_id HAVING count(*) > 1) d)                   AS applied_twice,
       count(DISTINCT r.id) FILTER (WHERE NOT EXISTS (
            SELECT 1 FROM processed_messages p3
             WHERE p3.reservation_id = r.id AND p3.outcome NOT LIKE 'SKIPPED%'))   AS never_applied
  FROM reservations r
  JOIN events e ON e.id = r.event_id
  LEFT JOIN processed_messages p ON p.reservation_id = r.id
 WHERE e.title = :'title';
"""


def health_ok(url: str) -> bool:
    try:
        with urllib.request.urlopen(f"{url}/actuator/health", timeout=2) as resp:
            return resp.status == 200
    except Exception:  # noqa: BLE001
        return False


def start_app(log_path: Path, app_url: str, timeout_s: float = 180) -> subprocess.Popen:
    log = open(log_path, "w", encoding="utf-8", errors="replace")
    proc = subprocess.Popen([str(BACKEND / "gradlew.bat"), "-p", str(BACKEND), "bootRun", "--console=plain"],
                            cwd=BACKEND, stdout=log, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if health_ok(app_url):
            return proc
        if proc.poll() is not None:
            raise RuntimeError(f"application exited during startup; see {log_path}")
        time.sleep(1)
    raise TimeoutError("application did not become healthy")


def pid_listening_on(port: int) -> int | None:
    out = subprocess.run(["netstat", "-ano", "-p", "tcp"], capture_output=True, text=True).stdout
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 5 and parts[1].endswith(f":{port}") and parts[3] == "LISTENING":
            return int(parts[4])
    return None


def hard_kill_app(gradle: subprocess.Popen, port: int) -> int | None:
    """애플리케이션 JVM(포트 소유 프로세스)을 강제 종료한다. Gradle 클라이언트도 정리한다."""
    pid = pid_listening_on(port)
    if pid:
        subprocess.run(["taskkill", "/F", "/PID", str(pid)], capture_output=True)
    subprocess.run(["taskkill", "/F", "/T", "/PID", str(gradle.pid)], capture_output=True)
    return pid


def snapshot(env: dict[str, str], title: str) -> str:
    return loadrun.psql(env, """
        SELECT 'reservations=' || count(r.id)
            || ' queued=' || count(r.id) FILTER (WHERE r.status = 'QUEUED')
            || ' confirmed=' || count(r.id) FILTER (WHERE r.status = 'CONFIRMED')
            || ' unpublished_outbox=' || (SELECT count(*) FROM outbox_events o JOIN reservations r2 ON r2.id = o.reservation_id
                                            JOIN events e2 ON e2.id = r2.event_id
                                           WHERE e2.title = :'title' AND o.published_at IS NULL)
          FROM reservations r JOIN events e ON e.id = r.event_id WHERE e.title = :'title';
        """, {"title": title}).strip()


def wait_drained(env: dict[str, str], title: str, timeout_s: float = 300) -> float:
    start = time.monotonic()
    query = """
        SELECT (SELECT count(*) FROM reservations r JOIN events e ON e.id = r.event_id
                 WHERE e.title = :'title' AND r.status = 'QUEUED')
             + (SELECT count(*) FROM outbox_events o JOIN reservations r ON r.id = o.reservation_id
                  JOIN events e ON e.id = r.event_id WHERE e.title = :'title' AND o.published_at IS NULL);
    """
    while time.monotonic() - start < timeout_s:
        if int(loadrun.psql(env, query, {"title": title}).strip() or 0) == 0:
            return time.monotonic() - start
        time.sleep(0.5)
    raise TimeoutError("not drained")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("scenario", choices=["spike", "stage"])
    parser.add_argument("--label", required=True)
    parser.add_argument("--kill-at", type=float, required=True, help="k6 시작 후 몇 초에 JVM을 죽일지")
    parser.add_argument("--users", type=int, default=1000)
    parser.add_argument("--tickets", type=int, default=100)
    parser.add_argument("--rate", type=int, default=300)
    parser.add_argument("--duration", type=int, default=10)
    parser.add_argument("--app-url", default="http://localhost:8080")
    args = parser.parse_args()

    if health_ok(args.app_url):
        print("An application is already running on 8080. Stop it first.", file=sys.stderr)
        return 2

    env = loadrun.read_env()
    LOGS.mkdir(exist_ok=True)
    prefix = "LOAD-01" if args.scenario == "spike" else "PERF-01"
    title = f"{prefix} {args.label}"
    script = "reservation-spike.js" if args.scenario == "spike" else "reservation-ramp.js"

    app = start_app(LOGS / f"{args.label}-app1.log", args.app_url)
    k6_cmd = ["docker", "run", "--rm", "-v", f"{loadrun.LOAD_TESTS}:/scripts"]
    for key, value in {"BASE_URL": "http://host.docker.internal:8080", "RUN_LABEL": args.label, "TITLE": title,
                       "USERS": str(args.users), "TICKETS": str(args.tickets), "RATE": str(args.rate),
                       "DURATION_S": str(args.duration)}.items():
        k6_cmd += ["-e", f"{key}={value}"]
    k6_cmd += [loadrun.K6_IMAGE, "run", "--quiet", f"/scripts/{script}"]

    k6 = subprocess.Popen(k6_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8")
    started = time.monotonic()
    time.sleep(max(0.0, args.kill_at - (time.monotonic() - started)))
    before_kill = snapshot(env, title)
    killed_pid = hard_kill_app(app, 8080)
    killed_at = time.monotonic() - started
    after_kill = snapshot(env, title)
    k6_out, _ = k6.communicate()
    k6_summary = k6_out[k6_out.find("=== k6 summary ==="):] if "=== k6 summary ===" in k6_out else k6_out

    restart_started = time.monotonic()
    app = start_app(LOGS / f"{args.label}-app2.log", args.app_url)
    restart_s = time.monotonic() - restart_started
    drain_s = wait_drained(env, title)
    after_restart_log = (LOGS / f"{args.label}-app2.log").read_text(encoding="utf-8", errors="replace")
    duplicates = len(re.findall(r"Duplicate message skipped", after_restart_log))
    already_final = len(re.findall(r"Reservation already final", after_restart_log))

    check = loadrun.psql(env, CHECK_SQL, {"title": title}, tuples_only=False)
    result = loadrun.psql(env, (loadrun.LOAD_TESTS / "sql" / "result.sql").read_text(encoding="utf-8"),
                          {"title": title}, tuples_only=False)
    hard_kill_app(app, 8080)  # 실험이 끝나면 앱을 내린다

    report = "\n".join([
        f"# crash experiment {args.label} ({title})",
        f"scenario={args.scenario} kill_at={killed_at:.1f}s killed_pid={killed_pid}",
        f"before_kill: {before_kill}",
        f"after_kill:  {after_kill}",
        f"restart_to_healthy_s={restart_s:.1f} drain_after_restart_s={drain_s:.1f}",
        f"after_restart_log: duplicate_message_skipped={duplicates} reservation_already_final={already_final}",
        k6_summary.strip(),
        check.strip(),
        result.strip(),
        "",
    ])
    (LOGS / f"{args.label}.txt").write_text(report, encoding="utf-8")
    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
