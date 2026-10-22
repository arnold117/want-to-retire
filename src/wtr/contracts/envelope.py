"""Raw record envelope (ADR 0003)."""
import json
import subprocess
import time

FIELDS = ("available_ts", "mono_ns", "boot_id", "conn_id", "source", "kind", "stream", "http_status", "raw")
SOURCES = {"live", "backfill", "history"}
KINDS = {"ws", "rest", "event"}


def _boot_id() -> str:
    try:
        with open("/proc/sys/kernel/random/boot_id") as f:
            return f.read().strip()
    except OSError:  # macOS, for local runs
        return subprocess.run(["sysctl", "-n", "kern.bootsessionuuid"], capture_output=True, text=True).stdout.strip()


BOOT_ID = _boot_id()


def stamp() -> tuple[int, int]:
    """(available_ts, mono_ns), read together."""
    return time.time_ns(), time.monotonic_ns()


def make(ts: tuple[int, int], conn_id: str, source: str, kind: str, stream: str, raw: str, http_status: int | None = None) -> dict:
    env = dict(zip(FIELDS, (ts[0], ts[1], BOOT_ID, conn_id, source, kind, stream, http_status, raw)))
    check(env)
    return env


def check(env: dict) -> None:
    if tuple(env) != FIELDS:
        raise ValueError(f"envelope fields {tuple(env)}")
    if not (isinstance(env["available_ts"], int) and isinstance(env["mono_ns"], int)):
        raise ValueError("timestamps must be int")
    if env["source"] not in SOURCES or env["kind"] not in KINDS:
        raise ValueError(f"bad source/kind {env['source']}/{env['kind']}")
    if not isinstance(env["raw"], str):
        raise ValueError("raw must be str")
    if (env["kind"] == "rest") != (env["http_status"] is not None):
        raise ValueError("http_status is for REST only")


def dumps(env: dict) -> str:
    return json.dumps(env, ensure_ascii=False, separators=(",", ":"))
