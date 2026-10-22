"""One decision round: circuit check -> snapshots -> strategies -> validation -> risk -> fills -> positions (PRD S3, S4).
Local version: each strategy is a subprocess (stdin = snapshot, stdout = decision). On the VPS it becomes a sandboxed
systemd unit (S3 L2) and LLM calls go through the gateway; neither exists yet.
Usage: python -m wtr.runner.round --root DATA --ledger SOCK --t 2026-10-22T04:00:30Z [--arms A0,B]
       python -m wtr.runner.round --root DATA --ledger SOCK --value-hour 2026-10-22T05:00:00Z"""
import argparse
import json
import secrets
import subprocess
import sys
import time
import tomllib
from decimal import Decimal
from pathlib import Path

from wtr.contracts import canonical
from wtr.gateway import server as gw
from wtr.ledger import chain, client
from wtr.pit import snapshot as pit
from wtr.risk.clip import clip
from wtr.sim import fill

REPO = Path(__file__).resolve().parents[3]
CLOSE_TO_T_NS = 30 * 10**9
CLOSE_TO_DEADLINE_NS = 600 * 10**9
MAX_OUTPUT = 64 * 1024
GATEWAY_PORT = 8787


def strs(x):
    """Decimal -> str recursively, so decisions can be hashed (no floats in canonical JSON)."""
    if isinstance(x, dict):
        return {k: strs(v) for k, v in x.items()}
    if isinstance(x, list):
        return [strs(v) for v in x]
    return str(x) if isinstance(x, Decimal) else x


def _reject_constant(c):
    raise ValueError(f"{c} is not allowed")


def validate(stdout: bytes, pool: list[str], arm: str) -> tuple[dict | None, str]:
    """PRD S3 L4. Returns (decision with Decimal weights, "ok") or (None, reason)."""
    if len(stdout) > MAX_OUTPUT:
        return None, "output over 64KB"
    try:
        d = json.loads(stdout, parse_float=Decimal, parse_constant=_reject_constant)
        w = None if d["weights"] is None else {k: Decimal(v) for k, v in d["weights"].items()}
        conf = Decimal(d["confidence"])
        rationale = d["rationale"]
    except (ValueError, KeyError, TypeError, AttributeError, ArithmeticError) as e:
        return None, f"unreadable: {e}"
    if not (isinstance(rationale, str) or (arm == "J" and isinstance(rationale, (dict, list)))):
        return None, "rationale must be text"
    if not (conf.is_finite() and 0 <= conf <= 1):
        return None, "confidence must be in [0, 1]"
    if w is None:
        return {"weights": None, "rationale": rationale, "confidence": conf}, "ok"  # hold (ADR 0001 amendment)
    if any(isinstance(v, bool) for v in d["weights"].values()):
        return None, "boolean weight"
    if unknown := sorted(set(w) - set(pool) - {"CASH"}):
        return None, f"unknown symbols {unknown}"
    if any(not v.is_finite() or v < 0 for v in w.values()):
        return None, "weights must be finite and non-negative"
    if abs(sum(w.values(), Decimal(0)) - 1) > Decimal("1e-6"):
        return None, "weights must sum to 1"
    return {"weights": w, "rationale": rationale, "confidence": conf}, "ok"


def circuit_ok(root: Path, t: int, pool: list[str]) -> bool:
    """PRD S4 L1: the latest collected spot 1m candle must close within 2 minutes before t."""
    latest = 0
    for env in pit.read(root, "spot", "kline_1m", t - 10 * 60 * 10**9, t):
        if env["kind"] == "ws":
            latest = max(latest, pit.raw(env)["data"]["k"]["T"] * 10**6)
    return latest >= t - 2 * 60 * 10**9


def holdings(root: Path, arm: str, t: int) -> dict[str, Decimal]:
    return {s: Decimal(q) for s, q in pit.holdings_at(root, arm, t).items()}


def run_round(root: Path, sock: Path, t: int, arms: dict, gateway: Path | None = None) -> None:
    def log(type_: str, payload: dict) -> dict:
        return client.append(sock, type_, strs(payload))

    deadline = t - CLOSE_TO_T_NS + CLOSE_TO_DEADLINE_NS
    live = time.time_ns() < deadline + fill.FILL_WINDOW_NS
    pool = pit.pool_at(root, t)
    last_pool = [json.loads(l)["payload"]["sha256"] for _, _, l in chain.events(root) if '"type":"universe_change"' in l]
    if not last_pool or last_pool[-1] != pool["sha256"]:
        log("universe_change", {"t": t, "sha256": pool["sha256"], "spot": pool["spot"]})
    breaker = not circuit_ok(root, t, pool["spot"])
    if breaker:
        log("circuit_break", {"t": t, "reason": "latest spot 1m candle older than t - 2min"})

    decisions = {}
    for arm, cfg in arms.items():
        if breaker:
            decisions[arm] = None
            continue
        spec = json.loads((REPO / cfg["spec"]).read_text())
        token = secrets.token_hex(16)
        snap, h, late = pit.build(root, t, arm, spec, round_token=token, live=live)
        snap_ev = log("snapshot", {"arm": arm, "t": t, "spec": spec, "snapshot_hash": h})
        if late:
            log("watermark_timeout", {"arm": arm, "t": t, "channels": late})
        budget = (deadline - time.time_ns()) / 1e9 if live else 600
        if gateway:
            gw.control(gateway, {"op": "register", "token": token, "arm": arm, "t": t,
                                 "deadline": deadline if live else time.time_ns() + 600 * 10**9})
        env = {"PATH": "/usr/bin:/bin", "WTR_GATEWAY_URL": f"http://127.0.0.1:{GATEWAY_PORT}/v1/chat/completions"}  # nothing else, no keys
        try:
            p = subprocess.run([sys.executable, str(REPO / cfg["entry"])], input=canonical.dumps(snap), env=env,
                               capture_output=True, timeout=max(budget, 0.001), cwd=REPO / Path(cfg["entry"]).parent)
            decision, status = validate(p.stdout, pool["spot"], arm) if p.returncode == 0 else (None, f"exit code {p.returncode}")
            output = p.stdout[:MAX_OUTPUT].decode(errors="replace")
        except subprocess.TimeoutExpired:
            decision, status, output = None, "timeout", ""
        if status != "ok":
            print(f"{arm}: {status}; stderr: {p.stderr.decode(errors='replace')[-2000:] if status != 'timeout' else ''}", file=sys.stderr)
        llm_calls = gw.control(gateway, {"op": "close", "token": token})["seqs"] if gateway else []
        log("decision", {"arm": arm, "t": t, "version": cfg["version"], "snapshot_hash": h, "snapshot_seq": snap_ev["seq"],
                         "status": status, "output": strs(decision) if decision else output, "llm_calls": llm_calls})
        decisions[arm] = decision

    if live:
        time.sleep(max(0, (deadline + fill.FILL_WINDOW_NS - time.time_ns()) / 1e9))
    held = {s for arm in arms for s in holdings(root, arm, t) if s != "USDT"}
    symbols = sorted(set(pool["spot"]) | held)
    books = fill.books_after(root, deadline, symbols)
    fallback = fill.first_open_after(root, deadline, symbols)
    rules = fill.rules_at(root, deadline + fill.FILL_WINDOW_NS, symbols)
    prices = {s: books[s].mid if books[s] else fallback.get(s) for s in symbols}

    for arm in arms:
        h0 = holdings(root, arm, t)
        d = decisions[arm]
        target = d["weights"] if d and d["weights"] is not None else fill.current_weights(h0, prices)  # hold: current holdings at fill-time prices
        clipped, rules_hit = clip(target, pool["spot"])
        if rules_hit:
            log("risk_clip", {"arm": arm, "t": t, "before": target, "after": clipped, "rules": rules_hit})
        h1, events = fill.execute(h0, clipped, books, fallback, rules)
        for type_, payload in events:
            log(type_, {"arm": arm, "t": t} | payload)
        log("position", {"arm": arm, "t": t, "kind": "round", "holdings": {s: q for s, q in h1.items() if q > 0 or s == "USDT"},
                         "nav": fill.value(h1, prices)})


def value_hour(root: Path, sock: Path, hour_end: int, arms: list[str]) -> None:
    """PRD S4 L4: hourly valuation at the exchange 1h close."""
    for arm in arms:
        h = holdings(root, arm, hour_end)
        held = [s for s in h if s != "USDT"]
        closes = fill.closes_1h(root, hour_end, held)
        if missing := sorted(set(held) - set(closes)):
            raise RuntimeError(f"{arm}: no 1h close for {missing}")
        client.append(sock, "position", strs({"arm": arm, "t": hour_end, "kind": "hourly", "holdings": h,
                                              "prices": closes, "nav": fill.value(h, closes)}))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, required=True)
    ap.add_argument("--ledger", type=Path, required=True)
    ap.add_argument("--t", type=pit.parse_t)
    ap.add_argument("--value-hour", type=pit.parse_t)
    ap.add_argument("--arms", default="A0")
    ap.add_argument("--gateway", type=Path, help="gateway control socket; needed for LLM arms")
    a = ap.parse_args()
    cfg = tomllib.loads((REPO / "config" / "arms.toml").read_text())
    arms = {k: cfg[k] for k in a.arms.split(",")}
    if a.value_hour:
        value_hour(a.root, a.ledger, a.value_hour, list(arms))
    else:
        run_round(a.root, a.ledger, a.t, arms, a.gateway)


if __name__ == "__main__":
    main()
