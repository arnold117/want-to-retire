"""Point-in-time snapshots (PRD S2, ADR 0002): only records with available_ts <= t.
Usage: python -m wtr.pit.snapshot --root DATA --t 2026-10-22T04:00:30Z --arm A0 --spec SPEC.json [--live] [--ledger SOCK]"""
import argparse
import datetime as dt
import gzip
import hashlib
import json
import time
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from wtr.collect.writer import HOUR_NS, hour_path
from wtr.contracts import canonical

INITIAL_HOLDINGS = {"USDT": "10000"}
UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


class SnapshotAssertFail(AssertionError):
    pass


def ns(span: str) -> int:
    return 0 if span in ("0", "") else int(span[:-1]) * UNITS[span[-1]] * 10**9


def files(root: Path, market: str, channel: str, hour: int) -> list[Path]:
    p = hour_path(root, market, channel, hour)
    return [q for q in (p.with_name(p.name + ".open"), p, p.with_name(p.name + ".gz")) if q.exists()]


def read(root: Path, market: str, channel: str, t_from: int, t: int):
    """Envelopes with t_from <= available_ts <= t. Reads one hour past t: a record that was written late lands in a later file."""
    for hour in range(t_from // HOUR_NS, t // HOUR_NS + 2):
        for p in files(root, market, channel, hour):
            with (gzip.open if p.name.endswith(".gz") else open)(p, "rt", encoding="utf-8") as f:
                for line in f:
                    if not line.endswith("\n"):
                        continue
                    env = json.loads(line)
                    if t_from <= env["available_ts"] <= t:
                        yield env


def raw(env: dict):
    return json.loads(env["raw"], parse_float=str)  # keep any non-string number as its exact text


def watermark_channel(market: str, channel: str) -> tuple[str, str] | None:
    """A busy channel on the same connection: once it has a record after t, everything before t on that connection is on disk."""
    if channel == "exchangeInfo":
        return None  # hourly REST; nothing arrives within the wait
    if channel == "openInterest":
        return "um", "openInterest"  # ponytail: concurrent REST polls are not strictly ordered
    if market == "spot":
        return "spot", "depth20"
    return ("um", "depth20") if channel == "depth20" else ("um", "markPrice")


def watermark(root: Path, t: int, needed: set[tuple[str, str]], timeout_s: float = 60) -> list[str]:
    """Wait until each needed channel has a record with available_ts > t. Returns the ones that timed out."""
    deadline = time.monotonic() + timeout_s
    while True:
        late = [f"{m}/{c}" for m, c in sorted(needed)
                if not any(True for _ in read(root, m, c, t + 1, t + 3600 * 10**9))]
        if not late or time.monotonic() > deadline:
            return late
        time.sleep(1)


def _kline_rows(env: dict) -> list[dict]:
    body = raw(env)
    if env["kind"] == "ws":
        k = body["data"]["k"]
        bars = [(k["s"], k["t"], k["T"], k["o"], k["h"], k["l"], k["c"], k["v"], k["q"], k["n"])]
    elif env["http_status"] == 200:
        sym = parse_qs(urlsplit(env["stream"].split(" ", 1)[1]).query)["symbol"][0]
        bars = [(sym, r[0], r[6], r[1], r[2], r[3], r[4], r[5], r[7], r[8]) for r in body]
    else:
        return []
    keys = ("symbol", "open_time", "close_time", "o", "h", "l", "c", "v", "q", "n")
    return [dict(zip(keys, b), available_ts=env["available_ts"], source=env["source"]) for b in bars]


def _dedup_key(market: str, channel: str, env: dict, data: dict):
    if channel == "depth20":
        return (env["stream"], data["lastUpdateId"]) if market == "spot" else (env["stream"], data["E"] // 1000)
    if channel == "markPrice":
        return env["stream"], data["E"]
    if channel == "forceOrder":
        return canonical.dumps(data["o"]), data["E"]
    if channel == "openInterest":
        return data["symbol"], data["time"]
    return env["stream"], env["available_ts"]


def query(root: Path, t: int, spec: list, symbols: set[str]) -> list[dict]:
    """One spec entry (market, channel, period, lookback, agg) -> rows, deterministic order."""
    market, channel, period, lookback, agg = spec
    t_from = t - ns(lookback)
    if channel.startswith("kline"):
        best: dict[tuple, dict] = {}
        for env in read(root, market, channel, t_from - ns(period), t):
            for row in _kline_rows(env):
                key = (row["symbol"], row["open_time"])
                if row["symbol"] in symbols and row["open_time"] * 10**6 >= t_from and (key not in best or row["available_ts"] < best[key]["available_ts"]):
                    best[key] = row
        return [best[k] for k in sorted(best)]

    seen, rows = set(), []
    for env in read(root, market, channel, t_from, t):
        if env["kind"] == "rest" and env["http_status"] != 200:
            continue
        body = raw(env)
        data = body["data"] if env["kind"] == "ws" else body
        sym = data.get("s") or data.get("symbol") or env["stream"].split("@")[0].upper()
        if channel == "forceOrder":
            sym = data["o"]["s"]
        if sym not in symbols:
            continue
        key = _dedup_key(market, channel, env, data)
        if key in seen:
            continue
        seen.add(key)
        rows.append({"symbol": sym, "available_ts": env["available_ts"], "source": env["source"], "data": data})
    rows.sort(key=lambda r: (r["symbol"], r["available_ts"]))
    if agg == "last":
        p = ns(period) or 1 << 62  # period "0": the single latest row per symbol
        last: dict[tuple, dict] = {}
        for r in rows:
            last[(r["symbol"], r["available_ts"] // p)] = r
        rows = [last[k] for k in sorted(last)]
    return rows


def pool_at(root: Path, t: int) -> dict:
    best = None
    for p in (root / "universe").glob("????-??.json"):
        data = p.read_bytes()
        u = json.loads(data)
        if u["available_ts"] <= t and (best is None or u["available_ts"] > best[0]["available_ts"]):
            best = (u, hashlib.sha256(data).hexdigest())
    if best is None:
        raise SnapshotAssertFail(f"no universe available at t={t}")
    u, sha = best
    return {"spot": [c["spot"] for c in u["coins"]], "um": [c["um"] for c in u["coins"] if c["um"]], "available_ts": u["available_ts"], "sha256": sha}


def holdings_at(root: Path, arm: str, t: int) -> dict:
    from wtr.ledger import chain  # ponytail: scans the whole chain; keep a per-arm index once the chain is large
    h = INITIAL_HOLDINGS
    for _, _, line in chain.events(root):
        ev = json.loads(line)
        if ev["ts"] > t:
            break
        if ev["type"] == "position" and ev["payload"].get("arm") == arm:
            h = ev["payload"]["holdings"]
    return h


def build(root: Path, t: int, arm: str, spec: list, round_token: str = "", upstream: dict | None = None, live: bool = False) -> tuple[dict, str, list[str]]:
    """Returns (snapshot, hash, channels whose watermark timed out)."""
    if t > time.time_ns():
        raise SnapshotAssertFail("t is in the future: records up to t are still arriving")
    pool = pool_at(root, t)
    late = []
    if live:
        late = watermark(root, t, {w for m, c, *_ in spec if (w := watermark_channel(m, c))})
    data = [query(root, t, s, set(pool["spot"] if s[0] == "spot" else pool["um"])) for s in spec]
    snap = {"t": t, "arm": arm, "round_token": round_token, "pool": pool, "holdings": holdings_at(root, arm, t),
            "upstream": upstream, "spec": [list(s) for s in spec], "data": data}
    bad = [r for rows in data for r in rows if r["available_ts"] > t] + ([pool] if pool["available_ts"] > t else [])
    if bad:
        raise SnapshotAssertFail(f"{len(bad)} records after t, first available_ts={bad[0]['available_ts']}")
    return snap, snapshot_hash(snap), late


def snapshot_hash(snap: dict) -> str:
    return canonical.sha256({k: v for k, v in snap.items() if k != "round_token"})


def parse_t(s: str) -> int:
    return int(s) if s.isdigit() else int(dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()) * 10**9


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, required=True)
    ap.add_argument("--t", type=parse_t, required=True)
    ap.add_argument("--arm", required=True)
    ap.add_argument("--spec", type=Path, required=True, help="JSON list of [market, channel, period, lookback, agg]")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--ledger", type=Path, help="ledger socket; appends a snapshot event")
    a = ap.parse_args()
    spec = json.loads(a.spec.read_text())
    t0 = time.monotonic()
    snap, h, late = build(a.root, a.t, a.arm, spec, live=a.live)
    print(f"hash {h}  built in {time.monotonic() - t0:.1f}s  size {len(canonical.dumps(snap)) // 1024}KB  late {late}")
    for s, rows in zip(spec, snap["data"]):
        print(f"  {s}: {len(rows)} rows")
    if a.ledger:
        from wtr.ledger import client
        print(client.append(a.ledger, "snapshot", {"arm": a.arm, "t": a.t, "spec": spec, "snapshot_hash": h}))


if __name__ == "__main__":
    main()
