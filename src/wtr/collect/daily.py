"""Daily checks wtr-daily (PRD S1 L5): gap report (T-1), candle reconciliation (T-2), hash spot check, mono_ns invariant.
Usage: python -m wtr.collect.daily --root DATA [--day YYYYMMDD]   (--day = the T-1 day; default yesterday UTC)"""
import argparse
import csv
import datetime as dt
import gzip
import hashlib
import io
import json
import os
import random
import sys
import tomllib
import urllib.error
import urllib.request
import zipfile
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from wtr.collect import universe
from wtr.collect.run import channel_of

VISION = "https://data.binance.vision/data"
GAP_CHANNELS = {("spot", "depth20"), ("um", "depth20"), ("um", "markPrice"), ("spot", "kline_1m"), ("um", "kline_1m")}


def records(root: Path, market: str, channel: str, day: str):
    for p in sorted((root / market / channel / day).glob("??.jsonl*")):
        if p.name.endswith(".tmp"):
            continue
        with (gzip.open if p.name.endswith(".gz") else open)(p, "rt", encoding="utf-8") as f:
            for line in f:
                if line.endswith("\n"):
                    yield p, json.loads(line)


def day_bounds(day: str) -> tuple[int, int]:
    start = int(dt.datetime.strptime(day, "%Y%m%d").replace(tzinfo=dt.UTC).timestamp()) * 10**9
    return start, start + 86400 * 10**9


def gaps(root: Path, day: str, silence_s: dict, coins: list[dict]) -> dict:
    """Intervals longer than the silence threshold with no live message, per stream. Also checks mono_ns per connection."""
    start, end = day_bounds(day)
    out, counts, mono_errors = {}, {}, []
    for market, channel in sorted(GAP_CHANNELS):
        last: dict[str, int] = {}
        found: dict[str, list] = defaultdict(list)
        mono: dict[tuple, int] = {}
        n = 0
        for path, env in records(root, market, channel, day):
            if env["kind"] != "ws":
                continue
            n += 1
            key = (path.name, env["boot_id"], env["conn_id"])
            if env["mono_ns"] < mono.get(key, 0):
                mono_errors.append(f"{market}/{channel}/{path.name} {env['conn_id']}")
            mono[key] = env["mono_ns"]
            s, t = env["stream"], env["available_ts"]
            if t - last.get(s, start) > silence_s[channel_of(s)] * 10**9:
                found[s].append([last.get(s, start), t])
            last[s] = max(t, last.get(s, start))
        for s, t in last.items():
            if end - t > silence_s[channel_of(s)] * 10**9:
                found[s].append([t, end])
        for s in expected_streams(market, channel, coins) - set(last):
            found[s].append([start, end])  # no message all day
        counts[f"{market}/{channel}"] = n
        for s, iv in found.items():
            out[f"{market}:{s}"] = {"intervals": iv, "seconds": sum(b - a for a, b in iv) / 1e9}
    return {"day": day, "messages": counts, "gaps": out, "mono_errors": mono_errors[:20]}


def expected_streams(market: str, channel: str, coins: list[dict]) -> set[str]:
    suffix = {("spot", "depth20"): "depth20@1000ms", ("um", "depth20"): "depth20@500ms", ("um", "markPrice"): "markPrice@1s"}.get((market, channel), channel)
    return {f"{c[market].lower()}@{suffix}" for c in coins if c[market]}


def vision_klines(market: str, symbol: str, day_iso: str) -> dict[int, tuple] | None:
    path = f"spot/daily/klines/{symbol}/1m" if market == "spot" else f"futures/um/daily/klines/{symbol}/1m"
    try:
        with urllib.request.urlopen(f"{VISION}/{path}/{symbol}-1m-{day_iso}.zip", timeout=60) as r:
            z = zipfile.ZipFile(io.BytesIO(r.read()))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    rows = csv.reader(io.TextIOWrapper(z.open(z.namelist()[0]), "utf-8"))
    out = {}
    for row in rows:
        if not row[0].isdigit():
            continue  # futures files have a header
        t = int(row[0])
        out[t // 1000 if t > 10**14 else t] = _bar(row[1:6] + [row[7], row[8]])  # spot files use microseconds since 2025-01
    return out


def _bar(fields: list) -> tuple:
    """open, high, low, close, volume, quote volume as Decimal; trade count as int."""
    return tuple(Decimal(x) for x in fields[:6]) + (int(fields[6]),)


def collected_klines(root: Path, market: str, day: str) -> dict[str, dict[int, tuple]]:
    """Closed 1m candles collected live or by backfill, deduplicated by open time (earliest available_ts wins)."""
    out: dict[str, dict[int, tuple]] = defaultdict(dict)
    best: dict[tuple, int] = {}
    days = [day, (dt.datetime.strptime(day, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")]  # 23:59 bars arrive after midnight
    for d in days:
        for _, env in records(root, market, "kline_1m", d):
            if env["kind"] == "ws":
                k = json.loads(env["raw"])["data"]["k"]
                bars = [(k["s"], k["t"], _bar([k["o"], k["h"], k["l"], k["c"], k["v"], k["q"], k["n"]]))]
            elif env["http_status"] == 200:
                sym = parse_qs(urlsplit(env["stream"].split(" ", 1)[1]).query)["symbol"][0]
                bars = [(sym, r[0], _bar(r[1:6] + [r[7], r[8]])) for r in json.loads(env["raw"])]
            else:
                continue
            for sym, t, bar in bars:
                if env["available_ts"] < best.get((sym, t), 1 << 63):
                    best[(sym, t)] = env["available_ts"]
                    out[sym][t] = bar
    return out


def reconcile(root: Path, day: str, coins: list[dict]) -> dict:
    start_ms = day_bounds(day)[0] // 10**6
    day_iso = f"{day[:4]}-{day[4:6]}-{day[6:]}"
    result = {"day": day, "symbols": {}, "missing": 0, "mismatched": 0, "unavailable": []}
    for market, key in (("spot", "spot"), ("um", "um")):
        ours = collected_klines(root, market, day)
        for c in coins:
            sym = c[key]
            if not sym:
                continue
            theirs = vision_klines(market, sym, day_iso)
            if theirs is None:
                result["unavailable"].append(f"{market}:{sym}")
                continue
            mine = {t: b for t, b in ours.get(sym, {}).items() if start_ms <= t < start_ms + 86_400_000}
            missing = sorted(set(theirs) - set(mine))
            bad = sorted(t for t in set(theirs) & set(mine) if theirs[t] != mine[t])
            result["symbols"][f"{market}:{sym}"] = {"missing": missing, "mismatched": bad, "extra": sorted(set(mine) - set(theirs))}
            result["missing"] += len(missing)
            result["mismatched"] += len(bad)
    return result


def hash_check(root: Path, k: int = 3) -> list[str]:
    """Re-hash k random sealed local files against their manifest. ponytail: local files only; download from R2 once it exists."""
    entries = [f for m in (root / "manifest").glob("*/*.json") for f in json.loads(m.read_text())["files"]]
    errors = []
    for f in random.sample(entries, min(k, len(entries))):
        p = root / f["path"]
        if p.exists() and hashlib.sha256(p.read_bytes()).hexdigest() != f["sha256"]:
            errors.append(f["path"])
    return errors


def write_report(root: Path, kind: str, day: str, report: dict) -> None:
    p = root / "reports" / kind / f"{day}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(report, indent=1))


def notify(text: str) -> None:
    url = os.environ.get("WTR_NTFY_URL")  # unset when running locally
    if url:
        urllib.request.urlopen(urllib.request.Request(url, data=text.encode(), method="POST"), timeout=10).close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, required=True)
    ap.add_argument("--day", default=(dt.datetime.now(dt.UTC) - dt.timedelta(days=1)).strftime("%Y%m%d"))
    a = ap.parse_args()
    cfg = tomllib.loads(universe.CONFIG.read_text())
    t2 = (dt.datetime.strptime(a.day, "%Y%m%d") - dt.timedelta(days=1)).strftime("%Y%m%d")

    coins, _ = universe.latest(a.root)
    g = gaps(a.root, a.day, cfg["collect"]["silence_s"], coins["coins"])
    write_report(a.root, "gaps", a.day, g)
    r = reconcile(a.root, t2, coins["coins"])
    write_report(a.root, "reconcile", t2, r)
    bad_hashes = hash_check(a.root)

    failed = bool(bad_hashes or g["mono_errors"])
    gap_s = sum(v["seconds"] for v in g["gaps"].values())
    lines = [
        f"{'FAIL' if failed else 'ok'} {a.day}",
        "messages " + ", ".join(f"{k} {v}" for k, v in g["messages"].items()),
        f"gaps: {len(g['gaps'])} streams, {gap_s:.0f} stream-seconds",
        f"reconcile {t2}: missing {r['missing']}, mismatched {r['mismatched']}, unavailable {len(r['unavailable'])}",
        f"hash check: {'bad ' + ' '.join(bad_hashes) if bad_hashes else 'ok'}; mono_ns: {len(g['mono_errors'])} errors",
    ]
    print("\n".join(lines))
    notify("\n".join(lines))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
