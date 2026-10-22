"""Monthly universe selection (ADR 0003). Usage: python -m wtr.collect.universe --root DATA [--at ISO8601]"""
import argparse
import datetime as dt
import hashlib
import json
import os
import time
import tomllib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote

from wtr.collect import rest

CONFIG = Path(__file__).resolve().parents[3] / "config" / "collect.toml"


def strip_prefix(base: str, prefixes: list[str]) -> str:
    for p in sorted(prefixes, key=len, reverse=True):
        if base.startswith(p) and len(base) > len(p) and not base[len(p)].isdigit():
            return base[len(p):]
    return base


def _json(url: str):
    _, status, body = rest.get(url)
    if status != 200:
        raise RuntimeError(f"{url} -> {status} {body[:200]}")
    return json.loads(body)


def select(at: dt.datetime, cfg: dict) -> dict:
    u, ep = cfg["universe"], cfg["endpoints"]
    excluded = set(u["pegged"]) | set(u["leveraged"])
    spot = _json(f"{ep['spot_rest']}/api/v3/exchangeInfo")["symbols"]
    cands = [s for s in spot if s["status"] == "TRADING" and s["quoteAsset"] == "USDT" and s["baseAsset"] not in excluded]
    end_ms = int(dt.datetime(at.year, at.month, at.day, tzinfo=dt.UTC).timestamp() * 1000) - 1

    def volume(sym: str) -> str:
        rows = _json(f"{ep['spot_rest']}/api/v3/klines?symbol={quote(sym)}&interval=1d&endTime={end_ms}&limit={u['lookback_days']}")
        return str(sum(float(r[7]) for r in rows))  # quoteAssetVolume; ranking only, so float is fine

    with ThreadPoolExecutor(4) as pool:  # weight 2 each, ~1000 of the 6000/min budget
        volumes = dict(zip((s["symbol"] for s in cands), pool.map(volume, (s["symbol"] for s in cands))))
    ranked = sorted(cands, key=lambda s: float(volumes[s["symbol"]]), reverse=True)[: u["size"]]

    perps = {}
    for p in _json(f"{ep['um_rest']}/fapi/v1/exchangeInfo")["symbols"]:
        if p["contractType"] == "PERPETUAL" and p["quoteAsset"] == "USDT" and p["status"] == "TRADING":
            perps.setdefault(strip_prefix(p["baseAsset"], u["perp_prefixes"]), p["symbol"])
    coins = [
        {"rank": i + 1, "spot": s["symbol"], "base": s["baseAsset"], "quote_volume": volumes[s["symbol"]],
         "um": perps.get(strip_prefix(s["baseAsset"], u["perp_prefixes"]))}
        for i, s in enumerate(ranked)
    ]
    return {"computed_at": at.isoformat(), "volume_end_ms": end_ms, "candidates": len(cands), "coins": coins}


def write(root: Path, result: dict, month: str) -> Path:
    path = root / "universe" / f"{month}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    result["available_ts"] = time.time_ns()
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(result, ensure_ascii=False, indent=1))
    with open(tmp) as f:
        os.fsync(f.fileno())
    tmp.rename(path)
    return path


def latest(root: Path) -> tuple[dict, str]:
    """Most recent universe file and its sha256."""
    files = sorted((root / "universe").glob("????-??.json"))
    if not files:
        raise SystemExit(f"no universe in {root}/universe; run python -m wtr.collect.universe first")
    data = files[-1].read_bytes()
    return json.loads(data), hashlib.sha256(data).hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, required=True)
    ap.add_argument("--at", type=dt.datetime.fromisoformat, default=None)
    a = ap.parse_args()
    at = a.at or dt.datetime.now(dt.UTC)
    cfg = tomllib.loads(CONFIG.read_text())
    result = select(at, cfg)
    path = write(a.root, result, at.strftime("%Y-%m"))
    for c in result["coins"]:
        print(c["rank"], c["spot"], c["um"], c["quote_volume"])
    print("wrote", path)


if __name__ == "__main__":
    main()
