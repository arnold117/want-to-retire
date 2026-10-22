"""B: an LLM decides directly. Reads a snapshot on stdin, asks DeepSeek through the local gateway, writes a decision.
Draft prompt; the version frozen before the forward test may differ."""
import datetime as dt
import json
import os
import sys
import urllib.request
from collections import defaultdict
from decimal import Decimal

GATEWAY = os.environ.get("WTR_GATEWAY_URL", "http://127.0.0.1:8787/v1/chat/completions")
REQUEST = {"model": "deepseek-flash", "thinking": {"type": "enabled"}, "reasoning_effort": "high",
           "response_format": {"type": "json_object"}, "max_tokens": 32000}

SYSTEM = """You manage a long-only spot crypto portfolio in a year-long forward test. Every 4 hours you set target
weights for the next 4 hours. Goal: the best risk-adjusted return (Sharpe ratio) over the year, after costs.

Rules:
- Coins: only the symbols listed under "pool"; anything else is cash (USDT). No leverage, no shorting.
- Weights are fractions of portfolio value, non-negative, summing to 1 including "CASH". Any coin above 0.5 is cut to 0.5.
- Orders execute about 10 minutes after the data time, against the order book, with a 0.1% fee per trade. Trading has a
  cost: change positions only when you expect it to pay.
- To keep the current holdings unchanged, return "weights": null.

Reply with one JSON object only:
{"weights": {"BTCUSDT": 0.3, ..., "CASH": 0.2} or null, "rationale": "<= 3 sentences", "confidence": 0.0 to 1.0}"""


def ts(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, dt.UTC).strftime("%m-%d %H:%M")


def prompt(snap: dict) -> str:
    candles, marks, oi, books = snap["data"]
    t = dt.datetime.fromtimestamp(snap["t"] / 1e9, dt.UTC)
    mid = {r["symbol"]: (Decimal(r["data"]["bids"][0][0]) + Decimal(r["data"]["asks"][0][0])) / 2 for r in books}
    spread = {r["symbol"]: (Decimal(r["data"]["asks"][0][0]) / Decimal(r["data"]["bids"][0][0]) - 1) * 10000 for r in books}
    held = {s: Decimal(q) for s, q in snap["holdings"].items()}
    nav = held.get("USDT", Decimal(0)) + sum(q * mid.get(s, 0) for s, q in held.items() if s != "USDT")

    lines = [f"Data time: {t:%Y-%m-%d %H:%M} UTC. Pool: {', '.join(snap['pool']['spot'])}.",
             f"Portfolio value ~ {nav:.2f} USDT. Holdings (value USDT): " +
             ", ".join(f"{s} {q * mid.get(s, 1) if s != 'USDT' else q:.2f}" for s, q in held.items() if q > 0)]
    lines.append("\nNow: symbol, mid price, spread (bp)")
    lines += [f"{s},{mid[s]:.8g},{spread[s]:.2f}" for s in sorted(mid)]

    lines.append("\n4h candles, last 7 days: symbol, close time UTC, open, high, low, close, quote volume (USDT)")
    for r in candles:
        lines.append(f"{r['symbol']},{ts(r['close_time'] + 1)},{r['o']},{r['h']},{r['l']},{r['c']},{Decimal(r['q']):.0f}")

    by = defaultdict(list)
    for r in marks:
        by[r["data"]["s"]].append(f"{ts(r['data']['E'])} {Decimal(r['data']['r']) * 100:.4f}%")
    lines.append("\nPerpetual funding rate (per 8h), one reading per 4h:")
    lines += [f"{s}: " + "; ".join(v) for s, v in sorted(by.items())]

    by = defaultdict(list)
    for r in oi:
        by[r["data"]["symbol"]].append(f"{ts(r['data']['time'])} {Decimal(r['data']['openInterest']):.6g}")
    lines.append("\nPerpetual open interest (contracts), one reading per 4h:")
    lines += [f"{s}: " + "; ".join(v) for s, v in sorted(by.items())]
    return "\n".join(lines)


def main() -> None:
    snap = json.load(sys.stdin)
    body = REQUEST | {"messages": [{"role": "system", "content": SYSTEM}, {"role": "user", "content": prompt(snap)}]}
    req = urllib.request.Request(GATEWAY, data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json", "X-Round-Token": snap["round_token"]})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))  # the gateway is local; never use a proxy
    with opener.open(req, timeout=560) as r:
        content = json.load(r)["choices"][0]["message"]["content"]
    sys.stdout.write(content)  # the runner validates it


if __name__ == "__main__":
    main()
