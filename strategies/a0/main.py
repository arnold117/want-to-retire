"""A0: equal-weight buy and hold of the pool. Rebalances only when the pool differs from what it holds."""
import json
import sys
from decimal import Decimal

snap = json.load(sys.stdin)
pool = snap["pool"]["spot"]
held = {s: Decimal(q) for s, q in snap["holdings"].items() if s != "USDT" and Decimal(q) > 0}

if set(held) == set(pool):
    # Hold: report current weights at the latest mid prices
    mid = {r["symbol"]: (Decimal(r["data"]["bids"][0][0]) + Decimal(r["data"]["asks"][0][0])) / 2 for r in snap["data"][0]}
    nav = Decimal(snap["holdings"]["USDT"]) + sum(q * mid[s] for s, q in held.items())
    weights = {s: float(q * mid[s] / nav) for s, q in held.items()}
    weights["CASH"] = max(0.0, 1 - sum(weights.values()))
    rationale = "hold"
else:
    weights = {s: 1 / len(pool) for s in pool} | {"CASH": 0.0}
    rationale = "pool changed: equal weights"

json.dump({"weights": weights, "rationale": rationale, "confidence": 1}, sys.stdout)
