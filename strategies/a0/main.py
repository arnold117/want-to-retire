"""A0: equal-weight buy and hold of the pool. Rebalances only when the pool differs from what it holds."""
import json
import sys
from decimal import Decimal

snap = json.load(sys.stdin)
pool = snap["pool"]["spot"]
held = {s: Decimal(q) for s, q in snap["holdings"].items() if s != "USDT" and Decimal(q) > 0}

if set(held) == set(pool):
    weights, rationale = None, "hold"  # keep current holdings (ADR 0001 amendment)
else:
    weights = {s: 1 / len(pool) for s in pool} | {"CASH": 0.0}
    rationale = "pool changed: equal weights"

json.dump({"weights": weights, "rationale": rationale, "confidence": 1}, sys.stdout)
