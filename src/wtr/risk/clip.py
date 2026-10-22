"""Risk clipping (PRD S4 L2), identical for every arm. Weights are Decimal; "CASH" is the cash weight."""
from decimal import Decimal

MAX_WEIGHT = Decimal("0.5")


def clip(weights: dict[str, Decimal], pool: list[str]) -> tuple[dict[str, Decimal], list[str]]:
    """Coins outside the pool go to cash; any coin above 50% is cut to 50%. Returns (weights summing to exactly 1, rules hit)."""
    rules, coins = [], {}
    for sym, w in weights.items():
        if sym == "CASH":
            continue
        if sym not in pool:
            if w > 0:
                rules.append(f"not_in_pool:{sym}")
            continue
        if w > MAX_WEIGHT:
            rules.append(f"max_weight:{sym}")
            w = MAX_WEIGHT
        if w > 0:
            coins[sym] = w
    total = sum(coins.values(), Decimal(0))
    if total > 1:  # validation allows a 1e-6 rounding error
        coins = {s: w / total for s, w in coins.items()}
    out = dict(sorted(coins.items())) | {"CASH": 1 - sum(coins.values(), Decimal(0))}
    check(out)
    return out, rules


def check(weights: dict[str, Decimal]) -> None:
    """Invariant (CLAUDE.md): non-negative, each coin within the cap, summing to 1 with cash."""
    if any(w < 0 for w in weights.values()) or any(w > MAX_WEIGHT for s, w in weights.items() if s != "CASH"):
        raise AssertionError(f"risk invariant violated: {weights}")
    if abs(sum(weights.values(), Decimal(0)) - 1) > Decimal("1e-18"):
        raise AssertionError(f"weights do not sum to 1: {weights}")
