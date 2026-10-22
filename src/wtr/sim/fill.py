"""Simulated execution (PRD S4 L3-L4). Pure functions on Decimal; loaders read raw data with available_ts <= the fill cutoff."""
import json
from dataclasses import dataclass, field
from decimal import ROUND_FLOOR, Decimal
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from wtr.pit.snapshot import raw, read

FEE = Decimal("0.001")
FALLBACK_SLIPPAGE = Decimal("0.0005")  # ~ 5 bp, PRD "对接时再定"
FILL_WINDOW_NS = 120 * 10**9
ZERO = Decimal(0)


@dataclass
class Book:
    bids: list[tuple[Decimal, Decimal]]
    asks: list[tuple[Decimal, Decimal]]
    ref: dict = field(default_factory=dict)  # where the book came from (relative stream, lastUpdateId, available_ts)

    @property
    def mid(self) -> Decimal:
        return (self.bids[0][0] + self.asks[0][0]) / 2


@dataclass
class Rules:
    step: Decimal
    min_qty: Decimal
    min_notional: Decimal


def floor_step(q: Decimal, step: Decimal) -> Decimal:
    return (q // step) * step if step > 0 else q


def walk(levels: list[tuple[Decimal, Decimal]], qty: Decimal) -> tuple[Decimal, Decimal]:
    """Take liquidity level by level. Returns (filled qty, total quote amount)."""
    filled = amount = ZERO
    for px, q in levels:
        take = min(q, qty - filled)
        filled, amount = filled + take, amount + take * px
        if filled >= qty:
            break
    return filled, amount


def current_weights(holdings: dict[str, Decimal], prices: dict[str, Decimal]) -> dict[str, Decimal]:
    nav = value(holdings, prices)
    w = {s: q * prices[s] / nav for s, q in holdings.items() if s != "USDT" and q > 0}
    return w | {"CASH": 1 - sum(w.values(), ZERO)}


def value(holdings: dict[str, Decimal], prices: dict[str, Decimal]) -> Decimal:
    return holdings.get("USDT", ZERO) + sum((q * prices[s] for s, q in holdings.items() if s != "USDT"), ZERO)


def execute(holdings: dict[str, Decimal], target: dict[str, Decimal], books: dict[str, Book | None],
            fallback: dict[str, Decimal], rules: dict[str, Rules]) -> tuple[dict[str, Decimal], list[tuple[str, dict]]]:
    """Trade from holdings toward target weights. Sells first; buys shrink by one ratio if cash is short.
    `books[s]` is None when no book arrived in the window: fill at `fallback[s]` (first 1m open after the deadline) +- slippage."""
    h = dict(holdings)
    syms = {s for s in set(h) | set(target) if s not in ("USDT", "CASH")}
    if missing := sorted(s for s in syms if not books.get(s) and s not in fallback):
        raise RuntimeError(f"no book and no fallback price for {missing}")
    prices = {s: books[s].mid if books.get(s) else fallback[s] for s in syms}
    nav = value(h, prices)
    delta = {s: target.get(s, ZERO) * nav - h.get(s, ZERO) * prices[s] for s in prices}
    events: list[tuple[str, dict]] = []

    def trade(s: str, side: str, qty: Decimal) -> Decimal:
        """Fill qty of s; returns the signed USDT change and records events."""
        r, book = rules.get(s), books.get(s)
        if r is None:
            events.append(("order_unfilled", {"symbol": s, "side": side, "qty": str(qty), "reason": "no_trading_rules"}))
            return ZERO
        qty = floor_step(qty, r.step)
        if qty <= 0 or qty < r.min_qty or qty * prices[s] < r.min_notional:
            if qty > 0:
                events.append(("order_unfilled", {"symbol": s, "side": side, "qty": str(qty), "reason": "below_minimum"}))
            return ZERO
        if book is None:
            px = fallback[s] * (1 + FALLBACK_SLIPPAGE if side == "buy" else 1 - FALLBACK_SLIPPAGE)
            filled, amount, kind, src = qty, qty * px, "fill_fallback", {"fallback_open": str(fallback[s])}
        else:
            filled, amount = walk(book.asks if side == "buy" else book.bids, qty)
            kind, src = "fill", {"book": book.ref}
            if filled < qty:
                events.append(("order_unfilled", {"symbol": s, "side": side, "qty": str(qty - filled), "reason": "book_depth"}))
        if filled <= 0:
            return ZERO
        fee = amount * FEE
        h[s] = h.get(s, ZERO) + (filled if side == "buy" else -filled)
        events.append((kind, {"symbol": s, "side": side, "qty_requested": str(qty), "qty": str(filled),
                              "avg_price": str(amount / filled), "amount": str(amount), "fee": str(fee)} | src))
        return -(amount + fee) if side == "buy" else amount - fee

    for s in sorted(s for s in delta if delta[s] < 0):
        qty = h[s] if target.get(s, ZERO) == 0 else min(h[s], -delta[s] / prices[s])
        h["USDT"] += trade(s, "sell", qty)

    buys = {s: delta[s] / prices[s] for s in sorted(delta) if delta[s] > 0}
    need = ZERO
    for s, q in buys.items():
        book = books.get(s)
        cost = walk(book.asks, q)[1] if book else q * fallback[s] * (1 + FALLBACK_SLIPPAGE)
        need += cost * (1 + FEE)
    if need > h["USDT"]:
        # Average price only rises with size, so scaling every quantity by k costs at most k * need
        k = (h["USDT"] / need).quantize(Decimal("1e-12"), rounding=ROUND_FLOOR)
        buys = {s: q * k for s, q in buys.items()}
        events.append(("risk_clip", {"rule": "cash_shortfall", "buy_scale": str(k)}))
    for s, q in buys.items():
        h["USDT"] += trade(s, "buy", q)

    if h["USDT"] < 0 or any(q < 0 for q in h.values()):
        raise AssertionError(f"position invariant violated: {h}")
    return h, events


# --- loaders (read raw data; same inputs live and on replay)

def books_after(root: Path, deadline: int, symbols: list[str]) -> dict[str, Book | None]:
    """First spot depth snapshot per symbol with deadline < available_ts <= deadline + 120s."""
    out: dict[str, Book | None] = {s: None for s in symbols}
    for env in read(root, "spot", "depth20", deadline + 1, deadline + FILL_WINDOW_NS):
        s = env["stream"].split("@")[0].upper()
        if s in out and out[s] is None:
            d = raw(env)["data"]
            out[s] = Book([(Decimal(p), Decimal(q)) for p, q in d["bids"]], [(Decimal(p), Decimal(q)) for p, q in d["asks"]],
                          {"stream": env["stream"], "lastUpdateId": d["lastUpdateId"], "available_ts": env["available_ts"]})
    return out


def first_open_after(root: Path, deadline: int, symbols: list[str]) -> dict[str, Decimal]:
    """Open price of the first 1m candle opening at or after the deadline, known by deadline + 120s."""
    out: dict[str, Decimal] = {}
    for env in read(root, "spot", "kline_1m", deadline, deadline + FILL_WINDOW_NS):
        rows = [raw(env)["data"]["k"]] if env["kind"] == "ws" else []
        for k in rows:
            if k["s"] in symbols and k["t"] * 10**6 >= deadline and k["s"] not in out:
                out[k["s"]] = Decimal(k["o"])
    return out


def rules_at(root: Path, ts: int, symbols: list[str]) -> dict[str, Rules]:
    """Trading rules per symbol from the latest spot exchangeInfo that listed it, known at ts (hourly pulls cover
    only the pool at the time, so a coin just dropped from the pool is found in an earlier pull; look back 3 hours)."""
    out = {}
    for env in sorted(read(root, "spot", "exchangeInfo", ts - 3 * 3600 * 10**9, ts), key=lambda e: e["available_ts"]):
        if env["http_status"] != 200:
            continue
        for s in json.loads(env["raw"])["symbols"]:
            if s["symbol"] in symbols:
                f = {x["filterType"]: x for x in s["filters"]}
                out[s["symbol"]] = Rules(Decimal(f["LOT_SIZE"]["stepSize"]), Decimal(f["LOT_SIZE"]["minQty"]), Decimal(f.get("NOTIONAL", {}).get("minNotional", "0")))
    return out


def closes_1h(root: Path, hour_end: int, symbols: list[str]) -> dict[str, Decimal]:
    """Close of the 1h candle ending at hour_end (ns), per symbol, as known by hour_end + 10 minutes."""
    out = {}
    for env in read(root, "spot", "kline_1h", hour_end - 3600 * 10**9, hour_end + 600 * 10**9):
        bars = [raw(env)["data"]["k"]] if env["kind"] == "ws" else [] if env["http_status"] != 200 else [
            {"s": parse_qs(urlsplit(env["stream"].split(" ", 1)[1]).query)["symbol"][0], "T": r[6], "c": r[4]} for r in raw(env)]
        for k in bars:
            if k["s"] in symbols and (k["T"] + 1) * 10**6 == hour_end:
                out.setdefault(k["s"], Decimal(k["c"]))
    return out
