from decimal import Decimal as D

from wtr.risk.clip import clip
from wtr.sim.fill import Book, Rules, current_weights, execute, value

R = Rules(step=D("0.001"), min_qty=D("0.001"), min_notional=D("5"))


def book(mid: str, depth: str = "1000") -> Book:
    m = D(mid)
    return Book(bids=[(m - 1, D(depth))], asks=[(m + 1, D(depth))], ref={"lastUpdateId": 1})


def types(events):
    return [e[0] for e in events]


def test_clip_pool_and_cap():
    w, rules = clip({"AAA": D("0.7"), "OLD": D("0.2"), "CASH": D("0.1")}, pool=["AAA", "BBB"])
    assert w == {"AAA": D("0.5"), "CASH": D("0.5")} and set(rules) == {"max_weight:AAA", "not_in_pool:OLD"}


def test_buy_sell_fees_and_invariants():
    h0 = {"USDT": D("10000")}
    h, ev = execute(h0, {"AAA": D("0.5"), "CASH": D("0.5")}, {"AAA": book("100")}, {}, {"AAA": R})
    fill = dict(ev)["fill"]
    assert fill["side"] == "buy" and D(fill["avg_price"]) == 101  # crossed the spread
    assert h["AAA"] == 50  # target value / mid; the spread and fee come out of cash
    assert h["USDT"] == D("10000") - D(fill["amount"]) - D(fill["fee"]) and D(fill["fee"]) == D(fill["amount"]) / 1000

    # hold = current weights at fill-time mids: no trades at all
    hold, ev = execute(h, current_weights(h, {"AAA": D("100")}), {"AAA": book("100")}, {}, {"AAA": R})
    assert ev == [] and hold == h

    # coin dropped from the pool is sold entirely
    w, _ = clip(current_weights(h, {"AAA": D("100")}), pool=["BBB"])
    sold, ev = execute(h, w, {"AAA": book("100")}, {}, {"AAA": R})
    assert sold["AAA"] == 0 and types(ev) == ["fill"]


def test_depth_minimum_shortfall_and_fallback():
    thin = Book(bids=[(D("99"), D("1"))], asks=[(D("101"), D("1"))])
    h, ev = execute({"USDT": D("10000")}, {"AAA": D("0.5"), "CASH": D("0.5")}, {"AAA": thin}, {}, {"AAA": R})
    assert h["AAA"] == 1 and "order_unfilled" in types(ev)  # ran out of book

    h, ev = execute({"USDT": D("3")}, {"AAA": D("1"), "CASH": D("0")}, {"AAA": book("100")}, {}, {"AAA": R})
    assert h == {"USDT": D("3")} and dict(ev)["order_unfilled"]["reason"] == "below_minimum"

    # cash short: two buys from all cash plus fees must shrink together, cash never negative
    h, ev = execute({"USDT": D("1000")}, {"AAA": D("0.5"), "BBB": D("0.5"), "CASH": D("0")},
                    {"AAA": book("100"), "BBB": book("50")}, {}, {"AAA": R, "BBB": R})
    assert h["USDT"] >= 0 and "risk_clip" in types(ev) and types(ev).count("fill") == 2

    h, ev = execute({"USDT": D("1000")}, {"AAA": D("0.5"), "CASH": D("0.5")}, {"AAA": None}, {"AAA": D("100")}, {"AAA": R})
    f = dict(ev)["fill_fallback"]
    assert D(f["avg_price"]) == D("100.05") and value(h, {"AAA": D("100")}) < 1000
