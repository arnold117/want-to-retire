import json

from wtr.collect.writer import Writer
from wtr.contracts import envelope
from wtr.runner.round import circuit_ok, validate

POOL = ["BTCUSDT", "ETHUSDT"]


def test_validate_rejects_malformed_decisions():
    ok = {"weights": {"BTCUSDT": 0.5, "CASH": 0.5}, "rationale": "x", "confidence": 0.7}
    assert validate(json.dumps(ok).encode(), POOL, "B")[1] == "ok"
    bad = [
        b"x" * (64 * 1024 + 1),
        b"{not json",
        json.dumps(ok | {"weights": {"BTCUSDT": -0.1, "CASH": 1.1}}).encode(),
        json.dumps(ok | {"weights": {"DOGEUSDT": 0.5, "CASH": 0.5}}).encode(),
        b'{"weights": {"BTCUSDT": NaN, "CASH": 0.5}, "rationale": "x", "confidence": 0.5}',
        json.dumps(ok | {"weights": {"BTCUSDT": 0.5, "CASH": 0.4}}).encode(),
        json.dumps(ok | {"confidence": 2}).encode(),
        json.dumps(ok | {"rationale": {"q": "?"}}).encode(),  # structured rationale is only for J
        json.dumps(ok | {"weights": {"BTCUSDT": True, "CASH": 0}}).encode(),
    ]
    for b in bad:
        assert validate(b, POOL, "B")[0] is None, b[:60]
    assert validate(json.dumps(ok | {"rationale": {"q": "?", "p": [0.6, 0.4]}}).encode(), POOL, "J")[1] == "ok"


def test_circuit_breaker(tmp_path):
    t = 1792641630 * 10**9  # 04:00:30
    w = Writer(tmp_path, {"spot": ["kline_1m"]}, t - 3600 * 10**9)
    k = {"s": "BTCUSDT", "t": (t - 30 * 10**9) // 10**6 - 60_000, "x": True}
    k["T"] = k["t"] + 59_999
    w.write("spot", "kline_1m", envelope.make((t - 29 * 10**9, 0), "c", "live", "ws", "btcusdt@kline_1m",
                                             json.dumps({"stream": "btcusdt@kline_1m", "data": {"k": k}})))
    w.close()
    assert circuit_ok(tmp_path, t, POOL)
    assert not circuit_ok(tmp_path, t + 5 * 60 * 10**9, POOL)  # nothing new for 5 minutes
