import json

import pytest

from wtr.collect.writer import Writer
from wtr.contracts import envelope
from wtr.pit import snapshot as S

T = 1792627230 * 10**9  # 2026-10-22T00:00:30Z
SPEC = [["spot", "kline_1m", "1m", "1h", "native"], ["um", "markPrice", "0", "1h", "last"]]


def kline_ws(open_ms: int, close: str) -> str:
    k = {"s": "BTCUSDT", "t": open_ms, "T": open_ms + 59_999, "o": "1", "h": "2", "l": "0.5", "c": close, "v": "3", "q": "4", "n": 5, "x": True}
    return json.dumps({"stream": "btcusdt@kline_1m", "data": {"e": "kline", "E": open_ms + 60_000, "s": "BTCUSDT", "k": k}})


def mark(e_ms: int, p: str) -> str:
    return json.dumps({"stream": "btcusdt@markPrice@1s", "data": {"e": "markPriceUpdate", "E": e_ms, "s": "BTCUSDT", "p": p, "r": "0.0001"}})


def setup(root):
    (root / "universe").mkdir()
    (root / "universe" / "2026-10.json").write_text(json.dumps({"available_ts": T - 10**12, "coins": [{"spot": "BTCUSDT", "um": "BTCUSDT"}]}))
    w = Writer(root, {"spot": ["kline_1m"], "um": ["markPrice"]}, T - 10**12)
    bar = T // 10**6 - 30_000 - 60_000  # the 1m bar that closed at 00:00:00
    w.write("spot", "kline_1m", envelope.make((T - 29 * 10**9, 1), "c1", "live", "ws", "btcusdt@kline_1m", kline_ws(bar, "1.5")))
    rest = f"GET /api/v3/klines?symbol=BTCUSDT&interval=1m&startTime={bar}"
    row = [bar, "1", "2", "0.5", "1.5", "3", bar + 59_999, "4", 5, "0", "0", "0"]
    w.write("spot", "kline_1m", envelope.make((T - 5 * 10**9, 2), "rest", "backfill", "rest", rest, json.dumps([row]), 200))  # duplicate, later
    w.write("um", "markPrice", envelope.make((T - 10**9, 3), "c2", "live", "ws", "btcusdt@markPrice@1s", mark(T // 10**6 - 1000, "100.1")))
    w.write("um", "markPrice", envelope.make((T + 10**9, 4), "c2", "live", "ws", "btcusdt@markPrice@1s", mark(T // 10**6 + 1000, "999")))  # after t
    w.close()


def test_snapshot_point_in_time(tmp_path):
    setup(tmp_path)
    snap, h, _ = S.build(tmp_path, T, "A0", SPEC, round_token="tok1")
    klines, marks = snap["data"]
    assert len(klines) == 1 and klines[0]["source"] == "live"  # dedup keeps the earliest copy
    assert [m["data"]["p"] for m in marks] == ["100.1"]  # the record after t is not visible
    assert snap["holdings"] == {"USDT": "10000"}
    assert S.build(tmp_path, T, "A0", SPEC, round_token="tok2")[1] == h  # round_token is not hashed; rebuild is identical

    with pytest.raises(S.SnapshotAssertFail):
        S.build(tmp_path, T - 2 * 10**12, "A0", SPEC)  # no universe available yet
    with pytest.raises(S.SnapshotAssertFail):
        S.build(tmp_path, 2**62, "A0", SPEC)  # future t
