import gzip
import json

import pytest

from wtr.collect import seal, universe
from wtr.collect.writer import HOUR_NS, Writer, hour_path
from wtr.contracts import envelope

H = 490_000 * HOUR_NS  # an arbitrary hour boundary
CH = {"spot": ["depth20", "kline_1m"], "sys": ["events"]}


def env(ts: int, raw: str = "{}") -> dict:
    return envelope.make((ts, ts), "c1", "live", "ws", "btcusdt@depth20@1000ms", raw)


def test_envelope_rejects_bad_records():
    with pytest.raises(ValueError):
        envelope.make((1, 1), "rest", "live", "rest", "GET /x", "{}")  # REST without status
    with pytest.raises(ValueError):
        envelope.make((1, 1), "c", "guess", "ws", "s", "{}")


def test_writer_rollover_recovery_and_seal(tmp_path):
    w = Writer(tmp_path, CH, H + 10)
    w.write("spot", "depth20", env(H + 10))
    w.write("spot", "depth20", env(H + HOUR_NS + 5))  # next hour, previous still open during grace
    w.rollover(H + HOUR_NS + 5, grace_ns=30 * 10**9)
    assert hour_path(tmp_path, "spot", "depth20", H // HOUR_NS).with_suffix(".jsonl.open").exists()

    w.rollover(H + HOUR_NS + 31 * 10**9, grace_ns=30 * 10**9)
    first = hour_path(tmp_path, "spot", "depth20", H // HOUR_NS)
    assert first.read_text().count("\n") == 1
    assert hour_path(tmp_path, "spot", "kline_1m", H // HOUR_NS).read_text() == ""  # empty hour still gets a file

    w.write("spot", "depth20", env(H + 20))  # late record for a closed hour lands in the oldest open hour
    w.close()

    # crash mid-line in the open hour; restart in a later hour closes it and drops the partial line
    second = hour_path(tmp_path, "spot", "depth20", H // HOUR_NS + 1).with_suffix(".jsonl.open")
    with open(second, "a") as f:
        f.write('{"available_ts":')
    Writer(tmp_path, CH, H + 3 * HOUR_NS)
    closed = second.with_suffix("")
    assert closed.read_text().count("\n") == 2 and closed.read_text().endswith("\n")

    # seal; then simulate a crash mid-compression of a fresh hour and rerun
    assert len(seal.seal(tmp_path, H + 3 * HOUR_NS)) == 2
    m = json.loads((tmp_path / "manifest" / first.parent.name / f"{first.name[:2]}.json").read_text())
    depth = next(f for f in m["files"] if "depth20" in f["path"])
    assert depth["lines"] == 1 and depth["first_available_ts"] == H + 10
    assert not first.exists() and first.with_name(first.name + ".gz").exists()

    third = hour_path(tmp_path, "spot", "depth20", H // HOUR_NS + 2)
    third.parent.mkdir(parents=True, exist_ok=True)
    third.write_text(envelope.dumps(env(H + 2 * HOUR_NS)) + "\n")
    third.with_name(third.name + ".gz.tmp").write_bytes(b"half")
    assert len(seal.seal(tmp_path, H + 3 * HOUR_NS)) == 1
    assert gzip.decompress(third.with_name(third.name + ".gz").read_bytes()).count(b"\n") == 1
    assert seal.seal(tmp_path, H + 3 * HOUR_NS) == []  # idempotent


def test_perp_prefix():
    p = ["1000000", "1000", "1M"]
    assert universe.strip_prefix("1000PEPE", p) == "PEPE"
    assert universe.strip_prefix("1000000MOG", p) == "MOG"
    assert universe.strip_prefix("1MBABYDOGE", p) == "BABYDOGE"
    assert universe.strip_prefix("1INCH", p) == "1INCH"
    assert universe.strip_prefix("BTC", p) == "BTC"
