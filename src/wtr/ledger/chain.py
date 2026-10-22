"""Hash chain (PRD S5): {root}/ledger/{YYYYMMDD}/{HH}.jsonl, one event per line, continuous across files.
Usage: python -m wtr.ledger.chain verify --root DATA"""
import argparse
import datetime as dt
import json
import sys
from pathlib import Path

from wtr.contracts import canonical

GENESIS = "0" * 64
HOUR_NS = 3600 * 10**9
TYPES = {
    "system_version", "universe_change", "snapshot", "watermark_timeout", "snapshot_assert_fail", "llm_call", "decision",
    "circuit_break", "risk_clip", "fill", "order_unfilled", "fill_fallback", "position", "model_retrain",
    "model_observation", "model_segment_change", "data_root", "gap_report",
}


def make(seq: int, ts: int, type_: str, payload: dict, prev_hash: str) -> dict:
    ev = {"seq": seq, "ts": ts, "type": type_, "payload": payload, "prev_hash": prev_hash}
    ev["hash"] = canonical.sha256(ev)
    return ev


def segment(root: Path, ts: int) -> Path:
    t = dt.datetime.fromtimestamp(ts // HOUR_NS * 3600, dt.UTC)
    return root / "ledger" / t.strftime("%Y%m%d") / f"{t:%H}.jsonl"


def segments(root: Path) -> list[Path]:
    return sorted((root / "ledger").glob("????????/??.jsonl"))


def events(root: Path):
    for p in segments(root):
        with open(p, encoding="utf-8") as f:
            for n, line in enumerate(f, 1):
                yield p, n, line


def tail(root: Path) -> tuple[int, str, int]:
    """(next seq, last hash, last ts) after the last complete line."""
    for p in reversed(segments(root)):
        lines = [l for l in p.read_text(encoding="utf-8").splitlines(keepends=True) if l.endswith("\n")]
        if lines:
            ev = json.loads(lines[-1])
            return ev["seq"] + 1, ev["hash"], ev["ts"]
    return 0, GENESIS, 0


def hour_heads(root: Path) -> dict[str, dict]:
    """Last event of every hour segment: {"YYYYMMDD/HH": {"seq", "hash"}}."""
    heads = {}
    for p in segments(root):
        lines = p.read_text(encoding="utf-8").splitlines()
        if lines:
            ev = json.loads(lines[-1])
            heads[f"{p.parent.name}/{p.stem}"] = {"seq": ev["seq"], "hash": ev["hash"]}
    return heads


def verify(root: Path) -> list[str]:
    """Recompute the whole chain. Returns errors naming the file, line and seq."""
    errors, want_seq, prev = [], 0, GENESIS
    for p, n, line in events(root):
        where = f"{p.relative_to(root)}:{n}"
        try:
            ev = json.loads(line)
            body = {k: ev[k] for k in ("seq", "ts", "type", "payload", "prev_hash")}
        except (ValueError, KeyError) as e:
            errors.append(f"{where}: unreadable ({e})")
            break
        if ev["seq"] != want_seq:
            errors.append(f"{where}: seq {ev['seq']}, expected {want_seq}")
        if ev["prev_hash"] != prev:
            errors.append(f"{where}: seq {ev['seq']} prev_hash does not match the previous event")
        if canonical.sha256(body) != ev["hash"] or set(ev) != set(body) | {"hash"}:
            errors.append(f"{where}: seq {ev['seq']} content does not match its hash")
        if segment(root, ev["ts"]) != p:
            errors.append(f"{where}: seq {ev['seq']} ts is outside this hour segment")
        want_seq, prev = ev["seq"] + 1, ev["hash"]
    return errors


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["verify"])
    ap.add_argument("--root", type=Path, required=True)
    a = ap.parse_args()
    errors = verify(a.root)
    for e in errors:
        print(e)
    seq, head, _ = tail(a.root)
    print(f"{'FAIL' if errors else 'ok'}: {seq} events, head {head}")
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
