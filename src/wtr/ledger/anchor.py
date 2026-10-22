"""Anchoring wtr-anchor (PRD S5 L3). Needs the `ots` client (uv tool install opentimestamps-client).
  hour  [--hour YYYYMMDD/HH]  stamp the chain head as of the end of that hour (default: the hour that just ended)
  daily [--date YYYY-MM-DD]   write anchors/YYYY-MM-DD.json with that day's 24 hour heads; upgrade older proofs
Usage: python -m wtr.ledger.anchor {hour,daily} --root DATA"""
import argparse
import datetime as dt
import json
import subprocess
from pathlib import Path

from wtr.ledger import chain


def head_at(root: Path, hour: str) -> dict | None:
    """Chain head as of the end of `hour` ("YYYYMMDD/HH"): the last event in that hour or before it."""
    before = [v for k, v in sorted(chain.hour_heads(root).items()) if k <= hour]
    return before[-1] if before else None


def stamp_hour(root: Path, hour: str) -> Path | None:
    p = root / "anchors" / "heads" / f"{hour}.txt"
    if p.with_name(p.name + ".ots").exists():
        return p  # already stamped; the head file must never change after its proof exists
    head = head_at(root, hour)
    if head is None:
        return None  # empty chain
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(f"{head['seq']} {head['hash']}\n")
    subprocess.run(["ots", "stamp", "--timeout", "30", str(p)], check=True, capture_output=True)
    return p


def daily(root: Path, date: dt.date) -> Path:
    day = date.strftime("%Y%m%d")
    hours = {}
    for h in range(24):
        if dt.datetime(date.year, date.month, date.day, h, tzinfo=dt.UTC) + dt.timedelta(hours=1) > dt.datetime.now(dt.UTC):
            break  # only hours that have ended
        head = head_at(root, f"{day}/{h:02d}")
        if head is not None:
            hours[f"{h:02d}"] = head
    out = root / "anchors" / f"{date.isoformat()}.json"
    out.write_text(json.dumps({"date": date.isoformat(), "hours": hours}, indent=1, sort_keys=True) + "\n")
    for ots in (root / "anchors" / "heads").glob("*/*.ots"):
        subprocess.run(["ots", "upgrade", str(ots)], capture_output=True)  # pending proofs get their Bitcoin attestation after a few hours
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["hour", "daily"])
    ap.add_argument("--root", type=Path, required=True)
    ap.add_argument("--hour")
    ap.add_argument("--date", type=dt.date.fromisoformat)
    a = ap.parse_args()
    now = dt.datetime.now(dt.UTC)
    if a.command == "hour":
        print(stamp_hour(a.root, a.hour or (now - dt.timedelta(hours=1)).strftime("%Y%m%d/%H")))
    else:
        print(daily(a.root, a.date or (now - dt.timedelta(days=1)).date()))


if __name__ == "__main__":
    main()
