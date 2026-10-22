"""Seal closed hours: gzip -> sha256 -> hourly manifest (PRD S1 L4, ADR 0003). Safe to rerun after a crash.
Usage: python -m wtr.collect.seal --root DATA"""
import argparse
import datetime as dt
import gzip
import hashlib
import json
import os
import shutil
import time
from collections import defaultdict
from pathlib import Path

from wtr.collect.writer import MARKETS


def _fsync_rename(tmp: Path, final: Path) -> None:
    with open(tmp, "rb") as f:
        os.fsync(f.fileno())
    tmp.rename(final)


def _gzip(src: Path) -> Path:
    gz = src.with_name(src.name + ".gz")
    if not gz.exists():
        tmp = gz.with_name(gz.name + ".tmp")
        with open(src, "rb") as fin, open(tmp, "wb") as raw:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as g:
                shutil.copyfileobj(fin, g, 1 << 20)
        _fsync_rename(tmp, gz)
    src.unlink()
    return gz


def _describe(root: Path, gz: Path) -> dict:
    h = hashlib.sha256()
    with open(gz, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    lines, lo, hi = 0, None, None
    with gzip.open(gz, "rb") as f:  # streamed: an hour of depth is ~100MB uncompressed
        for line in f:
            t = json.loads(line)["available_ts"]
            lines, lo, hi = lines + 1, t if lo is None else min(lo, t), t if hi is None else max(hi, t)
    return {"path": gz.relative_to(root).as_posix(), "sha256": h.hexdigest(), "bytes": gz.stat().st_size,
            "lines": lines, "first_available_ts": lo, "last_available_ts": hi}


def pending_hours(root: Path, now_ns: int) -> dict[tuple[str, str], list[Path]]:
    hours = defaultdict(list)
    for market in MARKETS:
        for p in (root / market).glob("*/*/??.jsonl*"):
            hours[(p.parent.name, p.name[:2])].append(p)
    now = dt.datetime.fromtimestamp(now_ns / 1e9, dt.UTC).strftime("%Y%m%d%H")
    return {
        k: files for k, files in sorted(hours.items())
        if k[0] + k[1] < now
        and not (root / "manifest" / k[0] / f"{k[1]}.json").exists()
        and not any(p.name.endswith(".open") for p in files)
    }


def seal(root: Path, now_ns: int) -> list[Path]:
    written = []
    for (day, hh), files in pending_hours(root, now_ns).items():
        for p in files:
            if p.name.endswith(".tmp"):
                p.unlink()  # crashed mid-compression; the .jsonl is still there
        for p in files:
            if p.name.endswith(".jsonl"):
                _gzip(p)
        gzs = sorted({p if p.name.endswith(".gz") else p.with_name(p.name + ".gz") for p in files if not p.name.endswith(".tmp")})
        manifest = {"hour": f"{day[:4]}-{day[4:6]}-{day[6:]}T{hh}:00:00Z", "files": [_describe(root, g) for g in gzs], "sealed_at_ns": time.time_ns()}
        out = root / "manifest" / day / f"{hh}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_name(out.name + ".tmp")
        tmp.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
        _fsync_rename(tmp, out)
        written.append(out)
    return written


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, required=True)
    a = ap.parse_args()
    for m in seal(a.root, time.time_ns()):
        print("sealed", m)


if __name__ == "__main__":
    main()
