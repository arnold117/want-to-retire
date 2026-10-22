"""Hourly append-only files: {root}/{market}/{channel}/{YYYYMMDD}/{HH}.jsonl[.open] (ADR 0003)."""
import datetime as dt
import logging
import os
from pathlib import Path

from wtr.contracts import envelope

HOUR_NS = 3600 * 10**9
MARKETS = ("spot", "um", "sys")
log = logging.getLogger(__name__)


def hour_path(root: Path, market: str, channel: str, hour: int) -> Path:
    t = dt.datetime.fromtimestamp(hour * 3600, dt.UTC)
    return root / market / channel / t.strftime("%Y%m%d") / f"{t:%H}.jsonl"


def _open_name(p: Path) -> Path:
    return p.with_name(p.name + ".open")


def truncate_partial_line(p: Path) -> None:
    with open(p, "r+b") as f:
        pos = f.seek(0, os.SEEK_END)
        while pos > 0:
            step = min(1 << 20, pos)
            pos -= step
            f.seek(pos)
            i = f.read(step).rfind(b"\n")
            if i >= 0:
                f.truncate(pos + i + 1)
                return
        f.truncate(0)


def _hour_of(p: Path) -> int:
    t = dt.datetime.strptime(p.parent.name + p.name[:2], "%Y%m%d%H").replace(tzinfo=dt.UTC)
    return int(t.timestamp()) // 3600


class Writer:
    """Single writer for all raw files. Each (market, channel) may have two hours open during the grace period."""

    def __init__(self, root: Path, channels: dict[str, list[str]], now_ns: int):
        self.root, self.channels = root, channels
        self.files: dict[tuple[str, str, int], object] = {}
        self.closed_before = now_ns // HOUR_NS  # hours before this are closed
        self.started_hour = self.closed_before
        self._recover()

    def _recover(self) -> None:
        for market in MARKETS:
            for p in (self.root / market).glob("*/*/*.jsonl.open"):
                truncate_partial_line(p)
                if _hour_of(p) < self.closed_before:
                    p.rename(p.with_name(p.name.removesuffix(".open")))

    def write(self, market: str, channel: str, env: dict) -> None:
        hour = env["available_ts"] // HOUR_NS
        if hour < self.closed_before:
            # ponytail: a record that arrives after its hour was closed goes into the oldest open hour; readers use available_ts, not the file name
            log.warning("late write %s/%s hour %s", market, channel, hour)
            hour = self.closed_before
        key = (market, channel, hour)
        f = self.files.get(key)
        if f is None:
            p = _open_name(hour_path(self.root, market, channel, hour))
            p.parent.mkdir(parents=True, exist_ok=True)
            f = self.files[key] = open(p, "a", encoding="utf-8")
        f.write(envelope.dumps(env) + "\n")
        f.flush()

    def fsync(self) -> None:
        """Blocking; run in a thread. Never runs concurrently with rollover (same task)."""
        for f in list(self.files.values()):
            os.fsync(f.fileno())

    def rollover(self, now_ns: int, grace_ns: int) -> None:
        """Close and rename every hour that ended more than grace_ns ago."""
        boundary = (now_ns - grace_ns) // HOUR_NS
        for hour in range(self.closed_before, boundary):
            for market, chans in self.channels.items():
                for channel in chans:
                    f = self.files.pop((market, channel, hour), None)
                    final = hour_path(self.root, market, channel, hour)
                    if f is not None:
                        os.fsync(f.fileno())
                        f.close()
                    if _open_name(final).exists():
                        _open_name(final).rename(final)
                    elif not final.exists() and hour >= self.started_hour:
                        final.parent.mkdir(parents=True, exist_ok=True)
                        final.touch()  # no data this hour while running: empty file
        self.closed_before = max(self.closed_before, boundary)

    def close(self) -> None:
        """Shutdown: flush and fsync, leave .open files for the next start to continue."""
        for f in self.files.values():
            os.fsync(f.fileno())
            f.close()
        self.files.clear()
