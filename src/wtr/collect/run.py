"""Collector process wtr-collect (PRD S1). Usage: python -m wtr.collect.run --root DATA"""
import argparse
import asyncio
import gzip
import hashlib
import json
import logging
import os
import shutil
import signal
import statistics
import subprocess
import time
import tomllib
import urllib.request
import uuid
from pathlib import Path
from urllib.parse import parse_qs, quote, urlsplit

from websockets.asyncio.client import connect

from wtr.collect import rest, universe
from wtr.collect.writer import HOUR_NS, Writer, hour_path
from wtr.contracts import envelope

log = logging.getLogger("wtr.collect")
CHANNELS = {
    "spot": ["depth20", "kline_1m", "kline_1h", "kline_4h", "exchangeInfo"],
    "um": ["depth20", "kline_1m", "kline_1h", "kline_4h", "markPrice", "forceOrder", "openInterest", "exchangeInfo"],
    "sys": ["events"],
}
INTERVAL_MS = {"kline_1m": 60_000, "kline_1h": 3_600_000, "kline_4h": 14_400_000}
SLOTS = {"spot": "spot", "um_public": "um", "um_market": "um"}
FIRST_MESSAGE_TIMEOUT_S = 30  # a wrong stream path connects fine and then stays silent


def channel_of(stream: str) -> str:
    return "forceOrder" if stream.startswith("!forceOrder") else stream.split("@")[1]


class Collector:
    def __init__(self, root: Path, cfg: dict):
        self.root, self.cfg, self.c = root, cfg, cfg["collect"]
        self.writer = Writer(root, CHANNELS, time.time_ns())
        self.rotate = {name: asyncio.Event() for name in SLOTS}
        self.depth_sec: dict[str, int] = {}  # um depth downsampling, shared across connections
        self.last_seen: dict[tuple[str, str], int] = {}  # (market, stream) -> mono_ns
        self.halted: set[tuple[str, str]] = set()
        self.clock_samples: list[int] = []  # E - available_ts, ns
        self.rest_slots = asyncio.Semaphore(4)  # 20 parallel TLS handshakes per minute lost ~25% of open-interest polls
        self.load_universe()

    # --- universe and streams
    def load_universe(self) -> None:
        self.universe, self.universe_sha = universe.latest(self.root)
        coins = self.universe["coins"]
        self.spot = [c["spot"] for c in coins]
        self.um = [c["um"] for c in coins if c["um"]]
        now = time.monotonic_ns()
        self.expected = {("spot", f"{s.lower()}@{x}") for s in self.spot for x in ("depth20@1000ms", "kline_1m")}
        self.expected |= {("um", f"{s.lower()}@{x}") for s in self.um for x in ("depth20@500ms", "markPrice@1s", "kline_1m")}
        for k in self.expected:
            self.last_seen.setdefault(k, now)

    def streams(self, slot: str) -> list[str]:
        if slot == "spot":
            return [f"{s.lower()}@{x}" for s in self.spot for x in ("depth20@1000ms", "kline_1m", "kline_1h", "kline_4h")]
        if slot == "um_public":
            return [f"{s.lower()}@depth20@500ms" for s in self.um]
        return [f"{s.lower()}@{x}" for s in self.um for x in ("markPrice@1s", "kline_1m", "kline_1h", "kline_4h")] + ["!forceOrder@arr"]

    def url(self, slot: str) -> str:
        return self.cfg["endpoints"][f"{slot}_ws"] + "/".join(quote(s, safe="@_!") for s in self.streams(slot))

    # --- records
    def event(self, kind: str, detail: dict) -> None:
        raw = json.dumps(detail, ensure_ascii=False, sort_keys=True)
        self.writer.write("sys", "events", envelope.make(envelope.stamp(), "event", "live", "event", kind, raw))

    def on_ws(self, market: str, conn_id: str, raw: str, ts: tuple[int, int]) -> None:
        try:
            msg = json.loads(raw)
            stream, data = msg["stream"], msg["data"]
            channel = channel_of(stream)
            if channel.startswith("kline") and not data["k"]["x"]:
                return  # only the closing update of each candle is kept
            if market == "um" and channel == "depth20":
                sec = data["E"] // 1000
                if self.depth_sec.get(stream) == sec:
                    return
                self.depth_sec[stream] = sec
            if market == "um" and isinstance(data, dict) and "E" in data:
                self.clock_samples.append(data["E"] * 1_000_000 - ts[0])
        except (ValueError, KeyError, TypeError, IndexError):
            self.event("ws_unparsed", {"conn_id": conn_id, "raw": raw})
            return
        self.last_seen[(market, stream)] = ts[1]
        self.writer.write(market, channel, envelope.make(ts, conn_id, "live", "ws", stream, raw))

    async def get(self, market: str, channel: str, base: str, path: str, source: str = "live") -> str:
        async with self.rest_slots:
            ts, status, body = await asyncio.to_thread(rest.get, base + path)
        self.writer.write(market, channel, envelope.make(ts, "rest", source, "rest", "GET " + path, body, status))
        if status != 200:
            log.warning("GET %s -> %s", path, status)
        return body

    # --- websocket slots
    async def read_conn(self, market: str, url: str, conn_id: str, first: asyncio.Event) -> None:
        async with connect(url, max_size=1 << 23, open_timeout=10) as ws:
            async for raw in ws:
                ts = envelope.stamp()
                first.set()
                self.on_ws(market, conn_id, raw, ts)

    async def slot(self, name: str) -> None:
        market, backoff, cur = SLOTS[name], 1, None
        try:
            while True:
                conn_id, first = str(uuid.uuid4()), asyncio.Event()
                task = asyncio.create_task(self.read_conn(market, self.url(name), conn_id, first))
                got = asyncio.create_task(first.wait())
                await asyncio.wait({task, got}, timeout=FIRST_MESSAGE_TIMEOUT_S, return_when=asyncio.FIRST_COMPLETED)
                got.cancel()
                if not first.is_set():
                    err = task.exception() if task.done() and not task.cancelled() else "no message"
                    task.cancel()
                    log.warning("%s: new connection failed (%r); retry in %ss", name, err, backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, self.c["backoff_max_s"])
                    continue
                backoff = 1
                if cur is not None:
                    cur.cancel()  # the old connection closes only after the new one has delivered data
                cur = task
                self.event("ws_connected", {"slot": name, "conn_id": conn_id, "streams": len(self.streams(name))})
                self.rotate[name].clear()
                timer = asyncio.create_task(asyncio.sleep(self.c["rotate_after_s"]))
                hup = asyncio.create_task(self.rotate[name].wait())
                await asyncio.wait({cur, timer, hup}, return_when=asyncio.FIRST_COMPLETED)
                timer.cancel()
                hup.cancel()
                if cur.done():
                    err = None if cur.cancelled() else cur.exception()
                    log.warning("%s: connection %s ended: %r", name, conn_id, err)
                    self.event("ws_closed", {"slot": name, "conn_id": conn_id, "error": repr(err)})
                    cur = None
        finally:
            if cur is not None:
                cur.cancel()

    # --- periodic tasks
    async def every(self, seconds: float, fn) -> None:
        while True:
            try:
                await fn()
            except Exception:
                log.exception("%s failed", fn.__name__)
            await asyncio.sleep(seconds)

    async def files_tick(self) -> None:
        n = 0
        while True:
            await asyncio.sleep(1)
            n += 1
            if n % self.c["fsync_every_s"] == 0:
                await asyncio.to_thread(self.writer.fsync)
            self.writer.rollover(time.time_ns(), self.c["rollover_grace_s"] * 10**9)

    async def open_interest(self) -> None:
        um = self.cfg["endpoints"]["um_rest"]
        await asyncio.gather(*(self.get("um", "openInterest", um, f"/fapi/v1/openInterest?symbol={quote(s)}") for s in self.um))

    async def exchange_info(self) -> None:
        ep = self.cfg["endpoints"]
        symbols = quote(json.dumps(self.spot, separators=(",", ":"), ensure_ascii=False))
        spot = json.loads(await self.get("spot", "exchangeInfo", ep["spot_rest"], f"/api/v3/exchangeInfo?symbols={symbols}"))
        um = json.loads(await self.get("um", "exchangeInfo", ep["um_rest"], "/fapi/v1/exchangeInfo"))
        halted = {("spot", s["symbol"]) for s in spot.get("symbols", []) if s["status"] != "TRADING"}
        halted |= {("um", s["symbol"]) for s in um.get("symbols", []) if s["symbol"] in self.um and s["status"] != "TRADING"}
        self.halted = halted

    def have_klines(self, market: str, channel: str, hours: range) -> set[tuple[str, int]]:
        seen = set()
        for hour in hours:
            p = hour_path(self.root, market, channel, hour)
            for path, opener in ((p.with_name(p.name + ".open"), open), (p, open), (p.with_name(p.name + ".gz"), gzip.open)):
                if not path.exists():
                    continue
                with opener(path, "rt", encoding="utf-8") as f:
                    for line in f:
                        if not line.endswith("\n"):
                            continue  # partial line being written
                        env = json.loads(line)
                        raw = json.loads(env["raw"])
                        if env["kind"] == "ws":
                            seen.add((raw["data"]["k"]["s"], raw["data"]["k"]["t"]))
                        elif env["http_status"] == 200:
                            sym = parse_qs(urlsplit(env["stream"].split(" ", 1)[1]).query)["symbol"][0]
                            seen |= {(sym, row[0]) for row in raw}
        return seen

    async def backfill(self) -> None:
        now_ns, ep = time.time_ns(), self.cfg["endpoints"]
        now_ms = now_ns // 10**6
        hours = range((now_ns - self.c["backfill_window_s"] * 10**9) // HOUR_NS, now_ns // HOUR_NS + 1)
        for market, symbols, base, path in (("spot", self.spot, ep["spot_rest"], "/api/v3/klines"), ("um", self.um, ep["um_rest"], "/fapi/v1/klines")):
            for channel, ms in INTERVAL_MS.items():
                seen = await asyncio.to_thread(self.have_klines, market, channel, hours)
                lo, hi = now_ms - self.c["backfill_window_s"] * 1000, now_ms - self.c["backfill_grace_s"][channel] * 1000
                for sym in symbols:
                    if (market, sym) in self.halted:
                        continue
                    opens = [t for t in range((lo // ms) * ms, hi, ms) if lo <= t + ms - 1 <= hi and (sym, t) not in seen]
                    if opens:
                        log.info("backfill %s %s %s: %d bars", market, sym, channel, len(opens))
                        q = f"?symbol={quote(sym)}&interval={channel[6:]}&startTime={opens[0]}&endTime={opens[-1] + ms - 1}&limit=1000"
                        await self.get(market, channel, base, path + q, source="backfill")

    async def monitor(self) -> None:
        problems = []
        now = time.monotonic_ns()
        for (market, stream) in sorted(self.expected):
            if (market, stream.split("@")[0].upper()) in self.halted:
                continue
            limit = self.c["silence_s"][channel_of(stream)]
            if now - self.last_seen.get((market, stream), now) > limit * 10**9:
                problems.append(f"silent {market}:{stream}")
        samples, self.clock_samples = self.clock_samples, []
        if samples:
            med_ms = statistics.median(samples) / 1e6
            # E after receipt means our clock is behind; far before receipt means our clock is ahead or latency is high
            if med_ms > self.c["clock_max_offset_ms"] or med_ms < -500:
                problems.append(f"clock: median E - available_ts = {med_ms:.0f}ms")
        if shutil.which("chronyc"):
            out = (await asyncio.to_thread(subprocess.run, ["chronyc", "-c", "tracking"], capture_output=True, text=True)).stdout.strip()
            self.event("clock_offset", {"chronyc_tracking": out})
            if out and abs(float(out.split(",")[4])) * 1000 > self.c["clock_max_offset_ms"]:  # field 5 = System time, seconds
                problems.append(f"clock: chrony offset {out.split(',')[4]}s")
        du = shutil.disk_usage(self.root)
        if du.used / du.total > self.c["disk_max_used"]:
            problems.append(f"disk {du.used / du.total:.0%}")
        for p in problems:
            log.warning(p)
        await asyncio.to_thread(heartbeat, problems)

    # --- lifecycle
    def reload(self) -> None:
        self.load_universe()
        self.event("universe_loaded", {"universe_sha256": self.universe_sha})
        for e in self.rotate.values():
            e.set()

    async def run(self) -> None:
        cfg_bytes = universe.CONFIG.read_bytes()
        self.event("process_start", {"git_commit": git_commit(), "config_sha256": hashlib.sha256(cfg_bytes).hexdigest(), "universe_sha256": self.universe_sha})
        loop, stop = asyncio.get_running_loop(), asyncio.Event()
        loop.add_signal_handler(signal.SIGHUP, self.reload)
        for s in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(s, stop.set)
        tasks = [asyncio.create_task(self.slot(n)) for n in SLOTS] + [
            asyncio.create_task(self.files_tick()),
            asyncio.create_task(self.every(self.c["open_interest_every_s"], self.open_interest)),
            asyncio.create_task(self.every(self.c["exchange_info_every_s"], self.exchange_info)),
            asyncio.create_task(self.every(self.c["heartbeat_every_s"], self.monitor)),
            asyncio.create_task(self.every(self.c["backfill_every_s"], self.backfill)),
        ]
        await stop.wait()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.event("process_stop", {})
        self.writer.close()


def git_commit() -> str:
    r = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, cwd=universe.CONFIG.parent)
    dirty = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True, cwd=universe.CONFIG.parent).stdout.strip()
    return r.stdout.strip() + ("-dirty" if dirty else "") if r.returncode == 0 else "unknown"


def heartbeat(problems: list[str]) -> None:
    url = os.environ.get("WTR_HC_URL")  # healthchecks.io ping URL; unset when running locally
    if not url:
        return
    try:
        req = urllib.request.Request(url + ("/fail" if problems else ""), data="\n".join(problems).encode(), method="POST")
        urllib.request.urlopen(req, timeout=10).close()
    except OSError as e:
        log.warning("heartbeat failed: %s", e)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, required=True)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = tomllib.loads(universe.CONFIG.read_text())
    asyncio.run(Collector(a.root, cfg).run())


if __name__ == "__main__":
    main()
