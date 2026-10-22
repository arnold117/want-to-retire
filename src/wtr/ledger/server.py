"""Ledger process wtr-ledger, the only writer of the chain (PRD S5 L1).
Protocol: one JSON line {"type", "payload"} in, one line {"seq", "hash"} or {"error"} out. Acked = written and fsynced.
Usage: python -m wtr.ledger.server --root DATA --socket PATH [--secrets /etc/wtr]"""
import argparse
import asyncio
import json
import logging
import os
import re
import time
from pathlib import Path

from wtr.collect.writer import truncate_partial_line
from wtr.contracts import canonical
from wtr.ledger import chain

log = logging.getLogger("wtr.ledger")
ABS_PATH = re.compile(r"""(?:^|[\s"'=:(])/(?:etc|home|Users|root|var|tmp|opt|srv|private|run|usr|mnt)/""")


def load_secrets(path: Path | None) -> set[str]:
    """Secret values from a file or every file in a directory; `KEY=VALUE` lines contribute VALUE."""
    if path is None or not path.exists():
        return set()
    vals = set()
    for p in [path] if path.is_file() else [p for p in path.rglob("*") if p.is_file()]:
        try:
            lines = p.read_text().splitlines()
        except (OSError, UnicodeDecodeError):
            continue
        for v in lines:
            v = v.strip()
            if v.startswith("#"):
                continue
            v = v.split("=", 1)[1].strip() if "=" in v and not v.startswith("-----") else v
            if len(v) >= 8:
                vals.add(v)
    return vals


class Ledger:
    def __init__(self, root: Path, secrets: set[str]):
        self.root, self.secrets = root, secrets
        segs = chain.segments(root)
        if segs:
            truncate_partial_line(segs[-1])  # a half-written event was never acknowledged
        self.seq, self.prev, self.last_ts = chain.tail(root)
        self.f, self.f_path = None, None

    def check(self, type_, payload) -> str | None:
        if type_ not in chain.TYPES:
            return f"unknown type {type_!r}"
        if not isinstance(payload, dict):
            return "payload must be an object"
        try:
            text = canonical.dumps(payload).decode()
        except (ValueError, TypeError) as e:
            return str(e)
        if any(s in text for s in self.secrets):
            return "payload contains a secret value"
        if ABS_PATH.search(text):
            return "payload contains an absolute path"
        return None

    def append(self, type_: str, payload: dict) -> dict:
        ts = max(time.time_ns(), self.last_ts)  # never goes backwards, so segments stay in order
        ev = chain.make(self.seq, ts, type_, payload, self.prev)
        path = chain.segment(self.root, ts)
        if path != self.f_path:
            if self.f:
                self.f.close()
            path.parent.mkdir(parents=True, exist_ok=True)
            self.f, self.f_path = open(path, "ab"), path
        self.f.write(canonical.dumps(ev) + b"\n")
        self.f.flush()
        os.fsync(self.f.fileno())
        self.seq, self.prev, self.last_ts = ev["seq"] + 1, ev["hash"], ts
        return {"seq": ev["seq"], "hash": ev["hash"]}

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while line := await reader.readline():
                try:
                    msg = json.loads(line)
                    err = self.check(msg.get("type"), msg.get("payload"))
                except (ValueError, AttributeError) as e:
                    err = f"bad request: {e}"
                if err:
                    log.warning("rejected: %s", err)
                    reply = {"error": err}
                else:
                    reply = self.append(msg["type"], msg["payload"])  # no await in between: appends are serialized
                writer.write(json.dumps(reply).encode() + b"\n")
                await writer.drain()
        finally:
            writer.close()


async def serve(root: Path, sock: Path, secrets: set[str]) -> None:
    ledger = Ledger(root, secrets)
    sock.parent.mkdir(parents=True, exist_ok=True)
    sock.unlink(missing_ok=True)
    server = await asyncio.start_unix_server(ledger.handle, path=str(sock))
    os.chmod(sock, 0o660)  # group wtr-core only (PRD deployment overview)
    log.info("ledger at seq %d, listening", ledger.seq)
    async with server:
        await server.serve_forever()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, required=True)
    ap.add_argument("--socket", type=Path, required=True)
    ap.add_argument("--secrets", type=Path, default=Path("/etc/wtr"), help="file or directory of secret values")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(serve(a.root, a.socket, load_secrets(a.secrets)))


if __name__ == "__main__":
    main()
