"""LLM gateway wtr-llm-gateway (PRD S3 L3). Holds the API key; strategies never see it.
HTTP on 127.0.0.1 (OpenAI-compatible POST /v1/chat/completions) for strategies; each request must carry a round token
(header X-Round-Token, or Authorization: Bearer <token>). A Unix control socket, reachable only by the runner, registers
tokens and returns the llm_call sequence numbers of a round. Every upstream attempt is one llm_call event in the chain,
with the full request and response bodies and no request headers.
Usage: python -m wtr.gateway.server --ledger SOCK --control SOCK [--port 8787]   (DEEPSEEK_API_KEY in the environment)"""
import argparse
import json
import logging
import os
import socketserver
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from wtr.ledger import client

UPSTREAM = {"deepseek": "https://api.deepseek.com/chat/completions"}  # whitelist; Jev joins when its API is known
RETRIES, BACKOFF_S = 2, 5
log = logging.getLogger("wtr.gateway")


class Rounds:
    def __init__(self):
        self.lock, self.tokens = threading.Lock(), {}

    def register(self, token: str, arm: str, t: int, deadline: int) -> None:
        with self.lock:
            self.tokens[token] = {"arm": arm, "t": t, "deadline": deadline, "seqs": []}

    def get(self, token: str) -> dict | None:
        with self.lock:
            return self.tokens.get(token)

    def close(self, token: str) -> list[int]:
        with self.lock:
            r = self.tokens.pop(token, None)
        return r["seqs"] if r else []


def make_handler(rounds: Rounds, ledger: Path, key: str):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            log.info(fmt, *args)

        def reply(self, status: int, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def refuse(self, status: int, msg: str) -> None:
            self.reply(status, json.dumps({"error": {"message": msg}}).encode())

        def do_POST(self):
            if self.path not in ("/v1/chat/completions", "/chat/completions"):
                return self.refuse(404, "only chat completions")
            token = self.headers.get("X-Round-Token") or self.headers.get("Authorization", "").removeprefix("Bearer ").strip()
            r = rounds.get(token)
            if r is None:
                return self.refuse(401, "unknown round token")
            if time.time_ns() > r["deadline"]:
                return self.refuse(403, "past the round deadline")
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            try:
                req = json.loads(body)
            except ValueError:
                return self.refuse(400, "body is not JSON")
            if req.get("stream"):
                return self.refuse(400, "streaming is not supported")
            if "thinking" not in req:
                return self.refuse(400, "set `thinking` explicitly (DeepSeek enables it by default)")

            for attempt in range(RETRIES + 1):
                left = (r["deadline"] - time.time_ns()) / 1e9
                if left <= 0:
                    return self.refuse(403, "past the round deadline")
                status, resp, trace, t0 = self.forward(body, left)
                ms = (time.monotonic_ns() - t0) // 10**6
                seq = self.record(r, attempt, body, status, resp, trace, ms)
                with rounds.lock:
                    r["seqs"].append(seq)
                if status < 500 or attempt == RETRIES:
                    return self.reply(status if status else 502, resp.encode())
                time.sleep(min(BACKOFF_S, max(0, (r["deadline"] - time.time_ns()) / 1e9)))

        def forward(self, body: bytes, timeout: float) -> tuple[int, str, str | None, int]:
            req = urllib.request.Request(UPSTREAM["deepseek"], data=body, method="POST",
                                         headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
            t0 = time.monotonic_ns()
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    return resp.status, resp.read().decode(), resp.headers.get("x-ds-trace-id"), t0
            except urllib.error.HTTPError as e:
                return e.code, e.read().decode(errors="replace"), e.headers.get("x-ds-trace-id"), t0
            except OSError as e:  # network error or timeout: status 0
                return 0, json.dumps({"error": {"message": f"upstream: {e}"}}), None, t0

        def record(self, r: dict, attempt: int, body: bytes, status: int, resp: str, trace: str | None, ms: int) -> int:
            try:
                parsed = json.loads(resp)
            except ValueError:
                parsed = {}
            payload = {"arm": r["arm"], "t": r["t"], "attempt": attempt, "upstream": "deepseek", "http_status": status,
                       "latency_ms": ms, "trace_id": trace, "model": parsed.get("model"),
                       "system_fingerprint": parsed.get("system_fingerprint"),
                       "usage": json.dumps(parsed.get("usage"), sort_keys=True),  # as text: may hold floats
                       "request": body.decode(), "response": resp}
            return client.append(ledger, "llm_call", payload)["seq"]

    return Handler


def control_server(path: Path, rounds: Rounds) -> socketserver.UnixStreamServer:
    class Control(socketserver.StreamRequestHandler):
        def handle(self):
            for line in self.rfile:
                m = json.loads(line)
                if m["op"] == "register":
                    rounds.register(m["token"], m["arm"], m["t"], m["deadline"])
                    out = {"ok": True}
                else:
                    out = {"seqs": rounds.close(m["token"])}
                self.wfile.write(json.dumps(out).encode() + b"\n")

    path.unlink(missing_ok=True)
    srv = socketserver.ThreadingUnixStreamServer(str(path), Control)
    os.chmod(path, 0o660)
    return srv


def control(path: Path, msg: dict) -> dict:
    """Runner side of the control socket."""
    import socket
    with socket.socket(socket.AF_UNIX) as s:
        s.connect(str(path))
        s.sendall(json.dumps(msg).encode() + b"\n")
        return json.loads(s.makefile().readline())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", type=Path, required=True)
    ap.add_argument("--control", type=Path, required=True)
    ap.add_argument("--port", type=int, default=8787)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    key = os.environ.pop("DEEPSEEK_API_KEY", "")
    if not key:
        raise SystemExit("DEEPSEEK_API_KEY is not set")
    rounds = Rounds()
    ctl = control_server(a.control, rounds)
    threading.Thread(target=ctl.serve_forever, daemon=True).start()
    http = ThreadingHTTPServer(("127.0.0.1", a.port), make_handler(rounds, a.ledger, key))
    log.info("gateway on 127.0.0.1:%d", a.port)
    http.serve_forever()


if __name__ == "__main__":
    main()
