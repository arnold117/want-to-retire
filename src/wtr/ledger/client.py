import json
import socket
from pathlib import Path


class LedgerError(RuntimeError):
    pass


def append(sock: Path, type_: str, payload: dict, timeout: float = 30) -> dict:
    """Blocking. Returns {"seq", "hash"} once the event is on disk; raises if the ledger refuses or is down."""
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        s.connect(str(sock))
        s.sendall(json.dumps({"type": type_, "payload": payload}, ensure_ascii=False).encode() + b"\n")
        buf = b""
        while not buf.endswith(b"\n"):
            chunk = s.recv(65536)
            if not chunk:
                raise LedgerError("ledger closed the connection")
            buf += chunk
    reply = json.loads(buf)
    if "error" in reply:
        raise LedgerError(reply["error"])
    return reply
