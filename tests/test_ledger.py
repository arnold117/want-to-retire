import asyncio
import tempfile
from pathlib import Path

from wtr.ledger import chain, client, server


def test_chain_tamper_reject_and_recover(tmp_path):
    led = server.Ledger(tmp_path, secrets={"sk-live-0123456789"})
    for i in range(3):
        led.append("snapshot", {"arm": "A0", "t": 1792641630000000000 + i, "hash": "ab" * 32})
    assert chain.verify(tmp_path) == []

    assert led.check("snapshot", {"x": 0.1}) is not None  # floats are not canonical
    assert led.check("snapshot", {"note": "key sk-live-0123456789"}) is not None
    assert led.check("snapshot", {"path": "/etc/wtr/r2.token"}) is not None
    assert led.check("snapshot", {"path": "artifacts/a/model.txt"}) is None
    assert led.check("whatever", {}) is not None

    seg = chain.segments(tmp_path)[0]
    original = seg.read_bytes()
    seg.write_bytes(original.replace(b'"arm":"A0"', b'"arm":"A1"', 1))  # change one byte in the first event
    errors = chain.verify(tmp_path)
    assert errors and "seq 0 content" in errors[0]
    seg.write_bytes(original)

    led.f.close()
    with open(seg, "ab") as f:
        f.write(b'{"seq":3,"ts"')  # crash mid-write
    led2 = server.Ledger(tmp_path, secrets=set())
    assert led2.seq == 3 and led2.append("position", {"arm": "A0"})["seq"] == 3
    assert chain.verify(tmp_path) == []


def test_socket_roundtrip():
    with tempfile.TemporaryDirectory(dir="/tmp") as d:  # short path: macOS limits socket paths to 104 bytes
        root, sock = Path(d), Path(d) / "l.sock"

        async def run():
            srv = asyncio.create_task(server.serve(root, sock, set()))
            while not sock.exists():
                await asyncio.sleep(0.01)
            ok = await asyncio.to_thread(client.append, sock, "snapshot", {"arm": "B"})
            try:
                await asyncio.to_thread(client.append, sock, "snapshot", {"p": "/Users/x/y"})
                refused = False
            except client.LedgerError:
                refused = True
            srv.cancel()
            return ok, refused

        ok, refused = asyncio.run(run())
        assert ok["seq"] == 0 and refused and chain.verify(root) == []
