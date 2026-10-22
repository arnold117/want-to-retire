import urllib.error
import urllib.request

from wtr.contracts import envelope

TIMEOUT_S = 10


def get(url: str) -> tuple[tuple[int, int], int, str]:
    """Blocking GET. Returns (stamp taken after the body is read, status, body). Call via asyncio.to_thread."""
    try:
        with urllib.request.urlopen(url, timeout=TIMEOUT_S) as r:
            body, status = r.read(), r.status
    except urllib.error.HTTPError as e:
        body, status = e.read(), e.code
    return envelope.stamp(), status, body.decode()
