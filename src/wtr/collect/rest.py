import time
import urllib.error
import urllib.request

from wtr.contracts import envelope

TIMEOUT_S = 10


def get(url: str, retries: int = 1) -> tuple[tuple[int, int], int, str]:
    """Blocking GET. Returns (stamp taken after the body is read, status, body). Call via asyncio.to_thread.
    Network errors (TLS handshake timeouts, resets) are retried once; HTTP errors are returned as they are."""
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=TIMEOUT_S) as r:
                body, status = r.read(), r.status
            break
        except urllib.error.HTTPError as e:
            body, status = e.read(), e.code
            break
        except OSError:
            if attempt == retries:
                raise
            time.sleep(1)
    return envelope.stamp(), status, body.decode()
