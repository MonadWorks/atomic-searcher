"""Retry transient transport failures inside the existing unlocked session."""
import threading
import httpx
from .core import RPCUnavailable, safe_error


def transient(error):
    if isinstance(error, (httpx.UnsupportedProtocol, httpx.LocalProtocolError)): return False
    return (isinstance(error, (RPCUnavailable, httpx.RequestError, TimeoutError, ConnectionError)) or
            isinstance(error, httpx.HTTPStatusError) and error.response.status_code in
            (408, 429, 500, 502, 503, 504))


def retry_connection(operation, *, stop=None, on_retry=None, pending=()):
    stop = stop if stop is not None else threading.Event()
    attempts = 0
    while not stop.is_set():
        try:
            return operation()
        except Exception as exc:
            if not transient(exc) and not isinstance(exc, pending): raise
            if stop.is_set(): break
            delay = min(30, 2**min(attempts, 5))
            attempts += 1
            if on_retry: on_retry(safe_error(exc), delay)
            if stop.wait(delay): break
    raise InterruptedError('connection retry stopped')
