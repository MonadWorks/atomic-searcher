"""User entry point for the connected pending-state strategy pipeline."""
import json
import shutil
import time
import fcntl
import signal
import threading
import os

from .core import ROOT, RPC, Reject, atomic_json, check, safe_error
from .engine import prepare
from .pipeline import run_pending
from .snapshots import read_universe, write_universe


def run(config, seconds=1800, executor=None):
    if config.get("structural", {}).get("enabled"):
        from .structural_runner import run as run_structural
        return run_structural(config, seconds, executor)
    check(0 <= seconds <= 86400, "invalid duration")
    check(shutil.disk_usage(ROOT).free > 3*1024**3, "less than 3 GiB free for evidence recording")
    (ROOT/"evidence").mkdir(parents=True, exist_ok=True)
    lock = open(ROOT/"evidence/runtime.lock", "a+")
    try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        lock.close(); raise RuntimeError("another searcher session is already running")
    session = ROOT/"evidence"/(time.strftime("session-%Y%m%d-%H%M%S")+f"-{os.getpid()}-{time.time_ns()%1000000:06d}")
    session.mkdir(parents=True, exist_ok=False)
    source = ROOT/"evidence/universe.json"
    stop = threading.Event()
    old_handlers = {}
    if threading.current_thread() is threading.main_thread():
        for sig in (signal.SIGINT, signal.SIGTERM):
            old_handlers[sig] = signal.signal(sig, lambda *_: stop.set())
    rpc = RPC(timeout=3, cancel_event=stop)
    from .discovery_reads import DiscoveryReads
    discovery_reads = DiscoveryReads(config, rpc, stop, root=ROOT, rpc_factory=RPC, prepare_fn=prepare)
    runtime = ROOT/"evidence/runtime.json"
    result = {"state":"stopped"}
    try:
        from .artifacts import source_manifest
        atomic_json(session/"source-manifest.json", source_manifest())
        atomic_json(session/"config.json", config)
        atomic_json(runtime, {"pid":os.getpid(), "session":str(session), "state":"starting", "executor_connected":executor is not None})
        prior = read_universe(source) if source.exists() else None
        # Refresh before any decision, including after a long offline period.
        from .recovery import retry_connection
        def report_retry(error, delay):
            atomic_json(runtime, {"pid":os.getpid(), "session":str(session), "state":"starting_retry",
                                  "error":error, "retry_in_seconds":delay,
                                  "executor_connected":executor is not None})
        try:
            saved = retry_connection(lambda: discovery_reads.prepare(16, previous=prior),
                                     stop=stop, on_retry=report_retry)
        except InterruptedError:
            if stop.is_set(): return {"state":"stopped", "session":str(session)}
            raise
        if stop.is_set(): return {"state":"stopped", "session":str(session)}
        write_universe(source, saved)
        write_universe(session/"initial-universe.json", saved)
        atomic_json(runtime, {"pid":os.getpid(), "session":str(session), "state":"running", "executor_connected":executor is not None})
        if executor is not None:
            print('已进入自动交易；满足利润和费用条件时会自动发送交易。按 Ctrl-C 或运行 python -m searcher stop 停止。', flush=True)
        result = run_pending(rpc, config, saved, seconds, session/"pipeline", executor, stop, source)
        atomic_json(session/"status.json", result)
        return {"session":str(session), **result}
    except Exception as exc:
        if stop.is_set() and isinstance(exc, Reject) and str(exc) == "RPC cancelled":
            return {"state":"stopped", "session":str(session)}
        result = {"state":"failed", "error":safe_error(exc)}
        atomic_json(session/"status.json", result)
        raise
    finally:
        stop.set(); rpc.close(); discovery_reads.close()
        atomic_json(runtime, {"pid":None, "session":str(session), **result, "executor_connected":False})
        for sig, handler in old_handlers.items(): signal.signal(sig, handler)
        fcntl.flock(lock, fcntl.LOCK_UN); lock.close()
