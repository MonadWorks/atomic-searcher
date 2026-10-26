"""Local process control for the read-only continuous runner."""
import fcntl
import json
import os
import signal
import subprocess
import sys
import time
from contextlib import contextmanager

from .core import ROOT, Reject, atomic_json, check, safe_error


def runner_status():
    evidence = ROOT/"evidence"
    evidence.mkdir(parents=True, exist_ok=True)
    with open(evidence/"runtime.lock", "a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = False
        except BlockingIOError: locked = True
    path = evidence/"runtime.json"
    state = json.loads(path.read_text()) if path.exists() else {"state":"not_started"}
    state["process_running"] = locked
    if locked and state.get("session"):
        health = __import__("pathlib").Path(state["session"])/"pipeline/health.json"
        if health.exists(): state["pipeline"] = json.loads(health.read_text())
    elif state["state"] in ("running", "starting", "starting_retry"):
        state["state"] = "unexpected_exit"
    return state


def trading_status():
    evidence = ROOT/'evidence'
    evidence.mkdir(parents=True, exist_ok=True)
    with open(evidence/'trading.lock', 'a+') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = False
        except BlockingIOError: locked = True
    path = evidence/'trading-runtime.json'
    state = json.loads(path.read_text()) if path.exists() else {'state': 'not_started'}
    return {**state, 'process_running': locked}


def status():
    trading, runner = trading_status(), runner_status()
    if trading['process_running']:
        return runner if runner['process_running'] and runner.get('pid') == trading.get('pid') else trading
    return runner


@contextmanager
def trading_session(command):
    """Make Stop and duplicate-start protection effective before wallet unlock."""
    evidence = ROOT/'evidence'
    evidence.mkdir(parents=True, exist_ok=True)
    with open(evidence/'trading.lock', 'a+') as lock:
        try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError: raise Reject('已有交易启动或运行进程；请先停止它。') from None
        path = evidence/'trading-runtime.json'
        state = {'pid': os.getpid(), 'state': 'starting_trading', 'command': command,
                 'executor_connected': False, 'started_at': time.time()}
        previous = {}
        def stop_startup(*args):
            # Interrupt getpass as well as RPC/setup. The durable journal is
            # written before broadcasting, so an interrupted setup can resume.
            raise SystemExit('已停止启动；已提交交易保留记录，重启后恢复核对。')
        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                previous[sig] = signal.signal(sig, stop_startup)
            atomic_json(path, state)
            yield
            state['state'] = 'stopped'
        except BaseException as exc:
            state.update(state='stopped' if isinstance(exc, SystemExit) and str(exc).startswith('已停止启动') else 'failed',
                         error=safe_error(exc))
            raise
        finally:
            state.update(pid=None, ended_at=time.time())
            atomic_json(path, state)
            for sig, handler in previous.items(): signal.signal(sig, handler)


def start(config_path):
    current = status()
    if current["process_running"]: return current
    with open(ROOT/"evidence/service.log", "w") as out:
        process = subprocess.Popen([sys.executable, "-m", "searcher", "--config", str(config_path),
                                    "run", "--seconds", "0"], cwd=ROOT, stdin=subprocess.DEVNULL,
                                   stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
    until = time.monotonic()+5
    while time.monotonic() < until:
        check(process.poll() is None, "runner exited during startup; see evidence/service.log")
        current = status()
        if current.get("pid") == process.pid and current["process_running"]: return current
        time.sleep(.05)
    raise RuntimeError("runner startup unconfirmed; inspect evidence/service.log")


def _stop_process(read_status):
    current = read_status()
    # The lifecycle lock precedes PID publication and outlives PID clearing.
    # A stop arriving in either narrow window must re-read before deciding.
    registering_until = time.monotonic()+2
    while current['process_running'] and current.get('pid') is None and time.monotonic() < registering_until:
        time.sleep(.025)
        current = read_status()
    if not current["process_running"]: return current
    pid = current.get("pid")
    check(isinstance(pid, int) and pid > 1 and pid != os.getpid(), "invalid runner process")
    # Check the live process, not only a stale PID in a previous session file.
    argv = subprocess.check_output(["ps", "-p", str(pid), "-o", "command="], text=True).strip()
    check("-m searcher" in argv and any(" "+command in argv for command in ("run", "trade", "launch", "setup-executor")), "process identity mismatch")
    os.kill(pid, signal.SIGTERM)
    until = time.monotonic()+15
    while time.monotonic() < until:
        current = read_status()
        if not current["process_running"]: return current
        time.sleep(.1)
    return {**current, "stop_requested":True}


def stop_runner():
    return _stop_process(runner_status)


def stop():
    current = _stop_process(trading_status)
    if current['process_running']: return {**status(), 'stop_requested': True}
    stop_runner()
    return status()
