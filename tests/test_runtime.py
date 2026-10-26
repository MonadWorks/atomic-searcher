import copy
import json
import threading
import time
from types import SimpleNamespace

import pytest

from searcher.core import Reject, load_config
from searcher.pipeline import run_pending
from searcher.pools import discover


class FakeRPC:
    def __init__(self, url="test", chain_id=8453, timeout=3, cancel_event=None):
        self.url, self.chain_id, self.cancel_event = url, chain_id, cancel_event
    def verify_chain(self): pass
    def close(self): pass


@pytest.mark.parametrize('scheme', ['socks5', 'socks5h'])
def test_rpc_initializes_with_terminal_socks_proxy(monkeypatch, scheme):
    from searcher.core import RPC
    for key in ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'NO_PROXY',
                'http_proxy', 'https_proxy', 'all_proxy', 'no_proxy'):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv('ALL_PROXY', f'{scheme}://127.0.0.1:1')
    # Previously failed here with ImportError, before opening any connection.
    rpc = RPC(timeout=1)
    rpc.close()
    assert rpc.loop.is_closed()


@pytest.mark.parametrize('starting', [True, False])
def test_stop_waits_for_lifecycle_pid_publication_or_lock_release(monkeypatch, starting):
    import searcher.service as service
    states = [{'process_running': True, 'pid': None}]
    if starting: states.append({'process_running': True, 'pid': 99999999})
    states.append({'process_running': False, 'pid': None})
    sent = []
    monkeypatch.setattr(service.time, 'sleep', lambda _: None)
    monkeypatch.setattr(service.subprocess, 'check_output', lambda *args, **kwargs: 'python -m searcher launch')
    monkeypatch.setattr(service.os, 'kill', lambda pid, sig: sent.append(pid))
    result = service._stop_process(lambda: states.pop(0))
    assert not result['process_running'] and sent == ([99999999] if starting else [])


@pytest.mark.parametrize('phase', ['preflight', 'wallet', 'setup', 'executor', 'handoff'])
def test_stop_finds_launch_before_runner_and_prevents_later_trading(tmp_path, monkeypatch, phase):
    """Real subprocess and SIGTERM, actual CLI/session/stop, no network or keys."""
    import subprocess
    import sys
    import searcher.service as service
    from searcher.core import ROOT
    package = tmp_path/'searcher'; package.mkdir()
    (package/'__init__.py').write_text(f'__path__.append({str(ROOT/"searcher")!r})\n')
    (package/'__main__.py').write_text('''
import importlib.util
import sys, time
from pathlib import Path
from types import SimpleNamespace
from searcher import service, execution, onboarding, runner
from searcher.core import ROOT, Reject
spec = importlib.util.spec_from_file_location('searcher.cli_under_test', ROOT/'searcher/__main__.py')
cli = importlib.util.module_from_spec(spec); spec.loader.exec_module(cli)
config=cli.load_config();config['v4']={'enabled':False};config['structural']={'enabled':False}
config['owner']='0x'+'2'*40  # Explicit synthetic owner, not the bundled example.
cli.load_config=lambda *a:config
directory = Path.cwd()
service.ROOT = directory
phase = sys.argv[-1]
def barrier(name):
    if name == phase:
        (directory/'ready').write_text(name)
        while True: time.sleep(.02)
class NoNetwork:
    def __init__(self, **kwargs): pass
    def verify_chain(self): pass
    def close(self): pass
def unlock(*args):
    barrier('wallet'); return SimpleNamespace(address='test public identity')
def setup(*args):
    barrier('setup'); return {'contract': 'synthetic-only'}
class LocalExecutor:
    def __init__(self, *args): barrier('executor')
    def close(self): pass
def handoff():
    barrier('handoff'); return {'process_running': False}
def unexpected_run(*args): (directory/'trading_started').write_text('unexpected')
execution.BroadcastRPC = NoNetwork
execution.verify_execution_context = lambda rpc: None
execution.Executor = LocalExecutor
onboarding.unlock_wallet = unlock
onboarding.preflight_setup = lambda *args: barrier('preflight')
onboarding.setup_executor = setup
service.runner_status = handoff
runner.run = unexpected_run
sys.argv = ['searcher', 'launch', '--seconds', '1']
try: cli.main()
except Reject as exc: raise SystemExit(str(exc)) from None
''')
    monkeypatch.setattr(service, 'ROOT', tmp_path)
    child = subprocess.Popen([sys.executable, '-m', 'searcher', 'launch', phase], cwd=tmp_path,
                             stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        wait_until(lambda: (tmp_path/'ready').exists())
        current = service.status()
        assert current['process_running'] and current['pid'] == child.pid
        # A second launch fails before wallet input while the first owns the session.
        with pytest.raises(Reject, match='已有交易'):
            with service.trading_session('launch'): pytest.fail('duplicate launch acquired the lock')
        stopped = service.stop()
        stdout, stderr = child.communicate(timeout=3)
        assert not stopped['process_running'] and '已停止启动' in stderr and 'Traceback' not in stderr
        assert not (tmp_path/'trading_started').exists()
        assert not service.trading_status()['process_running']
    finally:
        if child.poll() is None: child.kill()
        child.communicate(timeout=3)


def setup_runtime(monkeypatch):
    import searcher.pipeline as module
    monkeypatch.setattr(module, "RPC", FakeRPC)
    class Strategy:
        def __init__(self, *args): pass
        def on_frame(self, frame):
            return {"block":1, "decision_age_ms":0, "computation_ms":0, "candidates":[]}
    monkeypatch.setattr(module, "PendingStrategy", Strategy)
    class Feed:
        def __init__(self, rpc, config, saved): self.count = 0
        def poll(self):
            self.count += 1
            now = time.monotonic_ns()
            return SimpleNamespace(requested_ns=now, received_ns=now, received_utc=time.time(),
                                   header={"number": "0x1", "timestamp": "0x1", "hash": "0x"+"11"*32},
                                   preconfirmed=None, payload=lambda:{"receipts":[]})
    monkeypatch.setattr(module, "StateFeed", Feed)
    c = load_config(); c["policy"]["hot_lane_enabled"] = False
    c["v4"]={"enabled":False}  # Synthetic feed fixtures model CL/V2 only.
    c["policy"]["pending_poll_interval_seconds"] = .01
    c["policy"]["universe_refresh_seconds"] = .01
    return module, Feed, c


def wait_until(predicate, timeout=3):
    end = time.monotonic()+timeout
    while time.monotonic() < end:
        if predicate(): return
        time.sleep(.01)
    raise AssertionError("condition did not become true")


def test_stop_cancels_discovery_before_any_late_publication(monkeypatch, tmp_path):
    _, _, c = setup_runtime(monkeypatch)
    entered = threading.Event()
    def prepare(rpc, *args, **kwargs):
        entered.set(); rpc.cancel_event.wait(5)
        return {"pools":[{"valid":True}], "discovery_cursor":2}
    monkeypatch.setattr("searcher.engine.prepare", prepare)
    stop = threading.Event(); results = []
    t = threading.Thread(target=lambda:results.append(run_pending(FakeRPC(), c, {}, 0, tmp_path, stop_event=stop,
                                                                 universe_path=tmp_path/"durable.json")))
    t.start(); assert entered.wait(2); stop.set(); t.join(2)
    assert not t.is_alive() and results[0]["discovery_stopped"] and results[0]["reader_stopped"]
    assert not (tmp_path/"universe-latest.json").exists() and not (tmp_path/"durable.json").exists()


def test_lost_feed_reports_degraded_and_stops_cleanly(monkeypatch, tmp_path):
    module, Feed, c = setup_runtime(monkeypatch)
    c["policy"]["universe_refresh_seconds"] = 100
    class LostFeed(Feed):
        def poll(self):
            if self.count: raise OSError("test disconnected")
            return super().poll()
    monkeypatch.setattr(module, "StateFeed", LostFeed)
    stop = threading.Event()
    t = threading.Thread(target=lambda:run_pending(FakeRPC(), c, {}, 0, tmp_path, stop_event=stop))
    t.start()
    try:
        def degraded():
            p = tmp_path/"health.json"
            return p.exists() and json.loads(p.read_text())["state"] == "degraded"
        wait_until(degraded)
    finally: stop.set(); t.join(2)
    assert not t.is_alive()
    summary = json.loads((tmp_path/"feed-summary.json").read_text())
    assert summary["errors"] > 0


def test_empty_refresh_cannot_replace_good_feed(monkeypatch, tmp_path):
    module, Feed, c = setup_runtime(monkeypatch)
    accepted = []
    class RememberFeed(Feed):
        def __init__(self, rpc, config, saved):
            accepted.append(copy.deepcopy(saved)); super().__init__(rpc, config, saved)
    monkeypatch.setattr(module, "StateFeed", RememberFeed)
    monkeypatch.setattr("searcher.engine.prepare", lambda *args, **kwargs:{"pools":[], "discovery_cursor":2})
    run_pending(FakeRPC(), c, {"pools":[{"valid":True}]}, .12, tmp_path)
    assert len(accepted) == 1 and accepted[0]["pools"]
    assert json.loads((tmp_path/"discovery-health.json").read_text())["state"] == "retrying"


def test_accepted_refresh_is_persisted_for_restart(monkeypatch, tmp_path):
    _, _, c = setup_runtime(monkeypatch)
    updated = {"pools":[{"valid":True}], "discovery_cursor":55}
    monkeypatch.setattr("searcher.engine.prepare", lambda *args, **kwargs:copy.deepcopy(updated))
    source = tmp_path/"restart-universe.json"
    result = run_pending(FakeRPC(), c, {}, .1, tmp_path, universe_path=source)
    assert json.loads(source.read_text()) == updated and result["discovery_stopped"]


def test_failed_factory_reads_are_not_an_empty_success():
    class Missing:
        def views(self, calls, block): return [None]*len(calls)
    with pytest.raises(Reject, match="factory lookup unavailable"):
        discover(Missing(), load_config(), 1)


def test_cancelled_rpc_never_opens_network():
    from searcher.core import RPC
    stop = threading.Event(); stop.set()
    rpc = RPC("http://127.0.0.1:1", cancel_event=stop)
    with pytest.raises(Reject, match="cancelled"): rpc.request("eth_chainId", [])
    with pytest.raises(Reject, match="cancelled"): rpc.batch([("eth_chainId", [])])


def test_runner_refreshes_saved_discovery_before_first_decision(monkeypatch, tmp_path):
    import searcher.runner as runner
    evidence = tmp_path/"evidence"; evidence.mkdir()
    previous = {"discovery_cursor":12}; updated = {"discovery_cursor":15}
    (evidence/"universe.json").write_text(json.dumps(previous))
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "RPC", FakeRPC)
    captured = []
    def prepare(rpc, config, blocks, previous=None): captured.append(previous); return updated
    monkeypatch.setattr(runner, "prepare", prepare)
    def pipeline(rpc, config, saved, seconds, directory, executor, stop, source):
        assert saved == updated and json.loads(source.read_text()) == updated
        return {"state":"stopped"}
    monkeypatch.setattr(runner, "run_pending", pipeline)
    runner.run({**load_config(),'structural':{'enabled':False}}, 1)
    assert captured == [previous]


def test_runner_failure_is_not_reported_as_clean_stop(monkeypatch, tmp_path):
    import searcher.runner as runner
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "RPC", FakeRPC)
    def fail(*args, **kwargs): raise Reject("wrong chain")
    monkeypatch.setattr(runner, "prepare", fail)
    with pytest.raises(Reject, match="wrong chain"): runner.run({**load_config(),'structural':{'enabled':False}}, 1)
    state = json.loads((tmp_path/"evidence/runtime.json").read_text())
    assert state["state"] == "failed" and state["pid"] is None and state["error"] == "wrong chain"


def test_reader_initialization_failure_stops_every_worker(monkeypatch, tmp_path):
    module, _, c = setup_runtime(monkeypatch)
    class BrokenFeed:
        def __init__(self, *args): raise ValueError("missing runtime")
    monkeypatch.setattr(module, "StateFeed", BrokenFeed)
    result = run_pending(FakeRPC(), c, {}, 2, tmp_path)
    assert result["state"] == "failed" and result["worker_errors"] == ["missing runtime"]
    assert result["reader_stopped"] and result["discovery_stopped"]


def test_continuous_recording_expires_old_payloads_and_keeps_monotonic_ids(tmp_path):
    from searcher.recorder import Recorder, decode_payload
    r = Recorder(tmp_path/"bounded.sqlite", max_bytes=2500)
    for n in range(100):
        assert r.append(n, n+1, "test", {"receipts":[], "n":n, "data":list(range(100))}) == n+1
    summary = r.summary()
    assert summary["retention"]["expired_frames"] > 0
    assert summary["retention"]["retained_payload_bytes"] <= 2500
    rows = r.db.execute("SELECT id,payload FROM frames ORDER BY id").fetchall()
    assert rows[-1][0] == 100 and decode_payload(rows[-1][1])["n"] == 99
    assert rows[0][0] > 1
    r.close()
    r = Recorder(tmp_path/"bounded.sqlite", max_bytes=2500)
    assert r.append(100, 101, "test", {"receipts":[]}) == 101
    r.close()


def test_deep_reads_preserve_whole_routes_and_enforce_backoff():
    from searcher.statefeed import DeepReadScheduler
    q = DeepReadScheduler()
    routes = [(str(i), str(i+1), str(i+2)) for i in range(0, 60, 3)]
    available = {str(i) for i in range(60)}
    q.add(routes+routes, 1)
    first = q.take(available, 1)
    assert len(first) == 24 and len(q.pending) == 12
    q.add(routes[:8], 2)
    assert len(q.pending) == 12  # completed routes cannot monopolize the queue
    second = q.take(available, 2)
    assert not first & second
    q.add(routes[:8], 17)
    assert len(q.pending) == 12
    for route in routes:
        assert not (set(route) & first) or set(route).issubset(first)


def test_feed_reconnects_and_clears_error_state(monkeypatch, tmp_path):
    module, Feed, c = setup_runtime(monkeypatch)
    c["policy"]["universe_refresh_seconds"] = 100
    class ReconnectFeed(Feed):
        failed = False
        def poll(self):
            if not self.failed:
                self.failed = True
                raise OSError("test network outage")
            return super().poll()
    monkeypatch.setattr(module, "StateFeed", ReconnectFeed)
    result = run_pending(FakeRPC(), c, {}, 1.2, tmp_path)
    assert result["counts"]["decisions"] > 0
    health = json.loads((tmp_path/"feed-health.json").read_text())
    assert health["state"] == "connected" and "error" not in health


def test_expensive_deep_read_falls_back_without_stalling_decisions(monkeypatch, tmp_path):
    module, Feed, c = setup_runtime(monkeypatch)
    c["policy"]["universe_refresh_seconds"] = 100
    attempts = []
    class CostlyFeed(Feed):
        def __init__(self, *args):
            super().__init__(*args)
            from searcher.core import WETH, USDC
            from searcher.pools import Pool
            self.pools = [Pool(a,'factory','v2',WETH,USDC,0,3000) for a in ('a','b')]
        def poll(self, deep_pools=()):
            if deep_pools:
                attempts.append(deep_pools)
                raise OSError("read-only call exhausted gas allowance")
            frame = super().poll(); frame.transport = {}; return frame
    class Strategy:
        def __init__(self, *args):
            from searcher.route_feedback import RouteFeedback
            self.execution_feedback=RouteFeedback()
        def on_frame(self, frame):
            return {"block":1, "decision_age_ms":0, "computation_ms":0, "candidates":[], "deep_read_routes":[("a", "b")]}
    monkeypatch.setattr(module, "StateFeed", CostlyFeed)
    monkeypatch.setattr(module, "PendingStrategy", Strategy)
    result = run_pending(FakeRPC(), c, {}, .2, tmp_path)
    assert len(attempts) == 1 and result["counts"]["decisions"] > 2
    assert result["worker_errors"] == []


def test_profitable_probe_exceeding_first_transaction_budget_is_rejected(monkeypatch, tmp_path):
    module, _, c = setup_runtime(monkeypatch)
    c["policy"]["universe_refresh_seconds"] = 100
    token = c["settlement_tokens"][0]["address"]
    class Strategy:
        def __init__(self, *args):
            from searcher.route_feedback import RouteFeedback
            self.index = SimpleNamespace(routes={"r":"route"}, states={})
            self.execution_feedback = RouteFeedback()
        def on_frame(self, frame):
            return {"block":1, "anchor":frame.header, "decision_age_ms":0, "computation_ms":0, "candidates":[{
                "tokens":[token], "route_key":"r", "amount_in":100, "minimum_gross_surplus":10,
                "cost_budget":{"gas_limit":480000}}]}
    monkeypatch.setattr(module, "PendingStrategy", Strategy)
    monkeypatch.setattr(module, "probe_cycle", lambda *args, **kwargs:{
        "gross_surplus":100, "execution_gas":502332, "first_transaction_gas_budget":563124,
        "includes_pool_registration":True})
    result = run_pending(FakeRPC(), c, {}, .1, tmp_path)
    assert result["counts"].get("whole_cycle_passed", 0) == 0
    decisions = [json.loads(p.read_text()) for p in tmp_path.glob("decision-*.json")]
    assert decisions and all(d["candidates"][0]["status"] == "execution_rejected" for d in decisions)
    assert all("registration" in d["candidates"][0]["error"] for d in decisions)


@pytest.mark.parametrize("reason", ["count", "size"])
def test_opportunity_retention_preserves_latest_and_leaves_ledger_untouched(tmp_path, reason):
    import gzip
    from searcher.retention import save_opportunity
    ledger = tmp_path/"transactions.sqlite"; ledger.write_bytes(b"account evidence")
    for i in range(12):
        save_opportunity(tmp_path, i, {"id":i, "data":list(range(1000))},
                         5000 if reason == "size" else 1_000_000, 3 if reason == "count" else 100)
    files = list(tmp_path.glob("opportunity-*.json.gz"))
    assert len(files) <= 3 and ledger.read_bytes() == b"account evidence"
    assert json.loads(gzip.open(tmp_path/"opportunity-11.json.gz", "rt").read())["id"] == 11
    assert json.loads((tmp_path/"opportunity-retention.json").read_text())["expired_count"] > 0


@pytest.mark.parametrize("mode", ["cancel", "deadline"])
@pytest.mark.parametrize("phase", ["headers", "body"])
def test_dripping_http_response_has_cancel_and_total_deadline(mode, phase):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from searcher.core import RPC
    entered = threading.Event()
    class Drip(BaseHTTPRequestHandler):
        def do_POST(self):
            try:
                self.rfile.read(int(self.headers["Content-Length"]))
                if phase == "body":
                    self.send_response(200); self.send_header("Content-Length", "10000"); self.end_headers()
                else: self.wfile.write(b"HTTP/1.1 200 OK\r\nX-Drip: ")
                self.wfile.flush(); entered.set()
                for _ in range(100):
                    self.wfile.write(b" "); self.wfile.flush(); time.sleep(.025)
            except (BrokenPipeError, ConnectionResetError): pass
        def log_message(self, *args): pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Drip)
    serving = threading.Thread(target=server.serve_forever, daemon=True); serving.start()
    stop = threading.Event(); result = []
    # Finish client/certificate initialization before timing the request. The
    # assertion below concerns a dripping response, not host scheduling during
    # construction of the test client.
    rpc = RPC(f"http://127.0.0.1:{server.server_port}", timeout=.25 if mode == "deadline" else 3, cancel_event=stop)
    def request():
        try: rpc.request("eth_chainId", [])
        except Exception as exc: result.append(str(exc))
        finally: rpc.close()
    worker = threading.Thread(target=request); worker.start()
    try:
        assert entered.wait(1)
        started = time.monotonic()
        if mode == "cancel": stop.set()
        worker.join(1)
        assert not worker.is_alive() and time.monotonic()-started < .8
        assert result and ("cancelled" in result[0] if mode == "cancel" else "deadline" in result[0])
    finally: stop.set(); worker.join(4); server.shutdown(); server.server_close(); serving.join()
