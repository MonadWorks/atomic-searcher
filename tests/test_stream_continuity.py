"""Continuity must not grant quota or disable subscription/chain validation."""
import json
import threading
import time

import pytest

from searcher.activity import stream_session
from searcher.core import Reject, load_config
from searcher.discovery_reads import DiscoveryStreamBudget
from searcher.stream_usage import RequestBudget, StreamSelection
from test_activity import Socket, log
from test_searcher import A, B
from test_stream_pacing import DAY


def test_session_uses_existing_allowance_continuously_and_survives_restart(tmp_path):
    now = [DAY]
    path = tmp_path/'usage.json'
    budget = RequestBudget(path, 500, lambda: now[0])
    for _ in range(128): budget.add(1, 'notification')
    assert budget.pacing_delay() > 100
    budget = RequestBudget(path, 500, lambda: now[0], pacing_mode='session')
    assert budget.pacing_delay() == 0 and budget.state['units'] == 128
    for _ in range(200): budget.add(1, 'notification')
    budget.pause()
    assert budget.pacing_delay() == 0
    restarted = RequestBudget(path, 500, lambda: now[0], pacing_mode='session')
    assert restarted.state['units'] == 328 and restarted.state['bytes'] == 328
    assert restarted.details()['pacing_mode'] == 'session'
    daily = RequestBudget(path, 500, lambda: now[0])
    assert daily.state['units'] == 328 and daily.pacing_delay() > 0
    for _ in range(172): restarted.add(1, 'notification')
    assert restarted.remaining_seconds() > 0
    assert RequestBudget(path, 500, lambda: now[0], pacing_mode='session').remaining_seconds() > 0
    now[0] += 86400
    assert restarted.remaining_seconds() == 0 and restarted.state['units'] == 0


def test_shared_http_debits_remain_binding_in_session_mode(tmp_path):
    http = tmp_path/'http.json'
    http.write_text(json.dumps({'day':'2026-10-14', 'units':80, 'bytes':0}))
    b = DiscoveryStreamBudget(tmp_path/'ws.json', 100, http, split_active=False,
                              clock=lambda: DAY, pacing_mode='session')
    assert b.limit == 20
    for _ in range(20): b.add(1, 'notification')
    assert b.remaining_seconds() > 0 and b.details()['combined_daily_limit'] == 100
    restarted = DiscoveryStreamBudget(b.path, 100, http, split_active=False,
                                      clock=lambda: DAY, pacing_mode='session')
    assert restarted.state['units'] == 20 and restarted.remaining_seconds() > 0


def test_unsubscribe_buffer_does_not_disconnect_other_pools(tmp_path):
    stop = threading.Event()
    class BufferedSocket(Socket):
        closed = False
        def send(self, raw):
            obj = json.loads(raw); self.calls.append(obj)
            if obj['method'] == 'eth_chainId': result = '0x2105'
            elif obj['method'] == 'eth_subscribe':
                result = 'sub-'+str(obj['id'])
                self.messages.append(json.dumps({'id':obj['id'], 'result':result}))
                pool = A if A in obj['params'][1]['address'] else B
                if pool == A: self.old_sid = result
                for _ in range(4 if pool == A else 1):
                    self.messages.append(json.dumps({'method':'eth_subscription',
                        'params':{'subscription':result, 'result':log(pool)}}))
                return
            else:
                result = True
                for _ in range(12):
                    self.messages.append(json.dumps({'method':'eth_subscription',
                        'params':{'subscription':self.old_sid, 'result':log(A)}}))
            self.messages.append(json.dumps({'id':obj['id'], 'result':result}))
        def __exit__(self, *args): self.closed = True
    socket = BufferedSocket(stop)
    budget = RequestBudget(tmp_path/'usage.json', 80000, lambda: DAY)
    events = []
    with pytest.raises(InterruptedError):
        stream_session('wss://example.invalid', 8453, lambda:{A,B}, stop, time.monotonic()+3,
            lambda logs,source:events.extend(logs), lambda *a,**kw:None, budget,
            connect=lambda *a,**kw:socket, selector=StreamSelection(80000))
    assert [e['address'] for e in events] == [A]*16+[B]
    assert budget.state['units'] == 21 and budget.pacing_delay() == 0
    assert [c['method'] for c in socket.calls] == [
        'eth_chainId', 'eth_subscribe', 'eth_unsubscribe', 'eth_subscribe']


def test_session_flood_delivers_last_allowed_log_and_closes_at_daily_cap(tmp_path):
    stop = threading.Event()
    class Flood(Socket):
        closed = False
        def send(self, raw):
            obj = json.loads(raw); self.calls.append(obj)
            if obj['method'] == 'eth_chainId': result = '0x2105'
            elif obj['method'] == 'eth_subscribe': result = 'hot'
            else: result = True
            self.messages.append(json.dumps({'id':obj['id'], 'result':result}))
            if obj['method'] == 'eth_subscribe':
                for _ in range(500):
                    self.messages.append(json.dumps({'method':'eth_subscription',
                        'params':{'subscription':'hot', 'result':log(A)}}))
        def __exit__(self, *args): self.closed = True
    socket = Flood(stop); events = []
    budget = RequestBudget(tmp_path/'usage.json', 200, lambda: DAY, pacing_mode='session')
    with pytest.raises(Reject, match='budget'):
        stream_session('wss://example.invalid', 8453, lambda:{A}, stop, time.monotonic()+3,
            lambda logs,source:events.extend(logs), lambda *a,**kw:None, budget,
            connect=lambda *a,**kw:socket)
    assert socket.closed and len(events) == 198 and budget.state['units'] == 200
    assert budget.remaining_seconds() > 0


def test_invalid_pacing_configuration_rejected(tmp_path):
    config = load_config(); config['policy']['activity_ws_pacing_mode'] = 'unlimited'
    path = tmp_path/'config.json'; path.write_text(json.dumps(config))
    with pytest.raises(Reject, match='pacing'): load_config(path)
    with pytest.raises(Reject, match='pacing'):
        RequestBudget(tmp_path/'usage.json', 80000, pacing_mode='unlimited')


def test_dex_lane_passes_session_policy_to_shared_budget(tmp_path, monkeypatch):
    import searcher.dex_activity as module
    config = load_config(); config['policy']['activity_ws_pacing_mode'] = 'session'
    captured = []
    monkeypatch.setattr(module, 'ROOT', tmp_path)
    monkeypatch.setattr(module, 'websocket_url', lambda:'wss://base-mainnet.core.chainstack.com/test')
    def run(url, chain, pools, stop, finish, publish, health, budget, **kwargs):
        captured.append(budget)
    monkeypatch.setattr(module, 'run_stream', run)
    # stream() only needs these interfaces; never starts a thread/network client.
    from types import SimpleNamespace
    obj = SimpleNamespace(config=config, stream_priorities=lambda:[], stream_pools=lambda:set(),
        stop=threading.Event(), finish=time.monotonic()+1, publish=lambda *a:None, health=lambda *a,**k:None)
    module.DexActivity.stream(obj)
    assert len(captured) == 1 and captured[0].pacing_mode == 'session'
    assert captured[0].total == config['policy']['activity_ws_daily_request_limit'] == 80000
