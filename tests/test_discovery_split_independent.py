"""Independent local counterexamples for discovery routing and paid-node accounting.

HTTP tests use httpx.MockTransport only; pipeline tests replace every connection.
No local credentials, production journals, external RPC, or signing are used.
"""
import asyncio
import copy
from datetime import datetime, timezone
import json
import threading
import time
from types import SimpleNamespace

import httpx
import pytest

from searcher import core, discovery_reads as reads, engine
from searcher.core import Reject, RPCUnavailable
from searcher.discovery_reads import DiscoveryBudgetUnavailable, DiscoveryHTTPBudget, RecentDiscoveryRPC
from test_discovery_history import harness
from test_discovery_timing_independent import record_reads


DAY = 1791936000.0
WS = 'wss://example.chainstack.com/fake-private-token'


def journal(path, units, *, now=DAY, **extra):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(day=datetime.fromtimestamp(now, timezone.utc).strftime('%Y-%m-%d'),
                                    units=units, bytes=0, **extra)))


def budget(tmp_path, *, clock=lambda: DAY):
    return DiscoveryHTTPBudget(tmp_path/'http.json', tmp_path/'ws.json', 80_000, 40_000, clock=clock)


def mock_rpc(tmp_path, handler, *, stop=None):
    meter = budget(tmp_path)
    rpc = RecentDiscoveryRPC('https://example.chainstack.com/fake-private-token',
                             timeout=.5, cancel_event=stop, budget=meter)
    rpc.loop.run_until_complete(rpc.session.aclose())
    rpc.session = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    rpc.begin_round()
    return rpc, meter


def response(request, status=200):
    payload = json.loads(request.content)
    rows = payload if isinstance(payload, list) else [payload]
    result = [{'id': row['id'], 'jsonrpc': '2.0', 'result': '0x2105'} for row in rows]
    return httpx.Response(status, json=result if isinstance(payload, list) else result[0])


def test_legacy_ws_spend_is_preserved_and_http_cannot_claim_a_fresh_half(tmp_path):
    journal(tmp_path/'ws.json', 70_000)
    journal(tmp_path/'http.json', 9_999, pace_credit=1., pace_at=DAY, round_units=1)
    meter = budget(tmp_path)
    meter.acquire(1)
    with pytest.raises(DiscoveryBudgetUnavailable): meter.acquire(1)
    assert budget(tmp_path).details()['local_units'] == 10_000
    assert meter.details()['remaining_units'] == 0


def test_two_budget_instances_cannot_both_spend_the_last_unit(tmp_path):
    journal(tmp_path/'ws.json', 40_000)
    journal(tmp_path/'http.json', 39_999, pace_credit=1., pace_at=DAY, round_units=1)
    meters = [budget(tmp_path), budget(tmp_path)]
    barrier = threading.Barrier(2)
    outcomes = []
    def take(meter):
        barrier.wait()
        try: meter.acquire(1); outcomes.append('spent')
        except DiscoveryBudgetUnavailable: outcomes.append('exhausted')
    threads = [threading.Thread(target=take, args=(meter,)) for meter in meters]
    for thread in threads: thread.start()
    for thread in threads: thread.join(2)
    assert sorted(outcomes) == ['exhausted', 'spent']
    assert json.loads((tmp_path/'http.json').read_text())['units'] == 40_000


def test_restart_and_backward_utc_clock_do_not_restore_spent_units(tmp_path):
    now = [DAY+90_000]
    meter = budget(tmp_path, clock=lambda: now[0])
    meter.acquire(20)
    state = json.loads((tmp_path/'http.json').read_text())
    now[0] -= 90_000
    restored = budget(tmp_path, clock=lambda: now[0])
    assert restored.details()['local_units'] == 20
    assert restored.details()['usage_day_utc'] == state['day']
    assert restored.details()['pacing_credit'] <= state['pace_credit']


def test_http_batch_debits_each_rpc_method_before_network(tmp_path):
    seen = []
    def handler(request):
        seen.append(json.loads((tmp_path/'http.json').read_text())['units'])
        return response(request)
    rpc, meter = mock_rpc(tmp_path, handler)
    try:
        assert rpc.batch([('eth_chainId', []), ('eth_gasPrice', [])]) == ['0x2105', '0x2105']
        # Known recent-free method 1, unknown-age method conservatively 2.
        assert seen == [3] and meter.details()['local_units'] == 3
        assert rpc.measurements[-1]['attempt_timings'][0]['discovery_units_debited'] == 3
    finally: rpc.close()


def test_each_retry_and_failed_response_remains_durably_debited(tmp_path):
    class NoSleepStop:
        def is_set(self): return False
        def wait(self, timeout): return False
    seen = []
    def handler(request):
        seen.append(json.loads((tmp_path/'http.json').read_text())['units'])
        return response(request, 503 if len(seen) < 3 else 200)
    rpc, meter = mock_rpc(tmp_path, handler, stop=NoSleepStop())
    try:
        assert rpc.request('eth_chainId', []) == '0x2105'
        assert seen == [1, 2, 3]
        assert budget(tmp_path).details()['local_units'] == 3
        assert rpc.measurements[-1]['attempts'] == 3
    finally: rpc.close()


def test_lost_response_is_charged_and_bare_credentials_are_not_reported(tmp_path):
    def handler(request):
        raise httpx.ReadError('provider repeated fake-private-token without URL', request=request)
    rpc, meter = mock_rpc(tmp_path, handler)
    try:
        with pytest.raises(RPCUnavailable) as error: rpc.request('eth_chainId', [])
        assert 'fake-private-token' not in str(error.value)
        assert meter.details()['local_units'] == 1
        assert rpc.measurements[-1]['outcome'] == 'error'
    finally: rpc.close()


def test_failed_durable_write_sends_nothing(tmp_path, monkeypatch):
    sent = []
    rpc, _ = mock_rpc(tmp_path, lambda request: sent.append(request) or response(request))
    def failed_write(*args): raise OSError('disk unavailable')
    monkeypatch.setattr(reads, 'atomic_json', failed_write)
    try:
        with pytest.raises(RPCUnavailable): rpc.request('eth_chainId', [])
        assert sent == []
    finally: rpc.close()


def test_stop_after_debit_before_transport_keeps_debit_without_sending(tmp_path, monkeypatch):
    sent, stop = [], threading.Event()
    rpc, meter = mock_rpc(tmp_path, lambda request: sent.append(request) or response(request), stop=stop)
    acquire = meter.acquire
    def debit_then_stop(*args, **kwargs):
        acquire(*args, **kwargs); stop.set()
    monkeypatch.setattr(meter, 'acquire', debit_then_stop)
    try:
        with pytest.raises(Reject, match='cancelled'): rpc.request('eth_chainId', [])
        assert sent == [] and meter.details()['local_units'] == 1
    finally: rpc.close()


def test_history_hints_are_authenticated_and_snapshotted_only_on_recent_connection(harness, monkeypatch):
    recent, config, initial, existing, candidate, validated = harness
    history = copy.copy(recent)
    history.ranges = []; history.header_calls = type(recent.header_calls)()
    history.events = [(15, candidate.address)]
    recent.events = [(100, existing.address)]
    observations = []
    validate = engine.validate_pools
    def checked_validate(rpc, *args):
        observations.append(('auth', rpc))
        return validate(rpc, *args)
    from searcher import statefeed
    feed = statefeed.StateFeed
    class CheckedFeed:
        def __init__(self, rpc, *args):
            observations.append(('state', rpc)); self.delegate = feed(rpc, *args)
        def poll(self, *args): return self.delegate.poll(*args)
    monkeypatch.setattr(engine, 'validate_pools', checked_validate)
    monkeypatch.setattr(statefeed, 'StateFeed', CheckedFeed)
    result = engine.prepare(recent, config, previous=initial, history_rpc=history)
    assert history.ranges == [[11, 20]]
    assert all(lo >= 69 for lo, hi in recent.ranges)
    assert candidate.address in validated
    assert observations and all(rpc is recent for _, rpc in observations)
    assert result['discovery_recovered_range'] == [11, 20]


def test_history_without_measurements_cannot_make_whole_round_timing_complete(harness):
    recent, config, initial, *_ = harness
    history = copy.copy(recent)
    history.ranges = []; history.header_calls = type(recent.header_calls)()
    record_reads(recent, 100)
    result = engine.prepare(recent, config, previous=initial, history_rpc=history)
    timing = result['discovery_read_timing']
    assert result['discovery_recovered_range'] == [11, 20]
    assert timing['sources']['recent']['complete'] is True
    assert timing['sources']['history']['available'] is False
    assert timing['complete'] is False


def test_public_override_does_not_restore_ws_units_already_allocated_to_http(tmp_path, monkeypatch):
    """Exercise real pipeline WS budget construction, not a reconstructed formula."""
    import searcher.activity as activity
    import searcher.pipeline as pipeline
    from searcher.statefeed import StateFrame
    from test_fastlane import config
    from test_pipeline import saved, header, H0, H1
    from test_runtime import FakeRPC

    cfg = config(); cfg['v4'] = {'enabled': False}
    current = time.time()
    journal(tmp_path/'evidence/activity-stream-requests.json', 40_000, now=current)
    journal(tmp_path/'evidence/discovery-http-requests.json', 40_000, now=current,
            pace_credit=0., pace_at=current, round_units=21)
    monkeypatch.setattr(core, 'ROOT', tmp_path)
    monkeypatch.setenv('BASE_DISCOVERY_RPC_URL', 'https://base.drpc.org')
    captured, ready, stop = [], threading.Event(), threading.Event()
    class Fake(FakeRPC):
        def request(self, *args): return []
    class Feed:
        pages = None
        def __init__(self, *args): pass
        def poll(self, *args, **kwargs):
            assert ready.wait(3)
            stop.set()
            now = time.monotonic_ns()
            return StateFrame(header(2,H1,H0),header(2,H1,H0),saved()['pools'],now,now,time.time(),3,106)
    class Strategy:
        def __init__(self, *args): self.execution_feedback = object()
        def on_frame(self, frame): return {'block': 2, 'decision_age_ms': 0, 'candidates': []}
    def capture(*args, **kwargs):
        captured.append(args[7]); ready.set(); stop.wait(3)
    monkeypatch.setattr(pipeline, 'RPC', Fake)
    monkeypatch.setattr(pipeline, 'StateFeed', Feed)
    monkeypatch.setattr(pipeline, 'PendingStrategy', Strategy)
    monkeypatch.setattr(activity, 'websocket_url', lambda: WS)
    monkeypatch.setattr(activity, 'run_stream', capture)
    cfg['policy'].update(hot_lane_enabled=False, universe_refresh_seconds=300)
    pipeline.run_pending(Fake(), cfg, saved(), 5, tmp_path/'run', stop_event=stop)
    assert len(captured) == 1
    assert captured[0].state['units'] == 40_000
    assert captured[0].limit <= 40_000
    assert captured[0].remaining_seconds() > 0


def test_ws_mode_change_keeps_http_spend_until_real_next_utc_day(tmp_path):
    from searcher.discovery_reads import DiscoveryStreamBudget
    now = [DAY+5]
    journal(tmp_path/'ws.json', 40_000)
    journal(tmp_path/'http.json', 40_000)
    stream = DiscoveryStreamBudget(tmp_path/'ws.json', 80_000, tmp_path/'http.json',
                                   split_active=False, clock=lambda: now[0])
    assert stream.limit == 40_000 and stream.remaining_seconds() > 0
    now[0] -= 86_400
    assert stream.remaining_seconds() > 0 and stream.state['units'] == 40_000
    now[0] = DAY+86_405
    assert stream.remaining_seconds() == 0
    assert stream.limit == 80_000 and stream.state['units'] == 0


@pytest.mark.parametrize('user_stop', [False, True])
def test_real_history_transport_is_cancelled_and_its_event_restored(tmp_path, monkeypatch, user_stop):
    monkeypatch.delenv('BASE_DISCOVERY_RPC_URL', raising=False)
    stop, calls, cleanup = threading.Event(), [], []
    async def handler(request):
        calls.append('history')
        if len(calls) == 1:
            if user_stop: stop.set()
            try: await asyncio.sleep(1)
            finally: cleanup.append('operation_closed')
        return response(request)
    clients = []
    def public_factory(*args, **kwargs):
        rpc = core.RPC(*args, **kwargs)
        rpc.loop.run_until_complete(rpc.session.aclose())
        rpc.session = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        clients.append(rpc)
        return rpc
    class ShortPrivate(RecentDiscoveryRPC):
        MAX_ROUND_SECONDS = .04
    entries = []
    def prepare(rpc, config, recent_blocks, previous=None, history_rpc=None):
        entries.append('private' if history_rpc is not None else 'public')
        (history_rpc or rpc).request('eth_chainId', [])
        return {'pools': [], 'marker': entries[-1]}
    helper = reads.DiscoveryReads(
        {'policy': {'discovery_rpc_url': 'https://history.invalid'}},
        SimpleNamespace(url='https://state.invalid', chain_id=8453), stop, root=tmp_path,
        websocket=lambda: WS, rpc_factory=public_factory, private_factory=ShortPrivate, prepare_fn=prepare)
    started = time.monotonic()
    try:
        if user_stop:
            with pytest.raises(Reject, match='cancelled'): helper.prepare()
            assert entries == ['private']
        else:
            result = helper.prepare()
            assert entries == ['private', 'public']
            assert result['marker'] == 'public'
            assert result['discovery_read_route']['discarded_private_attempt']['outcome'] == 'discarded'
        assert cleanup == ['operation_closed']
        assert time.monotonic()-started < .5
        assert helper.public.cancel_event is stop and helper.private.cancel_event is stop
    finally: helper.close()
    assert all(rpc.loop.is_closed() for rpc in clients)


def test_real_engine_discards_private_only_metadata_after_final_reorg(harness, tmp_path, monkeypatch):
    monkeypatch.delenv('BASE_DISCOVERY_RPC_URL', raising=False)
    recent, config, initial, existing, candidate, validated = harness
    before = copy.deepcopy(initial)
    public = copy.copy(recent)
    public.ranges = []; public.header_calls = type(recent.header_calls)()
    public.events = [(100, existing.address)]
    recent.events = [(99, candidate.address), (100, existing.address)]
    recent.round_units = 0
    recent.cancel_event = public.cancel_event = threading.Event()
    closed = []
    recent.close = lambda: closed.append('private')
    public.close = lambda: closed.append('public')
    def begin(): recent.round_deadline = time.monotonic()+60
    recent.begin_round = begin
    original = recent.header
    injected = []
    def reorg_at_final(height=None):
        value = original(height)
        if height == 100 and recent.header_calls[100] >= 2:
            injected.append(height)
            value['hash'] = '0x'+'ff'*32
        return value
    recent.header = reorg_at_final
    helper = reads.DiscoveryReads(config, SimpleNamespace(url='https://state.invalid', chain_id=8453),
        threading.Event(), root=tmp_path, websocket=lambda: WS,
        rpc_factory=lambda *args, **kwargs: public, private_factory=lambda *args, **kwargs: recent)
    try:
        result = helper.prepare(previous=initial)
        assert result['discovery_read_route']['recent'] == 'public'
        assert result['discovery_read_route']['discarded_private_attempt']['outcome'] == 'discarded'
        assert injected == [100]
        assert candidate.address in validated
        assert candidate.address not in {pool['address'] for pool in result['known_pools']}
        assert initial == before
    finally: helper.close()
    assert closed == ['private', 'public']
