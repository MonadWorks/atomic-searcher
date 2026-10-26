"""Independent provider-gate counterexamples; no network sockets or accounts."""
import copy
import threading
import time

import httpx
import pytest

from searcher.core import PublicProviderBudget, RPC, Reject, batch_request_digest
from searcher.statefeed import complete_batch_observation_start
from test_rpc_timing import Session, transport


def test_cancelled_200_body_cannot_complete_a_recovery_streak(transport):
    rpc, clock = transport
    budget = rpc.provider_budget = PublicProviderBudget()
    ticket = budget.acquire(1)
    budget.finish(ticket, 429)
    # Seven complete replies are not enough to promote the rate. A cancelled
    # eighth reply must break that streak even though its HTTP headers are 200.
    rpc.session = Session(clock, statuses=(200,) * 7)
    for _ in range(7):
        rpc.request('eth_chainId', [])
    assert budget.successes == 7 and budget.rate == 1
    stop = rpc.cancel_event = threading.Event()

    class CancelBody:
        def stream(self, *args, **kwargs):
            class Stream:
                async def __aenter__(self):
                    class Response(httpx.Response):
                        async def aread(self):
                            stop.set()
                            return self.content
                    return Response(200, json={'id': 1, 'result': '0x2105'},
                                    request=httpx.Request('POST', 'https://base.drpc.org'))
                async def __aexit__(self, *args): pass
            return Stream()
        async def aclose(self): pass

    # Avoid a real Event.wait combined with the deterministic fake clock.
    clock.now = max(clock.now, budget.next_at)
    rpc.session = CancelBody()
    with pytest.raises(Reject, match='cancelled'):
        rpc.request('eth_chainId', [])
    assert budget.successes == 0 and budget.rate == 1 and budget.throttles == 1
    assert budget.active is None and not budget.queue


def test_cancelled_middle_waiter_does_not_steal_fifo_or_leak_ownership():
    budget = PublicProviderBudget(rate=10_000)
    original = budget.acquire(1)
    original_owned = True
    stop = [threading.Event() for _ in range(3)]
    entered = [threading.Event() for _ in range(3)]
    release = [threading.Event() for _ in range(3)]
    order, errors, threads = [], [], []

    def waiter(index):
        ticket = None
        try:
            ticket = budget.acquire(1, stop[index])
            order.append(index)
            entered[index].set()
            assert release[index].wait(2)
        except Exception as exc:
            errors.append((index, exc))
        finally:
            if ticket is not None:
                budget.finish(ticket, 200)

    try:
        for index in range(3):
            thread = threading.Thread(target=waiter, args=(index,))
            threads.append(thread)
            thread.start()
            until = time.monotonic() + 1
            while time.monotonic() < until:
                with budget.lock:
                    if len(budget.queue) == index + 1:
                        break
                time.sleep(.001)
            else:
                pytest.fail('Waiter did not enter the real shared queue')
        stop[1].set()
        threads[1].join(1)
        assert not threads[1].is_alive()
        assert [(index, type(exc)) for index, exc in errors] == [(1, Reject)]
        assert len(budget.queue) == 2
        budget.finish(original, 200)
        original_owned = False
        assert entered[0].wait(1)
        assert not entered[2].is_set(), 'A second request entered while the first owned the gate'
        release[0].set()
        assert entered[2].wait(1)
        release[2].set()
        for thread in threads:
            thread.join(1)
        assert order == [0, 2]
        assert all(not thread.is_alive() for thread in threads)
        assert budget.active is None and not budget.queue
    finally:
        for event in stop + release:
            event.set()
        if original_owned:
            budget.finish(original)
        for thread in threads:
            thread.join(2)


def test_split_batch_age_uses_first_fragment_and_binds_the_whole_request(transport):
    rpc, clock = transport
    rpc.provider_budget = PublicProviderBudget()
    rpc.session = Session(clock, statuses=(200,) * 3)
    calls = [('eth_call', [index]) for index in range(7)]
    before = round(clock.now * 1e9)
    assert rpc.batch(calls) == list(range(7))
    after = round(clock.now * 1e9)
    measured = rpc.measurements[-1]
    starts = [attempt['network_started_ns'] for attempt in measured['attempt_timings']]
    assert len(starts) == 3 and starts[-1] - starts[0] >= 3_000_000_000
    assert measured['network_started_ns'] == starts[0]
    assert measured['batch_request_digest'] == batch_request_digest(calls)
    assert complete_batch_observation_start(measured, calls, before, after) == starts[0]
    assert complete_batch_observation_start(measured, calls[:-1], before, after) is None


def test_preparation_occurs_after_shared_cooldown_without_recharging_or_rebinding(transport):
    rpc, clock = transport
    budget = rpc.provider_budget = PublicProviderBudget()
    ticket = budget.acquire(1)
    budget.finish(ticket, 429, '9')
    deadline = budget.cooldown_until
    prepared_at = []
    original = [('eth_call', ['old']), ('eth_getBlockByNumber', ['latest']),
                ('eth_getBlockByNumber', ['pending'])]
    actual = [('eth_call', ['current']), *original[1:]]

    def prepare():
        prepared_at.append(clock.now)
        return actual

    values, bound = rpc.batch_prepared(original, prepare)
    measurement = rpc.measurements[-1]
    assert values == ['current', 'latest', 'pending'] and bound == actual
    assert len(prepared_at) == 1 and prepared_at[0] >= deadline
    assert len(rpc.session.payloads) == 1
    assert budget.next_at == pytest.approx(prepared_at[0] + 3)
    assert measurement['batch_request_digest'] == batch_request_digest(actual)
    assert measurement['network_started_ns'] == round(prepared_at[0] * 1e9)
    # The gate uses a 1 ms minimum sleep to avoid a floating-point busy loop.
    assert 9000 <= measurement['public_limiter_wait_ms'] <= 9001.001
    assert original[0][1] == ['old']


def test_actual_pipeline_discovery_activity_and_v4_discovery_share_one_gate(tmp_path, monkeypatch):
    """Exercise actual worker constructors and RPC, with in-memory replies.

    Pool discovery work and market state are stubbed; this proves connection
    wiring/lifecycle, not live provider throughput, pricing, or profitability.
    """
    import searcher.activity as activity
    import searcher.core as core
    import searcher.engine as engine
    import searcher.pipeline as pipeline
    from searcher.statefeed import StateFrame
    from test_fastlane import config
    from test_pipeline import H0, H1, header, saved

    for variable in ('BASE_DISCOVERY_RPC_URL', 'BASE_ACTIVITY_RPC_URL'):
        monkeypatch.delenv(variable, raising=False)
    budget = PublicProviderBudget(rate=1000)
    monkeypatch.setattr(core, '_PUBLIC_DRPC_BUDGET', budget)
    required = {'pool-discovery', 'pool-activity', 'v4-canonical-discovery'}
    connected, clients, errors = set(), [], []
    lock, ready, stop = threading.Lock(), threading.Event(), threading.Event()

    class LocalSession:
        def stream(self, method, url, json):
            rows = json if isinstance(json, list) else [json]
            results = []
            for row in rows:
                if row['method'] == 'eth_chainId': value = '0x2105'
                elif row['method'] == 'eth_getLogs': value = []
                elif row['method'] == 'eth_getBlockByNumber': value = header(2, H1, H0)
                else: raise AssertionError('Unexpected test RPC method: ' + row['method'])
                results.append({'id': row['id'], 'result': value})
            class Stream:
                async def __aenter__(self):
                    return httpx.Response(200, json=results if isinstance(json, list) else results[0],
                                          request=httpx.Request('POST', url))
                async def __aexit__(self, *args): pass
            return Stream()
        async def aclose(self): pass

    class TrackingRPC(RPC):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.loop.run_until_complete(self.session.aclose())
            self.session = LocalSession()
            self.created_on = threading.current_thread().name
            self.closed = False
            clients.append(self)
        def request(self, *args):
            value = super().request(*args)
            if self.created_on in required:
                with lock:
                    connected.add(self.created_on)
                    if connected == required: ready.set()
            return value
        def close(self):
            super().close()
            self.closed = True

    class Feed:
        pages = None
        def __init__(self, *args): pass
        def poll(self, *args, **kwargs):
            if not ready.wait(4): errors.append('The three real worker connections did not send')
            stop.set()
            now = time.monotonic_ns()
            return StateFrame(header(2, H1, H0), header(2, H1, H0), saved()['pools'],
                              now, now, time.time(), 3, 106)

    class Strategy:
        def __init__(self, *args): self.execution_feedback = object()
        def on_frame(self, frame): return {'block': 2, 'decision_age_ms': 0, 'candidates': []}

    def discover(rpc, cfg, *args, **kwargs):
        rpc.request('eth_chainId', [])
        return dict(copy.deepcopy(saved()), discovery_cursor=2)

    monkeypatch.setattr(pipeline, 'RPC', TrackingRPC)
    monkeypatch.setattr(pipeline, 'StateFeed', Feed)
    monkeypatch.setattr(pipeline, 'PendingStrategy', Strategy)
    monkeypatch.setattr(engine, 'prepare', discover)
    monkeypatch.setattr(activity, 'websocket_url', lambda: None)
    cfg = config()
    cfg.pop('_diagnostic_executor', None)
    cfg['_v4_inventory_path'] = str(tmp_path / 'inventory.sqlite')
    cfg['policy'].update(hot_lane_enabled=False, universe_refresh_seconds=.01,
                         discovery_rpc_url='https://base.drpc.org',
                         activity_rpc_url='https://base.drpc.org')
    primary = TrackingRPC('https://state.example.invalid')
    try:
        result = pipeline.run_pending(primary, cfg, saved(), 5, tmp_path / 'run', stop_event=stop)
        assert not errors and connected == required
        workers = [client for client in clients if client.created_on in required]
        assert len(workers) == 3
        assert all(client.provider_budget is budget and client.closed for client in workers)
        assert all(client.provider_budget is None for client in clients if client not in workers)
        assert result['reader_stopped'] and result['v4_stopped']
        assert not budget.queue and budget.active is None
    finally:
        stop.set()
        primary.close()
