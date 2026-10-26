"""Bounded read retry counterexamples; in-memory transport, no node access."""
import asyncio
import threading
import time

import httpx
import pytest

from searcher.core import PublicProviderBudget, RPC, RPCError, Reject, batch_request_digest
from searcher.statefeed import complete_batch_observation_start
from test_rpc_timing import Session, transport


def provider(transport, statuses):
    rpc, clock = transport
    rpc.provider_budget = PublicProviderBudget()
    rpc.session = Session(clock, statuses=statuses)
    return rpc, clock


def test_late_throttled_split_resumes_without_replaying_completed_work(transport):
    rpc, clock = provider(transport, (200, 429, 200, 200))
    calls = [('eth_call', [index]) for index in range(7)]
    before = round(clock.now * 1e9)
    assert rpc.batch(calls) == list(range(7))
    payloads = [[row['params'][0] for row in payload] for payload in rpc.session.payloads]
    assert payloads == [[0, 1, 2], [3, 4, 5], [3, 4, 5], [6]]
    measured = rpc.measurements[-1]
    assert measured['attempts'] == 4 and measured['outcome'] == 'ok'
    attempts = measured['attempt_timings']
    assert attempts[2]['network_started_ns'] - attempts[1]['network_finished_ns'] >= 2_000_000_000
    assert [a['batch_request_digest'] for a in attempts] == [
        batch_request_digest(group) for group in (calls[:3], calls[3:6], calls[3:6], calls[6:])]
    assert complete_batch_observation_start(measured, calls, before, round(clock.now * 1e9)) == before
    assert rpc.provider_budget.active is None and not rpc.provider_budget.queue


def test_four_failed_attempts_stop_at_that_split_and_leave_tail_unread(transport):
    rpc, clock = provider(transport, (200, 429, 429, 429, 429))
    with pytest.raises(httpx.HTTPStatusError):
        rpc.batch([('eth_call', [index]) for index in range(7)])
    payloads = [[row['params'][0] for row in payload] for payload in rpc.session.payloads]
    assert payloads == [[0, 1, 2]] + [[3, 4, 5]] * 4
    measured = rpc.measurements[-1]
    assert measured['attempts'] == 5 and measured['outcome'] == 'error'
    assert rpc.provider_budget.throttles == 4
    assert rpc.provider_budget.active is None and not rpc.provider_budget.queue
    assert rpc.provider_budget.cooldown_until > clock.now


def test_prepared_retry_rebinds_after_readmission_but_keeps_earliest_age(transport):
    rpc, clock = provider(transport, (429, 200))
    before = round(clock.now * 1e9)
    prepared = []
    original = [('eth_call', ['template']), ('eth_getBlockByNumber', ['latest'])]

    def prepare():
        rows = [('eth_call', ['first' if not prepared else 'second']), original[1]]
        prepared.append((clock.now, rows))
        return rows

    values, actual = rpc.batch_prepared(original, prepare)
    assert values == ['second', 'latest'] and actual == prepared[-1][1]
    assert len(prepared) == len(rpc.session.payloads) == 2
    assert prepared[1][0] - prepared[0][0] >= 2
    measured = rpc.measurements[-1]
    attempts = measured['attempt_timings']
    assert [a['batch_request_digest'] for a in attempts] == [batch_request_digest(rows) for _, rows in prepared]
    assert measured['batch_request_digest'] == batch_request_digest(actual)
    observed = complete_batch_observation_start(measured, actual, before, round(clock.now * 1e9))
    assert observed == attempts[0]['network_started_ns'] == before
    assert observed < attempts[-1]['network_started_ns']
    assert original[0][1] == ['template']
    # The retry pays for the same two methods again, at the slowed rate1/s.
    assert rpc.provider_budget.next_at == pytest.approx(prepared[1][0] + 2)


@pytest.mark.parametrize('mutation', ['method', 'count'])
def test_second_preparation_cannot_change_admitted_read_permissions(transport, mutation):
    rpc, _ = provider(transport, (429,))
    original = [('eth_call', [])]
    count = 0
    def prepare():
        nonlocal count
        count += 1
        if count == 1: return original
        return [('eth_sendRawTransaction', ['synthetic-not-a-transaction'])] if mutation == 'method' else original * 2
    with pytest.raises(Reject, match='prepared RPC batch'):
        rpc.batch_prepared(original, prepare)
    assert count == 2 and len(rpc.session.payloads) == 1
    assert rpc.provider_budget.active is None and not rpc.provider_budget.queue


@pytest.mark.parametrize('batched', [False, True])
@pytest.mark.parametrize('status', [429, 502, 503, 504])
def test_write_capable_subclass_does_not_inherit_provider_write_retries(transport, batched, status):
    rpc, _ = provider(transport, (status, 200))
    # Same extension boundary used by BroadcastRPC. Payload is an inert marker
    # and transport is in memory; no signing, wallet data, or node is involved.
    rpc.METHODS = RPC.METHODS | {'eth_sendRawTransaction'}
    with pytest.raises(httpx.HTTPStatusError):
        if batched: rpc.batch([('eth_sendRawTransaction', ['inert'])])
        else: rpc.request('eth_sendRawTransaction', ['inert'])
    assert len(rpc.session.payloads) == 1
    assert rpc.provider_budget.active is None


def test_a_write_in_a_completed_split_disables_retry_for_the_entire_batch(transport):
    rpc, _ = provider(transport, (200, 429, 200))
    rpc.METHODS = RPC.METHODS | {'eth_sendRawTransaction'}
    calls = [('eth_sendRawTransaction', ['inert']), ('eth_call', ['a']),
             ('eth_call', ['b']), ('eth_call', ['c'])]
    with pytest.raises(httpx.HTTPStatusError):
        rpc.batch(calls)
    assert len(rpc.session.payloads) == 2
    assert [[row['method'] for row in rows] for rows in rpc.session.payloads] == [
        ['eth_sendRawTransaction', 'eth_call', 'eth_call'], ['eth_call']]
    assert rpc.provider_budget.active is None and not rpc.provider_budget.queue


@pytest.mark.parametrize('batched', [False, True])
def test_semantic_rpc_revert_does_not_become_a_transport_retry(transport, batched):
    rpc, clock = provider(transport, (200,))
    base = rpc.session
    class SemanticSession:
        def stream(self, *args, **kwargs):
            original = base.stream(*args, **kwargs)
            class Stream:
                async def __aenter__(self):
                    response = await original.__aenter__()
                    payload = kwargs['json']
                    rows = payload if isinstance(payload, list) else [payload]
                    replies = [{'id': row['id'], 'error': {'code': -32000, 'message': 'execution reverted',
                                                         'data': '0xdeadbeef'}} for row in rows]
                    return httpx.Response(200, json=replies if isinstance(payload, list) else replies[0],
                                          request=response.request)
                async def __aexit__(self, *args): return await original.__aexit__(*args)
            return Stream()
        async def aclose(self): pass
    rpc.session = SemanticSession()
    with pytest.raises(RPCError) as failure:
        if batched: rpc.batch([('eth_call', [])])
        else: rpc.request('eth_call', [])
    assert failure.value.revert_data == '0xdeadbeef'
    assert len(base.payloads) == 1 and rpc.provider_budget.active is None


def test_read_retry_rejoins_behind_waiting_other_worker(monkeypatch):
    budget = PublicProviderBudget(rate=1000)
    monkeypatch.setattr('searcher.core._PUBLIC_DRPC_BUDGET', budget)
    first_entered, release_first = threading.Event(), threading.Event()
    order, errors, replies = [], [], {}
    stop = threading.Event()
    class LocalSession:
        def __init__(self, label): self.label = label; self.calls = 0
        def stream(self, method, url, json):
            session = self
            class Stream:
                async def __aenter__(self):
                    session.calls += 1
                    first = session.label == 'A' and session.calls == 1
                    order.append((session.label, 429 if first else 200))
                    if first:
                        first_entered.set()
                        while not release_first.is_set():
                            await asyncio.sleep(.001)
                    return httpx.Response(429 if first else 200, json={'id': 1, 'result': '0x2105'},
                                          request=httpx.Request('POST', url))
                async def __aexit__(self, *args): pass
            return Stream()
        async def aclose(self): pass
    clients = []
    for label in ('A', 'B'):
        rpc = RPC('https://base.drpc.org', timeout=3, cancel_event=stop)
        rpc.loop.run_until_complete(rpc.session.aclose())
        rpc.session = LocalSession(label)
        clients.append(rpc)
    def worker(index):
        try: replies[index] = clients[index].request('eth_chainId', [])
        except Exception as exc: errors.append(exc)
        finally: clients[index].close()
    threads = [threading.Thread(target=worker, args=(index,)) for index in range(2)]
    try:
        threads[0].start()
        assert first_entered.wait(1)
        threads[1].start()
        until = time.monotonic() + 1
        while time.monotonic() < until:
            with budget.lock:
                if len(budget.queue) == 1: break
            time.sleep(.001)
        else: pytest.fail('Second worker did not join the admission queue')
        release_first.set()
        for thread in threads: thread.join(4)
        assert not errors and replies == {0: '0x2105', 1: '0x2105'}
        assert order == [('A', 429), ('B', 200), ('A', 200)]
        assert all(not thread.is_alive() for thread in threads)
        assert budget.active is None and not budget.queue
    finally:
        stop.set(); release_first.set()
        for thread in threads:
            if thread.ident is not None: thread.join(4)
        for rpc in clients:
            if not rpc.loop.is_closed(): rpc.close()
