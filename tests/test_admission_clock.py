"""Admission-time clock refresh uses real RPC accounting and in-memory HTTP."""
import copy
import json
import threading

import pytest

from searcher.context import SharedClockHints, simulation_context
from searcher.core import PublicRateLimiter, RPC, Reject, batch_request_digest
from searcher.statefeed import StateFeed, complete_batch_observation_start, observation_start_ns
from test_pipeline import clock_config, saved, header, H0, H1, H2, ZERO
from test_rpc_timing import transport, Session
from test_snapshot_freshness import SnapshotSession


def publish(cache, rpc, clock, height=2, *, url=None, chain=None):
    head = header(height, H1 if height == 2 else H2, H0 if height == 2 else H1)
    pending = header(height+1, ZERO, head['hash'])
    now = round(clock.now*1e9)
    assert cache.publish(url or rpc.url, rpc.chain_id if chain is None else chain,
                         head, pending, requested_ns=now-1, received_ns=now)
    return head, pending


def queued_feed(transport, monkeypatch, *, corrupt=None, update=True, expired=False,
                other_url=None, other_chain=None):
    rpc, clock = transport
    rpc.clock_hints = cache = SharedClockHints(clock=lambda:round(clock.now*1e9))
    publish(cache, rpc, clock)
    rpc.public_rate_limiter = limiter = PublicRateLimiter()
    limiter.acquire(18)
    rpc.session = SnapshotSession(clock, changed=True, corrupt=corrupt)
    waits = []
    def sleep(seconds):
        waits.append(seconds)
        clock.advance(seconds)
        if len(waits) == 1 and update:
            publish(cache, rpc, clock, 3, url=other_url, chain=other_chain)
            if expired: clock.advance(2)
    monkeypatch.setattr('searcher.core.time.sleep', sleep)
    return StateFeed(rpc, clock_config(), saved()), waits


def test_waiting_real_rpc_uses_new_peer_clock_once_and_binds_actual_request(transport, monkeypatch):
    rpc, clock = transport
    feed, waits = queued_feed(transport, monkeypatch)
    frame = feed.poll()
    assert waits and rpc.session.atomic_reads == 1
    assert frame.transport['admission_clock_updated'] is True
    assert frame.transport['clock_attempts'] == 1
    assert frame.transport['clock_bootstraps'] == 0
    assert frame.header['hash'] == H2 and frame.context_block == 4
    assert frame.transport['execution_context'] == simulation_context(header(3,H2,H1), header(4,ZERO,H2))
    assert frame.transport['poll_public_limiter_wait_ms'] == pytest.approx(1050)
    assert frame.received_ns-observation_start_ns(frame) == 350_000_000
    assert frame.received_ns-frame.requested_ns == 1_400_000_000
    calls = [(r['method'], r['params']) for r in rpc.session.payloads[0]]
    measurement = rpc.measurements[-1]
    assert measurement['batch_request_digest'] == batch_request_digest(calls)
    assert complete_batch_observation_start(measurement, calls, frame.requested_ns, frame.received_ns) == observation_start_ns(frame)
    old_calls = copy.deepcopy(calls)
    old_calls[0][1][3] = simulation_context(header(2,H1,H0), header(3,ZERO,H1))
    assert complete_batch_observation_start(measurement, old_calls, frame.requested_ns, frame.received_ns) is None
    forged = dict(measurement, batch_request_digest=batch_request_digest(old_calls))
    assert complete_batch_observation_start(forged, calls, frame.requested_ns, frame.received_ns) is None
    assert len(rpc.public_rate_limiter.sent) == 3, 'Preparation charged the same request twice.'


@pytest.mark.parametrize('case', ['no_new_hint','expired','other_endpoint','other_chain'])
def test_unavailable_or_unrelated_hint_keeps_full_retry_and_validation(transport, monkeypatch, case):
    rpc, _ = transport
    feed, waits = queued_feed(transport, monkeypatch,
        update=case != 'no_new_hint', expired=case == 'expired',
        other_url='https://different.invalid' if case == 'other_endpoint' else None,
        other_chain=1 if case == 'other_chain' else None)
    frame = feed.poll()
    assert waits and frame.header['hash'] == H2
    assert not frame.transport['admission_clock_updated']
    assert rpc.session.atomic_reads == frame.transport['clock_attempts'] == 2


def test_admission_hint_does_not_relax_returned_lens_clock(transport, monkeypatch):
    feed, _ = queued_feed(transport, monkeypatch, corrupt='clock')
    with pytest.raises(Reject, match='clock mismatch'):
        feed.poll()


def test_admission_clock_keeps_two_read_limit_when_anchor_continues_changing(transport, monkeypatch):
    rpc, clock = transport
    feed, _ = queued_feed(transport, monkeypatch)
    class ChangingHeads(SnapshotSession):
        def stream(self, *args, **kwargs):
            inner = super().stream(*args, **kwargs)
            session = self
            class Stream:
                async def __aenter__(self):
                    response = await inner.__aenter__()
                    rows = response.json()
                    n = 3+session.atomic_reads
                    h = header(n, '0x'+f'{n:064x}', H2)
                    for row in rows:
                        if row['id'] == 1: row['result'] = h
                        elif row['id'] == 2: row['result'] = header(n+1,ZERO,h['hash'])
                    response._content = json.dumps(rows).encode()
                    return response
                async def __aexit__(self, *args): return await inner.__aexit__(*args)
            return Stream()
    rpc.session = ChangingHeads(clock)
    with pytest.raises(Reject, match='anchor changed'):
        feed.poll()
    assert rpc.session.atomic_reads == 2
    assert len(feed.last_transport['rpc_batches']) == 2


def test_historical_read_never_uses_admission_callback_or_shared_hint(transport, monkeypatch):
    rpc, clock = transport
    rpc.clock_hints = SharedClockHints(clock=lambda:round(clock.now*1e9))
    publish(rpc.clock_hints, rpc, clock, 3)
    rpc.session = SnapshotSession(clock, changed=False)
    def forbidden(*args): raise AssertionError('historical read borrowed an execution clock')
    monkeypatch.setattr(rpc, 'batch_prepared', forbidden)
    frame = StateFeed(rpc, clock_config(), saved()).poll(block=2)
    assert frame.context_block == 2 and frame.observation_started_ns is None
    assert not frame.transport['admission_clock_updated']


@pytest.mark.parametrize('mutation', ['add','remove','replace_method','reorder','write','in_place','shape'])
def test_prepared_batch_cannot_change_admitted_methods_or_count(transport, mutation):
    rpc, clock = transport
    calls = [('eth_call',['one']), ('eth_getBlockByNumber',['two'])]
    def prepare():
        if mutation == 'in_place':
            calls[0] = ('eth_sendRawTransaction',['secret'])
            return calls
        value = copy.deepcopy(calls)
        if mutation == 'add': value.append(('eth_call',[]))
        elif mutation == 'remove': value.pop()
        elif mutation == 'replace_method': value[0] = ('eth_getBalance',[])
        elif mutation == 'reorder': value.reverse()
        elif mutation == 'write': value[0] = ('eth_sendRawTransaction',[])
        elif mutation == 'shape': value[0] = ('eth_call', {'bad':'params'})
        return value
    with pytest.raises(Reject, match='prepared RPC batch'):
        rpc.batch_prepared(calls, prepare)
    assert not rpc.session.payloads
    assert rpc.measurements[-1]['outcome'] == 'error'


def test_prepared_read_api_rejects_write_even_on_extended_transport(transport):
    rpc, _ = transport
    rpc.METHODS = set(RPC.METHODS) | {'eth_sendRawTransaction'}
    with pytest.raises(Reject, match='write/unknown'):
        rpc.batch_prepared([('eth_sendRawTransaction',[])], lambda:[('eth_sendRawTransaction',[])])
    assert not rpc.session.payloads


def test_prepared_batch_copies_input_before_wait_and_callback_result(transport):
    rpc, clock = transport
    calls = [('eth_call',['before'])]
    prepared = [('eth_call',['after'])]
    class MutatingWait:
        def acquire(self, *args, **kwargs):
            calls[0][1][0] = 'changed while queued'
            clock.advance(.01)
    rpc.public_rate_limiter = MutatingWait()
    values, actual = rpc.batch_prepared(calls, lambda:prepared)
    prepared[0][1][0] = 'changed after return'
    assert values == ['after'] and actual == [('eth_call',['after'])]
    assert rpc.measurements[-1]['batch_request_digest'] == batch_request_digest(actual)


@pytest.mark.parametrize('phase', ['before','wait','prepare'])
def test_cancellation_never_sends_prepared_payload(transport, phase):
    rpc, clock = transport
    rpc.cancel_event = stop = threading.Event()
    prepared = []
    if phase == 'before': stop.set()
    class CancelWait:
        def acquire(self, *args, **kwargs):
            if phase == 'wait': stop.set()
    rpc.public_rate_limiter = CancelWait()
    def prepare():
        prepared.append(True)
        if phase == 'prepare': stop.set()
        return [('eth_call',[])]
    with pytest.raises(Reject, match='RPC cancelled'):
        rpc.batch_prepared([('eth_call',[])], prepare)
    assert not rpc.session.payloads
    assert bool(prepared) == (phase == 'prepare')


def test_prepared_batch_is_bounded_and_charged_at_current_priority(transport):
    rpc, clock = transport
    with pytest.raises(Reject, match='pacing capacity'):
        rpc.batch_prepared([('eth_call',[])]*19, lambda:[])
    rpc.public_rate_limiter = limiter = PublicRateLimiter()
    limiter.acquire(18)
    rpc.critical_reads = True
    def prepare():
        assert len(limiter.sent) == 21
        return [('eth_call',[n]) for n in range(3)]
    values, calls = rpc.batch_prepared([('eth_call',[])]*3, prepare)
    assert values == [0,1,2] and len(calls) == 3 and len(limiter.sent) == 21
