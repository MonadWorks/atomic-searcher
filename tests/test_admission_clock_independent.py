"""Independent after-admission clock counterexamples; no socket or signer."""
import copy
import json

import pytest

from searcher.context import SharedClockHints, simulation_context
from searcher.core import Reject, batch_request_digest
from searcher.statefeed import StateFeed, complete_batch_observation_start, observation_start_ns
from test_pipeline import H0, H1, H2, ZERO, clock_config, header, saved
from test_rpc_timing import transport
from test_snapshot_freshness import SnapshotSession


H3, H4 = ['0x'+format(n, '064x') for n in (123, 124)]


def publish(rpc, clock, number, block_hash):
    anchor = header(number, block_hash, H1)
    pending = header(number+1, ZERO, block_hash)
    now = round(clock.now*1e9)
    assert rpc.clock_hints.publish(rpc.url, rpc.chain_id, anchor, pending,
                                   requested_ns=now-1, received_ns=now)
    return anchor, pending


class FixedHeads(SnapshotSession):
    def __init__(self, clock, hashes):
        super().__init__(clock, changed=True)
        self.hashes = hashes

    def stream(self, *args, **kwargs):
        inner = super().stream(*args, **kwargs)
        session = self
        class Stream:
            async def __aenter__(self):
                response = await inner.__aenter__()
                assert session.atomic_reads <= len(session.hashes)
                block_hash = session.hashes[session.atomic_reads-1]
                values = response.json()
                for row in values:
                    if row['id'] == 1:
                        row['result'] = header(3, block_hash, H1)
                    elif row['id'] == 2:
                        row['result'] = header(4, ZERO, block_hash)
                response._content = json.dumps(values).encode()
                return response
            async def __aexit__(self, *args):
                return await inner.__aexit__(*args)
        return Stream()


def test_clock_preparation_is_after_both_waits_and_binds_the_sent_request(transport, monkeypatch):
    rpc, clock = transport
    rpc.clock_hints = SharedClockHints(clock=lambda: round(clock.now*1e9))
    publish(rpc, clock, 2, H1)
    order = []
    rpc.minimum_request_interval, rpc.last_post_at = .2, clock.now
    def sleep(seconds):
        order.append('minimum_wait')
        clock.advance(seconds)
        publish(rpc, clock, 3, H2)
    monkeypatch.setattr('searcher.core.time.sleep', sleep)
    class Limiter:
        def acquire(self, count, cancelled=None, submission=False, critical=False):
            assert order == ['minimum_wait'] and count == 3 and not submission
            order.append('public_wait')
            clock.advance(.3)
            publish(rpc, clock, 3, H3)  # Same-height replacement during the second wait.
    rpc.public_rate_limiter = Limiter()
    rpc.session = FixedHeads(clock, [H3])
    original = rpc.batch_prepared
    actual = []
    def prepared(calls, callback):
        def record():
            assert order == ['minimum_wait', 'public_wait']
            order.append('prepare')
            return callback()
        values, used = original(calls, record)
        actual.extend(used)
        return values, used
    monkeypatch.setattr(rpc, 'batch_prepared', prepared)
    frame = StateFeed(rpc, clock_config(), saved()).poll()
    assert order == ['minimum_wait', 'public_wait', 'prepare']
    assert rpc.session.atomic_reads == 1 and frame.header['hash'] == H3
    expected = simulation_context(header(3, H3, H1), header(4, ZERO, H3))
    assert actual[0][1][3] == expected == rpc.session.payloads[0][0]['params'][3]
    measurement = rpc.measurements[-1]
    assert measurement['batch_request_digest'] == batch_request_digest(actual)
    assert frame.received_ns-frame.requested_ns == 850_000_000
    assert frame.received_ns-observation_start_ns(frame) == 350_000_000
    assert measurement['minimum_interval_wait_ms'] == pytest.approx(200)
    assert measurement['public_limiter_wait_ms'] == pytest.approx(300)
    assert measurement['network_started_ns'] == frame.observation_started_ns


def test_actual_call_provenance_mismatch_keeps_conservative_poll_start(transport, monkeypatch):
    rpc, clock = transport
    rpc.clock_hints = SharedClockHints(clock=lambda: round(clock.now*1e9))
    publish(rpc, clock, 2, H1)
    class Limiter:
        def acquire(self, *args, **kwargs):
            clock.advance(.5)
            publish(rpc, clock, 3, H2)
    rpc.public_rate_limiter = Limiter()
    rpc.session = FixedHeads(clock, [H2])
    original = rpc.batch_prepared
    def incorrect_provenance(calls, callback):
        stale = copy.deepcopy(calls)
        values, _ = original(calls, callback)
        return values, stale
    monkeypatch.setattr(rpc, 'batch_prepared', incorrect_provenance)
    frame = StateFeed(rpc, clock_config(), saved()).poll()
    assert frame.header['hash'] == H2 and frame.context_block == 4
    assert frame.transport['admission_clock_updated'] is True
    assert frame.observation_started_ns is None
    assert observation_start_ns(frame) == frame.requested_ns
    assert frame.received_ns-observation_start_ns(frame) == 850_000_000


def test_two_same_height_reorgs_reject_after_exactly_two_complete_reads(transport):
    rpc, clock = transport
    rpc.clock_hints = SharedClockHints(clock=lambda: round(clock.now*1e9))
    publish(rpc, clock, 3, H2)
    rpc.session = FixedHeads(clock, [H3, H4])
    feed = StateFeed(rpc, clock_config(), saved())
    with pytest.raises(Reject, match='snapshot anchor changed during read'):
        feed.poll()
    assert rpc.session.atomic_reads == 2
    assert [row['role'] for row in feed.last_transport['rpc_batches']] == ['atomic_snapshot', 'atomic_snapshot']
    first_context, second_context = [payload[0]['params'][3] for payload in rpc.session.payloads]
    assert first_context['blockHash']['3'] == H2
    assert second_context['blockHash']['3'] == H3
    assert all(row['method'] != 'eth_sendRawTransaction' for payload in rpc.session.payloads for row in payload)


def test_prepare_exception_after_admission_has_no_network_or_fresh_observation(transport):
    rpc, clock = transport
    charges = []
    class Limiter:
        def acquire(self, count, *args, **kwargs):
            charges.append(count)
            clock.advance(.5)
    rpc.public_rate_limiter = Limiter()
    calls = [('eth_call', []), ('eth_getBlockByNumber', ['latest', False])]
    def fail():
        assert charges == [2]
        raise Reject('synthetic invalid clock after admission')
    started = round(clock.now*1e9)
    with pytest.raises(Reject, match='invalid clock after admission'):
        rpc.batch_prepared(calls, fail)
    measurement = rpc.measurements[-1]
    assert not rpc.session.payloads
    assert measurement['outcome'] == 'error'
    assert measurement['network_started_ns'] is None
    assert measurement['network_total_ms'] == 0
    assert measurement['public_limiter_wait_ms'] == pytest.approx(500)
    assert complete_batch_observation_start(measurement, calls, started, round(clock.now*1e9)) is None


def test_cancel_during_minimum_interval_never_reaches_admission_or_prepare(transport):
    rpc, clock = transport
    class Stop:
        stopped = False
        def is_set(self): return self.stopped
        def wait(self, delay):
            clock.advance(.1)
            self.stopped = True
    class Limiter:
        def acquire(self, *args, **kwargs):
            raise AssertionError('cancelled request reached public admission')
    def prepare():
        raise AssertionError('cancelled request reached payload preparation')
    rpc.cancel_event = Stop()
    rpc.public_rate_limiter = Limiter()
    rpc.minimum_request_interval, rpc.last_post_at = .5, clock.now
    with pytest.raises(Reject, match='cancelled'):
        rpc.batch_prepared([('eth_call', [])], prepare)
    assert not rpc.session.payloads
    assert rpc.measurements[-1]['network_started_ns'] is None


@pytest.mark.parametrize('change', ['extra_read', 'write', 'reorder'])
def test_preparation_cannot_spend_admitted_count_on_a_different_method_sequence(transport, change):
    rpc, _ = transport
    charges = []
    class Limiter:
        def acquire(self, count, *args, **kwargs): charges.append(count)
    rpc.public_rate_limiter = Limiter()
    original = [('eth_call', []), ('eth_getBlockByNumber', ['latest', False])]
    prepared = copy.deepcopy(original)
    if change == 'extra_read': prepared.append(('eth_call', []))
    elif change == 'write': prepared[0] = ('eth_sendRawTransaction', ['synthetic-not-signed'])
    else: prepared.reverse()
    with pytest.raises(Reject, match='prepared RPC batch'):
        rpc.batch_prepared(original, lambda: prepared)
    assert charges == [2] and not rpc.session.payloads
