"""Independent acceptance-boundary checks for shared header hints."""
import time

import pytest
from eth_abi import decode, encode

from searcher.context import SharedClockHints
from searcher.core import Reject
from searcher.statefeed import STATE, StateFeed
from test_pipeline import A, B, H0, H1, H2, TX, ZERO, ClockRPC, clock_config, header, saved

URL = 'https://example.invalid/independent-reader'


def put(cache, n=2, h=H1, parent=H0):
    now = time.monotonic_ns()
    assert cache.publish(URL, 8453, header(n, h, parent), header(n+1, ZERO, h),
                         requested_ns=now-1, received_ns=now)


class Reader(ClockRPC):
    def __init__(self, heads, cache):
        super().__init__(heads)
        self.url, self.chain_id, self.clock_hints = URL, 8453, cache


def test_hint_never_replaces_a_new_atomic_pool_read_or_retains_pool_fields():
    cache = SharedClockHints()
    anchor = dict(header(2, H1, H0), pools=saved()['pools'], transactions=[TX])
    now = time.monotonic_ns()
    assert cache.publish(URL, 8453, anchor, header(3, ZERO, H1),
                         requested_ns=now-1, received_ns=now)
    hint = cache.get(URL, 8453)
    assert set(hint['anchor']) == {'number', 'timestamp', 'hash', 'parentHash'}
    anchor['hash'] = TX
    assert cache.get(URL, 8453)['anchor']['hash'] == H1

    class ChangingReader(Reader):
        def batch(self, calls):
            out = super().batch(calls)
            if calls[0][0] == 'eth_call':
                rows, block, stamp = decode([STATE+'[]', 'uint256', 'uint256'], bytes.fromhex(out[0][2:]))
                rows = [list(row) for row in rows]
                for row in rows: row[4] = 12345+len(self.calls)
                out = list(out)
                out[0] = '0x'+encode([STATE+'[]', 'uint256', 'uint256'], [rows, block, stamp]).hex()
            return out

    rpc = ChangingReader([header(2, H1, H0)]*2, cache)
    feed = StateFeed(rpc, clock_config(), saved())
    first, second = feed.poll(), feed.poll()
    assert len(rpc.calls) == 2
    assert first.snapshot_pools[0]['liquidity'] == 12346
    assert second.snapshot_pools[0]['liquidity'] == 12347
    assert first.transport['shared_clock_used']


@pytest.mark.parametrize('bad_layout', ['missing_pool', 'wrong_order'])
def test_borrowed_clock_does_not_waive_complete_atomic_layout(bad_layout):
    cache = SharedClockHints(); put(cache)

    class BadLayout(Reader):
        def batch(self, calls):
            out = super().batch(calls)
            if calls[0][0] == 'eth_call':
                rows, block, stamp = decode([STATE+'[]', 'uint256', 'uint256'], bytes.fromhex(out[0][2:]))
                rows = rows[:1] if bad_layout == 'missing_pool' else tuple(reversed(rows))
                out = list(out)
                out[0] = '0x'+encode([STATE+'[]', 'uint256', 'uint256'], [rows, block, stamp]).hex()
            return out

    error = 'incomplete atomic snapshot' if bad_layout == 'missing_pool' else 'snapshot pool order mismatch'
    with pytest.raises(Reject, match=error):
        StateFeed(BadLayout([header(2, H1, H0)], cache), clock_config(), saved()).poll()


def test_first_borrowed_atomic_transport_failure_recovers_next_poll():
    cache = SharedClockHints(); put(cache)

    class FailsOnce(Reader):
        fail = True
        def batch(self, calls):
            if self.fail and calls[0][0] == 'eth_call':
                self.fail = False
                raise OSError('controlled transport failure')
            return super().batch(calls)

    rpc = FailsOnce([header(2, H1, H0)], cache)
    feed = StateFeed(rpc, clock_config(), saved())
    with pytest.raises(OSError, match='controlled'): feed.poll()
    assert feed.anchor is None and feed.pending is None
    frame = feed.poll()
    assert frame.context_block == 3 and frame.header['hash'] == H1
    assert len(rpc.calls) == 1


def test_invalid_borrower_cannot_discard_concurrent_peer_update():
    cache = SharedClockHints(); put(cache)

    class BadThenRecovers(Reader):
        broken = True
        def batch(self, calls):
            out = super().batch(calls)
            if self.broken and calls[0][0] == 'eth_call':
                put(cache, 3, H2, H1)
                out = list(out)
                out[2] = dict(out[2], parentHash=TX)
            return out

    rpc = BadThenRecovers([header(2, H1, H0)]*2+[header(3, H2, H1)], cache)
    feed = StateFeed(rpc, clock_config(), saved())
    with pytest.raises(Reject, match='pending parent differs'): feed.poll()
    assert cache.get(URL, 8453)['anchor']['hash'] == H2
    rpc.broken = False
    frame = feed.poll()
    assert frame.header['hash'] == H2 and frame.context_block == 4
    assert frame.transport['shared_clock_used']
    assert frame.transport['clock_attempts'] == 1


def test_historical_read_neither_borrows_nor_publishes_shared_pending_hint():
    cache = SharedClockHints(); put(cache, 3, H2, H1)
    original = cache.get(URL, 8453)
    rpc = Reader([header(2, H1, H0)], cache)
    frame = StateFeed(rpc, clock_config(), saved()).poll(block=2)
    assert frame.context_block == 2
    assert not frame.transport['shared_clock_used']
    assert cache.get(URL, 8453) == original
    assert len(rpc.calls[0][0][1]) == 3
