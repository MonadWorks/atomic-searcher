from collections import deque

import pytest
from searcher.core import Reject
from searcher.statefeed import StateFeed
from test_pipeline import ClockRPC, clock_config, saved, header, H0, H1, H2, TX


class TimedClockRPC(ClockRPC):
    def __init__(self, heads):
        super().__init__(heads)
        self.measurements = deque(maxlen=2)

    def batch(self, calls):
        value = super().batch(calls)
        self.measurements.append(dict(elapsed_ms=12, minimum_interval_wait_ms=1,
            public_limiter_wait_ms=2, network_headers_ms=3, network_body_ms=4,
            network_total_ms=7, retry_wait_ms=0, timing_complete=True))
        return value


def test_feed_accounts_bootstrap_and_discarded_clock_attempt_not_only_last_batch():
    rpc = TimedClockRPC([header(3, H2, H1), header(3, H2, H1)])
    frame = StateFeed(rpc, clock_config(), saved()).poll()
    timing = frame.transport
    assert timing['clock_attempts'] == 2 and timing['clock_bootstraps'] == 1
    assert [x['role'] for x in timing['rpc_batches']] == ['clock_bootstrap', 'atomic_snapshot', 'atomic_snapshot']
    assert timing['poll_public_limiter_wait_ms'] == 6
    assert timing['poll_network_total_ms'] == 21
    assert timing['poll_rpc_elapsed_ms'] == sum(x['batch_wall_ms'] for x in timing['rpc_batches'])
    assert frame.header['hash'] == H2


def test_missing_rpc_instrumentation_is_unknown_and_next_poll_does_not_include_previous():
    rpc = ClockRPC([header(2, H1, H0), header(2, H1, H0)])
    feed = StateFeed(rpc, clock_config(), saved())
    first, second = feed.poll(), feed.poll()
    assert first.transport['clock_bootstraps'] == 1 and second.transport['clock_bootstraps'] == 0
    assert len(second.transport['rpc_batches']) == 1
    assert second.transport['poll_public_limiter_wait_ms'] is None


def test_rejected_clock_read_keeps_timings_without_publishing_mixed_state():
    feed = StateFeed(TimedClockRPC([header(3, H2, H1), header(4, TX, H2)]), clock_config(), saved())
    with pytest.raises(Reject, match='anchor changed'):
        feed.poll()
    assert len(feed.last_transport['rpc_batches']) == 3
