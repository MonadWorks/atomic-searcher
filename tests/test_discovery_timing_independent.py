"""Discovery diagnostics stay honest when the RPC measurement ring truncates."""
from collections import deque
import json

import pytest

from searcher import engine
from test_discovery_history import harness


FIELDS = ('elapsed_ms', 'public_limiter_wait_ms', 'minimum_interval_wait_ms',
          'network_total_ms', 'retry_wait_ms')


def record_reads(rpc, capacity, prior=False, row_update=None):
    rpc.measurements = deque(maxlen=capacity)
    if prior:
        rpc.measurements.append(dict(attempts=900, **{field:900 for field in FIELDS}))
    recorded = []
    for name in ('verify_chain', 'header', 'request'):
        original = getattr(rpc, name)
        def wrapped(*args, _original=original, **kwargs):
            result = _original(*args, **kwargs)
            row = dict(attempts=1, **{field:1 for field in FIELDS})
            row.update(row_update or {})
            recorded.append(row)
            rpc.measurements.append(row)
            return result
        setattr(rpc, name, wrapped)
    return recorded


@pytest.mark.parametrize('capacity,prior,complete', [
    (100, False, True), (2, False, False),
    (100, True, True), (2, True, False),
])
def test_refresh_reports_retained_window_and_detects_overflow(harness, capacity, prior, complete):
    rpc, config, initial, *_ = harness
    records = record_reads(rpc, capacity, prior)
    result = engine.prepare(rpc, config, previous=initial)
    timing = result['discovery_read_timing']
    retained = min(len(records), capacity)
    assert len(records) > 2
    assert timing['available'] is True
    assert timing['complete'] is complete
    assert timing['operations'] == timing['http_attempts'] == retained
    assert all(timing[field] == retained for field in FIELDS)
    assert all(value >= 0 for value in result['discovery_phase_ms'].values())
    assert sum(result['discovery_phase_ms'].values()) <= result['load_ms']


@pytest.mark.parametrize('has_empty_ring', [False, True])
def test_unavailable_measurements_do_not_become_zero(harness, has_empty_ring):
    rpc, config, initial, *_ = harness
    if has_empty_ring:
        rpc.measurements = deque(maxlen=2)
    result = engine.prepare(rpc, config, previous=initial)
    timing = result['discovery_read_timing']
    assert timing['available'] is False and timing['complete'] is False
    assert timing['operations'] == 0 and timing['http_attempts'] is None
    assert all(timing[field] is None for field in FIELDS)
    assert result['discovery_cursor'] == 100


@pytest.mark.parametrize('unknown', [None, float('nan'), float('inf'), -1, True])
def test_unknown_or_malformed_timing_cannot_abort_valid_refresh(harness, unknown):
    rpc, config, initial, *_ = harness
    record_reads(rpc, 100, row_update=dict(attempts=unknown, network_total_ms=unknown))
    result = engine.prepare(rpc, config, previous=initial)
    timing = result['discovery_read_timing']
    assert timing['available'] is True and timing['complete'] is True
    assert timing['http_attempts'] is None and timing['network_total_ms'] is None
    assert timing['elapsed_ms'] == timing['operations']
    assert result['discovery_cursor'] == 100


def test_diagnostics_do_not_copy_request_details(harness):
    rpc, config, initial, *_ = harness
    record_reads(rpc, 100, row_update=dict(endpoint='https://secret.invalid/key',
        calldata='private-request-data', attempt_timings=[{'endpoint':'private-attempt'}]))
    result = engine.prepare(rpc, config, previous=initial)
    encoded = json.dumps(result['discovery_read_timing'])
    assert 'secret.invalid' not in encoded and 'private-' not in encoded
    assert result['discovery_read_timing']['complete'] is True
