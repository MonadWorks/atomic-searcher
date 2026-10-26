"""Independent recovery counterexamples: transport, fairness, and admission."""
import copy
from collections import Counter

import httpx
import pytest
from eth_abi import encode

from searcher import engine
from searcher.discovery_history import (pool_hint_queue, select_pool_hints,
                                        finish_pool_hints)
from test_discovery_history import harness


def address(n):
    return '0x'+f'{n:040x}'


@pytest.mark.parametrize('kind', ['http_429', 'network_timeout'])
def test_actual_transport_failures_in_old_page_preserve_live_progress(harness, kind):
    rpc, config, previous, existing, candidate, validated = harness
    original = rpc.request
    request = httpx.Request('POST', 'http://127.0.0.1:1')
    failure = (httpx.HTTPStatusError('HTTP 429', request=request,
                                   response=httpx.Response(429, request=request))
               if kind == 'http_429' else httpx.ReadTimeout('read timed out', request=request))
    def fail_only_history(method, params):
        if int(params[0]['fromBlock'], 16) < 69:
            raise failure
        return original(method, params)
    rpc.request = fail_only_history
    unchanged = copy.deepcopy(previous)
    result = engine.prepare(rpc, config, previous=previous)
    assert previous == unchanged
    assert result['discovery_cursor'] == 100
    assert result['discovery_backlog'] == [[11, 68]]
    assert result['discovery_recovered_range'] is None
    assert result['discovery_history_error']
    assert result['last_seen'][existing.address] == 100
    assert candidate.address not in result['last_seen']
    assert candidate.address not in validated


def test_recovered_old_metadata_gets_retry_despite_full_rate_new_arrivals():
    old = address(1)
    queue, _, _ = pool_hint_queue({}, set(), {}, {old: 1})
    finish_pool_hints(queue, [old], [],
                      [{'pool': old, 'reason': 'metadata unavailable'}], 100)
    retried = False
    # Every round introduces enough valid new pools to consume the full budget.
    # The earlier pool has recovered, and its retry is now due.
    for turn in range(8):
        current = {address(100+turn*4+i): 102+turn for i in range(4)}
        previous_keys = set(queue)
        queue, overflow, _ = pool_hint_queue({'pending_discovery_pools': queue}, set(), current, {})
        assert not any(overflow)
        activity = Counter({key: 10 for key in current})
        selected = select_pool_hints(queue, activity, 102+turn, 4, previous_keys)
        assert len(selected) <= 4
        retried |= old in selected
        finish_pool_hints(queue, selected, selected, [], 102+turn)
    assert retried, 'due metadata retry was starved by new -1 last_attempt tasks'


def test_due_retry_progresses_behind_initial_backlog_and_sustained_full_arrivals():
    old = address(1)
    queue, _, _ = pool_hint_queue({}, set(), {}, {old: 1})
    finish_pool_hints(queue, [old], [],
                      [{'pool': old, 'reason': 'metadata unavailable'}], 100)
    waiting = {address(10+i): 90+i for i in range(8)}
    queue, _, _ = pool_hint_queue({'pending_discovery_pools': queue}, set(), {}, waiting)
    retry_round = None
    served_waiting = set()
    # One old-work slot per round meets one new leftover per round. Sorting all
    # untouched items by -1 would keep the waiting tail populated forever and
    # prevent the already-attempted pool from ever reaching that slot.
    for turn in range(32):
        current = {address(1000+turn*4+i): 102+turn for i in range(4)}
        carried = set(queue)
        queue, overflow, _ = pool_hint_queue({'pending_discovery_pools': queue}, set(), current, {})
        assert not any(overflow)
        selected = select_pool_hints(queue, Counter({key: 10 for key in current}),
                                     102+turn, 4, carried)
        assert len(selected) == 4
        served_waiting.update(set(selected) & waiting.keys())
        if old in selected:
            retry_round = turn
        finish_pool_hints(queue, selected, selected, [], 102+turn)
    assert served_waiting == set(waiting)
    assert retry_round is not None and retry_round <= len(waiting)


def test_full_backing_off_queue_still_authenticates_a_new_live_pool(harness, monkeypatch):
    from searcher import discovery_history
    rpc, config, previous, existing, candidate, validated = harness
    poisons = [address(700), address(701)]
    previous['pending_discovery_pools'] = {
        key: {'first_seen': 40+i, 'last_seen': 40+i, 'attempts': 10,
              'last_attempt_block': 90, 'retry_after_block': 1114}
        for i, key in enumerate(poisons)
    }
    previous['discovery_backlog'] = []
    previous['discovery_cursor'] = 99
    rpc.events = [(100, candidate.address)]
    actual = discovery_history.pool_hint_queue
    monkeypatch.setattr(discovery_history, 'pool_hint_queue',
                        lambda *a, **kw: actual(*a, capacity=2, **kw))
    result = engine.prepare(rpc, config, previous=previous)
    assert candidate.address in validated
    assert len(result['pending_discovery_pools']) <= 2
    # A displaced ambiguous hint must survive as pending work or its source
    # block range, rather than turn into an unrecorded permanent exclusion.
    for i, key in enumerate(poisons):
        assert (key in result['pending_discovery_pools'] or
                any(lo <= 40+i <= hi for lo, hi in result['discovery_backlog']))


def test_full_old_failures_do_not_seal_off_historical_pool_admission(harness, monkeypatch):
    from searcher import discovery_history
    rpc, config, previous, existing, candidate, validated = harness
    poisons = [address(800), address(801)]
    previous['pending_discovery_pools'] = {
        key: {'first_seen': 40+i, 'last_seen': 40+i, 'attempts': 10,
              'last_attempt_block': 90, 'retry_after_block': 1114}
        for i, key in enumerate(poisons)
    }
    actual = discovery_history.pool_hint_queue
    monkeypatch.setattr(discovery_history, 'pool_hint_queue',
                        lambda *a, **kw: actual(*a, capacity=2, **kw))
    result = engine.prepare(rpc, config, previous=previous)
    assert candidate.address in validated
    assert len(result['pending_discovery_pools']) <= 2
    assert result['discovery_recovered_range'] == [11, 20]
    for i, key in enumerate(poisons):
        assert (key in result['pending_discovery_pools'] or
                any(lo <= 40+i <= hi for lo, hi in result['discovery_backlog']))


def test_recovered_range_cannot_erase_a_displaced_failed_hint(harness, monkeypatch):
    from searcher import discovery_history
    rpc, config, previous, existing, candidate, validated = harness
    displaced = address(900)
    previous['pending_discovery_pools'] = {
        displaced: {'first_seen': 15, 'last_seen': 15, 'attempts': 10,
                    'last_attempt_block': 90, 'retry_after_block': 1114}}
    rpc.events = [(100, candidate.address)]
    actual = discovery_history.pool_hint_queue
    monkeypatch.setattr(discovery_history, 'pool_hint_queue',
                        lambda *a, **kw: actual(*a, capacity=1, **kw))
    result = engine.prepare(rpc, config, previous=previous)
    assert candidate.address in validated
    assert any(lo <= 15 <= hi for lo, hi in result['discovery_backlog'])


def test_historical_admission_does_not_evict_same_refresh_fresh_pool():
    from searcher.discovery_history import pool_hint_queue
    previous = {'pending_discovery_pools': {
        address(1): {'first_seen': 1, 'last_seen': 1, 'attempts': 10,
                     'last_attempt_block': 90, 'retry_after_block': 1114},
        address(2): {'first_seen': 2, 'last_seen': 2, 'attempts': 10,
                     'last_attempt_block': 90, 'retry_after_block': 1114}}}
    fresh, historical = address(3), address(4)
    queue, overflow, deferred = pool_hint_queue(previous, set(), {fresh: 100}, {historical: 10}, capacity=2)
    assert set(queue) == {fresh, historical}
    assert not any(overflow)
    assert sorted(deferred) == [[1, 1], [2, 2]]


def test_many_refreshes_keep_every_hint_owned_and_admit_recovered_pools(harness, monkeypatch, tmp_path):
    from searcher import discovery_history
    from searcher.snapshots import read_universe, write_universe
    rpc, config, previous, existing, candidate, _ = harness
    poisons = {address(801), address(802)}
    permanent = address(803)
    events = [(12, address(801)), (15, candidate.address),
              (18, address(802)), (21, permanent)]
    actual = discovery_history.pool_hint_queue
    monkeypatch.setattr(discovery_history, 'pool_hint_queue',
                        lambda *a, **kw: actual(*a, capacity=3, **kw))
    classified = set()
    round_number = [0]
    def authenticate(rpc, config, addresses, block):
        accepted, rejected = [], []
        assert len(addresses) <= config['policy']['max_pools']
        for key in addresses:
            if key == permanent:
                rejected.append({'pool': key, 'reason': 'factory outside supported registry'})
                classified.add(key)
            elif key in poisons or (key == candidate.address and round_number[0] < 4):
                rejected.append({'pool': key, 'reason': 'metadata unavailable'})
            else:
                pool = copy.deepcopy(candidate)
                pool.address = key
                accepted.append(pool)
        return accepted, rejected
    monkeypatch.setattr(engine, 'validate_pools', authenticate)
    observed = set()
    disk = tmp_path/'restart-universe.json'
    for turn in range(40):
        round_number[0] = turn
        rpc.height = 100 + 3*turn
        events.append((rpc.height, address(1000+turn)))
        rpc.events = list(events)
        prior = copy.deepcopy(previous)
        request_start = len(rpc.ranges)
        result = engine.prepare(rpc, config, previous=previous)
        assert previous == prior
        for lo, hi in rpc.ranges[request_start:]:
            observed.update(key for block, key in events if lo <= block <= hi)
        assert len(result['pending_discovery_pools']) <= 3
        assert len(result['discovery_backlog']) <= 64
        known = {pool['address'] for pool in result['known_pools']}
        for key in observed-known-classified:
            assert (key in result['pending_discovery_pools'] or
                    any(lo <= block <= hi for block, seen_key in events if seen_key == key
                        for lo, hi in result['discovery_backlog'])), (turn, key, result)
        # Restart every round through the real durable format. No progress may
        # live only in a mutated previous snapshot or an in-memory side queue.
        write_universe(disk, result)
        previous = read_universe(disk)
    known = {pool['address'] for pool in previous['known_pools']}
    assert candidate.address in known
    assert {address(1000+i) for i in range(34)} <= known


def test_displaced_long_lived_hint_requeues_only_last_actual_observation():
    previous = {'pending_discovery_pools': {
        address(1): {'first_seen': 10, 'last_seen': 52_000_000, 'attempts': 10,
                     'last_attempt_block': 52_000_001, 'retry_after_block': 52_001_025}}}
    queue, overflow, deferred = pool_hint_queue(previous, set(),
                                              {address(2): 52_000_002}, {}, capacity=1)
    assert set(queue) == {address(2)} and not any(overflow)
    assert deferred == [[52_000_000, 52_000_000]]


@pytest.mark.parametrize('reply, expected', [
    (None, 'factory reverse lookup unavailable'),
    (b'', 'factory reverse lookup unavailable'),
    (b'partial', 'factory reverse lookup unavailable'),
    (encode(['address'], [address(999)]), 'factory reverse lookup mismatch'),
])
def test_real_pool_authentication_distinguishes_missing_reverse_lookup(reply, expected):
    from searcher.pools import validate_pools
    factory, pool = address(100), address(200)
    config = {'factories': [{'address': factory, 'kind': 'v2'}]}
    class RPC:
        reads = 0
        def views(self, calls, block):
            assert block == 123
            self.reads += 1
            if self.reads == 1:
                assert len(calls) == 5
                return [encode(['address'], [value]) for value in (factory, address(1), address(2))] + [None, None]
            assert len(calls) == 1
            return [reply]
    valid, errors = validate_pools(RPC(), config, [pool], 123)
    assert not valid and errors == [{'pool': pool, 'reason': expected}]
