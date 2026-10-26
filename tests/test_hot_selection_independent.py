"""Independent selection counterexamples; no live RPC or transaction writes."""
import copy

from searcher.fastlane import HotRoutes
from test_fastlane import config
from test_pipeline import saved


def parallel_universe(count=10):
    universe = saved()
    template = universe['pools'][0]
    addresses = ['0x' + f'{i:040x}' for i in range(100, 100 + count)]
    universe['pools'] = [dict(template, address=address) for address in addresses]
    return universe, addresses


def test_jointly_active_pair_beats_unrelated_rotation_offsets():
    universe, addresses = parallel_universe()
    selector = HotRoutes(universe, config(), clock=lambda: 100.0)
    first, second = addresses[:2]
    # Existing scans have advanced both pools past each other's counterpart.
    selector.pair_offsets.update({first: 2, second: 2})
    selector.add([(first, 100.0), (second, 100.0)])
    selected = selector.take()
    assert tuple(sorted((first, second))) in selected['groups']


def test_priority_selection_keeps_saved_prices_as_metadata():
    universe, addresses = parallel_universe()
    before = copy.deepcopy(universe)
    selector = HotRoutes(universe, config(), clock=lambda: 100.0)
    selector.add([(addresses[0], 99.9), (addresses[1], 100.0)])
    selected = selector.take()
    original = {pool['address']: pool for pool in before['pools']}
    assert universe == before
    assert all(pool.json() == original[pool.address] for pool in selected['pools'])
    assert all(set(group) <= {pool.address for pool in selected['pools']}
               for group in selected['groups'])


def test_future_and_expired_events_do_not_create_hot_work():
    universe, addresses = parallel_universe()
    selector = HotRoutes(universe, config(), clock=lambda: 100.0)
    selector.add([(addresses[0], 100.1), (addresses[1], 98.0)])
    assert selector.take()['groups'] == []


def test_persistent_joint_activity_preserves_counterpart_rotation():
    universe, addresses = parallel_universe(count=16)
    now = [100.0]
    selector = HotRoutes(universe, config(), clock=lambda: now[0])
    visited = set()
    for _ in range(32):
        selector.add([(addresses[0], now[0]), (addresses[1], now[0])])
        selected = selector.take()
        assert len(selected['groups']) <= selector.group_limit
        assert len(selected['pools']) <= selector.pool_limit
        visited.update(pool.address for pool in selected['pools'])
        now[0] += .02
    assert visited == set(addresses)


def test_small_pool_budget_still_visits_a_disjoint_quiet_triangle():
    universe = saved()
    template = universe['pools'][0]
    a, b = template['token0'], template['token1']
    c, d, e = ['0x' + f'{i:040x}' for i in (21, 22, 23)]
    addresses = ['0x' + f'{i:040x}' for i in range(200, 206)]
    universe['pools'] = [dict(template, address=address, token0=x, token1=y)
                         for address, (x, y) in zip(addresses,
                             [(a, b), (b, c), (c, a), (a, d), (d, e), (e, a)])]
    cfg = config()
    cfg['policy'].update(hot_lane_max_pools=5, hot_lane_max_groups=3)
    now = [100.0]
    selector = HotRoutes(universe, cfg, clock=lambda: now[0])
    quiet_group = tuple(sorted(addresses[3:]))
    visited = set()
    for _ in range(12):
        # The first triangle is wholly active. Only one leg of the second is
        # active, so it must retain the ordinary event rotation's quiet legs.
        selector.add([(pool, now[0]) for pool in addresses[:4]])
        selected = selector.take()
        assert len(selected['pools']) <= 5
        visited.update(selected['groups'])
        now[0] += .02
    assert quiet_group in visited


def test_failed_priority_read_retains_same_version_for_retry():
    universe, addresses = parallel_universe()
    selector = HotRoutes(universe, config(), clock=lambda: 100.0)
    selector.add([(addresses[0], 100.0), (addresses[1], 100.0)])
    first = selector.take()
    group = tuple(addresses[:2])
    version = first['_coactive_versions'][group]
    selector.failed(first)
    retry = selector.take()
    assert retry['_coactive_versions'][group] == version
    assert group not in selector.read_versions
    selector.acknowledge(retry)
    assert selector.read_versions[group] == version
    assert group not in selector.pending


def test_successful_old_read_cannot_consume_newer_event_version():
    universe, addresses = parallel_universe()
    now = [100.0]
    selector = HotRoutes(universe, config(), clock=lambda: now[0])
    selector.add([(addresses[0], 100.0), (addresses[1], 100.0)])
    first = selector.take()
    group = tuple(addresses[:2])
    now[0] = 100.1
    selector.add([(addresses[0], now[0])])
    newer = selector.take()
    selector.acknowledge(first)
    assert selector.pending[group]['version'] == newer['_coactive_versions'][group]
    assert selector.pending[group]['version'] != selector.read_versions[group]
    selector.acknowledge(newer)
    assert group not in selector.pending


def test_pending_and_success_versions_expire_without_becoming_fresh():
    universe, addresses = parallel_universe()
    now = [100.0]
    selector = HotRoutes(universe, config(), clock=lambda: now[0])
    selector.add([(addresses[0], 100.0), (addresses[1], 100.0)])
    selected = selector.take()
    now[0] = 102.0
    selector.failed(selected)
    assert selector.take()['groups'] == []
    assert selector.summary()['tracked_expired_unread'] == 1
    assert not selector.read_versions


def test_resumed_universe_discards_group_that_is_no_longer_a_cycle():
    universe, addresses = parallel_universe(count=4)
    selector = HotRoutes(universe, config(), clock=lambda: 100.0)
    selector.add([(addresses[0], 100.0), (addresses[1], 100.0)])
    old = selector.take()
    old_group = tuple(addresses[:2])
    assert old_group in old['_coactive_versions']
    # Each old member remains known and belongs to some authenticated cycle,
    # but they no longer form a cycle together under replacement metadata.
    changed = copy.deepcopy(universe)
    for index in (1, 2):
        changed['pools'][index]['token1'] = '0x' + f'{23:040x}'
    replacement = HotRoutes(changed, config(), clock=lambda: 100.0)
    replacement.resume(selector)
    new = replacement.take()
    assert old_group not in new['groups']
    assert old_group not in replacement.pending


def test_expired_filter_recheck_restores_priority_without_new_event():
    universe, addresses = parallel_universe()
    admitted = [False]
    selector = HotRoutes(universe, config(), clock=lambda: 100.0,
                         group_allowed=lambda group, pools: admitted[0])
    selector.add([(addresses[0], 100.0), (addresses[1], 100.0)])
    assert selector.take()['groups'] == []
    admitted[0] = True
    restored = selector.take()
    assert tuple(addresses[:2]) in restored['_coactive_versions']


def test_changed_static_metadata_invalidates_previous_success_version():
    universe, addresses = parallel_universe(count=4)
    selector = HotRoutes(universe, config(), clock=lambda: 100.0)
    selector.add([(addresses[0], 100.0), (addresses[1], 100.0)])
    selected = selector.take()
    group = tuple(addresses[:2])
    selector.acknowledge(selected)
    changed = copy.deepcopy(universe)
    for pool in changed['pools']:
        pool['token1'] = '0x' + f'{23:040x}'
    replacement = HotRoutes(changed, config(), clock=lambda: 100.0)
    replacement.resume(selector)
    assert group in replacement.take()['_coactive_versions']


def test_reader_failure_retries_before_consuming_priority_version(tmp_path, monkeypatch):
    import threading
    import time
    import searcher.fastlane as module
    from searcher.activity import ActivityInbox
    from searcher.core import RPCUnavailable
    from searcher.statefeed import StateFrame
    from test_pipeline import header, H0, H1
    from test_runtime import FakeRPC

    universe, addresses = parallel_universe()
    cfg = config()
    cfg['policy']['hot_lane_event_ttl_seconds'] = 5
    selector_instances, attempts, output = [], [], []
    stop, inbox = threading.Event(), ActivityInbox()
    original = module.HotRoutes

    class TrackedSelector(original):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.pair_offsets.update({addresses[0]: 2, addresses[1]: 2})
            selector_instances.append(self)

    class Feed:
        def __init__(self, rpc, config, saved):
            from searcher.pools import Pool
            self.pools = [Pool.from_json(pool) for pool in saved['pools']]

        def poll(self):
            attempts.append({pool.address for pool in self.pools})
            if len(attempts) == 1:
                raise RPCUnavailable('independent test transient read failure')
            now = time.monotonic_ns()
            return StateFrame(header(2, H1, H0), header(2, H1, H0),
                              [pool.json() for pool in self.pools], now, now,
                              time.time(), 3, 106)

    def publish(lane, recorded):
        output.append(recorded[1])
        stop.set()

    monkeypatch.setattr(module, 'HotRoutes', TrackedSelector)
    inbox.publish(addresses[:2], time.monotonic())
    module.run_hot_reader(FakeRPC, cfg, lambda: (0, universe), inbox, stop,
                          time.monotonic() + 2, tmp_path, module.FrameSequence(),
                          publish, lambda *args, **kwargs: None, feed_factory=Feed)
    assert len(attempts) == 2 and len(output) == 1
    assert all(set(addresses[:2]) <= pools for pools in attempts)
    assert tuple(addresses[:2]) in output[0].transport['coactive_priority_groups']
    assert selector_instances[0].summary()['read_failures'] == 1
    assert selector_instances[0].summary()['coactive_read'] == 1
    assert '_coactive_versions' not in output[0].transport


def learned_pair():
    universe, addresses = parallel_universe()
    now = [100.0]
    selector = HotRoutes(universe, config(), clock=lambda: now[0])
    selector.add([(addresses[0], 100.0), (addresses[1], 100.0)])
    selector.acknowledge(selector.take())
    return universe, addresses, now, selector


def test_past_pair_single_new_leg_arms_complete_fresh_read():
    universe, addresses, now, selector = learned_pair()
    group = tuple(addresses[:2])
    now[0] = 103.0
    selector.pair_offsets[addresses[0]] = 4
    selector.turns.pop(addresses[0], None)
    selector.add([(addresses[0], now[0])])
    selected = selector.take()
    assert group in selected['groups']
    evidence = next(row for row in selected['priority_evidence']
                    if tuple(row['pools']) == group)
    assert evidence['source'] == 'remembered_coactivity'
    assert evidence['learned_at'] == 100.0
    assert evidence['last_coactive_at'] == 100.0
    assert evidence['triggers'] == {addresses[0]: 103.0}
    assert evidence['selected_at'] == 103.0
    original = {p['address']: p for p in universe['pools']}
    assert all(p.json() == original[p.address] for p in selected['pools'])


def test_single_leg_activity_does_not_renew_topology_memory():
    _, addresses, now, selector = learned_pair()
    group = tuple(addresses[:2])
    for at in (103.0, 130.0, 200.0, 219.0):
        now[0] = at
        selector.add([(addresses[0], at)])
        selector.acknowledge(selector.take())
        assert selector.remembered[group]['last_coactive_at'] == 100.0
    now[0] = 220.001
    selector.add([(addresses[0], now[0])])
    result = selector.take()
    assert group not in selector.remembered
    assert group not in result['_coactive_versions']
    assert selector.summary()['memory_expired'] == 1


def test_successful_pair_does_not_rearm_when_only_older_leg_expires():
    universe, addresses = parallel_universe()
    now = [100.5]
    selector = HotRoutes(universe, config(), clock=lambda: now[0])
    selector.add([(addresses[0], 100.0), (addresses[1], 100.5)])
    selector.acknowledge(selector.take())
    now[0] = 101.6  # First leg expires; the second still has the same event.
    result = selector.take()
    assert tuple(addresses[:2]) not in result['_coactive_versions']
    assert tuple(addresses[:2]) in selector.remembered


def test_new_complete_activity_legitimately_renews_memory():
    _, addresses, now, selector = learned_pair()
    group = tuple(addresses[:2])
    now[0] = 210.0
    selector.add([(addresses[0], now[0]), (addresses[1], now[0])])
    selector.acknowledge(selector.take())
    assert selector.remembered[group]['last_coactive_at'] == 210.0
    now[0] = 315.0
    selector.add([(addresses[0], now[0])])
    # A reserved rotation turn may select the group outside the priority slot;
    # membership in pending is the eligibility guarantee, not every-turn entry.
    selector.take()
    assert group in selector.pending


def test_late_old_ack_never_regresses_last_successful_trigger():
    _, addresses, now, selector = learned_pair()
    group = tuple(addresses[:2])
    now[0] = 103.0
    selector.add([(addresses[0], now[0])])
    older = selector.take()
    now[0] = 103.2
    selector.add([(addresses[0], now[0])])
    newer = selector.take()
    if group not in newer['_coactive_versions']:
        newer = selector.take()
    selector.acknowledge(newer)
    selector.acknowledge(older)
    assert selector._stamp(selector.read_versions[group]) == 103.2
    assert group not in selector.take()['_coactive_versions']


def test_memories_and_pending_tracking_remain_bounded_under_dense_activity():
    universe, addresses = parallel_universe(count=192)
    now = [100.0]
    selector = HotRoutes(universe, config(), clock=lambda: now[0])
    for _ in range(10):
        selector.add([(address, now[0]) for address in addresses])
        selected = selector.take()
        selector.acknowledge(selected)
        assert len(selector.remembered) <= 128
        assert len(selector.pending) <= 256
        assert len(selector.read_versions) <= 512
        assert len(selected['pools']) <= selector.pool_limit
        now[0] += .01
    assert selector.summary()['coactive_enumeration_capped']
    assert selector.summary()['memory_evicted'] > 0


def test_fresh_complete_pair_precedes_remembered_single_leg_backlog():
    universe, addresses = parallel_universe(count=4)
    for index in (2, 3):
        universe['pools'][index]['token1'] = '0x' + f'{23:040x}'
    now = [100.0]
    selector = HotRoutes(universe, config(), clock=lambda: now[0])
    selector.add([(addresses[0], 100.0), (addresses[1], 100.0)])
    selector.acknowledge(selector.take())
    now[0] = 103.0
    # Old remembered work arrives first in the pool queue. A new fully active
    # pair must still get the first priority slot rather than waiting behind it.
    selector.add([(addresses[0], 102.9), (addresses[2], 103.0), (addresses[3], 103.0)])
    selected = selector.take()
    assert selected['groups'][0] == tuple(addresses[2:])
    evidence = {tuple(row['pools']): row for row in selected['priority_evidence']}
    assert evidence[tuple(addresses[2:])]['source'] == 'current_coactivity'
    assert evidence[tuple(addresses[:2])]['source'] == 'remembered_coactivity'
