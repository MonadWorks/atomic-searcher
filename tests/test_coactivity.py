"""Independent regressions for activity-directed complete-cycle scheduling.

Logs are deliberately represented only by addresses and receipt times.  They
must never supply a price, replace pool state, or admit an unknown pool.
"""
from dataclasses import asdict

import pytest

from searcher.cycle_groups import CycleGroups
from searcher.pools import Pool
from searcher.universe import CyclePages


def address(n):
    return '0x' + f'{n:040x}'


def pool(n, a, b):
    return Pool(address(n), address(9999), 'v3', address(a), address(b), 1, 100,
                sqrt_price=2**96, tick=0, liquidity=10**18, block=7,
                block_hash='0x' + '77'*32)


def triangle_market(parallel):
    result = []
    for i, (a, b) in enumerate(((1, 2), (2, 3), (1, 3))):
        result.extend(pool(1000 + i*1000 + j, a, b) for j in range(parallel))
    return result


def freeze(monkeypatch, now=100.0):
    clock = [now]
    monkeypatch.setattr('searcher.universe.time.monotonic', lambda: clock[0])
    return clock


def test_settlement_iterator_survives_saved_coactivity_configuration(monkeypatch):
    freeze(monkeypatch)
    pools = triangle_market(1)
    pages = CyclePages(pools, iter([address(1)]), 3)
    assert len(pages.groups) == 1
    pages.prioritize([p.address for p in pools])
    pages.take(8, group_limit=4)
    assert pages.selected_coactive == [tuple(p.address for p in pools)]


def test_full_recent_triangle_reaches_page_among_million_parallel_combinations(monkeypatch):
    freeze(monkeypatch)
    pools = triangle_market(101)
    pages = CyclePages(pools, [address(1)], 3)
    assert len(pages.groups) > 1_000_000
    target = tuple(sorted(pools[i].address for i in (100, 201, 302)))
    # Separate arrivals mirror the inbox's per-pool update interface.
    for i, p in enumerate(target):
        pages.prioritize([p], observed_at=99 + i/10)
    before = {p.address: asdict(p) for p in pools}
    cold = pages.groups[pages.cursor]
    chosen = pages.take(24, group_limit=16)
    assert target in pages.selected_priority
    assert target in pages.selected_groups
    assert cold in pages.selected_groups
    assert set(target).issubset(p.address for p in chosen)
    assert before == {p.address: asdict(p) for p in pools}


def test_small_family_gets_turn_even_beside_large_parallel_family(monkeypatch):
    freeze(monkeypatch)
    busy_pair = [pool(100 + i, 1, 2) for i in range(100)]
    triangle = [pool(400, 1, 3), pool(401, 3, 4), pool(402, 1, 4)]
    pages = CyclePages(busy_pair + triangle, [address(1)], 3)
    pages.prioritize([p.address for p in busy_pair + triangle])
    target = tuple(p.address for p in triangle)
    first = pages.coactive_groups(100, 2)
    second = pages.coactive_groups(100, 2)
    assert target in first and target in second
    assert len(set(first + second)) >= 3  # Parallel variants also rotate.


def test_expired_leg_stops_joint_priority_and_old_arrival_cannot_refresh_it(monkeypatch):
    clock = freeze(monkeypatch)
    pools = triangle_market(1)
    pages = CyclePages(pools, [address(1)], 3)
    pages.prioritize([p.address for p in pools], observed_at=100)
    assert pages.coactive_groups(100, 8)
    pages.prioritize([pools[0].address], observed_at=99)
    clock[0] = 105.01
    pages.prioritize([pools[1].address, pools[2].address], observed_at=105)
    assert pages.coactive_groups(clock[0], 8) == []
    pages.prioritize([pools[0].address], observed_at=105.01)
    assert pages.coactive_groups(clock[0], 8)


def test_unauthenticated_and_excluded_pools_cannot_enter_coactive_graph(monkeypatch):
    freeze(monkeypatch)
    good = [pool(100, 1, 2), pool(101, 1, 2)]
    excluded = [pool(200, 1, 9), pool(201, 1, 9)]
    original = CyclePages(good + excluded, [address(1)], 3)
    original.prioritize([p.address for p in good + excluded])
    pages = CyclePages(good + excluded, [address(1)], 3,
                       groups=original.groups, excluded_tokens=[address(9)])
    pages.resume(original)
    pages.prioritize([address(999)] + [p.address for p in excluded])
    chosen = pages.take(8, group_limit=4)
    assert {p.address for p in chosen} == {p.address for p in good}
    assert set(pages.recent_activity) == {p.address for p in good}
    assert all(set(g).issubset(p.address for p in good) for g in pages.selected_priority)


def test_explicit_allowed_group_subset_is_respected(monkeypatch):
    freeze(monkeypatch)
    pools = triangle_market(2)
    allowed = [tuple(sorted((pools[0].address, pools[2].address, pools[4].address)))]
    pages = CyclePages(pools, [address(1)], 3, groups=allowed)
    pages.prioritize([p.address for p in pools])
    for _ in range(8):
        chosen = pages.take(6, group_limit=4)
        assert set(pages.selected_groups).issubset(allowed)
        assert set(pages.selected_priority).issubset(allowed)
        assert {p.address for p in chosen}.issubset(allowed[0])


def test_resume_preserves_variant_rotation_and_does_not_alias_state(monkeypatch):
    freeze(monkeypatch)
    pools = [pool(100 + i, 1, 2) for i in range(4)]
    old = CyclePages(pools, [address(1)], 3)
    old.prioritize([p.address for p in pools])
    first = old.coactive_groups(100, 1)
    refreshed = CyclePages(pools + [pool(300, 1, 5)], [address(1)], 3)
    refreshed.resume(old)
    assert refreshed.coactive_groups(100, 1) != first
    assert refreshed.family_offsets != old.family_offsets
    refreshed.prioritize([pools[0].address], observed_at=101)
    assert old.recent_activity[pools[0].address] == 100


@pytest.mark.parametrize('limit, group_limit', [(4, 2), (6, 3), (12, 4), (24, 8)])
def test_required_pools_group_bounds_and_complete_routes_are_preserved(monkeypatch, limit, group_limit):
    freeze(monkeypatch)
    pools = triangle_market(4)
    pages = CyclePages(pools, [address(1)], 3)
    pages.prioritize([p.address for p in pools])
    required = [pools[-1].address]
    for _ in range(8):
        chosen = pages.take(limit, required=required, group_limit=group_limit)
        addresses = {p.address for p in chosen}
        assert set(required).issubset(addresses)
        assert len(addresses) <= limit
        assert len(pages.selected_groups) <= group_limit
        assert all(set(g).issubset(addresses) for g in pages.selected_groups)
        assert all(g in pages.selected_groups for g in pages.selected_priority)
        assert all(g in pages.selected_priority for g in pages.selected_coactive)


def test_continuous_hot_activity_preserves_cold_coverage(monkeypatch):
    freeze(monkeypatch)
    pools = triangle_market(3)
    pages = CyclePages(pools, [address(1)], 3)
    target = [pools[i].address for i in (2, 5, 8)]
    seen = set()
    for _ in range(len(pages.groups)):
        pages.prioritize(target)
        pages.take(6, group_limit=4)
        seen.update(pages.selected_groups)
    assert seen == set(pages.groups)


def test_recent_signal_storage_is_bounded_and_four_hop_coverage_survives(monkeypatch):
    freeze(monkeypatch)
    pools = triangle_market(70)
    # Add a supported mixed four-hop route that is deliberately cold.
    four = [pool(5000, 1, 10), pool(5001, 10, 11), pool(5002, 11, 12), pool(5003, 1, 12)]
    four[0].kind = 'v2'; four[0].fee = 3000
    pages = CyclePages(pools + four, [address(1)], 4)
    pages.prioritize([p.address for p in pools])
    assert len(pages.recent_activity) <= 192
    wanted = tuple(p.address for p in four)
    pages.cursor = pages.groups.index(wanted)
    pages.take(24, group_limit=16)
    assert wanted in pages.selected_groups
    assert all(len(g) <= 3 for g in pages.selected_priority)


@pytest.mark.parametrize('truncate', [False, True])
def test_coactive_page_requires_fresh_complete_atomic_read(monkeypatch, truncate):
    from eth_abi import decode, encode
    from searcher.core import Reject, load_config
    from searcher.statefeed import StateFeed, STATE
    freeze(monkeypatch)
    pools = triangle_market(2)
    config = load_config()
    config['settlement_tokens'] = [dict(config['settlement_tokens'][0], address=address(1))]
    config['policy'].update(cycle_pages=True, max_hops=3, max_pools=12,
                            route_groups_per_page=8, snapshot_encoding='abi')
    config['unsupported_transfer_tokens'] = {}
    header = {'number': '0x7b', 'hash': '0x'+'88'*32, 'timestamp': '0xf6'}
    reads = []

    class RPC:
        def batch(self, calls):
            assert calls[0][0] == 'eth_call'
            transaction, tag, _ = calls[0][1]
            assert tag == '0x7b'
            addresses, _, _, _ = decode(['address[]', 'int24[]', 'uint8', 'uint8'],
                                        bytes.fromhex(transaction['data'][10:]))
            reads.append(addresses)
            rows = [(a, True, 2**97, 1, 10**19, 100, [], [], []) for a in addresses]
            if truncate:
                rows.pop()
            return ['0x'+encode([STATE+'[]', 'uint256', 'uint256'],
                                [rows, 123, 246]).hex(), header]

    feed = StateFeed(RPC(), config, {'pools': [p.json() for p in pools]})
    target = tuple(sorted(pools[i].address for i in (1, 3, 5)))
    feed.pages.prioritize(target)
    if truncate:
        with pytest.raises(Reject, match='incomplete atomic snapshot'):
            feed.poll(123)
    else:
        frame = feed.poll(123)
        assert target in frame.transport['coactive_priority_groups']
        assert set(target).issubset(reads[0])
        assert all(p['sqrt_price'] == 2**97 for p in frame.snapshot_pools)
        assert all(p.sqrt_price == 2**96 for p in feed.pages.pools.values())
