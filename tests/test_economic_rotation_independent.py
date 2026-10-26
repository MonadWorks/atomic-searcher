"""Independent graph oracle and admission-starvation counterexamples."""
from array import array
from collections import Counter, defaultdict
from itertools import combinations
import random
import time

import pytest

from searcher.cycle_groups import CycleGroups, EconomicRotation, Family
from searcher.pools import Pool
from searcher.universe import CyclePages


def address(n):
    return "0x" + f"{n:040x}"


def pool(n, x, y, kind="v3"):
    a, b = sorted((address(x), address(y)))
    return Pool(address(n), address(999999), kind, a, b,
                0 if kind == "v2" else 1, 3000)


def independent_groups(pools, settlements):
    """Exhaust pool-edge walks, without using CycleGroups family/rank code."""
    adjacent = defaultdict(list)
    for item in pools:
        adjacent[item.token0].append(item)
        adjacent[item.token1].append(item)
    result = set()
    for start in settlements:
        def walk(token, tokens, path):
            for item in adjacent[token]:
                if item in path:
                    continue
                nxt = item.token1 if token == item.token0 else item.token0
                chosen = path + [item]
                if nxt == start:
                    if 2 <= len(chosen) <= 4 and (len(chosen) < 4 or any(p.kind == "v2" for p in chosen)):
                        result.add(tuple(sorted(p.address for p in chosen)))
                elif nxt not in tokens and len(chosen) < 4:
                    walk(nxt, tokens | {nxt}, chosen)
        walk(start, {start}, [])
    return result


def economic_key(group, states):
    edges = sorted((states[a].token0, states[a].token1) for a in group)
    return len(group), tuple(edges[:1] if len(group) == 2 else edges)


@pytest.mark.parametrize("seed", range(8))
def test_small_multigraph_rotation_matches_independent_cycle_oracle(seed):
    rng = random.Random(seed)
    pools = []
    for x, y in combinations(range(1, 6), 2):
        for _ in range(rng.randrange(3)):
            pools.append(pool(100 + len(pools), x, y, rng.choice(("v2", "v3", "slipstream"))))
    settlements = [address(1), address(3)]
    expected = independent_groups(pools, settlements)
    groups = CycleGroups(pools, settlements, 4)
    assert set(groups) == expected
    assert len(groups) == len(expected), "factorized partitions must be disjoint"
    states = {p.address: p for p in pools}
    by_hop = {h: defaultdict(set) for h in (2, 3, 4)}
    for group in expected:
        by_hop[len(group)][economic_key(group, states)].add(group)
    for h, families in by_hop.items():
        assert len(groups.economic_spans[h]) // 2 == len(families)
    rotation = EconomicRotation(groups)
    if not expected:
        assert rotation.next() is None
        return
    rounds = max(len(families) * max(map(len, families.values()))
                 for families in by_hop.values() if families)
    seen = set()
    first_round = {h: [] for h in (2, 3, 4)}
    for _ in range(5 * rounds):
        group = rotation.next()
        assert group in expected
        seen.add(group)
        h = len(group)
        if len(first_round[h]) < len(by_hop[h]):
            first_round[h].append(economic_key(group, states))
    assert seen == expected
    for h, families in by_hop.items():
        assert len(first_round[h]) == len(families)
        assert len(set(first_round[h])) == len(families), "each economic family gets its first turn before any repeats"


def test_first_v2_partitions_do_not_multiply_one_cycle_entitlement():
    pools = []
    for x, y in [(1, 2), (2, 3), (3, 4), (4, 1)]:
        for kind in ("v2", "v3", "slipstream"):
            pools.append(pool(100 + len(pools), x, y, kind))
    for x, y in [(1, 5), (5, 6), (6, 7), (7, 1)]:
        pools.append(pool(100 + len(pools), x, y, "v2"))
    groups = CycleGroups(pools, [address(1)], 4)
    assert len(groups.economic_spans[4]) == 4
    states = {p.address: p for p in pools}
    rotation = EconomicRotation(groups)
    selected = []
    while len(selected) < 20:
        group = rotation.next()
        if len(group) == 4:
            selected.append(economic_key(group, states))
    assert sorted(Counter(selected).values()) == [10, 10]


def test_large_cartesian_family_has_compact_metadata_and_bounded_unranking(monkeypatch):
    pools = [pool(100 + i, 1, 2) for i in range(1000)]
    groups = CycleGroups(pools, [address(1)], 2)
    assert len(groups) == 499500 and len(groups.families) == 1
    assert isinstance(groups.economic_spans[2], array)
    assert len(groups.economic_spans[2]) == 2
    called = []
    original = Family.at
    def observed(self, index):
        called.append(index)
        return original(self, index)
    monkeypatch.setattr(Family, "at", observed)
    rotation = EconomicRotation(groups)
    seen = [rotation.next() for _ in range(37)]
    assert called == list(range(37))
    assert len(set(seen)) == 37


def test_refresh_preserves_next_linear_group_after_insertion_and_all_survivors():
    pools = [pool(100 + i, 2, 3) for i in range(5)]
    old = CyclePages(pools, [address(2)], 2)
    old.hot = []
    for _ in range(4):
        old.take(4, group_limit=2)
    expected = old.groups[old.cursor]
    fresh = CyclePages(pools + [pool(90, 1, 2), pool(91, 1, 2)], [address(2)], 2)
    fresh.hot = []
    fresh.resume(old)
    assert fresh.groups[fresh.cursor] == expected
    old_positions = dict(old.economic_rotation.positions)
    seen = set()
    for _ in range(len(fresh.groups)):
        fresh.take(4, group_limit=2)
        seen.update(fresh.selected_groups)
    assert seen == set(fresh.groups)
    assert old.economic_rotation.positions == old_positions


def test_linear_slot_covers_eligible_groups_even_when_rotation_revisits_small_family():
    pools = [pool(100 + i, 1, 2) for i in range(8)]
    pools += [pool(300, 1, 3), pool(301, 3, 4), pool(302, 4, 1)]
    pages = CyclePages(pools, [address(1)], 3)
    pages.hot = []
    blocked = (address(100), address(101))
    seen = set()
    for _ in range(len(pages.groups)):
        pages.take(8, group_limit=2, group_allowed=lambda group, states: group != blocked)
        assert len(pages.selected_groups) <= 2
        assert blocked not in pages.selected_groups
        seen.update(pages.selected_groups)
    assert seen == set(pages.groups) - {blocked}
    for _ in range(len(pages.groups)):
        pages.take(8, group_limit=2, group_allowed=lambda group, states: True)
        seen.update(pages.selected_groups)
    assert seen == set(pages.groups)


@pytest.mark.parametrize("group_limit", [4, 96])
def test_blocked_cold_prefix_cannot_spend_healthy_event_admission_budget(group_limit):
    pools = [pool(100 + i, 1, 2) for i in range(100)]
    pools += [pool(500, 1, 3), pool(501, 1, 3)]
    pages = CyclePages(pools, [address(1)], 2)
    healthy = (address(500), address(501))
    pages.prioritize(healthy, time.monotonic())
    inspected = []
    def eligible(group, states):
        inspected.append(group)
        return group == healthy
    pages.take(300, group_limit=group_limit, group_allowed=eligible)
    assert healthy in pages.selected_groups
    assert healthy in inspected and pages.cursor > 0
    assert len(inspected) <= max(64, group_limit * 8)
