"""Independent counterexamples for domain weighting and bounded page filtering."""
from collections import Counter, defaultdict

from searcher.cycle_groups import CycleGroups, EconomicRotation
from searcher.universe import CyclePages
from test_hot_transfer_filter import address, pool


def layout():
    a,b,c,d,e,f,g,h = [address(i) for i in range(1,9)]
    pools = []
    def leg(x,y,count=1,kind='v3'):
        for _ in range(count):
            item = pool(100+len(pools),x,y)
            item.kind = kind
            pools.append(item)
    leg(a,b,8)                 # 28 variants, one economic two-hop domain.
    leg(a,c,2)                 # One variant, another equal economic domain.
    leg(a,d,2); leg(d,e,3); leg(e,a,2)
    for x,y in [(a,f),(f,g),(g,h),(h,a)]:
        leg(x,y,kind='v2'); leg(x,y,kind='v3')
    return a,pools


def economic_key(group,states):
    # A token cycle is a set of unordered token edges, independent of fee/pool.
    return tuple(sorted({tuple(sorted((states[a].token0,states[a].token1))) for a in group}))


def test_economic_weight_is_per_token_cycle_not_variant_or_first_v2_partition():
    a,pools = layout(); states = {p.address:p for p in pools}
    groups = CycleGroups(pools,[a],4); rotation = EconomicRotation(groups)
    seen = [rotation.next() for _ in range(100)]
    assert Counter(map(len,seen)) == {2:40,3:40,4:20}
    pairs = Counter(economic_key(g,states) for g in seen if len(g)==2)
    # Parallel legs of the triangle/four-cycle also form valid two-hop domains.
    assert len(pairs) == 6 and max(pairs.values())-min(pairs.values()) <= 1
    assert pairs[((address(1),address(2)),)] <= 7, '28 variants cannot acquire 28 scheduling votes'
    four = [g for g in seen if len(g)==4]
    assert len({economic_key(g,states) for g in four}) == 1
    assert all(any(states[x].kind=='v2' for x in g) for g in four)
    expected = {g for g in groups if len(g)==4}
    assert len(expected) == 15 and set(four) == expected


def test_rotation_and_linear_selection_never_drop_a_valid_cartesian_variant():
    a,pools = layout(); groups = CycleGroups(pools,[a],4)
    rotation = EconomicRotation(groups)
    # Derive a sufficient full-coverage horizon from the actual topology.
    # Five turns include at least one turn for each nonempty hop bucket.
    variants = defaultdict(lambda: defaultdict(set))
    states = {item.address:item for item in pools}
    for group in groups:
        variants[len(group)][economic_key(group,states)].add(group)
    rounds = max(len(families)*max(map(len,families.values()))
                 for families in variants.values())
    seen = {rotation.next() for _ in range(5*rounds)}
    assert seen == set(groups)
    pages = CyclePages(pools,[a],4)
    seen = set()
    for _ in range(len(groups)):
        selected = pages.take(4,group_limit=2)
        seen.update(pages.selected_groups)
        assert len(selected) <= 4 and len(pages.selected_groups) <= 2
        assert all(set(g).issubset({p.address for p in selected}) for g in pages.selected_groups)
    assert seen == set(groups), 'dedicated linear slot preserves even large variants'


def test_all_deferred_huge_family_is_bounded_and_feedback_recovery_can_reenter():
    a,b = address(1),address(2)
    pools = [pool(i+100,a,b) for i in range(120)]
    pages = CyclePages(pools,[a],2)
    checked = []
    def deny(group,states): checked.append(group); return False
    assert pages.take(12,group_limit=4,group_allowed=deny) == []
    assert len(set(checked)) == len(checked)
    assert len(checked) <= max(64,4*8)
    assert pages.cursor == 16
    target = pages.groups[0]
    # The previous rejected topology remains reachable after eligibility changes.
    for _ in range(120):
        pages.take(12,group_limit=4,group_allowed=lambda group,states:group==target)
        if target in pages.selected_groups: break
    assert target in pages.selected_groups


def test_refresh_reuses_stable_linear_position_and_never_resurrects_excluded_pool():
    a,pools = layout(); old = CyclePages(pools,[a],4)
    old.take(7,group_limit=4)
    expected = old.groups[old.cursor]
    fresh = CyclePages(pools+[pool(999,a,address(30)),pool(1000,a,address(30))],[a],4)
    fresh.resume(old)
    assert fresh.groups[fresh.cursor] == expected
    assert fresh.economic_rotation.positions == old.economic_rotation.positions
    assert fresh.economic_rotation.positions is not old.economic_rotation.positions
    blocked_token = pools[0].token1
    excluded = CyclePages(pools,[a],4,groups=old.groups,excluded_tokens=[blocked_token])
    excluded.resume(old)
    seen = set()
    for _ in range(len(excluded.groups)):
        seen.update(p.address for p in excluded.take(7,group_limit=4))
    assert seen == excluded.covered
    assert all(blocked_token not in (excluded.pools[p].token0,excluded.pools[p].token1) for p in seen)


def test_required_depth_is_preserved_but_does_not_admit_unrequested_blocked_group():
    a,b,c = address(1),address(2),address(3)
    pools = [pool(11,a,b),pool(12,a,b),pool(13,a,c),pool(14,a,c)]
    pages = CyclePages(pools,[a],2)
    required = {p.address for p in pools[:2]}
    result = pages.take(4,required=required,group_limit=4,
        group_allowed=lambda g,states:not set(g).issubset(required))
    assert {p.address for p in result} == {p.address for p in pools}
    assert pages.selected_groups == [(pools[2].address,pools[3].address)]
    assert pages.selection_summary['quality_deferred_groups'] == 1


def test_empty_topology_and_legacy_groups_have_finite_correct_selection():
    a,pools = layout()
    assert EconomicRotation(CycleGroups([], [a],4)).next() is None
    empty = CyclePages([], [a],4)
    assert empty.take(4,group_limit=2) == []
    groups = list(CycleGroups(pools,[a],4))
    legacy = CyclePages(pools,[a],4,groups=groups)
    seen = set()
    for _ in range(len(groups)):
        legacy.take(4,group_limit=2)
        seen.update(legacy.selected_groups)
    assert seen == set(groups)
