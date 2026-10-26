import itertools
import random
from collections import defaultdict

from searcher.cycle_groups import CycleGroups
from searcher.universe import CyclePages
from searcher.pools import Pool


def address(n): return '0x'+f'{n:040x}'


def market(seed):
    rng = random.Random(seed)
    pools = []
    for a,b in itertools.combinations(range(1,7),2):
        for _ in range(rng.randrange(3)):
            pools.append(Pool(address(100+len(pools)), address(90), rng.choice(['v2','v3']),
                              address(a), address(b), 1, 3000))
    return pools


def brute_groups(pools, settlements, hops):
    # Independently enumerate pool combinations and verify degree/connectedness.
    expected = set()
    for n in range(2,hops+1):
        for selected in itertools.combinations(pools,n):
            if n == 4 and all(p.kind != 'v2' for p in selected): continue
            links = defaultdict(list)
            for p in selected:
                links[p.token0].append(p.token1); links[p.token1].append(p.token0)
            if len(links) != n or not set(links).intersection(settlements): continue
            if any(len(v) != 2 for v in links.values()): continue
            seen, stack = set(), [next(iter(links))]
            while stack:
                v = stack.pop()
                if v not in seen: seen.add(v); stack.extend(links[v])
            if len(seen) == n: expected.add(tuple(sorted(p.address for p in selected)))
    return expected


def test_products_match_independent_connected_cycle_enumeration():
    for seed in range(12):
        pools = market(seed)
        for hops in (2,3,4):
            for settlements in ([address(1)], [address(1),address(3)]):
                groups = CycleGroups(pools, settlements, hops)
                expected = brute_groups(pools, settlements, hops)
                assert len(groups) == len(expected)
                assert set(groups) == expected
                for i,g in enumerate(groups): assert groups.index(g) == i
                for p in pools:
                    containing = groups.containing(p.address)
                    assert len(containing) == len(set(containing))
                    assert set(containing) == {g for g in expected if p.address in g}


def test_millions_of_combinations_remain_small_and_event_indexed():
    pools=[]
    for a,b in [(1,2),(2,3),(3,4),(4,1)]:
        for _ in range(55):
            pools.append(Pool(address(100+len(pools)), address(90), 'v2', address(a),address(b),0,3000))
    pages=CyclePages(pools,[address(1)],4)
    assert len(pages.groups) > 9_000_000
    assert len(pages.groups.families) < 10
    assert len(pages.hot) <= 128
    last=pages.groups[-1]
    pages.prioritize([last[0]])
    pages.take(24,group_limit=16)
    assert pages.selected_priority
    assert all(set(g).issubset(pages.pools) for g in pages.selected_groups)
    pages.cursor=len(pages.groups)-1
    replacement=CyclePages(pools,[address(1)],4)
    replacement.resume(pages)
    assert replacement.groups[replacement.cursor]==last


def test_new_pool_refresh_preserves_the_next_complete_route():
    pools=market(8); pages=CyclePages(pools,[address(1)],4)
    pages.cursor=len(pages.groups)//2
    wanted=pages.groups[pages.cursor]
    extra=Pool(address(999),address(90),'v2',address(1),address(2),0,3000)
    new=CyclePages(pools+[extra],[address(1)],4);new.resume(pages)
    assert new.groups[new.cursor]==wanted
