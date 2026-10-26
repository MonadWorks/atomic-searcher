from collections import Counter

from searcher.cycle_groups import CycleGroups, EconomicRotation
from searcher.pools import Pool
from searcher.universe import CyclePages


def a(n): return '0x'+f'{n:040x}'


def pool(n,x,y,kind='v2'):
    return Pool(a(n),a(999999),kind,a(x),a(y),0 if kind=='v2' else 1,3000)


def test_small_cycles_not_weighted_by_hub_cartesian_products():
    ps=[pool(100+i,1,2) for i in range(80)]
    ps += [pool(300,1,3),pool(301,1,3)]
    ps += [pool(400,1,4),pool(401,4,5),pool(402,5,1)]
    ps += [pool(500,1,6),pool(501,6,7),pool(502,7,8),pool(503,8,1)]
    groups=CycleGroups(ps,[a(1)],4); rot=EconomicRotation(groups)
    observed=[rot.next() for _ in range(50)]
    assert Counter(map(len,observed))=={2:20,3:20,4:10}
    assert observed.count((a(300),a(301)))==10
    assert len({g for g in observed if a(100) in g})>1
    assert all(g in groups for g in observed)


def test_four_hop_partitions_are_one_economic_cycle_and_all_variants_rotate():
    ps=[]
    for x,y in ((1,2),(2,3),(3,4),(4,1)):
        for kind in ('v2','v3'):
            ps.append(pool(100+len(ps),x,y,kind))
    groups=CycleGroups(ps,[a(1)],4)
    spans=groups.economic_spans[4]
    assert len(spans)==2 and spans[1]-spans[0]==4
    expected={g for g in groups if len(g)==4}
    assert len(expected)==15
    rot=EconomicRotation(groups); actual=[]
    while len(actual)<len(expected):
        g=rot.next()
        if len(g)==4:actual.append(g)
    assert len(set(actual))==len(expected) and set(actual)==expected


def test_empty_state_metadata_does_not_become_permanent_exclusion():
    ps=[pool(100,1,2,'v3'),pool(101,1,2,'v3')]
    pages=CyclePages(ps,[a(1)],3)
    assert all(p.liquidity==0 for p in ps)
    assert {p.address for p in pages.take(4)}=={p.address for p in ps}


def test_all_deferred_has_bounded_work_and_later_recovers_without_new_topology():
    ps=[pool(100+i,1,2) for i in range(100)]
    pages=CyclePages(ps,[a(1)],3)
    calls=[]
    assert pages.take(12,group_limit=4,group_allowed=lambda g,p:(calls.append(g) or False))==[]
    assert len(calls)<=2*64+len(pages.hot[:2])
    assert pages.selection_summary['quality_deferred_groups']==len(calls)
    assert pages.take(12,group_limit=4,group_allowed=lambda g,p:True)
    assert pages.selection_summary['quality_deferred_groups']==0


def test_linear_coverage_and_required_pool_bounds_survive_rotation():
    ps=[pool(100+i,1,2) for i in range(5)]+[pool(200,1,3),pool(201,3,4),pool(202,4,1)]
    pages=CyclePages(ps,[a(1)],4)
    seen=set()
    for _ in range(len(pages.groups)):
        selected=pages.take(4,required=[a(200)],group_limit=2)
        assert a(200) in {p.address for p in selected}
        assert len(selected)<=4 and len(pages.selected_groups)<=2
        seen.update(pages.selected_groups)
    assert seen==set(pages.groups)


def test_refresh_preserves_economic_progress_and_does_not_share_mutable_cursors():
    ps=[pool(100+i,1,2) for i in range(4)]
    old=CyclePages(ps,[a(1)],3)
    old.economic_rotation.next();old.economic_rotation.next()
    new=CyclePages(ps+[pool(200,1,3),pool(201,1,3)],[a(1)],3)
    new.resume(old)
    assert new.economic_rotation.positions==old.economic_rotation.positions
    assert new.economic_rotation.positions is not old.economic_rotation.positions
    new.economic_rotation.next()
    assert new.economic_rotation.positions!=old.economic_rotation.positions


def test_activity_rejection_fanout_obeys_shared_work_budget(monkeypatch):
    ps=[pool(100+i,1,2) for i in range(100)]
    pages=CyclePages(ps,[a(1)],3)
    monkeypatch.setattr('searcher.universe.time.monotonic',lambda:100)
    for g in pages.groups[:4096]:pages.priority[g]=100
    calls=[]
    pages.take(12,group_limit=4,group_allowed=lambda g,p:(calls.append(g) or False))
    assert len(calls)<=pages.selection_summary['work_limit']==64
    assert pages.priority, 'uninspected event tasks must not be discarded as incompatible'
