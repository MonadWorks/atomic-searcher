"""Bounded scheduler service under continuous unrelated arrivals. No RPC."""
from searcher.statefeed import DeepReadScheduler


def group(index):
    return tuple('0x'+f'{10000+index*2+i:040x}' for i in range(2))


def test_warm_fifo_and_old_continuations_progress_under_sustained_new_depth():
    scheduler = DeepReadScheduler()
    old = [{'pools':group(i),'since':0,'deep':True,'route_keys':[f'old-{i}']}
           for i in range(1024)]
    assert scheduler.resume(old,0) == []
    warm = [dict(pools=group(2000+i),prewarm_lease_id=f'lease-{i}',
                 route_keys=[f'warm-{i}'],deep=False) for i in range(10)]
    assert set(scheduler.add(warm,0)['accepted']) == {f'lease-{i}' for i in range(10)}
    served, old_served = set(), set()
    available = {pool for row in old+warm for pool in row['pools']}
    for turn in range(10):
        arrivals = [dict(pools=group(4000+turn*128+i),score=10**9) for i in range(128)]
        available.update(pool for row in arrivals for pool in row['pools'])
        scheduler.add(arrivals,turn+1)
        scheduler.take(available,turn+1)
        selected = scheduler.selected_requests
        assert len(selected) <= 12 and len({p for row in selected for p in row['pools']}) <= 24
        warmed = [row for row in selected if scheduler.lease_ids(row)]
        assert len(warmed) == 1
        served.update(scheduler.lease_ids(warmed[0]))
        old_served.update(tuple(row['pools']) for row in selected if not scheduler.lease_ids(row))
        # Nothing completes: all old return obligations remain admitted, while
        # independent reservations rotate instead of starving behind them.
        assert scheduler.resume(selected,turn+1) == []
        assert len(scheduler.resuming) == 1024
        assert len(scheduler.prewarm) == 10
    assert served == {f'lease-{i}' for i in range(10)}
    assert old_served


def test_escalated_warm_depth_survives_merge_with_existing_shallow_request():
    scheduler = DeepReadScheduler()
    pools = group(1)
    scheduler.add([dict(pools=pools,deep=False,route_keys=['ordinary'])],0)
    scheduler.add([dict(pools=pools,deep=True,route_keys=['warm'],prewarm_lease_id='lease')],1)
    assert scheduler.take(set(pools),1) == set(pools)
    row, = scheduler.selected_requests
    assert row['deep'] and scheduler.selected_deep_pools == set(pools)
    assert set(row['route_keys']) == {'ordinary','warm'}
    assert scheduler.lease_ids(row) == {'lease'}
    assert scheduler.resume([row],2) == []
    scheduler.take(set(pools),2)
    assert scheduler.selected_deep_pools == set(pools)
    assert scheduler.lease_ids(scheduler.selected_requests[0]) == {'lease'}
