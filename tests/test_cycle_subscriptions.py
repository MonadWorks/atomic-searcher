"""Spend subscriptions on supported cycles without losing discovery coverage."""
from types import SimpleNamespace
import threading
import time

import pytest
from searcher.activity import CycleSubscriptions
from searcher.core import Reject
from searcher.universe import CyclePages
from searcher.cycle_groups import CycleGroups
from test_coactivity import address, pool


def saved(pools): return {'known_pools':[p.json() for p in pools], 'pools':[]}


def test_prepared_index_publishes_with_topology_without_temporary_subscription_expansion():
    p=[pool(101,1,2),pool(102,1,2),pool(103,1,3)]
    first=saved(p);first['cycle_groups']=CycleGroups(p,[address(1)],4)
    s=CycleSubscriptions(first)
    assert s.snapshot()=={address(101),address(102)}
    for _ in range(3):
        next_topology=saved(p);next_topology['cycle_groups']=first['cycle_groups']
        s.replace(next_topology)
        assert s.snapshot()=={address(101),address(102)}
        assert s.details()['subscription_scope']=='executable_cycle_members'
    legacy=saved(p);legacy['cycle_groups']=[[address(101),address(102)]]
    s.replace(legacy)
    assert s.snapshot()=={address(101),address(102),address(103)}


def test_full_cycle_index_keeps_unselected_triangles_and_supported_four_hops():
    # Pair, triangle, V2-containing quadrilateral, dead end, all-CL quadrilateral.
    p=[pool(101,1,2),pool(102,1,2),pool(103,1,3),pool(104,3,4),pool(105,1,4),
       pool(106,1,5),pool(107,5,6),pool(108,6,7),pool(109,1,7),pool(110,1,8),
       pool(111,1,9),pool(112,9,10),pool(113,10,11),pool(114,1,11)]
    p[6].kind='v2';p[6].fee=3000
    topology=saved(p);subscriptions=CycleSubscriptions(topology)
    assert subscriptions.snapshot()=={v.address for v in p}
    pages=CyclePages(p,[address(1)],4)
    assert not pages.selected_groups  # Coverage comes from full topology, not a read page.
    assert subscriptions.use_index(topology,pages)
    assert subscriptions.snapshot()=={address(i) for i in range(101,110)}
    assert subscriptions.details()['noncycle_subscription_pools_skipped']==5
    assert len(topology['known_pools'])==14


def test_discovery_restores_all_known_pools_until_new_index_and_ignores_old_result():
    p=[pool(101,1,2),pool(102,1,2),pool(103,1,3)]
    first=saved(p);s=CycleSubscriptions(first)
    s.use_index(first,CyclePages(p,[address(1)],4))
    assert address(103) not in s.snapshot()
    p.append(pool(104,1,3));second=saved(p);s.replace(second)
    assert s.snapshot()=={x.address for x in p}
    assert not s.use_index(first,CyclePages(p[:3],[address(1)],4))
    assert s.use_index(second,CyclePages(p,[address(1)],4))
    assert s.snapshot()=={x.address for x in p}
    # Equal-valued copies still represent a new discovery generation.
    s.replace(dict(second))
    assert not s.use_index(second,SimpleNamespace(covered=set()))
    assert s.details()['subscription_scope']=='known_pools_until_indexed'


def test_stale_index_finishing_during_discovery_cannot_remove_new_pool():
    first=saved([pool(101,1,2)]);second=saved([pool(101,1,2),pool(102,1,2)])
    s=CycleSubscriptions(first);entered=threading.Event();resume=threading.Event();result=[]
    class Covered:
        def __iter__(self):
            entered.set();assert resume.wait(2);return iter(())
    thread=threading.Thread(target=lambda:result.append(s.use_index(first,SimpleNamespace(covered=Covered()))))
    thread.start();assert entered.wait(2);s.replace(second);resume.set();thread.join(2)
    assert not thread.is_alive() and result==[False]
    assert s.snapshot()=={address(101),address(102)}


def test_registry_exclusions_and_removed_topology_do_not_return_through_index():
    p=[pool(101,1,2),pool(102,1,2),pool(103,1,3),pool(104,1,3)]
    topology=saved(p);s=CycleSubscriptions(topology)
    assert s.use_index(topology,CyclePages(p,[address(1)],4,excluded_tokens={address(2)}))
    assert s.snapshot()=={address(103),address(104)}
    new=saved(p[:2]);s.replace(new)
    assert not s.use_index(topology,SimpleNamespace(covered={address(103)}))
    assert s.snapshot()=={address(101),address(102)}


def test_missing_index_is_conservative_and_bad_index_or_mutable_copy_cannot_change_filter():
    p=[pool(101,1,2)];topology=saved(p);s=CycleSubscriptions(topology)
    assert not s.use_index(topology,None)
    assert not s.use_index(topology,SimpleNamespace(selected_groups=[]))
    with pytest.raises(Reject,match='unknown pools'):
        s.use_index(topology,SimpleNamespace(covered={address(999)}))
    copy=s.snapshot();copy.clear();assert s.snapshot()=={address(101)}
    assert s.use_index(topology,SimpleNamespace(covered=set()))
    assert s.snapshot()==set()


def test_empty_cycle_set_reports_waiting_without_opening_an_unfiltered_subscription(tmp_path):
    from test_activity import Socket
    from searcher.activity import stream_session, StreamBudget
    stop=threading.Event();socket=Socket(stop);states=[]
    with pytest.raises(InterruptedError):
        stream_session('wss://example.invalid',8453,set,stop,time.monotonic()+2,
                       lambda *args:None,lambda state,**kw:states.append(state),
                       StreamBudget(tmp_path/'budget.json',10000),connect=lambda *a,**kw:socket)
    assert states.count('waiting_for_pools')==1
    assert 'connected' not in states
    assert [c['method'] for c in socket.calls]==['eth_chainId']
