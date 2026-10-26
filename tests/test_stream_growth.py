import json
import threading
import time

import pytest

from searcher.activity import StreamBudget, stream_session, run_stream
from searcher.core import Reject
from searcher.stream_usage import SubscriptionRoster
from test_activity import Socket


def test_current_universe_exceeding_old_limit_subscribes_instead_of_reconnect_loop(tmp_path):
    pools = {'0x'+f'{n:040x}' for n in range(1,8501)}
    stop = threading.Event()
    socket = Socket(stop)
    states = []
    with pytest.raises(InterruptedError):
        stream_session('wss://example.invalid',8453,lambda:pools,stop,time.monotonic()+10,
            lambda *a:None,lambda state,**detail:states.append(dict(state=state,**detail)),
            StreamBudget(tmp_path/'budget.json',1_000_000),connect=lambda *a,**kw:socket)
    connected = [s for s in states if s['state']=='connected']
    assert connected and connected[-1]['filtered_pools']==8192
    assert connected[-1]['capacity_deferred_pools']==308
    subscriptions=[c['params'][1]['address'] for c in socket.calls if c['method']=='eth_subscribe']
    assert len(subscriptions)==64 and all(len(s)<=128 for s in subscriptions)
    assert len({p for group in subscriptions for p in group})==8192
    assert {c['method'] for c in socket.calls}<={'eth_chainId','eth_subscribe'}


def test_overflow_rotates_without_starvation_and_unaffected_filters_stay_stable():
    clock=[0]
    roster=SubscriptionRoster(capacity=8,batch_size=2,clock=lambda:clock[0])
    pools=set(range(11))
    first=roster.select(pools);seen=set(first)
    assert len(first)==8
    clock[0]=29;assert roster.select(pools)==first
    for when in [30,60,90]:
        clock[0]=when
        selected=roster.select(pools);seen.update(selected)
        assert len(first-selected)<=2 and len(selected)==8
        assert not set(roster.waiting)&selected
        first=selected
    assert seen==pools
    remaining={9,10,11}
    assert roster.select(remaining)==remaining and not roster.waiting


def test_roster_survives_reconnect_and_local_reason_excludes_provider_secrets(tmp_path,monkeypatch):
    import searcher.activity as module
    stop=threading.Event();rosters=[];states=[]
    class FastStop:
        def is_set(self):return stop.is_set()
        def wait(self,n):return stop.is_set()
    def fake(*args,**kw):
        rosters.append(kw['roster'])
        if len(rosters)==1:raise Reject('event provider rejected request')
        if len(rosters)==2:raise RuntimeError('wss://provider/secret-key')
        stop.set()
    monkeypatch.setattr(module,'stream_session',fake)
    run_stream('wss://example.invalid',8453,set,FastStop(),time.monotonic()+2,
        lambda *a:None,lambda state,**detail:states.append(dict(state=state,**detail)),
        StreamBudget(tmp_path/'budget.json',10000))
    assert len(rosters)==3 and len({id(x) for x in rosters})==1
    assert [s['reason'] for s in states if s['state']=='retrying']==['provider_rejected','transport_error']
    assert 'secret' not in json.dumps(states)
