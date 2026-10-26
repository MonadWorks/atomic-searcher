"""Budget trajectories and real socket lifecycle decisions, without a network."""
import copy
import json
import threading
import time
from pathlib import Path

import pytest

from searcher.activity import stream_session, run_stream
from searcher.stream_usage import RequestBudget, StreamSelection, SubscriptionRoster, StreamPacingPause
from test_activity import Socket, log
from test_searcher import A,B,C


DAY = 1791936000.0


def test_credit_and_all_message_kinds_persist_without_explicit_save(tmp_path):
    now=[DAY];p=tmp_path/'usage.json'
    b=RequestBudget(p,80_000,lambda:now[0])
    for kind in ('control','notification','invalid'):b.add(100,kind)
    copied=json.loads(p.read_text())
    assert copied['units']==3 and copied['pace_credit']==125
    restarted=RequestBudget(p,80_000,lambda:now[0])
    assert restarted.state==b.state and restarted.details()['message_units']=={'control':1,'notification':1,'invalid':1}
    now[0]-=1
    assert restarted.details()['pacing_credit']==125


def test_legacy_migration_grants_one_burst_and_keeps_spent_daily_units(tmp_path):
    p=tmp_path/'usage.json';p.write_text(json.dumps({'day':'2026-10-14','units':70000,'bytes':123}))
    b=RequestBudget(p,80_000,lambda:DAY+100)
    for _ in range(20):b.add(1,'control')
    restarted=RequestBudget(p,80_000,lambda:DAY+100)
    assert restarted.state['units']==70020 and restarted.state['pace_credit']==108


def test_pause_requires_full_batch_of_credit_instead_of_one_reply_reconnect(tmp_path):
    now=[DAY];b=RequestBudget(tmp_path/'usage.json',80_000,lambda:now[0])
    for _ in range(128):b.add(1,'control')
    delay=b.pacing_delay()
    assert delay>100
    now[0]+=2
    assert b.pacing_delay()>100
    b=RequestBudget(b.path,80_000,lambda:now[0])
    assert b.pacing_delay()>100 and b.state['units']==128
    now[0]+=160
    assert b.pacing_delay()==0 and b.state['units']==128


def test_small_remaining_quota_reduces_filter_rebuild_and_slows_fifo_rotation(tmp_path):
    p=tmp_path/'usage.json';p.write_text(json.dumps({'day':'2026-10-14','units':79500,'bytes':1}))
    b=RequestBudget(p,80_000,lambda:DAY)
    assert b.filter_capacity()==3 and b.rotation_seconds()>1000
    now=[0];roster=SubscriptionRoster(capacity=1024,clock=lambda:now[0])
    old=roster.select(set(range(1500)))
    roster.capacity=b.filter_capacity()*128;roster.rotation_seconds=b.rotation_seconds()
    kept=roster.select(set(range(1500)))
    assert len(kept)==384 and kept<=old and len(roster.waiting)==1116
    seen=set(kept)
    for _ in range(15):
        now[0]+=roster.rotation_seconds
        seen.update(roster.select(set(range(1500))))
    assert seen==set(range(1500))


def test_unadmitted_probe_has_no_lease_and_cannot_graduate_from_roster_delay():
    now=[0.];s=StreamSelection(80_000,clock=lambda:now[0])
    s.select({A,B});s.subscribed({A,B})
    for _ in range(4):s.observe(A)
    assert A not in s.select({A,B});s.unsubscribed({A})
    now[0]=121
    assert A in s.select({A,B})
    now[0]=160
    assert A in s.select({A,B}) and A in s.cool_until
    assert s.probes_started==0
    s.subscribed({A});assert s.probes_started==1
    now[0]=169;assert A in s.cool_until and A in s.select({A,B})
    now[0]=170;assert A in s.select({A,B}) and A not in s.cool_until
    assert A in s.history and A not in s.individual_pools(), 'Quiet completion may batch again without erasing prior failures.'


def test_persistent_hot_probe_never_forgets_history_and_one_at_time_fairness():
    now=[0.];s=StreamSelection(80_000,clock=lambda:now[0])
    s.select({A,B,C});s.subscribed({A,B,C})
    for pool in (A,B):
        for _ in range(4):s.observe(pool)
    assert s.select({A,B,C})=={C};s.unsubscribed({A,B})
    for expected,when in ((A,121),(B,152)):
        now[0]=when
        assert s.select({A,B,C})=={C,expected}
        s.subscribed({expected})
        for _ in range(4):s.observe(expected)
        assert s.select({A,B,C})=={C}
        s.unsubscribed({expected})
        assert s.history[expected]['strikes']==2
    assert s.probes_started==s.probes_limited==2


def test_all_day_twenty_notifications_per_second_probe_counterexample_is_bounded(tmp_path):
    # Isolate pure controller policy from filesystem benchmark; durability is
    # covered separately and this trajectory still charges every control reply.
    now=[0.];b=RequestBudget(tmp_path/'usage.json',80_000,lambda:DAY+now[0]);b.save=lambda:None
    s=StreamSelection(80_000,clock=lambda:now[0],budget=b)
    subscribed=set();controls=0;probe_bursts=[];burst=0
    for tick in range(86400*20):
        now[0]=tick/20
        desired=s.select({A}) if tick%20==0 or s.changed else set(s.selected)
        if desired!=subscribed:
            for pool in subscribed-desired:
                b.add(1,'control');controls+=1;s.unsubscribed({pool})
                probe_bursts.append(burst);burst=0
            for pool in desired-subscribed:
                b.add(1,'control');controls+=1;s.subscribed({pool})
            subscribed=desired
        if A in subscribed:
            b.add(1,'notification');s.observe(A);burst+=1
        assert not b.remaining_seconds()
    assert max(probe_bursts)<=4
    assert s.probes_started>10 and len(s.history)==1
    assert b.state['units']<2000 and b.state['kinds']['control']==controls


def test_high_notification_probe_is_really_unsubscribed_after_message_cap(tmp_path):
    now=[0.];stop=threading.Event();s=StreamSelection(80_000,clock=lambda:now[0])
    class HotSocket(Socket):
        closed=False
        def send(self,raw):
            obj=json.loads(raw);self.calls.append(obj)
            if obj['method']=='eth_chainId':response={'id':obj['id'],'result':'0x2105'}
            elif obj['method']=='eth_subscribe':
                response={'id':obj['id'],'result':'hot'}
                self.messages.append(json.dumps(response))
                self.messages.extend(json.dumps({'method':'eth_subscription','params':{'subscription':'hot','result':log(A)}}) for _ in range(4))
                return
            else:
                response={'id':obj['id'],'result':True};stop.set()
            self.messages.append(json.dumps(response))
        def __exit__(self,*args):self.closed=True
    socket=HotSocket(stop);b=RequestBudget(tmp_path/'usage.json',80_000,lambda:DAY)
    events=[]
    with pytest.raises(InterruptedError):
        stream_session('wss://example.invalid',8453,lambda:{A},stop,time.monotonic()+2,
                       lambda logs,source:events.extend(logs),lambda *a,**k:None,b,
                       connect=lambda *a,**k:socket,selector=s)
    assert [c['method'] for c in socket.calls]==['eth_chainId','eth_subscribe','eth_unsubscribe']
    assert len(events)==4 and b.state['units']==6 and socket.closed


def test_messages_flooding_unsubscribe_force_physical_close_and_all_are_charged(tmp_path):
    stop=threading.Event();s=StreamSelection(80_000)
    class FloodSocket(Socket):
        closed=False
        def send(self,raw):
            obj=json.loads(raw);self.calls.append(obj)
            if obj['method']=='eth_chainId':self.messages.append(json.dumps({'id':obj['id'],'result':'0x2105'}))
            elif obj['method']=='eth_subscribe':
                self.messages.append(json.dumps({'id':obj['id'],'result':'hot'}))
                for _ in range(4):self.messages.append(json.dumps({'method':'eth_subscription','params':{'subscription':'hot','result':log(A)}}))
            else:
                for _ in range(1000):self.messages.append(json.dumps({'method':'eth_subscription','params':{'subscription':'hot','result':log(A)}}))
        def __exit__(self,*args):self.closed=True
    socket=FloodSocket(stop);b=RequestBudget(tmp_path/'usage.json',80_000,lambda:DAY)
    events=[]
    with pytest.raises(StreamPacingPause):
        stream_session('wss://example.invalid',8453,lambda:{A},stop,time.monotonic()+2,
                       lambda logs,source:events.extend(logs),lambda *a,**k:None,b,
                       connect=lambda *a,**k:socket,selector=s)
    assert socket.closed and len(events)==126 and b.state['units']==128
    assert b.state['kinds']=={'control':2,'notification':126}
    assert b.pacing_delay()>100


def test_actual_filter_weight_preserves_fifo_when_all_history_is_isolated():
    now=[0.];roster=SubscriptionRoster(capacity=64*128,clock=lambda:now[0])
    pools=set(range(140));seen=set()
    for _ in range(90):
        chosen=roster.select(pools,individual=pools)
        assert len(chosen)<=64
        seen.update(chosen)
        now[0]+=30
    assert seen==pools and len(roster.waiting)==76


def test_unconfirmed_filter_tail_returns_to_fifo_without_claiming_admission():
    now=[0.];roster=SubscriptionRoster(capacity=4,batch_size=1,clock=lambda:now[0])
    assert roster.select(set(range(8)))==set(range(4))
    roster.defer({2,3})
    assert set(roster.selected)=={0,1} and list(roster.waiting)[:2]==[2,3]
    assert roster.select(set(range(8)))==set(range(4))
    now[0]=30
    assert roster.select(set(range(8)))=={1,2,3,4}


def test_partial_startup_reconnect_prioritizes_unconfirmed_pools_without_rotating_them_away():
    now=[0.];r=SubscriptionRoster(capacity=4,batch_size=1,clock=lambda:now[0],confirm_admissions=True)
    r.select(set(range(8)))
    r.confirmed([0,1])
    assert list(r.selected)==[2,3,0,1]
    now[0]=40
    r.select(set(range(8)))
    assert list(r.selected)[:2]==[2,3]
    assert 0 in r.waiting and 4 in r.selected


def test_orphaned_cooldown_is_repaired_without_excluding_pool_forever():
    now=[200.];s=StreamSelection(80_000,clock=lambda:now[0])
    # A legacy/inconsistent in-memory orphan is never a permanent exclusion or
    # an indexing failure. It must return to ordinary budgeted observation.
    s.cool_until[A]=100.;s.probe_candidate=A
    s.live[A]={'since':190.,'messages':0,'probe':True}
    s.disconnected()
    assert s.select({A,B})=={A,B}
    assert not s.cool_until and s.probe_candidate is None


def test_removed_live_probe_does_not_resurrect_history_on_disconnect():
    now=[0.];s=StreamSelection(80_000,clock=lambda:now[0])
    s.select({A,B});s.subscribed({A})
    for _ in range(4):s.observe(A)
    s.unsubscribed({A})
    now[0]=121.;s.select({A,B});s.subscribed({A})
    assert s.live[A]['probe'] and A in s.history
    s.select({B})
    assert not s.live[A]['probe'] and A not in s.history
    s.disconnected()
    assert A not in s.cool_until
    assert A in s.select({A,B}), 'Reintroduced topology remains eligible for new observation.'
