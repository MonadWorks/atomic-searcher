import copy
import json
import queue
import sqlite3
import threading
import time
from types import SimpleNamespace

import pytest

from searcher.activity import ActivityInbox
from searcher.core import load_config
from searcher.fastlane import FrameSequence, HotRoutes, LatestFrames, run_hot_reader
from searcher.recorder import Recorder
from searcher.statefeed import StateFrame
from test_pipeline import saved, A, B, C, header, H0, H1
from test_runtime import wait_until, FakeRPC


def config():
    c = load_config()
    c['settlement_tokens'] = [{'address':A}]
    c['policy'].update(hot_lane_enabled=True, hot_lane_min_interval_seconds=.2,
                       activity_priority=True, universe_refresh_seconds=100)
    return c


def test_single_changed_pool_arms_quiet_counterpart_and_rejects_unknown_events():
    selector = HotRoutes(saved(), config(), clock=lambda:10)
    selector.add([(A,10),('unverified',10)])
    selected = selector.take()
    assert selected['groups'] == [(A,B)]
    assert {p.address for p in selected['pools']} == {A,B}
    assert selected['trigger_pools'] == [A]
    assert all(p.block == 1 for p in selected['pools'])  # metadata, not a new price


def test_hot_selection_is_bounded_rotates_variants_and_expires():
    stamp = [10]
    universe = saved(); template = universe['pools'][0]
    universe['pools'] = [dict(template,address='0x'+f'{i:040x}') for i in range(1,31)]
    selector = HotRoutes(universe, config(), clock=lambda:stamp[0])
    selector.add([(A,10)])
    partners=set()
    for _ in range(29):
        result=selector.take()
        assert len(result['groups'])<=4 and len(result['pools'])<=12
        partners.update(p.address for p in result['pools'] if p.address != A)
    assert len(partners)==29
    stamp[0]=12;assert selector.take()['groups']==[]


def test_latest_frames_keep_separate_lanes_and_bound_hot_burst():
    frames=LatestFrames()
    frames.publish('ordinary','old');frames.publish('ordinary','new')
    frames.publish('hot','h1');assert frames.get(.01)==('hot','h1')
    frames.publish('hot','h2');assert frames.get(.01)==('hot','h2')
    frames.publish('hot','h3');assert frames.get(.01)==('ordinary','new')
    assert frames.get(.01)==('hot','h3')
    with pytest.raises(queue.Empty):frames.get(.001)


def test_readers_have_unique_numeric_ids_in_independent_databases(tmp_path):
    sequence=FrameSequence(10)
    ordinary=Recorder(tmp_path/'first-seen.sqlite', id_source=sequence.next)
    hot=Recorder(tmp_path/'hot-first-seen.sqlite', id_source=sequence.next)
    try:
        ids=[ordinary.append(1,2,'ordinary',{}),hot.append(1,2,'hot',{}),ordinary.append(1,2,'ordinary',{})]
        assert ids==[11,12,13]
        assert ordinary.db.execute('SELECT id FROM frames ORDER BY id').fetchall()==[(11,),(13,)]
        assert hot.db.execute('SELECT id FROM frames').fetchall()==[(12,)]
    finally:ordinary.close();hot.close()


@pytest.mark.parametrize('completion_order', ['older_first','newer_first'])
def test_event_during_inflight_read_survives_and_all_pools_are_reread(tmp_path,completion_order):
    from contextlib import closing
    from searcher.recorder import decode_payload
    stop=threading.Event();entered=threading.Event();new_entered=threading.Event()
    release_old=threading.Event();release_new=threading.Event()
    inbox=ActivityInbox();frames=[];calls=[];states=[]
    class Feed:
        def __init__(self,rpc,c,universe):
            assert not c['policy']['cycle_pages']
            assert c['policy']['snapshot_ticks_per_side']==16
            from searcher.pools import Pool
            self.pools=[Pool.from_json(p) for p in universe['pools']]
        def poll(self):
            started=time.monotonic_ns();calls.append({p.address for p in self.pools})
            if len(calls)==1:
                entered.set();assert release_old.wait(4)
            else:
                new_entered.set();assert release_new.wait(4)
            now=time.monotonic_ns()
            return StateFrame(header(2,H1,H0), header(2,H1,H0),
                              [p.json() for p in self.pools],started,now,time.time(),3,106)
    inbox.publish([A],time.monotonic())
    thread=threading.Thread(target=run_hot_reader,kwargs=dict(rpc_factory=FakeRPC,config=config(),
        universe=lambda:(0,saved()),inbox=inbox,stop=stop,finish=time.monotonic()+5,
        directory=tmp_path,sequence=FrameSequence(),publish=lambda lane,x:frames.append(x),
        health=lambda *a,**kw:states.append(a[0]),feed_factory=Feed))
    def recorded():
        with closing(sqlite3.connect(f'file:{tmp_path}/hot-first-seen.sqlite?mode=ro',uri=True)) as db:
            return [decode_payload(raw)['transport'] for raw, in
                    db.execute('SELECT payload FROM frames ORDER BY id')]
    thread.start()
    try:
        assert entered.wait(1)
        # A newer generation arrives while the old generation is being read.
        new_event_at=time.monotonic();inbox.publish([B],new_event_at)
        assert new_entered.wait(1)
        if completion_order=='older_first':
            release_old.set()
            wait_until(lambda:any(f.transport['dispatch_id']==1 for _,f in frames))
            release_new.set()
        else:
            release_new.set()
            wait_until(lambda:any(B in f.transport['trigger_pools'] for _,f in frames))
            release_old.set()
        wait_until(lambda:any(B in f.transport['trigger_pools'] for _,f in frames))
        wait_until(lambda:len(recorded())>=2)
        assert len(calls)==2 and all(x=={A,B} for x in calls)
        newer=next(f for _,f in frames if B in f.transport['trigger_pools'])
        assert newer.transport['dispatch_id']==2
        assert newer.transport['group_event_versions'][0]['triggers'][B]==new_event_at
        assert all(f.transport['route_groups']==[(A,B)] for _,f in frames)
        published=[f.transport['dispatch_id'] for _,f in frames]
        assert published==([1,2] if completion_order=='older_first' else [2])
        assert {r['dispatch_id']:r['hot_read_disposition'] for r in recorded()}=={
            1:'published' if completion_order=='older_first' else 'out_of_order',2:'published'}
    finally:release_old.set();release_new.set();stop.set();thread.join(2)
    assert not thread.is_alive() and 'connected' in states


def test_hot_frame_is_consumed_while_ordinary_rpc_remains_blocked(tmp_path,monkeypatch):
    import searcher.pipeline as module
    import searcher.activity as activity
    c=config();stop=threading.Event();ordinary_entered=threading.Event();ordinary_release=threading.Event()
    decision_written=threading.Event()
    original_atomic_json=module.atomic_json
    def record_completed_decision(path,report):
        original_atomic_json(path,report)
        if path.name.startswith('decision-') and report.get('lane')=='hot':
            decision_written.set()
    monkeypatch.setattr(module,'atomic_json',record_completed_decision)
    seen=[]
    class Feed:
        def __init__(self,rpc,c,universe):
            from searcher.pools import Pool
            self.hot=not c['policy'].get('cycle_pages')
            self.pools=[Pool.from_json(p) for p in universe['pools']]
        def poll(self):
            started=time.monotonic_ns()
            if not self.hot:
                ordinary_entered.set();ordinary_release.wait(3)
            now=time.monotonic_ns()
            return StateFrame(header(2,H1,H0),header(2,H1,H0),[p.json() for p in self.pools],started,now,time.time(),3,106)
    class Strategy:
        def __init__(self,*args):
            from searcher.route_feedback import RouteFeedback
            self.execution_feedback=RouteFeedback();self.sizing_progress={}
        def on_frame(self,frame):
            seen.append(frame.transport.get('lane','ordinary'))
            return {'block':2,'decision_age_ms':0,'candidates':[]}
    def stream(url,chain,pools,stop,finish,publish,health,budget,**kw):
        assert ordinary_entered.wait(1)
        from test_activity import log
        publish([log(A)],'pendingLogs')
        stop.wait(3)
    monkeypatch.setattr(module,'RPC',FakeRPC)
    monkeypatch.setattr(module,'StateFeed',Feed)
    monkeypatch.setattr(module,'PendingStrategy',Strategy)
    monkeypatch.setattr(activity,'websocket_url',lambda:'wss://example.invalid/key')
    monkeypatch.setattr(activity,'run_stream',stream)
    monkeypatch.setattr(activity,'ROOT',tmp_path)
    # Keep fake HTTP fallback quiet; the test event comes only from the stream.
    monkeypatch.setattr(FakeRPC,"request",lambda *args:[], raising=False)
    result=[]
    thread=threading.Thread(target=lambda:result.append(module.run_pending(FakeRPC(),c,saved(),3,tmp_path,stop_event=stop)))
    thread.start()
    try:
        wait_until(lambda:'hot' in seen)
        assert decision_written.wait(2), 'hot decision must be committed before its file is inspected'
        assert 'ordinary' not in seen and not ordinary_release.is_set()
        reports=[json.loads(p.read_text()) for p in tmp_path.glob('decision-*.json')]
        assert reports and reports[0]['lane']=='hot'
        assert reports[0]['recording_database']=='hot-first-seen.sqlite'
    finally:ordinary_release.set();stop.set();thread.join(4)
    assert not thread.is_alive() and result[0]['hot_reader_stopped'] and result[0]['reader_stopped']


@pytest.mark.parametrize('key,value', [('hot_lane_max_pools',4),('hot_lane_max_groups',2)])
def test_reject_capacity_that_cannot_finish_one_changed_pools_short_routes(key,value):
    from searcher.core import Reject
    c=config();c['policy'][key]=value
    with pytest.raises(Reject,match='bounds'):HotRoutes(saved(),c)


def test_optional_hot_endpoint_is_local_and_validated(tmp_path,monkeypatch):
    from searcher import core
    from searcher.fastlane import endpoint_url
    monkeypatch.setattr(core,'ROOT',tmp_path)
    monkeypatch.delenv('BASE_HOT_RPC_URL',raising=False)
    assert endpoint_url() is None
    (tmp_path/'config').mkdir()
    (tmp_path/'config/node.local.json').write_text(json.dumps({'hot_rpc_url':'https://example.invalid/private'}))
    assert endpoint_url()=='https://example.invalid/private'
    monkeypatch.setenv('BASE_HOT_RPC_URL','https://another.invalid/key')
    assert endpoint_url()=='https://another.invalid/key'
    monkeypatch.setenv('BASE_HOT_RPC_URL','http://example.invalid/key')
    with pytest.raises(core.Reject,match='HTTPS'):endpoint_url()


def test_hot_primary_connection_failures_fall_back_with_fresh_feed(tmp_path):
    from searcher.core import RPCUnavailable
    stop=threading.Event();inbox=ActivityInbox();sources=[];health=[]
    c=config();c['policy']['hot_lane_event_ttl_seconds']=5
    inbox.publish([A],time.monotonic())
    class Primary(FakeRPC):
        def verify_chain(self):raise RPCUnavailable('temporary network failure')
        def close(self):sources.append('primary-closed')
    class Fallback(FakeRPC):
        def verify_chain(self):sources.append('fallback-verified')
    class Feed:
        def __init__(self,rpc,c,universe):
            assert isinstance(rpc,Fallback)
            from searcher.pools import Pool
            self.pools=[Pool.from_json(p) for p in universe['pools']]
        def poll(self):
            now=time.monotonic_ns()
            return StateFrame(header(2,H1,H0),header(2,H1,H0),[p.json() for p in self.pools],now,now,time.time(),3,106)
    frames=[]
    def publish(lane,frame):frames.append(frame);stop.set()
    run_hot_reader(Primary,c,lambda:(0,saved()),inbox,stop,time.monotonic()+4.5,
        tmp_path,FrameSequence(),publish,lambda state,**detail:health.append((state,detail)),
        feed_factory=Feed,fallback_factory=Fallback)
    assert sources==['primary-closed','fallback-verified'] and frames
    assert any(d.get('channel')=='fallback' for _,d in health)
