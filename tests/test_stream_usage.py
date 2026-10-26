import json
import threading
import time
from pathlib import Path

import pytest

from searcher.stream_usage import RequestBudget, StreamSelection
from searcher.activity import stream_session
from searcher.core import Reject, load_config
from test_activity import Socket, log
from test_searcher import A, B, C


def test_request_budget_counts_messages_not_bytes_and_survives_restart(tmp_path):
    now=[1791698907.0];clock=lambda:now[0];path=tmp_path/'requests.json'
    b=RequestBudget(path,3,clock)
    b.add(600_000);b.add(1);b.save()
    b=RequestBudget(path,3,clock)
    assert b.state['units']==2 and b.remaining_seconds()==0
    b.add(20);b.save()
    assert b.remaining_seconds()>0
    assert b.details()['budget_kind']=='local_request_units'
    assert b.details()['account_quota_known'] is False
    now[0]-=86400
    assert RequestBudget(path,3,clock).remaining_seconds()>86400
    now[0]+=86400*2
    b=RequestBudget(path,3,clock)
    assert b.state['units']==0 and b.remaining_seconds()==0


def test_corrupt_usage_cannot_reset_budget_silently(tmp_path):
    p=tmp_path/'requests.json';p.write_text('{"day":"2026-10-11","units":-1,"bytes":0}')
    with pytest.raises(Reject):RequestBudget(p,80_000)


def test_final_allowed_notification_is_delivered_before_budget_pause(tmp_path):
    stop=threading.Event();socket=Socket(stop);received=[]
    # chain ID, subscription response, then one notification.
    b=RequestBudget(tmp_path/'requests.json',3)
    with pytest.raises(Reject,match='budget'):
        stream_session('wss://example.invalid',8453,lambda:{A},stop,time.monotonic()+2,
                       lambda logs,src:received.extend(logs),lambda *a,**kw:None,b,
                       connect=lambda *a,**kw:socket)
    assert [e['address'] for e in received]==[A] and b.state['units']==3


def test_hot_pools_shift_to_http_and_return_for_bounded_probes():
    now=[0.];selector=StreamSelection(80_000,clock=lambda:now[0])
    assert selector.select({A,B,C})=={A,B,C}
    selector.subscribed({A,B,C})
    for _ in range(200):selector.observe(A)
    selector.observe(B)
    now[0]=10
    assert selector.select({A,B,C})=={B,C}
    assert selector.details()['http_only_pools']==1
    selector.unsubscribed({A})
    now[0]=131
    assert A in selector.select({A,B,C})
    selector.subscribed({A})
    # A confirmed re-probe has a strict received-message allowance.
    for _ in range(4):selector.observe(A)
    assert A not in selector.select({A,B,C})
    assert selector.history[A]['strikes']==2
    selector.select({B,C})
    assert A not in selector.cool_until


def test_only_one_hot_pool_is_retried_per_interval():
    now=[0.];s=StreamSelection(80_000,clock=lambda:now[0]);s.select({A,B,C});s.subscribed({A,B,C})
    for _ in range(200):s.observe(A);s.observe(B)
    now[0]=10;assert s.select({A,B,C})=={C}
    now[0]=131;assert len(s.select({A,B,C}))==2
    assert len(s.select({A,B,C}))==2


def test_http_observer_keeps_running_when_only_some_pools_have_push(tmp_path,monkeypatch):
    import searcher.activity as activity
    from test_runtime import setup_runtime, FakeRPC
    module,_,config=setup_runtime(monkeypatch)
    config['policy'].update(activity_priority=True,activity_poll_seconds=.01,universe_refresh_seconds=100)
    monkeypatch.setattr(activity,'ROOT',tmp_path)
    monkeypatch.setattr(activity,'websocket_url',lambda:'wss://base-mainnet.core.chainstack.com/test')
    def fake(url,chain,pools,stop,finish,publish,health,budget,selector=None):
        health('connected',http_only_pools=1)
        stop.wait(.25)
    monkeypatch.setattr(activity,'run_stream',fake)
    calls=[]
    class ReadRPC(FakeRPC):
        def request(self,method,params):calls.append(method);return [log()]
    monkeypatch.setattr(module,'RPC',ReadRPC)
    result=module.run_pending(ReadRPC(),config,{},.15,tmp_path/'pipeline')
    assert calls.count('eth_getLogs')>1 and result['counts']['decisions']>0


def test_free_plan_request_budget_is_validated(tmp_path):
    c=load_config();c['policy']['activity_ws_daily_request_limit']=True
    p=tmp_path/'config.json';p.write_text(json.dumps(c))
    with pytest.raises(Reject,match='budget'):load_config(p)
