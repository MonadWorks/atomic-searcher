import json
import threading
import time
from collections import deque

import pytest

from searcher.activity import ActivityInbox, ActivityJournal, StreamBudget, stream_session, run_stream, websocket_url
from searcher.core import Reject
from searcher.events import V3_SWAP
from searcher.statefeed import DeepReadScheduler
from test_searcher import A, B, C


def log(pool=A):
    return {'address':pool,'topics':[V3_SWAP],'data':'0x', 'transactionHash':'0x'+'12'*32,'logIndex':'0x0'}


def test_activity_bursts_coalesce_without_losing_older_live_pools():
    inbox=ActivityInbox();inbox.publish([A],1);inbox.publish([B],2);inbox.publish([A,C],3)
    assert dict(inbox.drain(4))=={A:3,B:2,C:3}
    assert inbox.drain(4)==[]
    inbox.publish([A],1);assert inbox.drain(7)==[]


def test_priority_depth_keeps_exploration_and_admits_high_value_after_full_queue():
    q=DeepReadScheduler();available=set()
    for i in range(160):
        group=[f'{i}a',f'{i}b'];available.update(group)
        q.add([{'pools':group,'score':0}],1)
    high=['winner-a','winner-b'];available.update(high)
    q.add([{'pools':high,'score':100_000}],2)
    assert len(q.pending)==128 and tuple(high) in q.pending
    chosen=q.take(available,2)
    assert {'0a','0b','1a','1b',*high} <= chosen and len(chosen)<=24
    # Repeated priority traffic cannot erase protected old work.
    seen=set(chosen)
    for n in range(15):
        pair=[f'hot{n}a',f'hot{n}b'];available.update(pair)
        q.add([{'pools':pair,'score':10**9}],3+n)
        seen.update(q.take(available,3+n))
    assert {f'{i}{side}' for i in range(32) for side in 'ab'} <= seen


def test_repeated_cold_request_keeps_age_after_admission_eviction():
    q=DeepReadScheduler();available=set();seen=set()
    def high(i):
        pair=[f'{i}a',f'{i}b'];available.update(pair)
        return {'pools':pair,'score':10**9}
    q.add([high(i) for i in range(128)],1);q.take(available,1)
    cold=['cold-a','cold-b'];available.update(cold)
    for n in range(40):
        q.add([{'pools':cold,'score':0}]+[high(128+n*12+i) for i in range(12)],2+n)
        seen.update(q.take(available,2+n))
        if set(cold)<=seen:break
    assert set(cold)<=seen


def test_stream_budget_survives_restart_and_journal_bounds_disk(tmp_path):
    path=tmp_path/'usage.json';b=StreamBudget(path,300);b.add(250);b.save()
    restarted=StreamBudget(path,300)
    with pytest.raises(Reject,match='limit'): restarted.add(60)
    restarted.save();assert StreamBudget(path,300).remaining_seconds()>3500
    journal=ActivityJournal(tmp_path/'events.jsonl',700)
    for _ in range(10): journal.append([dict(log(),secret='must not persist')],'pendingLogs',time.time())
    for p in tmp_path.glob('*.jsonl'):
        assert p.stat().st_size<=700 and 'secret' not in p.read_text()


class Socket:
    def __init__(self,stop,chain='0x2105',reject_subscribe=False):
        self.stop,self.chain,self.reject_subscribe=stop,chain,reject_subscribe
        self.messages=deque();self.calls=[];self.events=0
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def send(self,raw):
        obj=json.loads(raw);self.calls.append(obj)
        result=self.chain if obj['method']=='eth_chainId' else ('sub-'+str(obj['id']) if obj['method']=='eth_subscribe' else True)
        response={'id':obj['id'],'result':result}
        if obj['method']=='eth_subscribe' and self.reject_subscribe:response={'id':obj['id'],'error':{'message':'secret-api-key'}}
        self.messages.append(json.dumps(response))
        if obj['method']=='eth_subscribe' and not self.reject_subscribe:
            for pool in (A,B):self.messages.append(json.dumps({'method':'eth_subscription','params':{'subscription':result,'result':log(pool)}}))
    def recv(self,timeout):
        if self.messages:return self.messages.popleft()
        self.stop.set();raise TimeoutError


def test_stream_checks_chain_filters_events_and_never_requests_transaction_submission(tmp_path):
    stop=threading.Event();socket=Socket(stop);events=[];states=[]
    with pytest.raises(InterruptedError):
        stream_session('wss://example.invalid',8453,lambda:{A},stop,time.monotonic()+2,
                       lambda logs,source:events.extend(logs),lambda state,**kw:states.append(state),
                       StreamBudget(tmp_path/'budget.json',10000),connect=lambda *a,**k:socket)
    assert [event['address'] for event in events]==[A] and 'connected' in states
    assert [c['method'] for c in socket.calls]==['eth_chainId','eth_subscribe']
    assert socket.calls[1]['params'][1]['address']==[A]
    assert socket.calls[1]['params'][0]=='pendingLogs'


@pytest.mark.parametrize('chain,reject,match',[('0x1',False,'chain mismatch'),('0x2105',True,'rejected')])
def test_wrong_chain_or_unsupported_subscription_never_delivers_events(tmp_path,chain,reject,match):
    stop=threading.Event();socket=Socket(stop,chain,reject);events=[]
    with pytest.raises(Reject,match=match):
        stream_session('wss://example.invalid',8453,lambda:{A},stop,time.monotonic()+2,
                       lambda *x:events.append(x),lambda *a,**kw:None,StreamBudget(tmp_path/'budget.json',10000),
                       connect=lambda *a,**k:socket)
    assert not events


def test_unsolicited_provider_limit_envelope_forces_fallback(tmp_path):
    stop=threading.Event()
    class Limited(Socket):
        def send(self,raw):
            super().send(raw)
            if json.loads(raw)['method']=='eth_subscribe':
                self.messages.append(json.dumps({'jsonrpc':'2.0','error':{'code':429,'message':'secret'}}))
    socket=Limited(stop)
    with pytest.raises(Reject,match='rejected'):
        stream_session('wss://example.invalid',8453,lambda:{A},stop,time.monotonic()+2,
                       lambda *a:None,lambda *a,**kw:None,StreamBudget(tmp_path/'budget.json',10000),
                       connect=lambda *a,**k:socket)


def test_old_subscription_event_cannot_disable_fallback_while_resubscribing(tmp_path):
    stop=threading.Event();states=[];desired={A}
    class Switching(Socket):
        def send(self,raw):
            obj=json.loads(raw)
            if obj['method']=='eth_unsubscribe':
                self.messages.append(json.dumps({'method':'eth_subscription','params':{'subscription':'sub','result':log()}}))
            super().send(raw)
    socket=Switching(stop)
    def publish(*args):desired.discard(A);desired.add(B)
    # Keep session alive long enough to switch the filter once.
    count=[0]
    def recv(timeout):
        if socket.messages:return socket.messages.popleft()
        count[0]+=1
        if count[0]>7:stop.set()
        time.sleep(.15);raise TimeoutError
    socket.recv=recv
    def health(state,**kw):states.append((state,len(socket.calls)))
    try:
        stream_session('wss://example.invalid',8453,lambda:set(desired),stop,time.monotonic()+3,
                       publish,health,StreamBudget(tmp_path/'budget.json',10000),connect=lambda *a,**k:socket)
    except InterruptedError:pass
    assert any(c['method']=='eth_unsubscribe' for c in socket.calls)
    # During unsubscribe (third request), old events must not claim connection readiness.
    assert ('connected',3) not in states


def test_pipeline_http_fallback_continues_after_stream_failure(tmp_path,monkeypatch):
    import searcher.activity as activity
    from test_runtime import setup_runtime, FakeRPC, wait_until
    module,_,config=setup_runtime(monkeypatch)
    config['policy'].update(activity_priority=True,activity_poll_seconds=.01,universe_refresh_seconds=100)
    monkeypatch.setattr(activity,'ROOT',tmp_path)
    monkeypatch.setattr(activity,'websocket_url',lambda:'wss://example.invalid/key')
    monkeypatch.setattr(activity,'stream_session',lambda *a,**k:(_ for _ in ()).throw(RuntimeError('secret')))
    calls=[]
    class ReadRPC(FakeRPC):
        def request(self,method,params):calls.append(method);return [log()]
    monkeypatch.setattr(module,'RPC',ReadRPC)
    stop=threading.Event();results=[]
    worker=threading.Thread(target=lambda:results.append(
        module.run_pending(ReadRPC(),config,{},3,tmp_path/'pipeline',stop_event=stop)))
    worker.start()
    try:
        wait_until(lambda:'eth_getLogs' in calls and any((tmp_path/'pipeline').glob('decision-*.json')))
    finally:
        stop.set();worker.join(timeout=4)
    assert not worker.is_alive() and results
    result=results[0]
    assert 'eth_getLogs' in calls and result['counts']['decisions']>0
    assert result['activity_stream_stopped'] and result['activity_stopped']


def test_run_stream_reconnects_redacts_errors_and_preserves_usage(tmp_path,monkeypatch):
    import searcher.activity as module
    stop=threading.Event();calls=[];states=[]
    def fake(*args,**kwargs):
        calls.append(1)
        if len(calls)==1:raise RuntimeError('wss://key.example/secret secret-key')
        stop.set()
    monkeypatch.setattr(module,'stream_session',fake)
    class FastStop:
        def is_set(self):return stop.is_set()
        def wait(self,n):return stop.is_set()
    run_stream('wss://key.example/secret',8453,lambda:{A},FastStop(),time.monotonic()+2,lambda *a:None,
               lambda state,**kw:states.append({'state':state,**kw}),StreamBudget(tmp_path/'budget.json',10000))
    assert len(calls)==2 and 'secret' not in json.dumps(states)
    assert states[-1]['state']=='stopped'


def test_local_configuration_is_private_and_ignored_by_session_config(tmp_path,monkeypatch):
    import searcher.activity as module
    (tmp_path/'config').mkdir();monkeypatch.setattr(module,'ROOT',tmp_path)
    monkeypatch.delenv('BASE_ACTIVITY_WS_URL',raising=False)
    monkeypatch.setattr('getpass.getpass',lambda _: 'wss://base-mainnet.g.alchemy.com/v2/local-only-key')
    monkeypatch.setattr(module,'verify_endpoint',lambda url:None)
    module.configure();path=tmp_path/'config/node.local.json'
    assert path.stat().st_mode & 0o777 == 0o600
    assert websocket_url().endswith('local-only-key')
    monkeypatch.setenv('BASE_ACTIVITY_WS_URL','http://bad.example')
    with pytest.raises(Reject):websocket_url()


def test_configure_keeps_existing_endpoint_when_probe_fails(tmp_path,monkeypatch):
    import searcher.activity as module
    (tmp_path/'config').mkdir();monkeypatch.setattr(module,'ROOT',tmp_path)
    path=tmp_path/'config/node.local.json';path.write_text('{"activity_ws_url":"wss://old.example"}')
    monkeypatch.setattr('getpass.getpass',lambda _: 'wss://wrong.example')
    monkeypatch.setattr(module,'verify_endpoint',lambda url:(_ for _ in ()).throw(Reject('wrong chain')))
    with pytest.raises(Reject):module.configure()
    assert json.loads(path.read_text())['activity_ws_url']=='wss://old.example'


def test_configuration_probe_requires_actual_base_subscription_acceptance(monkeypatch):
    from searcher.activity import verify_endpoint
    stop=threading.Event();socket=Socket(stop)
    monkeypatch.setattr('websockets.sync.client.connect',lambda *a,**kw:socket)
    verify_endpoint('wss://example.invalid')
    assert [call['method'] for call in socket.calls]==['eth_chainId','eth_subscribe']
    socket.chain='0x1';socket.messages.clear()
    with pytest.raises(Reject,match='chain mismatch'):verify_endpoint('wss://example.invalid')


def test_real_websocket_frames_stop_and_pool_filter_refresh(tmp_path):
    from websockets.sync.server import serve
    from websockets.sync.client import connect
    stop=threading.Event();seen=[];calls=[];wanted={A};errors=[]
    def handler(ws):
        try:
            for raw in ws:
                obj=json.loads(raw);calls.append(obj['method'])
                if obj['method']=='eth_chainId':result='0x2105'
                elif obj['method']=='eth_unsubscribe':result=True
                else:result='subscription-'+str(len(calls))
                ws.send(json.dumps({'id':obj['id'],'result':result}))
                if obj['method']=='eth_subscribe':
                    for pool in obj['params'][1]['address']:
                        ws.send(json.dumps({'method':'eth_subscription','params':{'subscription':result,'result':log(pool)}}))
        except Exception as exc:errors.append(type(exc).__name__)
    with serve(handler,'127.0.0.1',0) as server:
        server_thread=threading.Thread(target=server.serve_forever);server_thread.start()
        port=server.socket.getsockname()[1]
        def publish(logs,source):
            seen.extend(entry['address'] for entry in logs)
            if seen==[A]:wanted.add(B)
            if B in seen:stop.set()
        def local_connect(url,**kw):return connect(url,proxy=None,**kw)
        try:
            stream_session(f'ws://127.0.0.1:{port}',8453,lambda:set(wanted),stop,time.monotonic()+5,
                           publish,lambda *a,**kw:None,StreamBudget(tmp_path/'budget.json',10000),connect=local_connect)
        except InterruptedError:pass
        finally:server.shutdown();server_thread.join(2)
    assert B in seen and calls.count('eth_subscribe')==2 and 'eth_unsubscribe' not in calls and not server_thread.is_alive()
