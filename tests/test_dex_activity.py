import json
import threading
import time
from types import SimpleNamespace
import pytest
from eth_abi import encode

from searcher.core import USDC, WETH, load_config
from searcher.dex_activity import DexActivity
from searcher.dex_screen import Scheduler
from searcher.events import POOL_ACTIVITY_TOPICS
from test_dex_screen import address, pool, row, settlements


@pytest.mark.parametrize('kind,signature',[(1,'Swap(address,address,int256,int256,uint160,uint128,int24)'),
    (2,'Swap(address,address,int256,int256,uint160,uint128,int24,uint128,uint128)'),
    (3,'Sync(uint112,uint112)'),(4,'Sync(uint256,uint256)')])
def test_every_executable_pool_family_can_trigger_paired_screen(tmp_path,kind,signature):
    from searcher.events import topic
    from searcher.morpho_universe import Pool
    from test_dex_screen import F
    a=activity(tmp_path);s=Scheduler(settlements(),4)
    first=Pool(address(1000),*sorted((USDC,address(100))),F,kind,0)
    second=pool(1001,USDC,address(100));s.sync({p.address:p for p in (first,second)})
    event={**log(),'address':first.address,'topics':[topic(signature)],
           'data':'0x'+encode(['uint256','uint256'],[10**30,10**29]).hex()}
    a.publish([event],'pendingLogs')
    s.prioritize(a.inbox.drain(time.monotonic()))
    assert s.page()[:2]==[first,second] and a.events==1


def test_fresh_pair_read_together_despite_full_universe_and_busy_queues():
    now=[100.];s=Scheduler(settlements(),4,lambda:now[0])
    ps=[pool(10000+i,USDC,address(1000+i)) for i in range(5500)]
    trigger,counterpart=pool(20000,USDC,address(9000)),pool(20001,USDC,address(9000))
    ps.extend((trigger,counterpart));s.sync({p.address:p for p in ps})
    s.hot.extend(p.address for p in ps[:500])
    s.prioritize([(p.address,99.) for p in ps[:1000]])
    s.prioritize([(trigger.address,100.)])
    selected=s.page();chosen=[p.address for p in selected]
    assert chosen[:2]==[trigger.address,counterpart.address]
    assert len(chosen)==len(set(chosen))==96 and len(s.activity)<=512
    assert s.cursor>=32  # Event storms retain fair discovery capacity.
    assert not s.samples  # Events cannot inject executable prices.
    s.record(selected,[row(p,USDC) if p!=counterpart else (False,0,0,0,0,0,0,0) for p in selected],1)
    assert trigger.address not in s.activity and counterpart.address in s.activity
    assert counterpart in s.page()
    now[0]=106.;s.prioritize(())
    assert not s.activity


def test_small_pages_keep_trigger_first_and_expired_or_unknown_events_out():
    now=[10.];s=Scheduler(settlements(),4,lambda:now[0]);s.page_size=16
    token=address(999)
    ps=[pool(1000+i,USDC,token) for i in range(40)]
    s.sync({p.address:p for p in ps})
    s.prioritize([(address(9000),10.),(ps[0].address,4.),(ps[1].address,11.)])
    assert not s.activity
    s.prioritize([(ps[-1].address,10.)]);page=s.page()
    assert page[0]==ps[-1] and len(s.last_activity_reads)==8 and len(page)==16
    s.record(page,[row(p,USDC) for p in page],1)
    assert all(p.address not in s.activity for p in page)
    s.sync({p.address:p for p in ps[:-1]});s.page()
    assert ps[-1].address not in s.activity


def activity(tmp_path):
    u=SimpleNamespace(lock=threading.Lock(),graph=SimpleNamespace(pools={address(1):None}))
    return DexActivity(u,load_config(),threading.Event(),tmp_path)


def log(i=1):
    return dict(address=address(1),transactionHash='0x'+format(i,'064x'),logIndex='0x0',
        blockNumber='0x10',blockHash='0x'+'ab'*32,topics=[POOL_ACTIVITY_TOPICS[0]],data='0x')


def test_delivery_deduplicates_http_and_push_and_records_local_receive_time(tmp_path):
    a=activity(tmp_path);start=time.time();r=log()
    a.publish([r],'pendingLogs');a.publish([r],'http-sealed-logs')
    assert a.events==1 and a.inbox.drain(time.monotonic())[0][0]==address(1)
    saved=json.loads((tmp_path/'dex-activity.jsonl').read_text())
    assert saved['received_utc']>=start and saved['source']=='pendingLogs'
    assert a.pools()=={address(1)}
    a.health('connected',http_only_pools=0);assert not a.needs_http()
    assert a.needs_http(time.time()+4)
    a.health('connected',http_only_pools=1);assert a.needs_http()
    a.health('retrying');assert a.needs_http()


def test_late_http_batch_cannot_displace_a_fresh_pushed_trigger(tmp_path):
    a=activity(tmp_path)
    a.publish([log(1)],'pendingLogs')
    a.publish([{**log(2),'address':address(2)}],'http-pending-logs')
    assert a.fresh()
    assert [p for p,_ in a.fast_inbox.drain(time.monotonic())]==[address(1)]
    assert {p for p,_ in a.inbox.drain(time.monotonic())}=={address(1),address(2)}


def test_observed_closed_flow_prioritizes_its_prepared_market_without_new_queries(tmp_path):
    from searcher.events import V3_SWAP
    a=activity(tmp_path);token=address(100)
    p,q=pool(1000,USDC,token),pool(1001,USDC,token)
    from dataclasses import replace
    p,q=replace(p,kind=1),replace(q,kind=1)
    a.u.graph.pools={v.address:v for v in (p,q)}
    a.prepare_stream([((0,0,1),(p.address,q.address))])
    def event(pool,amounts,index):
        return {**log(),'address':pool.address,'logIndex':hex(index),'topics':[V3_SWAP],
                'data':'0x'+encode(['int256','int256'],amounts).hex()}
    a.publish([event(p,(-100,200_000_000),0)],'http-pending-logs')
    assert a.stream_priorities()[0][0][0]==0
    a.publish([event(q,(100,-201_000_000),1)],'http-pending-logs')
    assert a.stream_priorities()[0][0][0]==2
    assert not a.fast_inbox.pending  # Historical completion never becomes a fresh trade trigger.
    assert a.stream_pools()==set(a.u.graph.pools)  # No rank-driven filter reconstruction.
    a.prepared_at-=46;assert not a.stream_priorities()


def test_unbalanced_pool_flow_does_not_masquerade_as_a_closed_surplus(tmp_path):
    from searcher.events import V3_SWAP
    a=activity(tmp_path);token=address(100);p,q=pool(1000,USDC,token),pool(1001,USDC,token)
    from dataclasses import replace
    p,q=replace(p,kind=1),replace(q,kind=1)
    a.u.graph.pools={v.address:v for v in (p,q)}
    a.prepare_stream([((0,0,1),(p.address,q.address))])
    logs=[{**log(),'address':v.address,'logIndex':hex(i),'topics':[V3_SWAP],
           'data':'0x'+encode(['int256','int256'],amounts).hex()}
          for i,(v,amounts) in enumerate(((p,(-100,200_000_000)),(q,(90,-201_000_000))))]
    a.publish(logs,'pendingLogs')
    assert not a.closed_flows


def test_incomplete_pending_height_keeps_receiver_alive_without_fast_clock_claim(tmp_path):
    a=activity(tmp_path);a.publish([{**log(),'blockNumber':None}],'pendingLogs')
    assert a.events==1 and not a.fast_inbox.pending
    a.publish([log(2)],'pendingLogs')
    assert a.events==2 and a.fresh()


def test_stream_uses_shared_persistent_budget_and_configured_endpoint(tmp_path,monkeypatch):
    import searcher.dex_activity as da
    a=activity(tmp_path);seen={}
    monkeypatch.setattr(da,'ROOT',tmp_path)
    monkeypatch.setattr(da,'websocket_url',lambda:'wss://base-mainnet.core.chainstack.com/test-local-only')
    def run(url,chain,pools,stop,finish,publish,health,budget,selector):
        seen.update(chain=chain,pools=pools(),limit=budget.limit,path=budget.path)
        health('connected',http_only_pools=0);publish([log()],'pendingLogs')
    monkeypatch.setattr(da,'run_stream',run);a.stream()
    assert seen['chain']==8453 and seen['pools']=={address(1)} and seen['limit']==80000
    assert seen['path']==tmp_path/'evidence/activity-stream-requests.json'
    assert a.events==1 and a.ws['state']=='connected'
    assert 'test-local-only' not in (tmp_path/'activity-stream-health.json').read_text()


def test_unconfigured_stream_leaves_http_fallback_enabled(tmp_path,monkeypatch):
    import searcher.dex_activity as da
    a=activity(tmp_path);monkeypatch.setattr(da,'websocket_url',lambda:None)
    a.stream();assert a.ws['state']=='not_configured' and a.needs_http()


def test_pending_continues_while_sealed_read_is_blocked(tmp_path,monkeypatch):
    import searcher.event_intake as ei
    a=activity(tmp_path);blocked=threading.Event();release=threading.Event();calls=[]
    class RPC:
        active_host='pending.test'
        def __init__(self,*args,**kwargs):pass
        def header(self):
            blocked.set();release.wait(4);return {'number':'0x10'}
        def request(self,method,args):
            calls.append(args[0]['fromBlock'])
            if args[0]['fromBlock']=='pending':return [log(len(calls))]
            return []
        def close(self):pass
    monkeypatch.setattr(ei,'EventRPC',RPC)
    sealed=threading.Thread(target=a.sealed_http);pending=threading.Thread(target=a.http)
    sealed.start();assert blocked.wait(1);pending.start()
    try:
        deadline=time.monotonic()+2.5
        while a.events<2 and time.monotonic()<deadline:time.sleep(.01)
        assert calls.count('pending')>=2 and not release.is_set()
        assert a.events>=2
    finally:
        a.stop.set();release.set();sealed.join(2);pending.join(2)
    assert not sealed.is_alive() and not pending.is_alive()


@pytest.mark.parametrize('logs',[[],[log()]])
def test_pending_signal_does_not_wait_for_sealed_header(tmp_path,monkeypatch,logs):
    import searcher.event_intake as ei
    a=activity(tmp_path);calls=[]
    class RPC:
        active_host='pending.test'
        def __init__(self,url,**kwargs):pass
        def header(self):raise AssertionError('pending intake must never request sealed header')
        def request(self,method,args):
            assert self.live_events
            calls.append(args);a.stop.set();return logs
        def close(self):calls.append('closed')
    monkeypatch.setattr(ei,'EventRPC',RPC);a.http()
    assert calls[0][0]['fromBlock']==calls[0][0]['toBlock']=='pending'
    assert calls[-1]=='closed'
    status=json.loads((tmp_path/'activity-health.json').read_text())
    assert status['state']=='connected' and status['pending_events']==len(logs)
    assert status['source']=='http-pending-logs' and a.events==len(logs)


def test_independent_http_health_does_not_mislabel_stale_success(tmp_path,monkeypatch):
    import searcher.dex_activity as da
    now=[100.];monkeypatch.setattr(da.time,'time',lambda:now[0]);a=activity(tmp_path)
    a.http_health('pending',{'state':'connected','pending_events':1})
    a.http_health('sealed',{'state':'retrying','error':'offline'})
    status=lambda:json.loads((tmp_path/'activity-health.json').read_text())
    assert status()['source']=='http-pending-logs'
    now[0]+=6;a.http_health('sealed',{'state':'retrying','error':'offline'})
    assert status()['state']=='retrying' and status()['source'] is None
    a.http_health('sealed',{'state':'connected','block':16})
    assert status()['source']=='http-sealed-logs'


def test_sealed_read_is_bounded_recent_page(tmp_path,monkeypatch):
    import searcher.event_intake as ei
    a=activity(tmp_path);calls=[]
    class RPC:
        def __init__(self,*args,**kwargs):pass
        def header(self):return {'number':'0x10'}
        def request(self,method,args):calls.append(args);a.stop.set();return [log()]
        def close(self):calls.append('closed')
    monkeypatch.setattr(ei,'EventRPC',RPC);a.sealed_http()
    assert calls[0][0]['fromBlock']=='0xf' and calls[0][0]['toBlock']=='0x10'
    assert calls[-1]=='closed' and a.events==1


def test_read_evidence_contains_returned_state_and_is_bounded(tmp_path):
    a=activity(tmp_path);p=pool(1000,USDC,address(100));r=row(p,USDC)
    for _ in range(70):
        a.record_read([p],[r],[p.address],100.,101.,{'number':'0x10','hash':'0xabc','secret':'omit'},
            {'number':'0x11','time':'0x20'})
    files=list(tmp_path.glob('dex-activity-read-*.json'));assert len(files)==64
    value=json.loads(files[0].read_text())
    assert value['rows'][0]['state']==list(r) and value['received_at']==101.
    assert value['call_clock']['number']=='0x11' and 'secret' not in value['anchor']


def test_only_actual_successful_changed_read_reopens_old_native_quote():
    now=[10.];s=Scheduler(settlements(),4,lambda:now[0]);a,b=pool(1000,USDC,address(100)),pool(1001,USDC,address(100))
    s.sync({p.address:p for p in (a,b)})
    rows=[row(a,USDC,2),row(b,USDC)];s.record([a,b],rows,1)
    path=(a.hop(USDC),b.hop(address(100)))
    s.native_results([(path,100,settlements()[0])],[0])
    assert path in s.native_failures
    s.prioritize([(a.address,10.)]);assert path in s.native_failures
    s.record([a,b],rows,2);assert path in s.native_failures
    s.record([a],[(False,*rows[0][1:])],3);assert path in s.native_failures
    s.record([a],[row(a,USDC,3)],4);assert path not in s.native_failures
