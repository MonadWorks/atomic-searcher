"""Local-only discovery split, durable debits and same-provider fallback."""
import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import httpx
import pytest

from searcher import engine
from searcher.core import RPC, RPCAccessDenied, RPCUnavailable, Reject, load_config
from searcher.discovery_reads import (DiscoveryReads, DiscoveryHTTPBudget, DiscoveryBudgetUnavailable,
                                     RecentDiscoveryExpired, RecentDiscoveryRPC, DiscoveryStreamBudget, split_settings)
from test_discovery_history import harness

WS = 'wss://base-test.p2pify.com/test-secret?key=another-secret'
NOW = datetime(2026, 10, 14, 12, tzinfo=timezone.utc).timestamp()


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def budget(tmp_path, total=80000, limit=40000, now=None):
    now = now if now is not None else [NOW]
    return DiscoveryHTTPBudget(tmp_path/'http.json', tmp_path/'ws.json', total, limit, clock=lambda:now[0])


@pytest.fixture(autouse=True)
def clean_discovery_override(monkeypatch):
    monkeypatch.delenv('BASE_DISCOVERY_RPC_URL', raising=False)


@pytest.mark.parametrize('value', [WS, 'wss://foo.chainstack.com/key'])
def test_split_valid_chainstack_endpoint_only_in_memory(value):
    config = load_config(); before = copy.deepcopy(config)
    result = split_settings(config, websocket=lambda:value)
    assert result['url'] == value.replace('wss://','https://',1)
    assert result['ws'] == result['http'] == 40000
    assert config == before and 'test-secret' not in json.dumps(config)
    config['policy']['activity_ws_daily_request_limit'] = 501
    result = split_settings(config, websocket=lambda:value)
    assert result['ws'] == result['http'] == 250 and result['total'] == 501


@pytest.mark.parametrize('value', [None, 'https://foo.chainstack.com/key', 'wss://evilchainstack.com/key',
    'wss://foo.chainstack.com.evil.com/key', 'wss://user@foo.chainstack.com/key',
    'wss://foo.chainstack.com/key#fragment', 'wss://foo.chainstack.com:bad/key'])
def test_invalid_or_other_provider_preserves_unsplit_policy(value):
    assert split_settings(load_config(), websocket=lambda:value) is None


def test_explicit_discovery_override_wins_without_reading_websocket(monkeypatch):
    monkeypatch.setenv('BASE_DISCOVERY_RPC_URL','https://public-override.invalid')
    assert split_settings(load_config(), websocket=lambda:pytest.fail('must not read WS')) is None


def test_legacy_ws_usage_and_migrated_probe_usage_reduce_http(tmp_path):
    b = budget(tmp_path)
    write(b.ws_path, {'day':'2026-10-14','units':79900, 'bytes':123})
    write(b.path, {'day':'2026-10-14','units':100})
    assert not b.ready()
    with pytest.raises(DiscoveryBudgetUnavailable): b.acquire(1)
    assert json.loads(b.path.read_text())['units'] == 100
    assert json.loads(b.ws_path.read_text())['units'] == 79900
    assert b.details()['remaining_units'] == 0


def test_http_capacity_is_min_suballocation_and_total_less_ws(tmp_path):
    b = budget(tmp_path, total=100, limit=50)
    write(b.ws_path, {'day':'2026-10-14','units':97})
    b.acquire(3)
    with pytest.raises(DiscoveryBudgetUnavailable): b.acquire(1)
    assert b.details()['local_units'] == 3


def test_clock_rollback_restart_and_forward_day_never_reset_same_day(tmp_path):
    now = [NOW]; b = budget(tmp_path, total=100, limit=50, now=now)
    b.acquire(40)
    now[0] -= 86400
    again = budget(tmp_path,total=100,limit=50,now=now)
    again.acquire(10)
    with pytest.raises(DiscoveryBudgetUnavailable): again.acquire(1)
    now[0] = NOW+86400
    next_day = budget(tmp_path,total=100,limit=50,now=now)
    next_day.acquire(50)
    now[0] = NOW
    with pytest.raises(DiscoveryBudgetUnavailable): b.acquire(1)
    assert b.details()['usage_day_utc'] == '2026-10-15'


def test_future_ws_day_prevents_http_clock_rollback_allowance(tmp_path):
    b=budget(tmp_path,total=100,limit=50)
    write(b.ws_path, {'day':'2026-10-15','units':96})
    b.acquire(4)
    with pytest.raises(DiscoveryBudgetUnavailable): b.acquire(1)
    assert b.details()['usage_day_utc'] == '2026-10-15'


def test_multithread_instances_cannot_multiply_credit(tmp_path):
    b=budget(tmp_path,total=100,limit=50)
    barrier=threading.Barrier(10); passed=[]; failed=[]
    def worker():
        local=budget(tmp_path,total=100,limit=50)
        barrier.wait()
        try:
            local.acquire(10, deadline=time.monotonic()+2)
            passed.append(True)
        except DiscoveryBudgetUnavailable: failed.append(True)
    threads=[threading.Thread(target=worker) for _ in range(10)]
    for t in threads:t.start()
    for t in threads:t.join(3)
    assert all(not t.is_alive() for t in threads)
    assert len(passed)==len(failed)==5
    assert b.details()['local_units']==50


def test_pacing_persists_across_restart_and_recovers_without_new_burst(tmp_path):
    now=[NOW]; b=budget(tmp_path,now=now)
    b.acquire(128)
    recreated=budget(tmp_path,now=now)
    with pytest.raises(DiscoveryBudgetUnavailable): recreated.acquire(1,deadline=time.monotonic()+.01)
    assert recreated.details()['pacing_credit']==0
    now[0]+=120
    recreated.acquire(80)
    assert recreated.details()['local_units']==208
    assert recreated.details()['pacing_credit']<48


def test_pacing_wait_and_file_lock_are_cancellable(tmp_path):
    import fcntl
    b=budget(tmp_path); b.acquire(128)
    stop=threading.Event(); errors=[]; entered=threading.Event()
    def waiting():
        entered.set()
        try:b.acquire(1,stop)
        except Exception as exc:errors.append(exc)
    t=threading.Thread(target=waiting);t.start();assert entered.wait(1)
    stop.set();t.join(.5)
    assert not t.is_alive() and isinstance(errors[0],Reject)
    stop=threading.Event(); errors=[]
    with open(b.path.with_suffix('.json.lock'),'a') as handle:
        fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
        t=threading.Thread(target=lambda: _details_wait(b,stop,errors));t.start()
        stop.set();t.join(.5)
        assert not t.is_alive() and isinstance(errors[0],Reject)
        fcntl.flock(handle,fcntl.LOCK_UN)


def _details_wait(b,stop,errors):
    try:b.details(stop)
    except Exception as exc:errors.append(exc)


def private_rpc(tmp_path, handler, stop=None):
    b=budget(tmp_path)
    rpc=RecentDiscoveryRPC('https://base-test.p2pify.com/test-secret', budget=b, cancel_event=stop)
    rpc.loop.run_until_complete(rpc.session.aclose())
    rpc.session=httpx.AsyncClient(transport=httpx.MockTransport(handler))
    rpc.begin_round()
    return rpc,b


def test_actual_request_retry_debits_each_http_attempt_even_failed_response(tmp_path):
    sent=[]
    def handler(request):
        sent.append(json.loads(request.content))
        if len(sent)==1:return httpx.Response(503,headers={'Retry-After':'0'})
        return httpx.Response(200,json={'jsonrpc':'2.0','id':1,'result':'0x2105'})
    rpc,b=private_rpc(tmp_path,handler)
    try:
        assert rpc.request('eth_chainId',[])=='0x2105'
        assert len(sent)==2 and b.details()['local_units']==2
        assert rpc.measurements[-1]['attempts']==2
        assert sum(a['discovery_units_debited'] for a in rpc.measurements[-1]['attempt_timings'])==2
    finally:rpc.close()


def test_batch_counts_methods_not_envelope_or_multicall_inner_calls(tmp_path):
    sent=[]
    def handler(request):
        payload=json.loads(request.content);sent.append(payload)
        return httpx.Response(200,json=[{'jsonrpc':'2.0','id':i,'result':'0x'} for i,_ in enumerate(payload)])
    rpc,b=private_rpc(tmp_path,handler)
    try:
        rpc.anchor_height=100
        rpc.batch([('eth_call',[{'to':'0x'+'11'*20,'data':'0x1234'},'0x64'])]*3)
        assert b.details()['local_units']==3 and len(sent)==1
        # Unknown-age requests get the conservative tier.
        rpc.batch([('eth_getTransactionReceipt',['0x'+'22'*32])]*2)
        assert b.details()['local_units']==7
    finally:rpc.close()


def test_cancelled_request_no_send_and_stale_anchor_no_private_send(tmp_path):
    sent=[]; stop=threading.Event()
    rpc,b=private_rpc(tmp_path,lambda request:sent.append(request),stop)
    try:
        stop.set()
        with pytest.raises(Reject,match='cancelled'):rpc.request('eth_chainId',[])
        assert not sent and not b.path.exists()
        stop.clear();rpc.anchor_height=100
        with pytest.raises(RecentDiscoveryExpired):rpc.request('eth_getBlockByNumber',['0x1',False])
        rpc.round_deadline=time.monotonic()-1
        with pytest.raises(RecentDiscoveryExpired):rpc.request('eth_chainId',[])
        assert not sent and not b.path.exists()
    finally:rpc.close()


def test_stale_latest_header_rejected_and_provider_secret_never_persistable(tmp_path):
    replies=[{'jsonrpc':'2.0','id':1,'result':{'number':'0x64','timestamp':hex(int(time.time())-90)}},
             {'jsonrpc':'2.0','id':1,'error':{'code':-32000,'message':'bad api key test-secret'}}]
    rpc,b=private_rpc(tmp_path,lambda _:httpx.Response(200,json=replies.pop(0)))
    try:
        with pytest.raises(RecentDiscoveryExpired):rpc.header()
        with pytest.raises(RPCUnavailable) as error:rpc.request('eth_chainId',[])
        assert 'test-secret' not in str(error.value)
        assert b.details()['local_units']==2
    finally:rpc.close()


def test_history_headers_logs_confirmation_stay_public_with_fixed_private_anchor(harness,monkeypatch):
    private,config,initial,*_=harness
    public=copy.deepcopy(private); public.ranges=[];public.header_calls.clear()
    private.discovery_log_chunk_size=32
    original=engine.validate_pools
    anchors=[]
    def validate(rpc,config,addresses,block):
        anchors.append((rpc,block));return original(rpc,config,addresses,block)
    monkeypatch.setattr(engine,'validate_pools',validate)
    result=engine.prepare(private,config,previous=initial,history_rpc=public)
    assert private.ranges==[[69,100]]
    assert public.ranges==[[11,20]]
    assert public.header_calls[20]==2 and private.header_calls[20]==0
    assert all(rpc is private and block==100 for rpc,block in anchors)
    assert result['discovery_cursor']==100 and result['discovery_recovered_range']==[11,20]


@pytest.mark.parametrize('error',[RPCAccessDenied('archive forbidden'),RPCUnavailable('historical read unavailable')])
def test_history_error_retains_backlog_and_publishes_recent_without_private_history(harness,error):
    private,config,initial,*_=harness
    public=copy.deepcopy(private)
    public.request=lambda *args:(_ for _ in ()).throw(error)
    private.discovery_log_chunk_size=32
    result=engine.prepare(private,config,previous=initial,history_rpc=public)
    assert result['discovery_recovered_range'] is None and result['discovery_backlog']==[[11,68]]
    assert result['discovery_cursor']==100 and private.ranges==[[69,100]]


@pytest.mark.parametrize('size',[0,33,True,1.5])
def test_invalid_private_chunk_cannot_enlarge_or_drop_fresh_range(harness,size):
    rpc,config,initial,*_=harness;rpc.discovery_log_chunk_size=size
    with pytest.raises(Reject,match='chunk size'):engine.prepare(rpc,config,previous=initial)


class StubRPC:
    def __init__(self,url,chain_id=8453,timeout=10,cancel_event=None,**kwargs):
        self.url,self.chain_id,self.cancel_event=url,chain_id,cancel_event
        self.closed=False;self.round_units=0;self.budget=kwargs.get('budget')
    def begin_round(self):self.round_units=0;self.round_deadline=time.monotonic()+60
    def close(self):self.closed=True


def manager(tmp_path,**kwargs):
    return DiscoveryReads(load_config(),SimpleNamespace(url='https://base.drpc.org',chain_id=8453),
        threading.Event(),root=tmp_path,websocket=lambda:WS,rpc_factory=StubRPC,private_factory=StubRPC,**kwargs)


def test_private_failure_restarts_whole_round_from_original_checkpoint_and_closes(monkeypatch,tmp_path):
    m=manager(tmp_path); prior={'cursor':12}; calls=[]
    def prepare(rpc,config,recent_blocks,previous=None,history_rpc=None):
        calls.append((rpc,previous,history_rpc))
        if rpc is m.private:
            rpc.round_units=3
            raise RPCAccessDenied('forbidden test-secret')
        return {'pools':[{'valid':True}],'discovery_cursor':99}
    monkeypatch.setattr(engine,'prepare',prepare)
    try:
        result=m.prepare(previous=prior)
        assert calls==[(m.private,prior,m.public),(m.public,prior,None)]
        assert result['discovery_read_route']['recent']=='public'
        assert result['discovery_read_route']['discarded_private_attempt']['units_debited']==3
        assert 'test-secret' not in json.dumps(result)
        assert result['discovery_read_route']['total_round_ms']>=result['discovery_read_route']['public_round_ms']
        calls.clear();m.prepare(previous=prior)
        assert calls==[(m.public,prior,None)]
        m.retry_at=0
        calls.clear();m.prepare(previous=prior)
        assert len(calls)==2  # Optional node is retried after the bounded cooldown.
    finally:m.close()
    assert m.private.closed and m.public.closed


def test_optional_constructor_failure_does_not_close_public_and_recovers(monkeypatch,tmp_path):
    made=[]
    def private_factory(*args,**kwargs):
        made.append(True)
        if len(made)==1:raise ImportError('optional transport unavailable')
        return StubRPC(*args,**kwargs)
    m=DiscoveryReads(load_config(),SimpleNamespace(url='https://base.drpc.org',chain_id=8453),
        threading.Event(),root=tmp_path,websocket=lambda:WS,rpc_factory=StubRPC,private_factory=private_factory)
    seen=[]
    monkeypatch.setattr(engine,'prepare',lambda rpc,*a,**kw:seen.append(rpc) or {})
    try:
        assert m.private is None and not m.public.closed
        m.prepare();assert seen==[m.public]
        m.retry_at=0;m.prepare();assert seen[-1] is m.private
    finally:m.close()


def test_low_pacing_credit_uses_public_without_wasting_private_partial_round(monkeypatch,tmp_path):
    m=manager(tmp_path);m.budget.acquire(128);seen=[]
    monkeypatch.setattr(engine,'prepare',lambda rpc,*a,**kw:seen.append(rpc) or {})
    try:
        result=m.prepare()
        assert seen==[m.public]
        assert result['discovery_read_route']['fallback_reason']=='local_budget_pacing'
        assert m.budget.details()['local_units']==128
    finally:m.close()


def test_stop_after_private_failure_never_starts_public_fallback(monkeypatch,tmp_path):
    m=manager(tmp_path);seen=[]
    def prepare(rpc,*a,**kw):
        seen.append(rpc);m.stop.set();raise Reject('RPC cancelled')
    monkeypatch.setattr(engine,'prepare',prepare)
    try:
        with pytest.raises(Reject,match='cancelled'):m.prepare()
        assert seen==[m.private]
    finally:m.close()


def test_disabling_private_or_lowering_total_cannot_regrant_spent_http_to_ws(tmp_path):
    now=[NOW]; http=tmp_path/'http.json';ws=tmp_path/'ws.json'
    write(http,{'day':'2026-10-14','units':75})
    write(ws,{'day':'2026-10-14','units':10,'bytes':10})
    b=DiscoveryStreamBudget(ws,100,http,split_active=False,clock=lambda:now[0])
    assert b.limit==25
    for _ in range(15):b.add(1)
    assert b.remaining_seconds()>0
    assert json.loads(http.read_text())['units']+b.state['units']==100
    now[0]+=86400
    assert b.remaining_seconds()==0 and b.limit==100 and b.state['units']==0
    b.add(1)
    assert b.details()['usage_day_utc']=='2026-10-15'


def test_ws_budget_mode_change_preserves_existing_journal_and_half_after_http_use(tmp_path):
    http=tmp_path/'http.json';ws=tmp_path/'ws.json'
    write(http,{'day':'2026-10-14','units':100})
    write(ws,{'day':'2026-10-14','units':39000,'bytes':123})
    b=DiscoveryStreamBudget(ws,80000,http,split_active=False,clock=lambda:NOW)
    assert b.limit==40000 and b.state['units']==39000 and b.state['bytes']==123
    b.add(1)
    assert json.loads(ws.read_text())['units']==39001


def test_ws_future_http_high_watermark_survives_restart_and_clock_rollback(tmp_path):
    http=tmp_path/'http.json';ws=tmp_path/'ws.json'
    write(http,{'day':'2026-10-15','units':90})
    b=DiscoveryStreamBudget(ws,100,http,split_active=False,clock=lambda:NOW)
    assert b.limit==10 and b.state['day']=='2026-10-15'
    b.add(1)
    restarted=DiscoveryStreamBudget(ws,100,http,split_active=False,clock=lambda:NOW)
    assert restarted.state['units']==1 and restarted.limit==10


def test_private_round_deadline_cancels_history_wait_restores_public_and_retries(monkeypatch,tmp_path):
    m=manager(tmp_path); seen=[]; original_stop=m.public.cancel_event
    def begin():
        m.private.round_units=0;m.private.round_deadline=time.monotonic()+.04
    m.private.begin_round=begin
    def prepare(rpc,*a,history_rpc=None,**kw):
        seen.append(rpc)
        if rpc is m.private:
            assert history_rpc is m.public
            assert history_rpc.cancel_event.wait(10)
            raise Reject('RPC cancelled')
        assert rpc.cancel_event is original_stop and not rpc.cancel_event.is_set()
        return {}
    monkeypatch.setattr(engine,'prepare',prepare)
    try:
        started=time.monotonic();result=m.prepare()
        assert time.monotonic()-started<.5
        assert seen==[m.private,m.public]
        assert m.private.cancel_event is original_stop and m.public.cancel_event is original_stop
        assert result['discovery_read_route']['discarded_private_attempt']['outcome']=='discarded'
    finally:m.close()


def test_accounted_private_round_admission_remains_usable_after_initial_burst(tmp_path,monkeypatch):
    m=manager(tmp_path);now=[NOW-43200];m.budget.clock=lambda:now[0];seen=[]
    def prepare(rpc,*a,**kw):
        seen.append(rpc)
        if rpc is m.private:
            # A representative recurring round costs 30 actual RPC methods.
            m.budget.acquire(30, m.stop, deadline=time.monotonic()+.1)
            rpc.round_units=30
        return {}
    monkeypatch.setattr(engine,'prepare',prepare)
    try:
        for _ in range(8):
            m.prepare();now[0]+=30
        # At sustainable credit the complete private rounds still alternate
        # with complete public rounds; low credit never starts a partial one.
        assert 4<len([r for r in seen if r is m.private])<8
        assert m.budget.details()['local_units']==30*len([r for r in seen if r is m.private])
        assert len(seen)==8
    finally:m.close()


def test_separate_history_timing_is_included_and_attributed(harness):
    from test_discovery_timing_independent import record_reads
    private,config,initial,*_=harness
    public=copy.deepcopy(private)
    p=record_reads(private,100);h=record_reads(public,100)
    result=engine.prepare(private,config,previous=initial,history_rpc=public)
    timing=result['discovery_read_timing']
    assert timing['http_attempts']==len(p)+len(h)
    assert timing['sources']['recent']['http_attempts']==len(p)
    assert timing['sources']['history']['http_attempts']==len(h)
    assert timing['sources']['history']['public_limiter_wait_ms']==len(h)
    assert timing['complete'] is True


def test_future_ws_journal_carries_existing_http_debit_during_clock_rollback(tmp_path):
    b=budget(tmp_path,total=100,limit=50)
    b.acquire(20)
    write(b.ws_path, {'day':'2026-10-15','units':90})
    with pytest.raises(DiscoveryBudgetUnavailable): b.acquire(1)
    assert b.details()['local_units']==20
    assert b.details()['usage_day_utc']=='2026-10-15'
