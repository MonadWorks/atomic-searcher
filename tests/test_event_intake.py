import httpx
import pytest
from searcher.event_intake import EventIntake
from searcher.core import Reject

M='0x'+'11'*20;F='0x'+'22'*20;O='0x'+'33'*20
GROUPS={'morpho':[M],'factories':[F],'oracles':[O]}

def header(n):return {'number':hex(n),'hash':'0x'+format(n,'064x')}
def log(a,n,index=0):
    return dict(address=a,blockNumber=hex(n),blockHash=header(n)['hash'],transactionHash='0x'+format(n+index,'064x'),logIndex=hex(index),topics=['topic'],data='0x')
def timeout():
    r=httpx.Response(408,request=httpx.Request('POST','https://node.invalid'))
    return httpx.HTTPStatusError('timeout',request=r.request,response=r)

def test_event_transport_returns_throttle_without_sleeping_or_replaying(monkeypatch):
    from searcher.event_intake import EventRPC
    from searcher.core import RPC as BaseRPC
    rpc=EventRPC.__new__(EventRPC);rpc.deadline_ns=None;rpc.cancel_event=None;rpc.provider_budget=None;calls=[]
    def post(self,payload,timings):
        assert self.deadline_ns is None
        calls.append(payload)
        return httpx.Response(429,request=httpx.Request('POST','https://node.invalid'))
    monkeypatch.setattr(BaseRPC,'_timed_post',post)
    monkeypatch.setattr(BaseRPC,'_record_timing',lambda *args,**kw:None)
    with pytest.raises(httpx.HTTPStatusError):rpc.request('eth_getLogs',[{}])
    assert len(calls)==1 and rpc.deadline_ns is None

def test_event_read_retains_queue_place_past_old_three_second_deadline():
    import time
    from searcher.event_intake import EventRPC
    from searcher.core import PublicProviderBudget
    rpc=EventRPC('https://node.invalid',timeout=.2)
    rpc.loop.run_until_complete(rpc.session.aclose())
    rpc.session=httpx.AsyncClient(transport=httpx.MockTransport(lambda request:
        httpx.Response(200,json={'jsonrpc':'2.0','id':1,'result':[]})))
    rpc.provider_budget=PublicProviderBudget(.25)
    rpc.provider_budget.next_at=time.monotonic()+3.1
    rpc.verified_peers.add(0)
    try:
        assert rpc.request('eth_getLogs',[{}])==[]
        assert rpc.measurements[-1]['public_limiter_wait_ms']>=3000
        assert not rpc.provider_budget.queue and rpc.provider_budget.active is None
    finally:rpc.close()

class RPC:
    def __init__(self):self.height=100;self.fail=set();self.calls=[];self.logs=[];self.reorg=False
    def check_cancelled(self):pass
    def header(self,n=None):
        h=header(self.height if n is None else n)
        if n is not None and self.reorg:h['hash']='fork'
        return h
    def request(self,method,params):
        q=params[0];self.calls.append(q)
        if set(q['address'])&self.fail:raise timeout()
        lo,hi=int(q['fromBlock'],16),int(q['toBlock'],16)
        return [x for x in self.logs if x['address'] in q['address'] and lo<=int(x['blockNumber'],16)<=hi]

def test_slow_oracle_does_not_erase_morpho_or_factory_progress():
    now=[1];rpc=RPC();rpc.fail={O};reader=EventIntake(legacy_cursor=99,clock=lambda:now[0]);got=[]
    reader.poll(rpc,GROUPS,lambda _,x:got.append(x),['topic'])
    rpc.height=103;now[0]+=3
    result=reader.poll(rpc,GROUPS,lambda _,x:got.append(x),['topic'])
    assert reader.cursors[M]==104 and reader.cursors[F]==101 and reader.cursors[O]==99
    assert result['primary_block']==103 and result['state']=='degraded'
    saved=dict(reader.cursors)
    restored=EventIntake(saved,legacy_cursor=min(saved.values()),clock=lambda:now[0])
    rpc.height=105;restored.poll(rpc,GROUPS,lambda *_:None,['topic'])
    assert restored.cursors[M]==106 and restored.cursors[O]==99

def test_adaptive_pages_resume_without_skipping_failed_addresses():
    now=[1];rpc=RPC();oracles=['0x'+format(i,'040x') for i in range(100,132)]
    groups={**GROUPS,'oracles':oracles};reader=EventIntake(legacy_cursor=99,clock=lambda:now[0]);rpc.fail=set(oracles)
    reader.poll(rpc,groups,lambda *_:None,['topic']);now[0]+=3
    reader.poll(rpc,groups,lambda *_:None,['topic'])
    assert reader.width['oracles']==8 and all(reader.cursors[a]==99 for a in oracles)
    rpc.fail.clear()
    for _ in range(10):
        now[0]+=3;reader.poll(rpc,groups,lambda *_:None,['topic'])
    assert all(reader.cursors[a]==101 for a in oracles)
    assert all(len(q['address'])<=8 for q in rpc.calls[4:] if any(a in oracles for a in q['address']))

def test_changed_anchor_cannot_apply_or_advance():
    rpc=RPC();rpc.reorg=True;rpc.logs=[log(M,100)];reader=EventIntake(legacy_cursor=99);got=[]
    reader.poll(rpc,GROUPS,lambda _,x:got.append(x),['topic'])
    assert not got and reader.cursors[M]==99

def test_old_checkpoint_does_not_delay_live_events_or_skip_backfill():
    rpc=RPC();rpc.height=1000;rpc.logs=[log(M,999),log(M,99)];reader=EventIntake(legacy_cursor=99);got=[]
    result=reader.poll(rpc,GROUPS,lambda _,x:got.append(x['blockNumber']),['topic'])
    assert got==[hex(999),hex(99)] and reader.cursors[M]==129
    assert result['primary_block']==1000 and result['block']<1000
    rpc.height=1010;rpc.logs.append(log(M,1005))
    reader.poll(rpc,GROUPS,lambda _,x:got.append(x['blockNumber']),['topic'])
    assert hex(1005) in got and reader.cursors[M]<1000
    # A prolonged throttle must not turn the "recent" page into another old
    # backlog. Its missing historical range remains in the main checkpoint.
    rpc.height=2000;rpc.logs.append(log(M,1999))
    result=reader.poll(rpc,GROUPS,lambda _,x:got.append(x['blockNumber']),['topic'])
    assert hex(1999) in got and result['primary_block']==2000 and reader.cursors[M]<1000

def test_poisoned_identity_does_not_hide_later_events_and_is_retried():
    now=[1];rpc=RPC();rpc.logs=[log(M,99),log(M,100)];reader=EventIntake(legacy_cursor=99,clock=lambda:now[0]);got=[];fail=[True]
    def apply(_,x):
        if x['blockNumber']==hex(99) and fail[0]:raise Reject('identity read failed')
        got.append(x['blockNumber'])
    reader.poll(rpc,GROUPS,apply,['topic'])
    assert got==[hex(100)] and reader.cursors[M]==101 and len(reader.pending)==1
    # Pending identities and cursor are checkpointed together across restart.
    reader=EventIntake(reader.cursors,clock=lambda:now[0],pending=list(reader.pending.values()))
    for row in reader.pending.values():row['next_retry_at']=0
    fail[0]=False;now[0]+=3;reader.poll(rpc,GROUPS,apply,['topic'])
    assert got[-1]==hex(99) and reader.cursors[M]==101 and not reader.pending

def test_overlap_deduplicates_but_new_fork_event_is_processed():
    rpc=RPC();rpc.logs=[log(M,100)];reader=EventIntake(legacy_cursor=99);got=[]
    for _ in range(2):reader.poll(rpc,GROUPS,lambda _,x:got.append(x),['topic'])
    assert len(got)==1
    rpc.logs=[{**log(M,100),'blockHash':'other-canonical-fork'}]
    reader.poll(rpc,GROUPS,lambda _,x:got.append(x),['topic'])
    assert len(got)==2

def test_obsolete_recent_failure_does_not_leave_health_stuck_degraded():
    rpc=RPC();reader=EventIntake({M:100,F:100,O:100})
    reader.pages['morpho_recent']={'state':'retrying'};reader.recent_cursor=90
    out=reader.poll(rpc,GROUPS,lambda *_:None,['topic'])
    assert out['state']=='connected' and 'morpho_recent' not in out['pages']

def test_throttle_keeps_page_size_and_rejects_out_of_scope_results():
    rpc=RPC();reader=EventIntake(legacy_cursor=99)
    def limited(*_):
        r=httpx.Response(429,request=httpx.Request('POST','https://node.invalid'))
        raise httpx.HTTPStatusError('rate limit',request=r.request,response=r)
    rpc.request=limited;reader.poll(rpc,GROUPS,lambda *_:None,['topic'])
    assert reader.width['factories']==8 and reader.cursors[M]==99
    rpc=RPC();rpc.request=lambda *_:[log(O,100)];reader=EventIntake(legacy_cursor=99)
    reader.poll(rpc,GROUPS,lambda *_:pytest.fail('invalid page applied'),['topic'])
    assert reader.cursors[M]==99

def test_new_morpho_borrow_event_reaches_live_market_queue():
    from searcher.core import load_config
    from searcher.morpho_universe import Universe
    from searcher.morpho_worker import UniverseWorker,BORROW,MORPHO
    from test_morpho_universe import make_market
    u=Universe(load_config());m=make_market();u.add([m]);u.urgent.clear();u.pending.clear()
    worker=UniverseWorker.__new__(UniverseWorker);worker.universe=u;worker.unknown_checked=set()
    borrower='0x'+'55'*20
    event={**log(MORPHO,100),'topics':[BORROW,m.id,'0x'+'00'*32,'0x'+'00'*12+borrower[2:]]}
    rpc=RPC();rpc.logs=[event];reader=EventIntake(legacy_cursor=99)
    reader.poll(rpc,{'morpho':[MORPHO],'factories':[],'oracles':[]},worker.apply_log)
    assert borrower in u.books[m.id].identities()
    assert u.choose()[0].id==m.id

def test_new_factory_and_oracle_events_bypass_history_without_claiming_coverage():
    rpc=RPC();rpc.height=50_000;rpc.logs=[log(F,49_999),log(O,49_999,1),log(F,100)]
    reader=EventIntake(legacy_cursor=99);got=[]
    for _ in range(2):result=reader.poll(rpc,GROUPS,lambda _,x:got.append((x['address'],int(x['blockNumber'],16))),['topic'])
    assert (F,49_999) in got and (O,49_999) in got and (F,100) in got
    assert reader.cursors[F]<1000 and reader.cursors[O]<1000
    assert result['recent_lag_by_group']['oracles']==0 and result['lag_blocks']>40_000
    assert got.count((F,49_999))==1

def test_historical_span_and_address_width_recover_but_stay_provider_bounded():
    rpc=RPC();rpc.height=100_000;reader=EventIntake({O:0});reader.width['oracles']=1
    for _ in range(40):assert reader.read(rpc,'oracles',[O],rpc.header(),lambda *_:None)
    assert reader.width['oracles']==16 and reader.span['oracles']==1000
    assert all(int(q['toBlock'],16)-int(q['fromBlock'],16)+1<=1000 for q in rpc.calls)

def test_recent_reorg_does_not_advance_recent_or_historical_coverage():
    rpc=RPC();rpc.height=1000;rpc.reorg=True;rpc.logs=[log(F,999)];reader=EventIntake({F:99})
    assert not reader.read(rpc,'factories',[F],rpc.header(),lambda *_:pytest.fail('fork event applied'),['topic'],recent=True)
    assert reader.cursors[F]==99 and F not in reader.recent_cursors

def test_recent_group_progress_does_not_claim_unread_addresses():
    rpc=RPC();reader=EventIntake({F:0,O:0});rpc.height=1000
    reader.read(rpc,'factories',[F],rpc.header(),lambda *_:None,['topic'],recent=True)
    assert reader.recent_cursors[F]==1001 and O not in reader.recent_cursors
    assert reader.cursors=={F:0,O:0}

def test_provider_range_limit_in_error_data_shrinks_without_skipping_page():
    from searcher.core import RPCError
    rpc=RPC();reader=EventIntake({O:99});reader.width['oracles']=1;reader.span['oracles']=1000
    def request(*_):raise RPCError('eth_getLogs',{'code':-32602,'message':'invalid params',
        'data':'Block range too large for public access: maximum 1000 blocks.'})
    rpc.request=request
    assert not reader.read(rpc,'oracles',[O],rpc.header(),lambda *_:None)
    assert reader.span['oracles']==500 and reader.cursors[O]==99
