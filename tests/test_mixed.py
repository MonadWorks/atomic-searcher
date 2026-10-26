import copy
import random
import time
from types import SimpleNamespace
import pytest
from eth_abi import encode, decode
from searcher.core import Incomplete, Reject, data, number, load_config
from searcher.pools import Pool, validate_pools
from searcher.routes import RouteIndex, marginal_profitable, gross_upper_bound, route_capacity, optimize
from searcher.statefeed import decode_packed, StateFeed, DeepReadScheduler
from searcher.universe import CyclePages, ActivityHints
from searcher.events import V2_SYNC, StateBook
from searcher.engine import make_hops, evaluate
from test_execution import market, artifact

def addr(n): return '0x'+f'{n:040x}'
A,B,C,D=[addr(n) for n in range(1,5)]
FACTORY=addr(900)
def v2(n,a=A,b=B,r0=10**9,r1=2*10**9):
    return Pool(addr(n),FACTORY,'v2',a,b,0,3000,reserve0=r0,reserve1=r1)

def test_v2_quote_integer_invariant_and_full_reserve_limit():
    rng=random.Random(822)
    for _ in range(200):
        rin,rout,x=[rng.randrange(1,10**20) for _ in range(3)]
        p=v2(10,r0=rin,r1=rout);out,_=p.quote(A,x)
        assert out == x*997*rout//(rin*1000+x*997)
        assert ((rin+x)*1000-x*3)*(rout-out)>=rin*rout*1000
        if out+1<rout: assert ((rin+x)*1000-x*3)*(rout-out-1)<rin*rout*1000
        assert p.quote(B,x)[0] == x*997*rin//(rout*1000+x*997)
    p=v2(10);cap=(1<<112)-1-p.reserve0
    p.quote(A,cap)
    with pytest.raises(Incomplete):p.quote(A,cap+1)

def test_four_hop_index_both_directions_and_fair_cycle_pages():
    pools=[v2(10,A,B),v2(11,B,C),v2(12,C,D),v2(13,A,D)]
    assert not RouteIndex(pools,[A],3).routes
    ix=RouteIndex(pools,[A],4);assert len(ix.routes)==2
    assert all(len(r.pools)==4 and len(set(r.tokens[:-1]))==4 for r in ix.routes.values())
    page=CyclePages(pools,[A],4);assert len(page.take(4))==4
    scheduler=DeepReadScheduler();scheduler.add([list(ix.states)],time.monotonic())
    assert scheduler.take(ix.states,time.monotonic()) == set(ix.states)

def test_v2_upper_bound_never_rejects_discrete_profit_and_sizing_finds_edge():
    rng=random.Random(512)
    for _ in range(30):
        pools=[v2(10,r0=rng.randrange(1000,10000),r1=rng.randrange(1000,10000)),v2(11,r0=rng.randrange(1000,10000),r1=rng.randrange(1000,10000))]
        ix=RouteIndex(pools,[A],2)
        for r in ix.routes.values():
            best=max(r.quote(ix.states,x)[0]-x for x in range(1,1001))
            bound=gross_upper_bound(r,ix.states)
            assert bound>=best
            if best>0:assert marginal_profitable(r,ix.states)
            found=optimize(r,ix.states,1000)
            assert found['gross_surplus'] >= best-3

def test_packed_v2_decoder_and_sync_event_do_not_invent_cl_state():
    p=v2(10);header={'number':'0x7','hash':'0x'+'cc'*32}
    raw=b'\2'+(7).to_bytes(32,'big')+(123).to_bytes(32,'big')+b'\0\1'+bytes.fromhex(p.address[2:])+b'\2'+p.reserve0.to_bytes(14,'big')+p.reserve1.to_bytes(14,'big')+(3000).to_bytes(3,'big')
    states,b,t=decode_packed('0x'+encode(['bytes'],[raw]).hex())
    assert (b,t)==(7,123) and states[0][9:]==(p.reserve0,p.reserve1)
    for i in range(len(raw)):
        with pytest.raises(Reject):decode_packed('0x'+encode(['bytes'],[raw[:i]]).hex())
    event={'topics':[V2_SYNC],'address':p.address,'data':'0x'+encode(['uint112','uint112'],[333,444]).hex(),
           'blockHash':header['hash'],'transactionHash':'0x'+'aa'*32,'transactionIndex':'0x0','logIndex':'0x1'}
    book=StateBook([p],header);assert book.apply_visible_batch([event])=={p.address}
    assert (p.reserve0,p.reserve1)==(333,444)
    assert ActivityHints().accept([event])=={p.address}

def test_v2_authentication_allows_missing_cl_metadata_but_rejects_wrong_pair():
    p=v2(10)
    class RPC:
        good=True
        def views(self,calls,block):
            if len(calls)==5:return [encode(['address'],[x]) for x in (FACTORY,A,B)]+[None,None]
            return [encode(['address'],[p.address if self.good else addr(99)])]
    rpc=RPC();c={'factories':[{'address':FACTORY,'kind':'v2'}]}
    valid,errors=validate_pools(rpc,c,[p.address],123);assert len(valid)==1 and not errors
    rpc.good=False;valid,errors=validate_pools(rpc,c,[p.address],123);assert not valid and errors


def test_snapshot_reserve_only_change_requeues_routes(monkeypatch):
    import searcher.pipeline as pipeline
    pools=[v2(10),v2(11,r1=10**9)]
    index=RouteIndex(pools,[A],4);index.work_queue=[];index.queued_routes=set()
    # Construct through its public constructor with a harmless read-only stub.
    cls=pipeline.PendingStrategy
    config=load_config();config['settlement_tokens']=[dict(config['settlement_tokens'][0],address=A)]
    header={'number':'0x1','hash':'0x'+'ab'*32,'baseFeePerGas':'0x1'}
    saved={'header':header,'pools':[p.json() for p in pools]}
    strategy=cls(SimpleNamespace(),config,saved)
    seen=[]
    def evaluate(index,config,header,changed):
        seen.append(changed);index.last_evaluated_keys=[]
        return {'candidates':[]}
    monkeypatch.setattr(pipeline,'evaluate',evaluate)
    from searcher.statefeed import StateFrame
    def frame(ps):return StateFrame(header,header,[p.json() for p in ps],1,2,time.time(),2,4)
    strategy.on_snapshot(frame(pools));pools=copy.deepcopy(pools);pools[0].reserve0+=999
    strategy.on_snapshot(frame(pools));assert seen[-1]=={pools[0].address}


def test_low_decimal_v2_output_plateau_is_sized_at_real_edge():
    pools=[v2(10,r0=10000,r1=5),v2(11,r0=20000,r1=5)]
    ix=RouteIndex(pools,[A],2)
    route=next(r for r in ix.routes.values() if r.pools[0] == addr(10))
    maximum,_=route_capacity(route,ix.states)
    result=optimize(route,ix.states,maximum,lambda _:500)
    assert result['amount_in']==2508
    assert result['gross_surplus']==816
    assert result['net_after_modelled_cost']==316


def test_v2_large_output_plateau_seed_recovers_profitable_triangle():
    pools=[v2(10,A,B,100000000,129),v2(11,B,C,129,105000000),v2(12,A,C,10**18,10**18)]
    ix=RouteIndex(pools,[A],3);route=next(r for r in ix.routes.values() if r.pools[0]==addr(10))
    maximum,_=route_capacity(route,ix.states)
    result=optimize(route,ix.states,maximum)
    assert result['gross_surplus']>=19270
    assert result['amount_in']==783601


def test_mixed_atomic_read_probe_and_signed_executor_settlement(market):
    from eth_abi import encode
    from searcher.core import WETH, USDC
    from searcher.engine import probe_cycle
    from searcher.execution import Executor
    rpc,c,_,signer,directory,old_index,_,_,send=market
    def deploy(file,name,types=(),values=()):
        return send(calldata=artifact(file,name)['bytecode']['object']+encode(types,values).hex())['contractAddress'].lower()
    factory=deploy('MixedCycle.t.sol','V2FactoryMock')
    pair=deploy('MixedCycle.t.sol','V2PairMock',['address']*3,[factory,WETH,USDC])
    send(factory,data('add(address,address,address)',['address']*3,[WETH,USDC,pair]))
    for token,amount in ((WETH,10**21),(USDC,2*10**21)):
        send(token,data('mint(address,uint256)',['address','uint256'],[pair,amount]))
    send(pair,data('sync()'))
    cl=next(p for p in old_index.states.values() if p.fee==500)
    contract=deploy('AtomicCycle.sol','AtomicCycle',['address','uint256','address[]','uint8[]','address[]','uint256[]'],
                   [signer.address,8453,[factory,cl.factory],[3,1],[WETH],[10**20]])
    send(contract,data('setPaused(bool)',['bool'],[False]))
    p=Pool(pair,factory,'v2',WETH,USDC,0,3000,reserve0=10**21,reserve1=2*10**21)
    head=rpc.header();p.block=cl.block=number(head['number']);p.block_hash=cl.block_hash=head['hash']
    # Real deployed lens bytecode reads the actual V2 pair on this local chain.
    read_config=copy.deepcopy(c);read_config['policy']['cycle_pages']=False
    frame=StateFeed(rpc,read_config,{'pools':[p.json()]}).poll(number(head['number']))
    actual=Pool.from_json(frame.snapshot_pools[0]);assert actual.valid
    assert (actual.reserve0,actual.reserve1)==(p.reserve0,p.reserve1)
    ix=RouteIndex([p,cl],[WETH],4);report=evaluate(ix,c,head)
    candidate=next(x for x in report['candidates'] if x['pools'][0]==pair)
    route=ix.routes[candidate['route_key']]
    evidence=probe_cycle(rpc,route,ix.states,candidate['amount_in'],candidate['minimum_gross_surplus'],number(head['number']))
    assert evidence['gross_surplus']>candidate['minimum_gross_surplus']
    executor=Executor(rpc,c,contract,signer,directory/'mixed-executor')
    try:
        executor.refresh_context()
        result=executor.execute(candidate,route,ix.states,head,time.monotonic_ns()+2_000_000_000)
        assert result['status']=='submitted'
        for _ in range(3):rpc.request('evm_mine',[])
        executor.reconcile()
        row=executor.db.execute('SELECT state,outcome FROM attempts JOIN intents ON attempts.id=intents.attempt').fetchone()
        import json
        assert row[0]=='success' and json.loads(row[1])['net_profit_raw']>0
    finally:executor.close()


def test_streamed_cycle_groups_match_independent_bruteforce_with_v2_last_leg():
    rng=random.Random(8348)
    for _ in range(12):
        tokens=[addr(i) for i in range(1,7)];pools=[]
        for n in range(16):
            a,b=sorted(rng.sample(tokens,2));p=v2(100+n,a,b)
            if rng.randrange(3):p.kind='v3';p.spacing=60
            pools.append(p)
        expected=set()
        def brute(token, assets, path):
            for p in pools:
                if token not in (p.token0,p.token1) or p.address in path:continue
                nxt=p.other(token);trail=path+(p.address,)
                if nxt==A and len(trail)>=2:
                    if len(trail)<4 or any(q.kind=='v2' for q in pools if q.address in trail):expected.add(trail)
                elif nxt not in assets and len(trail)<4:brute(nxt,assets+(nxt,),trail)
        brute(A,(A,),())
        ix=RouteIndex(pools,[A],4)
        assert {r.pools for r in ix.routes.values()}==expected
        pages=CyclePages(pools,[A],4)
        assert set(pages.groups)=={tuple(sorted(t)) for t in expected}


def test_four_hop_page_rejects_unusable_capacity():
    from searcher.core import Reject
    pools=[v2(10,A,B),v2(11,B,C),v2(12,C,D),v2(13,A,D)]
    with pytest.raises(Reject,match='small'):CyclePages(pools,[A],4).take(3)


def test_factory_expansion_revisits_cached_tokens_and_progresses_to_quiet_tokens():
    from searcher.engine import expansion_plan
    c=load_config();c['seed_tokens']=[A];c['policy']['max_new_tokens_per_refresh']=1
    known={p.address:p for p in [v2(10,A,B),v2(11,B,C)]}
    tokens,expanded,key=expansion_plan(c,{'expanded_tokens':[A,B,C]},known,{})
    assert expanded==set() and B in tokens
    previous={'expanded_tokens':tokens,'factory_registry_id':key}
    tokens,expanded,_=expansion_plan(c,previous,known,{})
    assert C in tokens and B in expanded
    changed=copy.deepcopy(c);changed['factories']=changed['factories'][:-1]
    _,expanded,new_key=expansion_plan(changed,previous,known,{})
    assert new_key!=key and not expanded


def test_quiet_pairs_across_token_batches_are_queried_and_resume_after_failure(monkeypatch):
    from collections import Counter
    import searcher.engine as engine
    c=load_config();c['seed_tokens']=[A]
    c['policy'].update(max_new_tokens_per_refresh=1,cycle_pages=False,discovery_mode='logs')
    old_factory=addr(901)
    c['factories']=[{'address':old_factory,'kind':'v3','keys':[3000]},
                    {'address':FACTORY,'kind':'v2','keys':[]}]
    pools=[v2(10,A,B),v2(11,B,C),v2(12,C,D),v2(13,A,D)]
    for p in pools:p.kind='v3';p.factory=old_factory;p.spacing=60
    header={'number':'0x64','hash':'0x'+'ab'*32}
    class RPC:
        fail=False
        pairs=set()
        def verify_chain(self):pass
        def header(self,*args):return header
        def views(self,calls,block):
            replies=[]
            for target,calldata in calls:
                if target==FACTORY:
                    self.pairs.add(tuple(decode(['address','address'],bytes.fromhex(calldata[10:]))))
                    if self.fail:
                        self.fail=False;replies.append(None);continue
                replies.append(encode(['address'],[addr(0)]))
            return replies
    class Feed:
        def __init__(self,rpc,config,saved):self.pools=saved['pools']
        def poll(self,height):return SimpleNamespace(snapshot_pools=self.pools)
    monkeypatch.setattr('searcher.universe.swap_activity',lambda *args:(Counter(),{}))
    monkeypatch.setattr('searcher.statefeed.StateFeed',Feed)
    rpc=RPC();previous={'known_pools':[p.json() for p in pools]}
    previous=engine.prepare(rpc,c,previous=previous)
    checkpoint=copy.deepcopy(previous);rpc.fail=True
    with pytest.raises(Reject,match='factory lookup unavailable'):engine.prepare(rpc,c,previous=previous)
    assert previous==checkpoint
    for _ in range(5):previous=engine.prepare(rpc,c,previous=previous)
    expected={(A,B),(B,C),(C,D),(A,D)}
    assert expected <= rpc.pairs
    assert set(map(tuple,previous['expanded_factory_pairs']))==expected
    assert not previous['pending_factory_pairs']
    # Registry changes invalidate pair progress independently of token batches.
    chosen,done,pending=engine.pair_expansion_plan(c,previous,{p.address:p for p in pools},'new-registry')
    assert not done and set(chosen+pending)==expected
