import json
import threading
import time
from types import SimpleNamespace
from pathlib import Path
import pytest
from eth_abi import encode,decode
from eth_utils import keccak
from searcher.core import USDC,data,unhex,Reject
from searcher.venue import Edge,cycles,cycle_route,created_address,plan_batch,Catalog,KEY
from searcher.protocol_execution import deployment,encode_plan,contract_names
from searcher.context import simulation_context
from searcher.structural_runner import complete_quote
from searcher.execution import Executor
from test_execution import market,artifact
from test_structural import structural_market

X='0x'+'00'*18+'0111';Y='0x'+'00'*18+'0222'
M='0x498581ff718922c3f8e6a244956af099b2652b2b';B='0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb'

def test_all_mixed_directions_discovered_with_distinct_v4_pool_ids():
    a=Edge('0x'+'ab'*20,USDC,X,5,b'1'*32)
    b=Edge('0x'+'ac'*20,Y,USDC,5,b'2'*32)
    keys=[(X,Y,fee,1,'0x'+'00'*20) for fee in (100,500)]
    edges=[a,b]+[Edge(M,X,Y,6,encode([KEY],[k])) for k in keys]
    paths=[p for p in cycles(edges) if p]
    assert len(paths)==4
    routes=[cycle_route(p) for p in paths]
    assert len({r.key for r in routes})==4
    assert all(r.contract_name=='VenueExecutor' and r.tokens[0]==USDC for r in routes)
    assert len([p for p in cycles(edges,2) if p])==0

def test_quote_encoder_and_final_binding_are_consistent():
    r=cycle_route([('0x'+'ab'*20,USDC,X,5,b'1'*32),('0x'+'ac'*20,X,USDC,5,b'2'*32)])
    c={'execution_kind':'protocol-v1','route_key':r.key,'pools':r.pools,'tokens':r.tokens}
    common=[None,10**6,100000,100,1000,99,b'1'*32,101,1]
    assert encode_plan(c,r,common).startswith('0x') and plan_batch([r],[10**6]).startswith('0x')
    with pytest.raises(Reject):encode_plan({**c,'route_key':'other'},r,common)
    assert 'VenueExecutor' in contract_names({'venues':{'enabled':True}})
    assert 'VenueExecutor' not in contract_names({})

def test_factory_enumeration_page_retries_without_skipping(monkeypatch,tmp_path):
    import searcher.venue as v
    monkeypatch.setattr(v,'FACTORY_HASH','0x'+keccak(b'factory').hex())
    monkeypatch.setattr(v,'authenticate_runtime',lambda raw:'0x'+keccak(unhex(raw)).hex())
    class R:
        fail=True
        def header(self,*args):return {'number':'0x10','hash':'0x'+'12'*32}
        def request(self,m,p):
            if m=='eth_getTransactionCount':return '0x4'
            if p[0]==v.FACTORY:return '0x'+b'factory'.hex()
            if p[0]==created_address(2) and self.fail:raise TimeoutError()
            return '0x1234'
        def batch(self,calls):return ['0x'+encode(['address'],[t]).hex() for t in (USDC,X)]
    r=R();c=Catalog(tmp_path/'catalog.json')
    with pytest.raises(TimeoutError):c.refresh(r)
    assert c.cursor==1
    r.fail=False;assert c.refresh(r) and set(c.rows)=={created_address(n) for n in range(1,4)}
    assert Catalog(c.path).cursor==4

@pytest.fixture
def venue_market(structural_market):
    rpc,c,old,structural,signer,directory,_,send=structural_market
    for t in (X,Y):rpc.request('anvil_setCode',[t,artifact('StructuralExecutor.t.sol','StructuralTokenMock')['deployedBytecode']['object']])
    for addr,name in [(B,'VenueBlueMock'),(M,'VenueManagerMock')]:rpc.request('anvil_setCode',[addr,artifact('VenueExecutor.t.sol',name)['deployedBytecode']['object']])
    pools=[]
    for t0,t1,n,d in ((USDC,X,1,1),(Y,USDC,11,10)):
        a=send(calldata=artifact('VenueExecutor.t.sol','VenueForteMock')['bytecode']['object'])['contractAddress'].lower()
        send(a,data('initialize(address,address,uint256,uint256)',['address','address','uint256','uint256'],[t0,t1,n,d]));pools.append(a)
    key=(X,Y,100,1,'0x'+'00'*20)
    send(M,data(f'setRate({KEY},uint256)',[KEY,'uint256'],[key,10**6]))
    for t in (USDC,X,Y):
        for a in (B,M,*pools):send(t,data('mint(address,uint256)',['address','uint256'],[a,10**12]))
    contract=send(calldata=deployment(c,'VenueExecutor')['creation_data'])['contractAddress'].lower()
    send(contract,data('setPaused(bool)',['bool'],[False]))
    hashes=[keccak(unhex(rpc.request('eth_getCode',[p,'latest']))) for p in pools]
    legs=[(pools[0],USDC,X,5,hashes[0]),(M,X,Y,6,encode([KEY],[key])),(pools[1],Y,USDC,5,hashes[1])]
    yield rpc,c,old,structural,signer,directory,contract,cycle_route(legs),send,pools

def test_discovery_quote_shared_executor_receipt_and_actual_net(venue_market,monkeypatch):
    from searcher.strategy_suite import Suite
    rpc,c,old,structural,signer,directory,contract,r,send,pools=venue_market
    ex=Executor(rpc,c,old,signer,directory,structural_contract=structural,protocol_contracts={'VenueExecutor':contract})
    try:
        raw=rpc.request('eth_call',[{'from':c['owner'],'to':contract,'data':plan_batch([r],[10**7]),'gas':hex(6000000)},'pending'])
        quotes,errors=decode(['uint256[2][]','bytes4[]'],unhex(raw));assert quotes[0][0]==10**6 and errors[0]==b'\0'*4
        h=ex.context['head'];candidate,started=complete_quote(rpc,c,contract,r,10**7,h,simulation_context(h,ex.context['pending_head']),ex.context['nonce'])
        suite=Suite(c,SimpleNamespace(),{'VenueExecutor':contract},lambda:ex.context,threading.Event(),directory,rpc.url)
        suite.report('venues',state='scanning');suite.offer('venues',candidate,r,{},h,started)
        result=suite.consume(ex);assert result['status']=='submitted'
        rpc.request('evm_mine',[]);ex.reconcile()
        assert ex.db.execute('SELECT state FROM attempts').fetchone()[0]=='success'
        outcome=json.loads(ex.db.execute('SELECT outcome FROM intents').fetchone()[0])
        assert outcome['gross_profit_raw']==1000000 and outcome['net_after_reserved_fee_raw']>0
    finally:ex.close()

def test_competitor_consumes_profit_before_submission(venue_market):
    rpc,c,old,structural,signer,directory,contract,r,send,pools=venue_market
    ex=Executor(rpc,c,old,signer,directory,structural_contract=structural,protocol_contracts={'VenueExecutor':contract})
    try:
        h=ex.context['head'];candidate,_=complete_quote(rpc,c,contract,r,10**7,h,simulation_context(h,ex.context['pending_head']),ex.context['nonce'])
        send(pools[1],data('initialize(address,address,uint256,uint256)',['address','address','uint256','uint256'],[Y,USDC,99,100]));ex.refresh_context()
        with pytest.raises(Reject):ex.execute(candidate,r,{},ex.context['head'],time.monotonic_ns()+5_000_000_000)
        assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
    finally:ex.close()

def test_scanner_discovers_mixed_route_and_admits_full_profit(venue_market,monkeypatch):
    import searcher.venue as v
    import math
    rpc,c,old,structural,signer,directory,contract,r,send,pools=venue_market
    legs=decode([v.LEG+'[]'],r.arguments)[0]
    edges=[Edge(h[0],h[1],h[2],h[3],h[4]) for h in legs]
    class Cat:
        rejected={}
        @property
        def rows(self):return {e.pool:{} for e in edges if e.kind==5}
        def refresh(self,rpc):return True
        def edges(self):return [e for e in edges if e.kind==5]
    monkeypatch.setattr(v,'live_forte',lambda rpc,e:e)
    monkeypatch.setattr(v,'v4_edges',lambda:[e for e in edges if e.kind==6])
    rates={(e.identity,t):(math.log(1.1) if e is edges[2] else 0)*(1 if t==e.a else -1) for e in edges for t in (e.a,e.b)}
    monkeypatch.setattr(v,'venue_rates',lambda rpc,e,units=None:rates)
    universe=SimpleNamespace(lock=threading.RLock(),graph=SimpleNamespace(pools={}))
    scanner=v.Scanner(rpc,c,universe,contract,threading.Event(),lambda **values:None,catalog=Cat())
    iterator=scanner.proposals();proposal=None
    for _ in range(12):
        proposal=next(iterator)
        if proposal:break
    assert proposal and proposal[0].key==r.key and proposal[1]>0
    ex=Executor(rpc,c,old,signer,directory,structural_contract=structural,protocol_contracts={'VenueExecutor':contract})
    try:
        route,amount=proposal;h=ex.context['head']
        # First tiny proposal can equal the floor before fees; larger sizes
        # follow in the same complete-cycle batch and must remain reachable.
        if amount==10**6:route,amount=next(iterator)
        candidate,_=complete_quote(rpc,c,contract,route,amount,h,simulation_context(h,ex.context['pending_head']),ex.context['nonce'])
        assert candidate['gross_surplus']>=candidate['minimum_gross_surplus']
    finally:ex.close();iterator.close()

def test_paused_or_unread_pools_wait_for_a_fresh_status(monkeypatch):
    import searcher.morpho_universe as mu
    from searcher.venue import live_forte
    edges=[Edge('0x'+str(i)*40,USDC,X,5,b'1'*32) for i in (1,2,3)]
    monkeypatch.setattr(mu,'optional_views',lambda rpc,calls:[(0).to_bytes(32,'big'),(1).to_bytes(32,'big'),None])
    assert live_forte(None,edges)==edges[:1]

def test_many_irrelevant_v4_branches_do_not_precede_every_forte_anchor():
    core=[Edge('0x'+'aa'*20,USDC,X,5,b'1'*32),Edge('0x'+'bb'*20,X,USDC,5,b'2'*32)]
    irrelevant=[Edge('0x'+format(i+1000,'040x'),USDC,'0x'+format(i+2000,'040x'),1) for i in range(500)]
    iterator=cycles(irrelevant+core);found=[]
    for _ in range(100):
        try:path=next(iterator)
        except StopIteration:break
        if path:found.append(path)
    assert len(found)==2

def test_venue_setup_is_resumable_without_redeploying(structural_market,monkeypatch):
    from searcher.protocol_onboarding import setup_executor
    from searcher.core import number
    rpc,c,old,structural,signer,directory,_,send=structural_market
    request=rpc.request
    def sealed(method,params):
        value=request(method,params)
        if method=='eth_sendRawTransaction':request('evm_mine',[])
        return value
    monkeypatch.setattr(rpc,'request',sealed)
    before=number(rpc.request('eth_getTransactionCount',[signer.address,'latest']))
    first=setup_executor(rpc,c,signer,directory,require_validation=False,name='VenueExecutor')
    after=number(rpc.request('eth_getTransactionCount',[signer.address,'latest']))
    second=setup_executor(rpc,c,signer,directory,require_validation=False,name='VenueExecutor')
    assert first['contract']==second['contract'] and not first['paused']
    assert after==before+2 and number(rpc.request('eth_getTransactionCount',[signer.address,'latest']))==after
