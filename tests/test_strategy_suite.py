"""Parallel strategy isolation and full signed execution on a local EVM."""
import copy
import json
import threading
import time
import pytest
from eth_abi import encode
from eth_utils import keccak
from searcher.core import USDC,Reject,data,unhex
from searcher.protocol_execution import route,deployment,EVC,MIDNIGHT,MARKET,OFFER,TAKE,midnight_id
from searcher.structural_runner import complete_quote
from searcher.execution import Executor
from searcher.context import simulation_context
from searcher.strategy_suite import StopScope,Mailbox,Job,ReadBudget
from test_structural import structural_market
from test_execution import market,artifact

C='0x0000000000000000000000000000000000000111';P='0x0000000000000000000000000000000000000222'
F='0x33128a8fc17869897dce68ed026d694621f6fdfd';COMET='0xb125e6687d4313864e53df431d5425969c15eb2f';BLUE='0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb'
BORROWER='0x'+'77'*20
H=[(P,C,USDC,1)]

def test_child_crash_does_not_stop_parent_or_sibling():
    parent=threading.Event();a=StopScope(parent);b=StopScope(parent)
    a.set();assert a.is_set() and not parent.is_set() and not b.is_set()
    parent.set();assert b.wait(100)

def test_latest_slots_expire_and_do_not_build_unbounded_queue():
    box=Mailbox()
    for n in range(5000):box.offer(Job('euler',{'gross_surplus':n,'minimum_gross_surplus':0},None,{},None,100))
    box.offer(Job('dex',{'gross_surplus':10,'minimum_gross_surplus':0},None,{},None,1000))
    assert len(box.slots)==2 and box.take(101).lane=='dex' and box.take(102) is None

def test_cancelled_rate_wait_releases_its_ticket():
    budget=ReadBudget(4)
    class R:
        def check_cancelled(self):raise Reject('stopped')
    with pytest.raises(Reject):budget.acquire(R(),1)
    assert not budget.queue

@pytest.fixture
def protocol_market(structural_market):
    rpc,c,old,structural,signer,directory,_,send=structural_market
    def code(addr,file,name):rpc.request('anvil_setCode',[addr,artifact(file,name)['deployedBytecode']['object']])
    code('0x0000000000000000000000000000000000000333','ProtocolExecutor.t.sol','SuiteOracleMock')
    code(C,'StructuralExecutor.t.sol','StructuralTokenMock');code(P,'StructuralExecutor.t.sol','StructuralPoolMock')
    send(P,data('initialize(address,address,address,uint256,uint256)',['address']*3+['uint256']*2,[F,C,USDC,1,1]));send(F,data('add(address,address,address)',['address']*3,[C,USDC,P]))
    for addr,name in [(BLUE,'SuiteBlueMock'),(COMET,'SuiteCometMock'),(MIDNIGHT,'SuiteMidnightMock'),(EVC,'SuiteEVCMock')]:code(addr,'ProtocolExecutor.t.sol',name)
    for t in (C,USDC):
        for addr in (BLUE,COMET,MIDNIGHT,P):send(t,data('mint(address,uint256)',['address','uint256'],[addr,10**12]))
    contracts={}
    for name in ('ProtocolExecutor','BookExecutor'):
        contracts[name]=send(calldata=deployment(c,name)['creation_data'])['contractAddress'].lower()
        send(contracts[name],data('setPaused(bool)',['bool'],[False]))
    def deploy(name):return send(calldata=artifact('ProtocolExecutor.t.sol',name)['bytecode']['object'])['contractAddress'].lower()
    pre=deploy('SuitePreMock');send(C,data('mint(address,uint256)',['address','uint256'],[pre,10**12]))
    dv=deploy('SuiteVaultMock');cv=deploy('SuiteVaultMock');send(dv,data('initialize(address)',['address'],[USDC]));send(cv,data('initialize(address)',['address'],[C]));send(C,data('mint(address,uint256)',['address','uint256'],[cv,10**12]))
    m=(8453,MIDNIGHT,USDC,[(C,8*10**17,3*10**17,'0x0000000000000000000000000000000000000333')],2000,0,'0x'+'00'*20,'0x'+'00'*20)
    send(EVC,data('setTestCollateral(address)',['address'],[cv]))
    market_id=unhex(midnight_id(m))
    o=(m,False,BORROWER,0,2**64,1,b'\x00'*32,'0x'+'00'*20,b'','0x'+'00'*20,'0x'+'00'*20,False,10**7,10**7,0)
    b=list(o);b[1]=True;b[5]=2
    routes=[route(1,[COMET,C,[],H,[]],(C,USDC)),route(2,[dv,BORROWER,cv,H,[]],(C,USDC,cv)),route(3,[pre,BORROWER,10**7,H,[]],(C,USDC)),route(4,[market_id,BORROWER,0,False,H,[]],(C,USDC)),route(5,[[(o,b'',10**7)],[(tuple(b),b'',10**7)],[]],(USDC,))]
    yield rpc,c,old,structural,signer,directory,contracts,routes,send

@pytest.mark.parametrize('strategy',range(1,6))
def test_each_protocol_full_quote_sign_submission_receipt_and_net_accounting(protocol_market,strategy):
    rpc,c,old,structural,signer,directory,contracts,routes,send=protocol_market
    ex=Executor(rpc,c,old,signer,directory,structural_contract=structural,protocol_contracts=contracts)
    try:
        r=routes[strategy-1];h=ex.context['head'];clock=simulation_context(h,ex.context['pending_head'])
        # Use a larger opportunity to clear unchanged configured fee + net floor.
        amount=10**7
        candidate,started=complete_quote(rpc,c,contracts[r.contract_name],r,amount,h,clock,ex.context['nonce'])
        assert candidate['gross_surplus']==1000000
        out=ex.execute(candidate,r,{},h,time.monotonic_ns()+5_000_000_000)
        assert out['status']=='submitted'
        rpc.request('evm_mine',[]);ex.reconcile()
        assert ex.db.execute('SELECT state FROM attempts').fetchone()[0]=='success'
        receipt=json.loads(ex.db.execute('SELECT outcome FROM intents').fetchone()[0])
        assert receipt['execution_kind']=='protocol-v1' and receipt['gross_profit_raw']==1000000 and receipt['net_after_reserved_fee_raw']>0
    finally:ex.close()

def test_two_lanes_cannot_reserve_same_wallet_budget_or_nonce(protocol_market):
    rpc,c,old,structural,signer,directory,contracts,routes,send=protocol_market
    ex=Executor(rpc,c,old,signer,directory,structural_contract=structural,protocol_contracts=contracts)
    try:
        h=ex.context['head'];clock=simulation_context(h,ex.context['pending_head']);ready=[]
        for r in (routes[0],routes[2]):ready.append((r,complete_quote(rpc,c,contracts[r.contract_name],r,10**7,h,clock,ex.context['nonce'])[0]))
        assert ex.execute(ready[0][1],ready[0][0],{},h,time.monotonic_ns()+5_000_000_000)['status']=='submitted'
        with pytest.raises(Reject,match='unresolved'):ex.execute(ready[1][1],ready[1][0],{},h,time.monotonic_ns()+5_000_000_000)
        assert ex.db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0]==1
    finally:ex.close()

def test_competitor_consumed_inventory_is_rejected_before_signing(protocol_market):
    rpc,c,old,structural,signer,directory,contracts,routes,send=protocol_market
    ex=Executor(rpc,c,old,signer,directory,structural_contract=structural,protocol_contracts=contracts)
    try:
        r=routes[0];h=ex.context['head'];clock=simulation_context(h,ex.context['pending_head'])
        candidate,_=complete_quote(rpc,c,contracts[r.contract_name],r,10**7,h,clock,ex.context['nonce'])
        send(COMET,data('consume()'));ex.refresh_context();h=ex.context['head']
        with pytest.raises(Reject):ex.execute(candidate,r,{},h,time.monotonic_ns()+5_000_000_000)
        assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
    finally:ex.close()


def test_constructor_budget_preserves_cap_and_requires_margin():
    from searcher.onboarding import setup_gas_limit
    assert setup_gas_limit(5_600_000,capped_deployment=True)==6_000_000
    with pytest.raises(Reject):setup_gas_limit(5_900_000,capped_deployment=True)
    with pytest.raises(Reject):setup_gas_limit(5_600_000)

@pytest.mark.parametrize('name',['compound','compound_absorb','euler','preliquidation','midnight_liquidation','midnight_books'])
def test_discovery_feeds_complete_executable_profit(protocol_market,name,monkeypatch):
    from searcher.protocol_scanners import Scanners
    from searcher.morpho_universe import Universe,Pool
    import searcher.morpho_universe as mu
    import searcher.core as core
    from eth_abi import decode
    rpc,c,old,structural,signer,directory,contracts,routes,send=protocol_market
    from searcher.compound_accounts import CompoundAccounts
    import searcher.compound_accounts as ca
    lane='compound' if name=='compound_absorb' else name
    catalog=CompoundAccounts([COMET],c,threading.Event(),lambda **v:None,path=directory/'accounts.json')
    if name=='compound_absorb':
        send(COMET,data('prepareAbsorb(address,uint256)',['address','uint256'],[BORROWER,11000000]))
        catalog.accounts[COMET][BORROWER]=1
    catalog.refresh=lambda:None
    monkeypatch.setattr(ca,'CompoundAccounts',lambda *args,**kwargs:catalog)
    monkeypatch.setattr(mu,'MULTICALL_HASH',core.MULTICALL_HASH)
    u=Universe(c)
    # Authenticate the canonical mock pool before scanners can select it.
    canonical={'address':F,'kind':'v3','keys':[500]};u.graph.factories[F]=canonical
    u.graph.authenticate(rpc,[Pool(P,C,USDC,F,1,500)])
    euler_values=decode(['('+','.join(__import__('searcher.protocol_execution',fromlist=['TYPES']).TYPES[2])+')'],routes[1].arguments)[0]
    pre_values=decode(['('+','.join(__import__('searcher.protocol_execution',fromlist=['TYPES']).TYPES[3])+')'],routes[2].arguments)[0]
    mid_args=decode(['('+','.join(__import__('searcher.protocol_execution',fromlist=['TYPES']).TYPES[4])+')'],routes[3].arguments)[0]
    m=decode([TAKE+'[]',TAKE+'[]','(address,address,address,uint8)[]'],routes[4].arguments)[0][0][0][0]
    wire_m={'chain_id':8453,'midnight':MIDNIGHT,'loan_token':USDC,'collaterals':[{'token':C,'lltv':str(8*10**17),'liquidation_cursor':str(3*10**17),'oracle':'0x0000000000000000000000000000000000000333'}], 'maturity':2000,'rcf_threshold':'0','enter_gate':'0x'+'00'*20,'liquidator_gate':'0x'+'00'*20}
    def offer(buy):return {'market_id':midnight_id(m),'units':'10000000','ratifier_data':'0x','offer':{'market':wire_m,'buy':buy,'maker':BORROWER,'start':'0','expiry':str(2**64),'tick':'2' if buy else '1','group':'0x'+'00'*32,'callback':'0x'+'00'*20,'callback_data':'0x','receiver_if_maker_is_seller':'0x'+'00'*20,'ratifier':'0x'+'00'*20,'reduce_only':False,'max_units':'10000000','max_assets':'10000000','continuous_fee_cap':'0'}}
    class Response:
        def __init__(self,v):self.v=v
        def raise_for_status(self):pass
        def json(self):return self.v
    class Client:
        def post(self,url,json):
            q=json['query']
            if 'trackingVaultBalances' in q:return Response({'data':{'trackingVaultBalances':[{'id':'a','vault':euler_values[0],'account':BORROWER,'debt':'10000000'}]}})
            if 'marketPositions' in q:return Response({'data':{'marketPositions':{'items':[{'user':{'address':BORROWER}}],'pageInfo':{'countTotal':1}}}})
            return Response({'data':{'markets':{'items':[{'marketId':'0x'+'11'*32,'preLiquidations':{'items':[{'address':pre_values[0]}],'pageInfo':{'countTotal':1}}}],'pageInfo':{'countTotal':1}}}})
        def get(self,url,params):
            if 'liquidation-candidates' in url:rows=[{'chain_id':8453,'market_id':midnight_id(m),'borrower':BORROWER}]
            elif '/books/' in url:rows=[offer('/bids/' in url)]
            else:rows=[{'market_id':midnight_id(m),'loan_token':USDC,'lend_depth':1,'borrow_depth':1}]
            return Response({'cursor':None,'data':rows})
    scanner=Scanners(rpc,Client(),c,u,contracts,lambda **v:None,threading.Event())
    iterator=getattr(scanner,lane)();proposal=None
    for _ in range(10):
        proposal=next(iterator)
        if proposal:break
    assert proposal is not None
    r,amount=proposal
    if name=='compound_absorb':
        from searcher.protocol_execution import TYPES
        decoded=decode(['('+','.join(TYPES[1])+')'],r.arguments)[0]
        assert list(decoded[2])==[BORROWER]
        assert int.from_bytes(rpc.call(COMET,data('getCollateralReserves(address)',['address'],[C]),'latest'),'big')==0
    ex=Executor(rpc,c,old,signer,directory,structural_contract=structural,protocol_contracts=contracts)
    try:
        h=ex.context['head'];clock=simulation_context(h,ex.context['pending_head'])
        candidate,started=complete_quote(rpc,c,contracts[r.contract_name],r,amount,h,clock,ex.context['nonce'])
        assert candidate['gross_surplus']>=candidate['minimum_gross_surplus']
        from searcher.strategy_suite import Suite
        suite=Suite(c,u,contracts,lambda:ex.context,threading.Event(),directory,rpc.url)
        suite.report(name,state='scanning')
        suite.offer(name,candidate,r,{},h,started)
        assert suite.consume(ex)['status']=='submitted'
        assert suite.health[name]['complete_quotes']==suite.health[name]['submitted']==1
        rpc.request('evm_mine',[]);ex.reconcile()
        assert ex.db.execute('SELECT state FROM attempts').fetchone()[0]=='success'
    finally:ex.close()


def test_lost_opportunity_releases_executor_for_next_profitable_lane(protocol_market):
    from searcher.strategy_suite import Suite
    from types import SimpleNamespace
    rpc,c,old,structural,signer,directory,contracts,routes,send=protocol_market
    ex=Executor(rpc,c,old,signer,directory,structural_contract=structural,protocol_contracts=contracts)
    suite=Suite(c,SimpleNamespace(),contracts,lambda:ex.context,threading.Event(),directory,rpc.url)
    try:
        h=ex.context['head'];clock=simulation_context(h,ex.context['pending_head'])
        r=routes[0];candidate,started=complete_quote(rpc,c,contracts[r.contract_name],r,10**7,h,clock,ex.context['nonce'])
        suite.report('compound',state='scanning');suite.offer('compound',candidate,r,{},h,started)
        send(COMET,data('consume()'));ex.refresh_context()
        assert suite.consume(ex)['status']=='execution_rejected'
        assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
        r=routes[2];h=ex.context['head'];clock=simulation_context(h,ex.context['pending_head'])
        candidate,started=complete_quote(rpc,c,contracts[r.contract_name],r,10**7,h,clock,ex.context['nonce'])
        suite.report('preliquidation',state='scanning');suite.offer('preliquidation',candidate,r,{},h,started)
        assert suite.consume(ex)['status']=='submitted'
        rpc.request('evm_mine',[]);ex.reconcile()
        assert ex.db.execute('SELECT state FROM attempts').fetchall()==[('success',)]
    finally:ex.close()

@pytest.mark.parametrize('name',['ProtocolExecutor','BookExecutor'])
def test_new_contract_setup_recovers_lost_response_and_is_idempotent(protocol_market,monkeypatch,name):
    from searcher.protocol_onboarding import setup_executor,preflight_all
    rpc,c,old,structural,signer,directory,contracts,routes,send=protocol_market
    original=rpc.request;sent=[]
    def request(method,params):
        result=original(method,params)
        if method=='eth_sendRawTransaction':
            sent.append(result);original('evm_mine',[])
            if len(sent)==1:raise TimeoutError('test lost deployment response')
        return result
    monkeypatch.setattr(rpc,'request',request)
    out=directory/'new-protocol-setup'
    first=setup_executor(rpc,c,signer,out,require_validation=False,name=name)
    assert first['paused'] is False and len(sent)==2
    second=setup_executor(rpc,c,signer,out,require_validation=False,name=name)
    assert first['contract']==second['contract'] and len(sent)==2
    check=preflight_all(rpc,c,out)
    assert check['contracts'][name]['state']=='ready' and check['balance_sufficient']


def test_fresh_frame_reserves_shared_budget_before_clock_and_charges_retries(monkeypatch):
    from searcher.strategy_suite import SuiteRPC
    from searcher.core import RPC
    events=[]
    class Budget:
        def acquire(self,rpc,count):events.append(('suite',count))
    class Limiter:
        def acquire(self,count,*args,**kwargs):events.append(('public',count))
    rpc=SuiteRPC.__new__(SuiteRPC);rpc.suite_budget=Budget();rpc.public_rate_limiter=Limiter();rpc.cancel_event=None
    def post(self,payload,**kwargs):
        count=len(payload)
        if self.public_rate_limiter:self.public_rate_limiter.acquire(count)
        events.append(('read',count));return None
    monkeypatch.setattr(RPC,'post',post)
    original=rpc.public_rate_limiter
    with pytest.raises(RuntimeError):
        with rpc.frame_reads():
            events.append(('clock_selected',0))
            rpc.post([{},{}]);rpc.post([{},{}])
            rpc.post([{},{}]) # A retry must pay again, never reuse the lease.
            raise RuntimeError('frame interrupted')
    assert events==[('suite',4),('public',4),('clock_selected',0),('read',2),('read',2),('suite',2),('public',2),('read',2)]
    assert rpc.public_rate_limiter is original and rpc.public_prepaid_reads==0 and rpc.prepaid_reads==0
