"""Real local-EVM financing and scanner-to-receipt tests for mixed venues."""
import json
import threading
import time
from contextlib import nullcontext
from types import SimpleNamespace

import pytest
from eth_abi import decode
from searcher.core import USDC,WETH,Reject,data,unhex,number
from searcher.structural import cycle_route,MORPHO,POOL2,HOPS,clock_call
from searcher.structural_runner import complete_quote
from searcher.execution import Executor
from searcher.strategy_suite import Suite
from searcher.context import simulation_context
from test_strategy_suite import protocol_market,C,P,F
from test_structural import structural_market
from test_execution import market,artifact

AERO='0x420dd381b31aef6683db6b902084cb0ffece40da'
Q='0x0000000000000000000000000000000000000aa1'

def setup_cycle(fixture,stable=False,aero_first=True,loan=USDC):
    rpc,c,old,structural,signer,directory,contracts,routes,send=fixture
    for addr,file,name in [(AERO,'AerodromeConversion.t.sol','AeroFactoryMock'),(Q,'AerodromeConversion.t.sol','AeroPoolMock'),(P,'FundedConversionCycle.t.sol','FundedCycleCLMock')]:
        rpc.request('anvil_setCode',[addr,artifact(file,name)['deployedBytecode']['object']])
    for token in (C,loan):send(token,data('mint(address,uint256)',['address','uint256'],[Q,10**12]))
    send(Q,data('initialize(address,address,address,bool,uint256)',['address','address','address','bool','uint256'],[AERO,C,loan,stable,75]))
    send(AERO,data('add(address,address,bool,address)',['address','address','bool','address'],[C,loan,stable,Q]))
    send(P,data('initialize(address,address,address,uint256,uint256)',['address']*3+['uint256']*2,[F,C,loan,2 if aero_first else 1,1 if aero_first else 2]))
    send(F,data('add(address,address,address)',['address']*3,[C,loan,P]))
    for token in (C,loan):send(token,data('mint(address,uint256)',['address','uint256'],[P,10**12]))
    send(loan,data('mint(address,uint256)',['address','uint256'],[MORPHO,10**12]))
    if loan==WETH:
        c['settlement_tokens'].append({'address':WETH,'symbol':'WETH','decimals':18,'max_input_raw':str(10**15),'min_net_raw':'1'})
        quoter=next(f['quoter'] for f in c['factories'] if f['kind']=='v3')
        rpc.request('anvil_setCode',[quoter,artifact('FundedConversionCycle.t.sol','FundedCycleQuoteMock')['deployedBytecode']['object']])
        for a,b,n,q in ((USDC,WETH,int(c['policy']['max_total_fee_wei']),500000),(WETH,USDC,c['policy']['min_net_profit_usdc_raw'],100000)):
            send(quoter,data('configure(address,address,uint256,uint256)',['address','address','uint256','uint256'],[a,b,n,q]))
    path=((Q,loan,C,4),(P,C,loan,1)) if aero_first else ((P,loan,C,1),(Q,C,loan,4))
    settlement=((POOL2,WETH,USDC,1),) if loan==WETH else ()
    return cycle_route(path,settlement),path

def balance(rpc,token,owner):return number(rpc.request('eth_call',[{'to':token,'data':data('balanceOf(address)',['address'],[owner])},'latest']))

@pytest.mark.parametrize('stable',[False,True])
@pytest.mark.parametrize('aero_first',[False,True])
@pytest.mark.parametrize('loan',[USDC,WETH])
def test_financing_mixed_venues_repayment_and_actual_owner_payout(protocol_market,stable,aero_first,loan):
    rpc,c,old,target,signer,directory,_,_,send=protocol_market
    route,path=setup_cycle(protocol_market,stable,aero_first,loan)
    # Pre-existing balances must survive; they cannot be counted as income.
    for token in (USDC,C,WETH):send(token,data('mint(address,uint256)',['address','uint256'],[target,12345]))
    before={t:balance(rpc,t,target) for t in (USDC,C,WETH)}
    lender_before=balance(rpc,loan,MORPHO);owner_before=balance(rpc,USDC,signer.address)
    ex=Executor(rpc,c,old,signer,directory,structural_contract=target)
    try:
        h=ex.context['head'];clock=simulation_context(h,ex.context['pending_head'])
        quote,_=complete_quote(rpc,c,target,route,10**7,h,clock,ex.context['nonce'])
        assert quote['gross_surplus']>9_000_000
        result=ex.execute(quote,route,{},h,time.monotonic_ns()+5_000_000_000)
        assert result['status']=='submitted'
        rpc.request('evm_mine',[]);ex.reconcile()
        outcome=json.loads(ex.db.execute('select outcome from intents').fetchone()[0])
        assert outcome['gross_profit_raw']==quote['gross_surplus']
        assert outcome['net_after_reserved_fee_raw']>0 and outcome['amount_semantics']=='flash_loan_principal'
        assert outcome['funding_token']==loan and outcome['settlement']==USDC
        assert balance(rpc,USDC,signer.address)-owner_before==quote['gross_surplus']
        assert balance(rpc,loan,MORPHO)==lender_before
        assert {t:balance(rpc,t,target) for t in before}==before
        assert number(rpc.request('eth_call',[{'to':loan,'data':data('allowance(address,address)',['address','address'],[target,MORPHO])},'latest']))==0
    finally:ex.close()

@pytest.mark.parametrize('limited_funding',[False,True])
def test_suite_dispatches_aero_quote_to_funded_executor_then_real_receipt(protocol_market,limited_funding):
    from searcher.pools import Pool
    from searcher.routes import Route
    from searcher.costs import cost_budget,quote_profit_floor
    from searcher.morpho_universe import optional_views
    from searcher.engine import probe_cycle
    rpc,c,old,target,signer,directory,contracts,_,send=protocol_market
    route,path=setup_cycle(protocol_market,stable=True)
    if limited_funding:
        rpc.request('anvil_setCode',[MORPHO,artifact('FundedConversionCycle.t.sol','FundedCycleLenderMock')['deployedBytecode']['object']])
        send(MORPHO,data('drain(address,uint256)',['address','uint256'],[USDC,balance(rpc,USDC,MORPHO)-5_000_000]))
    ex=Executor(rpc,c,old,signer,directory,structural_contract=target)
    u=SimpleNamespace(route_book=None,graph=None)
    suite=Suite(c,u,{**contracts,'StructuralExecutor':target},lambda:ex.context,threading.Event(),directory,rpc.url)
    suite.report('dex',state='starting');rpc.read_slot=lambda:nullcontext()
    try:
        # No legacy Pool conversion is supplied: any fallback into the CL/V2
        # encoder fails this test before a candidate can reach the account.
        suite.dex_quote(rpc,[(path,10**7,c['settlement_tokens'][0])],{},Pool,Route,cost_budget,quote_profit_floor,optional_views,probe_cycle)
        job=suite.mailbox.take();assert job and job.route==route
        assert job.candidate['amount_in']==(5_000_000 if limited_funding else 10_000_000)
        suite.mailbox.offer(job);assert suite.consume(ex)['status']=='submitted'
        rpc.request('evm_mine',[]);ex.reconcile()
        assert ex.db.execute('select state from attempts').fetchone()[0]=='success'
    finally:ex.close()

@pytest.mark.parametrize('change',['fee','funding','membership'])
def test_changed_market_or_funding_rejected_before_signing(protocol_market,change):
    rpc,c,old,target,signer,directory,_,_,send=protocol_market
    route,path=setup_cycle(protocol_market,stable=True)
    ex=Executor(rpc,c,old,signer,directory,structural_contract=target)
    try:
        h=ex.context['head'];q,_=complete_quote(rpc,c,target,route,10**7,h,simulation_context(h,ex.context['pending_head']),ex.context['nonce'])
        if change=='fee':send(Q,data('setFee(uint256)',['uint256'],[9000]))
        elif change=='membership':send(AERO,data('add(address,address,bool,address)',['address','address','bool','address'],[C,USDC,True,'0x'+'00'*20]))
        else:rpc.request('anvil_setCode',[MORPHO,'0x'])
        ex.refresh_context();h=ex.context['head']
        with pytest.raises(Reject):ex.execute(q,route,{},h,time.monotonic_ns()+5_000_000_000)
        assert ex.db.execute('select count(*) from attempts').fetchone()[0]==0
    finally:ex.close()

def test_weth_funding_cap_and_forged_flash_callback_rejected(protocol_market):
    rpc,c,old,target,signer,directory,_,_,send=protocol_market
    route,path=setup_cycle(protocol_market,stable=True,loan=WETH)
    c['settlement_tokens'][-1]['max_input_raw']='100'
    ex=Executor(rpc,c,old,signer,directory,structural_contract=target)
    try:
        h=ex.context['head'];q,_=complete_quote(rpc,c,target,route,10**7,h,simulation_context(h,ex.context['pending_head']),ex.context['nonce'])
        with pytest.raises(Reject,match='funding asset input cap'):ex.execute(q,route,{},h,time.monotonic_ns()+5_000_000_000)
        with pytest.raises(Reject):rpc.request('eth_call',[{'from':signer.address,'to':target,'data':data('onMorphoFlashLoan(uint256,bytes)',['uint256','bytes'],[10**7,b''])},'latest'])
    finally:ex.close()

def test_cyclic_metadata_and_full_width_clock_are_bound():
    r=cycle_route(((Q,USDC,C,4),(P,C,USDC,1)))
    assert r.input_token==USDC
    encoded=clock_call(r,{'number':'0x1'})[1][0]['data']
    pools,kinds=decode(['address[]','int24[]'],unhex(encoded)[4:]);assert kinds==(-1,1)
    with pytest.raises(Reject):cycle_route(((Q,USDC,C,4),(Q,C,USDC,4)))
    with pytest.raises(Reject):cycle_route(((Q,USDC,C,4),(P,C,WETH,1)))
    with pytest.raises(Reject):cycle_route(((Q,WETH,C,4),(P,C,WETH,1)))

def test_quote_dispatch_preserves_existing_cl_cycle_executor(market):
    from searcher.pools import Pool
    from searcher.routes import Route
    from searcher.costs import cost_budget,quote_profit_floor
    from searcher.morpho_universe import optional_views,Pool as ConversionPool
    from searcher.engine import probe_cycle
    rpc,c,target,signer,directory,index,candidate,_,_=market
    route=index.routes[candidate['route_key']]
    path=tuple((pool,route.tokens[i],route.tokens[i+1],1) for i,pool in enumerate(route.pools))
    pools={p.address:ConversionPool(p.address,p.token0,p.token1,p.factory,1,p.fee) for p in index.states.values()}
    ex=Executor(rpc,c,target,signer,directory)
    suite=Suite(c,SimpleNamespace(route_book=None,graph=None),{},lambda:ex.context,threading.Event(),directory,rpc.url)
    suite.report('dex',state='starting');rpc.read_slot=lambda:nullcontext()
    try:
        suite.dex_quote(rpc,[(path,10**18,c['settlement_tokens'][0])],pools,Pool,Route,cost_budget,quote_profit_floor,optional_views,probe_cycle)
        job=suite.mailbox.take();assert job and job.candidate['execution_kind']=='mixed-v1'
        suite.mailbox.offer(job);assert suite.consume(ex)['status']=='submitted'
        rpc.request('evm_mine',[]);ex.reconcile()
        assert ex.db.execute('select state from attempts').fetchone()[0]=='success'
    finally:ex.close()
