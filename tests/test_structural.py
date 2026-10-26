import copy
import json
import time
from collections import OrderedDict
import pytest
from eth_abi import decode,encode
from searcher.core import USDC,WETH,Reject,data,number,unhex
from searcher.structural import (MORPHO,CBETH,SUSDS,PSM,POOL1,POOL2,PSM_POOLS,
    deployment,morpho_route,psm_route,encode_plan,PLAN)
from searcher.structural_runner import BorrowerBook,liquidation_batches,candidate_for,complete_quote,fetch_seed,sized_requests
from searcher.strategy_access import svr_notice,ATLAS,CONTROL,ETH_AGGREGATOR
from searcher.execution import Executor
from searcher.context import simulation_context
from test_execution import market,artifact

def test_changed_positions_and_cold_rotation_are_not_starved():
    book=BorrowerBook();rows=[{'borrower':f'0x{i:040x}','collateral':100,'shares':10,'debt':10,'boundary':1000-i} for i in range(1,250)]
    book.update(rows);seen=set()
    for i in range(25):
        book.add([f'0x{1000+i:040x}']);batch=book.batch(64);assert f'0x{1000+i:040x}' in batch;seen.update(batch)
    assert {r['borrower'] for r in rows}<=seen

def test_liquidation_sizing_and_current_trigger():
    rows=[dict(borrower=f'0x{i:040x}',shares=10,collateral=10**36,debt=10,boundary=101) for i in range(1,20)]
    assert not liquidation_batches(rows,101,1000)
    groups=liquidation_batches(rows,100,50000)
    assert sum(map(len,groups))==19 and all(sum(r[2] for r in g)<=50000 and len(g)<=8 for g in groups)
    rows[0]['collateral']=0;rows[1]['shares']=0;assert sum(map(len,liquidation_batches(rows,100,50000)))==17

def test_insolvent_and_depth_limited_positions_use_partial_sizes():
    row={'borrower':'0x'+'11'*20,'shares':100*10**12,'debt':100*10**6,'collateral':10**18,'boundary':110*10**24}
    groups=liquidation_batches([row],100*10**24,10**15)
    assert len(groups)==1 and 0<groups[0][0][1]<row['shares']
    sizes=list(sized_requests(groups[0]));assert len(sizes)==4
    assert sizes[3][0][1]==sizes[0][0][1]//8 and sizes[3][0][2]>0

def test_parallel_svr_auctions_preserved_and_bounded():
    seen=OrderedDict();p={'params':{'result':{'auction_id':'a','partial_user_operation':{'chainId':8453,'to':ATLAS,'control':CONTROL,
        'hints':{'aggregator':ETH_AGGREGATOR,'medianPrice':123},'deadline':100,'maxFeePerGas':10}}}}
    assert svr_notice(p,seen);assert svr_notice(p,seen) is None
    p['params']['result']['auction_id']='b';assert svr_notice(p,seen)
    p['params']['result']['partial_user_operation']['chainId']=1;assert svr_notice(p,seen) is None

@pytest.fixture
def structural_market(market):
    rpc,c,old,signer,directory,index,candidate,head,send=market
    c['settlement_tokens']=[{'address':USDC,'symbol':'USDC','decimals':6,'max_input_raw':str(10**15),'min_net_raw':'100000'}]
    c['policy']['min_net_profit_usdc_raw']=100000;c['structural']={'enabled':True}
    def code(addr,name):rpc.request('anvil_setCode',[addr,artifact('StructuralExecutor.t.sol',name)['deployedBytecode']['object']])
    for t in (USDC,WETH,CBETH,SUSDS):code(t,'StructuralTokenMock')
    factory='0x33128a8fc17869897dce68ed026d694621f6fdfd';slip='0x5e7bb104d84c7cb9b682aac2f3d509f5f406809a'
    for f in (factory,slip):code(f,'StructuralFactoryMock')
    for pool,f,a,b,n,d in ((POOL1,slip,CBETH,WETH,1,1),(POOL2,factory,WETH,USDC,1,1),(PSM_POOLS[0],factory,SUSDS,USDC,100,110)):
        code(pool,'StructuralPoolMock')
        send(pool,data('initialize(address,address,address,uint256,uint256)',['address']*3+['uint256']*2,[f,a,b,n,d]))
        send(f,data('add(address,address,address)',['address']*3,[a,b,pool]))
        for t in (a,b):send(t,data('mint(address,uint256)',['address','uint256'],[pool,10**15]))
    code(MORPHO,'StructuralBlueMock');code(PSM,'StructuralPSMMock')
    for t,target in ((CBETH,MORPHO),(USDC,PSM),(SUSDS,PSM)):send(t,data('mint(address,uint256)',['address','uint256'],[target,10**15]))
    for borrower,debt in [('0x'+'11'*20,1_000_000),('0x'+'22'*20,2_000_000)]:send(MORPHO,data('setDebt(address,uint256)',['address','uint256'],[borrower,debt]))
    quoter=next(f['quoter'] for f in c['factories'] if f['kind']=='v3')
    rpc.request('anvil_setCode',[quoter,artifact('ContextAggregator.t.sol','ContextQuoteMock')['deployedBytecode']['object']])
    send(quoter,data('configure(address,address,uint256,uint256)',['address','address','uint256','uint256'],[USDC,WETH,int(c['policy']['max_total_fee_wei']),500000]))
    target=send(calldata=deployment(c)['creation_data'])['contractAddress'].lower();send(target,data('setPaused(bool)',['bool'],[False]))
    route=morpho_route([('0x'+'11'*20,1000000,1010000),('0x'+'33'*20,1,1),('0x'+'22'*20,2000000,2020000)])
    yield rpc,c,old,target,signer,directory,route,send

def test_complete_quote_preflight_send_and_receipt_share_ledger(structural_market):
    rpc,c,old,target,signer,directory,route,send=structural_market
    ex=Executor(rpc,c,old,signer,directory,structural_contract=target)
    try:
        anchor=ex.context['head'];clock=simulation_context(anchor,ex.context['pending_head'])
        candidate,started=complete_quote(rpc,c,target,route,3030001,anchor,clock,ex.context['nonce'])
        assert candidate['gross_surplus']==300000
        result=ex.execute(candidate,route,{},anchor,time.monotonic_ns()+5_000_000_000,submit=False)
        assert result['status']=='execution_preflight_passed' and not ex.db.execute('SELECT 1 FROM attempts').fetchone()
        result=ex.execute(candidate,route,{},anchor,time.monotonic_ns()+5_000_000_000)
        assert result['status']=='submitted';rpc.request('evm_mine',[]);ex.reconcile()
        assert ex.db.execute('SELECT state FROM attempts').fetchone()[0]=='success'
        outcome=json.loads(ex.db.execute('SELECT outcome FROM intents').fetchone()[0])
        assert outcome['gross_profit_raw']==300000 and outcome['net_after_reserved_fee_raw']>0 and outcome['execution_kind']=='structural-v1'
        assert outcome['amount_semantics']=='maximum_debt'
    finally:ex.close()

def test_competitor_wins_after_quote_no_signature_or_fee(structural_market):
    rpc,c,old,target,signer,directory,route,send=structural_market
    ex=Executor(rpc,c,old,signer,directory,structural_contract=target)
    try:
        h=ex.context['head'];clock=simulation_context(h,ex.context['pending_head'])
        candidate,_=complete_quote(rpc,c,target,route,3030001,h,clock,ex.context['nonce'])
        send(MORPHO,data('setDebt(address,uint256)',['address','uint256'],['0x'+'11'*20,0]))
        send(MORPHO,data('setDebt(address,uint256)',['address','uint256'],['0x'+'22'*20,0]))
        ex.refresh_context();h=ex.context['head']
        with pytest.raises(Reject):ex.execute(candidate,route,{},h,time.monotonic_ns()+5_000_000_000)
        assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
    finally:ex.close()

def test_psm_whole_path_uses_same_fee_and_receipt_guards(structural_market):
    rpc,c,old,target,signer,directory,_,send=structural_market
    ex=Executor(rpc,c,old,signer,directory,structural_contract=target)
    try:
        route=psm_route(PSM_POOLS[0]);h=ex.context['head'];clock=simulation_context(h,ex.context['pending_head'])
        candidate,_=complete_quote(rpc,c,target,route,10000000,h,clock,ex.context['nonce'])
        ex.execute(candidate,route,{},h,time.monotonic_ns()+5_000_000_000);rpc.request('evm_mine',[]);ex.reconcile()
        outcome=json.loads(ex.db.execute('SELECT outcome FROM intents').fetchone()[0]);assert outcome['gross_profit_raw']==1000000
    finally:ex.close()

def test_route_and_debt_tampering_rejected(structural_market):
    rpc,c,old,target,signer,directory,route,_=structural_market
    h=rpc.header();candidate=candidate_for(route,3030001,c,h)
    common=[None,1,100,1000000,number(h['timestamp'])+6,number(h['number']),unhex(h['hash']),number(h['number'])+2,0]
    with pytest.raises(Reject,match='debt budget'):encode_plan(candidate,route,common)

def test_additional_deployment_resumes_without_duplicate_fee(structural_market,monkeypatch):
    from searcher.structural_onboarding import setup_executor,preflight
    rpc,c,old,target,signer,directory,route,send=structural_market
    original=rpc.request
    def advancing(method,params):
        result=original(method,params)
        if method=='eth_sendRawTransaction':original('evm_mine',[])
        return result
    monkeypatch.setattr(rpc,'request',advancing)
    assert preflight(rpc,c,directory)['balance_sufficient']
    first=setup_executor(rpc,c,signer,directory,require_validation=False)
    nonce=number(rpc.request('eth_getTransactionCount',[signer.address,'latest']))
    second=setup_executor(rpc,c,signer,directory,require_validation=False)
    assert first['contract']==second['contract'] and not second['paused']
    assert number(rpc.request('eth_getTransactionCount',[signer.address,'latest']))==nonce

def test_lost_submission_response_keeps_same_nonce_and_recovers(structural_market,monkeypatch):
    rpc,c,old,target,signer,directory,route,send=structural_market
    ex=Executor(rpc,c,old,signer,directory,structural_contract=target)
    try:
        h=ex.context['head'];clock=simulation_context(h,ex.context['pending_head'])
        candidate,_=complete_quote(rpc,c,target,route,3030001,h,clock,ex.context['nonce'])
        original=rpc.request;lost=[False]
        def request(method,params):
            result=original(method,params)
            if method=='eth_sendRawTransaction' and not lost[0]:lost[0]=True;raise TimeoutError('local accepted, response lost')
            return result
        monkeypatch.setattr(rpc,'request',request)
        with pytest.raises(TimeoutError):ex.execute(candidate,route,{},h,time.monotonic_ns()+5_000_000_000)
        assert ex.db.execute('SELECT state FROM attempts').fetchone()[0]=='submitted'
        assert ex.db.execute("SELECT json_extract(payload,'$.route_key') FROM intents").fetchone()[0]==route.key
        rpc.request('evm_mine',[]);ex.reconcile()
        assert ex.db.execute('SELECT state FROM attempts').fetchone()[0]=='success'
        assert ex.db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0]==1
    finally:ex.close()

def test_launch_connects_new_contract_to_actual_executor(structural_market,monkeypatch):
    import sys
    from searcher import __main__ as cli,onboarding,structural_onboarding,execution,runner,service
    rpc,c,old,target,signer,directory,route,send=structural_market
    original=rpc.request
    def advancing(method,params):
        result=original(method,params)
        if method=='eth_sendRawTransaction':original('evm_mine',[])
        return result
    monkeypatch.setattr(rpc,'request',advancing)
    monkeypatch.setattr(cli,'load_config',lambda *a:c)
    monkeypatch.setattr(cli,'ROOT',directory)
    monkeypatch.setattr(service,'ROOT',directory)
    monkeypatch.setattr(execution,'BroadcastRPC',lambda **kw:rpc)
    monkeypatch.setattr(onboarding,'preflight_setup',lambda *a: {'state':'ready'})
    monkeypatch.setattr(onboarding,'setup_executor',lambda *a: {'contract':old})
    unlocks=[];monkeypatch.setattr(onboarding,'unlock_wallet',lambda *a:unlocks.append(True) or signer)
    real_setup=structural_onboarding.setup_executor;real_preflight=structural_onboarding.preflight
    monkeypatch.setattr(structural_onboarding,'preflight',lambda r,c:real_preflight(r,c,directory))
    monkeypatch.setattr(structural_onboarding,'setup_executor',lambda r,c,s:real_setup(r,c,s,directory,require_validation=False))
    observed=[]
    def actual_execution(config,seconds,ex):
        h=ex.context['head'];clock=simulation_context(h,ex.context['pending_head'])
        candidate,_=complete_quote(rpc,config,ex.structural_contract,route,3030001,h,clock,ex.context['nonce'])
        observed.append(ex.execute(candidate,route,{},h,time.monotonic_ns()+5_000_000_000))
        rpc.request('evm_mine',[]);ex.reconcile()
        assert ex.db.execute('SELECT state FROM attempts').fetchone()[0]=='success'
        return {'state':'stopped'}
    monkeypatch.setattr(runner,'run',actual_execution)
    monkeypatch.setattr(sys,'argv',['searcher','launch','--seconds','1'])
    cli.main();assert len(unlocks)==1 and observed[0]['status']=='submitted'

def test_startup_failure_records_failed_state(tmp_path,monkeypatch):
    from searcher import structural_runner as module
    class BrokenRPC:
        def __init__(self,*a,**kw):self.url='local-test'
        def verify_chain(self):raise Reject('wrong chain')
        def close(self):pass
    monkeypatch.setattr(module,'ROOT',tmp_path);monkeypatch.setattr(module,'RPC',BrokenRPC)
    from searcher.core import load_config
    with pytest.raises(Reject,match='wrong chain'):module.run(load_config(),1)
    runtime=json.loads((tmp_path/'evidence/runtime.json').read_text());assert runtime['state']=='failed' and runtime['pid'] is None
