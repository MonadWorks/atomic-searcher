"""Local Base-shaped chain with public V4 manager prestate. No mainnet sends.

Discovery events/owner identity are synthetic. These tests prove integration,
not historical first-seen timing or auction priority.
"""
import copy
import json
import time
from pathlib import Path
import pytest
from eth_abi import encode,decode
from searcher.core import ROOT,USDC,WETH,Reject,data,number,unhex
from searcher.execution import Executor
from searcher.v4 import MANAGER,Reader,Route,evaluate,deployment,output_cap
from searcher.v4_market import Key,INITIALIZE
from searcher.v4_runtime import Inventory,Worker
from test_execution import market,artifact

FIXTURES=Path(__file__).parent/'fixtures/v4'

def event(key,head):
    return {'address':MANAGER,'topics':[INITIALIZE,key.id,'0x'+encode(['address'],[key.currency0]).hex(),
            '0x'+encode(['address'],[key.currency1]).hex()],
            'data':'0x'+encode(['uint24','int24','address','uint160','int24'],[key.fee,key.spacing,key.hooks,1<<96,0]).hex(),
            'blockNumber':head['number'],'blockHash':head['hash']}

@pytest.fixture
def v4market(market):
    rpc,c,cl,signer,directory,index,cl_candidate,head,send=market
    c['settlement_tokens']=[{'address':USDC,'decimals':6,'symbol':'USDC','max_input_raw':str(20_000*10**6),'min_net_raw':'10000'}]
    c['policy']['max_quote_ms']=300
    c['v4']={'enabled':True,'bitmap_radius':16,'tick_limit':128}
    rpc.METHODS=rpc.METHODS|{'anvil_setStorageAt'}
    prestate=json.loads((FIXTURES/'prestate.json').read_text())['response']['result']
    for addr,account in prestate.items():
        if account.get('code'):rpc.request('anvil_setCode',[addr,account['code']])
        for slot,value in account.get('storage',{}).items():rpc.request('anvil_setStorageAt',[addr,slot,value])
    quoter=next(f['quoter'] for f in c['factories'] if f['kind']=='v3')
    rpc.request('anvil_setCode',[quoter,artifact('ContextAggregator.t.sol','ContextQuoteMock')['deployedBytecode']['object']])
    send(quoter,data('configure(address,address,uint256,uint256)',['address','address','uint256','uint256'],
        [USDC,WETH,int(c['policy']['max_total_fee_wei']),500000]))
    dep=deployment(c);v4=send(calldata=dep['creation_data'])['contractAddress'].lower()
    send(v4,data('setPaused(bool)',['bool'],[False]))
    keys=[Key(*r['key']) for r in json.loads((FIXTURES/'route.json').read_text())]
    head=rpc.header();inv=Inventory(directory/'inventory.sqlite')
    inv.accept([event(k,head) for k in keys],number(head['number']),number(head['number']),{number(head['number']):head['hash']})
    inv.close();inv=Inventory(directory/'inventory.sqlite')
    pairs=inv.registry.pairs(USDC)
    assert len(pairs)==2
    reader=Reader(rpc,c);frame=reader.read(keys);report=evaluate(frame,c,[Route(*p) for p in pairs])
    for _ in range(4):
        if report['candidates']:break
        reader.request_missing(keys,report['missing_slots']);frame=reader.read(keys);report=evaluate(frame,c,[Route(*p) for p in pairs])
    assert report['candidates']
    # Quote reads are captured before the public reference swap, including fee words.
    captured={int(s,16) for s in prestate[MANAGER]['storage']}
    candidate=report['candidates'][0];route=Route(Key(*candidate['v4_keys'][0]),Key(*candidate['v4_keys'][1]))
    frame.snapshot.reads.clear();frame.snapshot.deadline_ns=None
    frame.snapshot.cycle(route.sell,route.buy,USDC,candidate['amount_in'])
    assert frame.snapshot.reads<=captured
    yield rpc,c,cl,v4,signer,directory,frame,candidate,route,send
    inv.close()

def test_v4_discovery_restart_atomic_depth_preflight_submit_receipt(v4market):
    rpc,c,cl,v4,signer,directory,frame,candidate,route,send=v4market
    ex=Executor(rpc,c,cl,signer,directory,v4_contract=v4)
    try:
        before=number(rpc.request('eth_call',[{'to':USDC,'data':data('balanceOf(address)',['address'],[signer.address])},'latest']))
        pre=ex.execute(candidate,route,{},frame.anchor,time.monotonic_ns()+5_000_000_000,submit=False)
        assert pre['status']=='execution_preflight_passed' and not pre['signed']
        assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
        import threading
        worker=Worker(None,c,directory,threading.Event(),time.monotonic()+10)
        worker.offer(frame,{'block':number(frame.anchor['number']),'candidates':[candidate]})
        result=worker.consume(ex)
        assert result['status']=='submitted'
        rpc.request('evm_mine',[]);ex.reconcile()
        state,fee=ex.db.execute('SELECT state,actual FROM attempts').fetchone()
        outcome=json.loads(ex.db.execute('SELECT outcome FROM intents').fetchone()[0])
        after=number(rpc.request('eth_call',[{'to':USDC,'data':data('balanceOf(address)',['address'],[signer.address])},'latest']))
        assert state=='success' and int(fee)>0
        assert after-before==outcome['gross_profit_raw'] and outcome['net_after_reserved_fee_raw']>0
        assert outcome['execution_kind']=='v4-pair-v1' and outcome['amount_semantics']=='exact_settlement_output'
        intent=json.loads(ex.db.execute('SELECT payload FROM intents').fetchone()[0])
        assert intent['transaction']['to'].lower()==v4
    finally:ex.close()

def test_v4_rejects_stale_account_nonce_before_signing(v4market,monkeypatch):
    rpc,c,cl,v4,signer,directory,frame,candidate,route,send=v4market
    ex=Executor(rpc,c,cl,signer,directory,v4_contract=v4)
    original=rpc.batch
    def changed(calls):
        values=original(calls)
        if len(calls)==5 and calls[-1][0]=='eth_getTransactionCount':values[-1]=hex(number(values[-1])+1)
        return values
    monkeypatch.setattr(rpc,'batch',changed)
    try:
        with pytest.raises(Reject,match='nonce changed'):ex.execute(candidate,route,{},frame.anchor,time.monotonic_ns()+5_000_000_000)
        assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
    finally:ex.close()

def test_v4_lost_send_response_recovers_same_intent(v4market,monkeypatch):
    rpc,c,cl,v4,signer,directory,frame,candidate,route,send=v4market
    ex=Executor(rpc,c,cl,signer,directory,v4_contract=v4)
    original=rpc.request;lost=[False]
    def send_lost(method,params):
        result=original(method,params)
        if method=='eth_sendRawTransaction' and not lost[0]:lost[0]=True;raise TimeoutError('local response lost after acceptance')
        return result
    monkeypatch.setattr(rpc,'request',send_lost)
    try:
        with pytest.raises(TimeoutError):ex.execute(candidate,route,{},frame.anchor,time.monotonic_ns()+5_000_000_000)
        assert ex.db.execute('SELECT state FROM attempts').fetchone()[0]=='submitted'
        with pytest.raises(Reject,match='previous transaction unresolved'):
            ex.execute(candidate,route,{},frame.anchor,time.monotonic_ns()+5_000_000_000)
        ex.close();ex=None
        # Local context_stop is rebound on restart, as main launch does.
        rpc.cancel_event=None
        ex=Executor(rpc,c,cl,signer,directory,v4_contract=v4)
        rpc.request('evm_mine',[]);ex.reconcile()
        assert ex.db.execute('SELECT state FROM attempts').fetchall()==[('success',)]
    finally:
        if ex:ex.close()

def test_v4_shared_account_lock_prevents_another_executor(v4market):
    rpc,c,cl,v4,signer,directory,*_=v4market
    ex=Executor(rpc,c,cl,signer,directory,v4_contract=v4)
    try:
        with pytest.raises(RuntimeError,match='another executor'):Executor(rpc,c,cl,signer,directory,v4_contract=v4)
    finally:ex.close()

def test_v4_protocol_output_bound_preserves_generic_cap():
    from searcher.core import load_config
    c=load_config();assert output_cap(c)==(1<<127)-1
    assert deployment(c)['max_output_raw']==(1<<127)-1

def test_v4_setup_uses_separate_journals_and_resumes_without_new_sends(v4market,monkeypatch):
    from searcher.v4_onboarding import setup_executor
    rpc,c,cl,v4,signer,directory,frame,candidate,route,send=v4market
    original=rpc.request;submitted=[]
    def mine(method,params):
        result=original(method,params)
        if method=='eth_sendRawTransaction':submitted.append(result);original('evm_mine',[])
        return result
    monkeypatch.setattr(rpc,'request',mine)
    out=directory/'onboarding'
    first=setup_executor(rpc,c,signer,out,require_validation=False)
    second=setup_executor(rpc,c,signer,out,require_validation=False)
    assert first['contract']==second['contract'] and not first['paused'] and len(submitted)==2
    assert len(list((out/'v4').glob('*/setup-deploy.json')))==1
    assert not (out/'setup-deploy.json').exists() and not (out/'deployments').exists()
