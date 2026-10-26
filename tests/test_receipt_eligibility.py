import copy
import json
import time

import httpx
import pytest
from eth_abi import encode

from searcher.core import RPC, RPCError, Reject, USDC, WETH, data, load_config
from searcher.engine import evaluate
from searcher.execution import (GasValidationExpired, execution_failure_details,
                                verified_call_gas_budget, required_net_profit)
from searcher.costs import quote_profit_floor
from searcher.pipeline import candidate_attempt_order
from searcher.pools import Pool
from searcher.routes import RouteIndex
from searcher.statefeed import StateFeed
from searcher.universe import CyclePages


BAD = '0x5c4348e963369e124cb8e9b1a619f2f6f1717b58'
HEADER = {'number':'0x64', 'timestamp':'0xc8', 'hash':'0x'+'11'*32, 'baseFeePerGas':'0x1'}


def pools():
    return [Pool('0x'+f'{i:040x}', '0x'+'22'*20, 'v2', WETH, token, 0, 3000,
                 block=100, block_hash=HEADER['hash'], reserve0=10**20, reserve1=mult*10**20)
            for i,token,mult in [(1,BAD,4),(2,BAD,1),(3,USDC,2),(4,USDC,1)]]


def test_incompatible_quote_does_not_displace_real_profitable_route():
    c=load_config(); c['policy']['max_quote_ms']=500; c['policy']['sizing_mode']='fixed'
    c['settlement_tokens']=[dict(c['settlement_tokens'][0],max_input_raw=str(10**18))]
    ix=RouteIndex(pools(),[WETH],3)
    report=evaluate(ix,c,HEADER)
    assert report['rejected']['unsupported_token_transfer']==2
    assert report['candidates'] and all(BAD not in v['tokens'] for v in report['candidates'])
    assert all(v['net_after_modelled_cost']>0 for v in report['candidates'])
    assert all(not set(g)&{p.address for p in pools()[:2]} for g in report['deep_read_routes'])


def test_cached_universe_pages_and_resume_never_restore_blocked_pools():
    c=load_config(); ps=pools(); old=CyclePages(ps,[WETH],3)
    old.prioritize([ps[0].address])
    fresh=CyclePages(ps,[WETH],3,groups=old.groups,excluded_tokens=c['unsupported_transfer_tokens'])
    fresh.resume(old)
    assert fresh.groups and set(fresh.pools)=={p.address for p in ps[2:]}
    assert not fresh.priority
    saved={'header':HEADER,'pools':[p.json() for p in ps], 'cycle_groups':old.groups}
    c['policy']['cycle_pages']=True
    feed=StateFeed(None,c,saved)
    assert {p.address for p in feed.pools}=={p.address for p in ps[2:]}
    assert feed.pages.groups==fresh.groups


@pytest.mark.parametrize('bad_registry', [[BAD], {'bad-address':'shortfall'}, {BAD:''}, {WETH:'shortfall'}])
def test_registry_is_validated_before_start(tmp_path,bad_registry):
    c=load_config();c['unsupported_transfer_tokens']=bad_registry
    p=tmp_path/'config.json';p.write_text(json.dumps(c))
    with pytest.raises(Reject):load_config(p)


@pytest.mark.parametrize('batched',[False,True])
def test_rpc_preserves_revert_data_even_after_long_message(monkeypatch,batched):
    rpc=RPC('https://example.invalid')
    error={'code':3,'message':'execution reverted '+'detail '*100,'data':{'data':data('BadCallback()')}}
    body={'jsonrpc':'2.0','id':0 if batched else 1,'error':error}
    response=httpx.Response(200,json=[body] if batched else body,request=httpx.Request('POST','https://example.invalid'))
    response.extensions['headers_elapsed_ms']=0
    monkeypatch.setattr(rpc,'post',lambda _:response)
    try:
        with pytest.raises(RPCError) as caught:
            if batched:rpc.batch([('eth_call',[])])
            else:rpc.request('eth_call',[])
        assert execution_failure_details(caught.value)['revert_name']=='BadCallback'
    finally:rpc.close()


@pytest.mark.parametrize('name',['BadCallback','InsufficientProfit','InvalidPlan','InvalidPool','Unauthorized'])
def test_terminal_revert_is_returned_once_with_original_cause(name):
    calls=[]
    class Reverting:
        def request(self,method,params):
            calls.append(params)
            raise RPCError(method,{'code':3,'message':'execution reverted','data':data(name+'()')})
    with pytest.raises(RPCError) as caught:
        verified_call_gas_budget(Reverting(),{}, {},100_000,1_000_000,time.monotonic_ns()+1_000_000_000)
    assert len(calls)==1
    assert execution_failure_details(caught.value)['revert_name']==name


def test_genuine_gas_exhaustion_can_still_reach_success():
    attempts=[]
    class NeedsGas:
        def request(self,method,params):
            gas=int(params[0]['gas'],16);attempts.append(gas)
            if gas<400_000:raise RPCError(method,{'code':3,'message':'out of gas','data':'0x'})
            return '0x'+encode(['uint256'],[100]).hex()
    assert verified_call_gas_budget(NeedsGas(),{}, {},100_000,1_000_000,time.monotonic_ns()+1_000_000_000)==400_000
    assert attempts==[100_000,200_000,400_000]


def test_deadline_retains_original_revert_and_attempts(monkeypatch):
    clock=[0];monkeypatch.setattr('searcher.execution.time.monotonic_ns',lambda:clock[0])
    class SlowFailure:
        def request(self,method,params):
            clock[0]=11
            raise RPCError(method,{'code':3,'message':'execution reverted','data':'0x'})
    with pytest.raises(GasValidationExpired) as caught:
        verified_call_gas_budget(SlowFailure(),{}, {},100_000,1_000_000,10)
    details=execution_failure_details(caught.value)
    assert 'execution reverted' in details['cause'] and details['validation_deadline_reached']
    assert details['gas_attempts']==[100_000]


@pytest.mark.parametrize('later_result', ['late_success', 'transport_failure'])
def test_later_failure_retains_prior_revert_without_mislabeling_final_cause(monkeypatch,later_result):
    clock=[0];attempts=[]
    monkeypatch.setattr('searcher.execution.time.monotonic_ns',lambda:clock[0])
    class Retry:
        def request(self,method,params):
            attempts.append(params)
            if len(attempts)==1:
                raise RPCError(method,{'code':3,'message':'execution reverted','data':data('TransferFailed()')})
            clock[0]=11
            if later_result=='transport_failure':raise TimeoutError('transport deadline')
            return '0x'+encode(['uint256'],[100]).hex()
    with pytest.raises(Exception) as caught:
        verified_call_gas_budget(Retry(),{}, {},100_000,1_000_000,10)
    details=execution_failure_details(caught.value)
    assert details['previous_gas_failure']['revert_selector']==data('TransferFailed()')
    assert details['gas_attempts']==[100_000,200_000]
    if later_result=='late_success':
        assert details['validation_outcome']=='call_succeeded_after_deadline'
        assert 'observation expired' in details['cause']
    else:
        assert isinstance(caught.value,TimeoutError) and details['cause']=='transport deadline'


@pytest.mark.parametrize('ratio,large_fee_floor', [(0,100_000),(10000,200_000)])
def test_quote_floor_enforces_fee_margin_and_usdc_floor_early(ratio,large_fee_floor):
    c=load_config();settlement=next(t for t in c['settlement_tokens'] if t['address']==USDC)
    c['policy']['min_net_profit_usdc_raw']=100_000
    c['policy']['min_net_profit_fee_bps']=ratio
    for fee,expected in [(1000,100_000),(200_000,large_fee_floor)]:
        assert quote_profit_floor(c,settlement,fee)==required_net_profit(c,settlement,{},fee)==expected


def test_alternative_candidates_keep_rank_and_settlement_fairness():
    candidates=[{'tokens':[token],'id':(token,i)} for token in [USDC,WETH] for i in range(5)]
    selected=candidate_attempt_order(candidates,[USDC,WETH],1)
    assert [v['id'] for v in selected]==[(t,i) for i in range(3) for t in [WETH,USDC]]
