"""Profit-priced fees, funded loss limits and high-price cancellation."""
import json
import time
from eth_abi import encode
import pytest

from searcher import atlas
from searcher.core import Reject, USDC, WETH, load_config
from searcher.costs import cost_budget, competitive_bid
from searcher.context import simulation_context
from searcher.execution import Executor
from searcher.structural import psm_route
from searcher.structural_runner import complete_quote
from searcher.structural_screening import UnprofitableQuote
from test_auction_runtime import manager
from test_execution import market
from test_structural import structural_market
from test_atlas import notice


@pytest.mark.parametrize('token', [USDC, WETH])
def test_price_above_old_ceiling_is_admitted_within_total_fee_budget(token):
    policy=load_config()['policy']
    budget=cost_budget(policy,token,60_000_000,2)
    assert budget.max_fee_per_gas>100_000_000
    assert budget.total_fee_wei<=int(policy['max_total_fee_wei'])
    policy['max_total_fee_wei']=str(budget.total_fee_wei-1)
    with pytest.raises(Reject,match='per-attempt fee'):
        cost_budget(policy,token,60_000_000,2)


@pytest.mark.parametrize('token', [USDC, WETH])
def test_competitive_tip_uses_profit_liability_and_balance_instead_of_unit_cap(token):
    policy=load_config()['policy']
    context={'observed_priority_fee':350_000_000,'extra_fee':10**12,'balance':10**18,
             'native_cap':10**14,'usdc_at_native_cap':500_000}
    gross=10**18 if token==WETH else 100_000_000
    tip,details=competitive_bid(policy,token,context,gross,600_000,60_000_000,1_000_000,1)
    assert tip>100_000_000
    assert (120_000_000+tip)*600_000+context['extra_fee']<=int(policy['max_total_fee_wei'])
    context['balance']=10**14
    smaller,_=competitive_bid(policy,token,context,gross,600_000,60_000_000,1_000_000,1)
    assert smaller<tip
    assert (120_000_000+smaller)*600_000+context['extra_fee']<=context['balance']
    assert details['maximum_affordable_fee_per_gas_wei']>100_000_000


def test_profit_floor_allows_fees_above_half_of_gross_without_spending_retained_profit():
    policy=load_config()['policy']
    context={'observed_priority_fee':10**9,'extra_fee':10**12,'balance':10**18}
    gross=10**14;floor=2*10**13
    tip,details=competitive_bid(policy,WETH,context,gross,400000,1_000_000,1_000_000,floor)
    fee=(2_000_000+tip)*400000+context['extra_fee']
    assert gross//2<fee<=gross-floor
    assert details['profit_fee_budget_raw']==gross-floor


@pytest.mark.parametrize('remaining', [99_999,100_000])
def test_usdc_full_measurement_enforces_exact_ten_cent_retained_floor(remaining):
    config=load_config();route=psm_route('0x'+'31'*20)
    head={'number':'0x64','timestamp':'0x100','hash':'0x'+'12'*32,'baseFeePerGas':hex(60_000_000)}
    class RPC:
        gross=1
        def request(self,method,args):
            return '0x'+encode(['uint256','uint256'],[self.gross,300000]).hex()
    rpc=RPC()
    with pytest.raises(UnprofitableQuote) as measured:
        complete_quote(rpc,config,'0x'+'32'*20,route,10**9,head,simulation_context(head),1)
    fee=measured.value.evidence()['required_gross_surplus']-100_000
    assert fee>100_000
    rpc.gross=fee+remaining
    if remaining<100_000:
        with pytest.raises(UnprofitableQuote):
            complete_quote(rpc,config,'0x'+'32'*20,route,10**9,head,simulation_context(head),1)
    else:
        candidate,_=complete_quote(rpc,config,'0x'+'32'*20,route,10**9,head,simulation_context(head),1)
        assert candidate['gross_surplus']==candidate['minimum_gross_surplus']


@pytest.mark.parametrize('profitable', [True,False])
def test_structural_whole_quote_prices_measured_gas_at_high_price(profitable):
    config=load_config();route=psm_route('0x'+'31'*20)
    head={'number':'0x64','timestamp':'0x100','hash':'0x'+'12'*32,'baseFeePerGas':hex(60_000_000)}
    class RPC:
        def request(self,method,args):
            assert method=='eth_call'
            assert int(args[0]['maxFeePerGas'],16)>100_000_000
            return '0x'+encode(['uint256','uint256'],[10_000_000 if profitable else 1,300000]).hex()
    if profitable:
        candidate,_=complete_quote(RPC(),config,'0x'+'32'*20,route,10**9,head,simulation_context(head),1)
        assert candidate['gross_surplus']>=candidate['minimum_gross_surplus']
        assert candidate['cost_budget']['total_fee_wei']<=int(config['policy']['max_total_fee_wei'])
    else:
        with pytest.raises(UnprofitableQuote):
            complete_quote(RPC(),config,'0x'+'32'*20,route,10**9,head,simulation_context(head),1)


def priced_auction(manager,monkeypatch,gross,fee):
    a,n,sent,_,_=manager
    n['partial_user_operation']['maxFeePerGas']=300_000_000
    def quote(rpc,meta,op,owner,executor,plans,per_plan,gas_limit,overrides,clock):
        assert op['maxFeePerGas']==300_000_000
        assert all(p[7]==op['maxFeePerGas'] for p in plans)
        return [(gross,400000,fee) for _ in plans]
    monkeypatch.setattr(atlas,'measure_charges',quote)
    return a,n,sent


def test_high_price_auction_can_sign_reserve_and_submit_positive_bid(manager,monkeypatch):
    gross,fee=10**15,15*10**13
    a,n,sent=priced_auction(manager,monkeypatch,gross,fee)
    out=a.process(n)
    assert out['status']=='auction_submitted' and len(sent)==1
    assert out['gas_price_wei']>100_000_000 and out['whole_quotes_measured']>0
    assert sent[0]['bidAmount']+fee+out['retained_minimum_wei']==gross
    assert a.e.db.execute('SELECT reserved,state FROM attempts').fetchone()==(str(fee),'submitted')


@pytest.mark.parametrize('reason', ['fee','profit','bond','daily'])
def test_high_price_never_bypasses_profit_or_funded_loss_budget(manager,monkeypatch,reason):
    gross,fee=10**15,15*10**13
    a,n,sent=priced_auction(manager,monkeypatch,gross,fee)
    if reason=='fee':a.e.config['policy']['max_total_fee_wei']=str(fee-1)
    if reason=='bond':a.e.bond_snapshot['balance']=fee-1
    if reason=='daily':a.e.ledger.daily_cap=fee-1
    if reason=='profit':a,n,sent=priced_auction(manager,monkeypatch,fee,fee)
    def forbidden(*args,**kwargs):raise AssertionError('unfunded or losing operation was signed')
    monkeypatch.setattr(atlas,'sign_solution',forbidden)
    if reason in ('bond','daily'):
        with pytest.raises(Reject):a.process(n)
    else:
        result=a.process(n)
        assert result['status']=='no_executable_auction'
        assert any(('fee cap' if reason=='fee' else 'cannot cover gas') in x for x in result['reasons'])
    assert not sent and a.e.db.execute('SELECT 1 FROM attempts').fetchone() is None


def test_quote_failure_is_distinct_from_measured_unprofitable_execution(manager,monkeypatch):
    a,n,sent,_,_=manager
    monkeypatch.setattr(atlas,'measure_charges',lambda *args: [(0,0,None)]*len(args[5]))
    result=a.process(n)
    assert result['whole_quotes_failed']==result['packages_compared']>0
    assert result['whole_quotes_measured']==0
    assert result['best_measured_net_before_bid_wei'] is None
    assert result['eligibility_evidence']=='whole_execution_quote_failed'
    assert not sent


def test_batch_decoder_preserves_failed_measurement():
    op=notice()['partial_user_operation'];owner='0x'+'23'*20;executor='0x'+'34'*20
    meta=dict(simulator='0x'+'45'*20,call_config=41732,dapp_gas=2000000,solver_gas=6000000,
              surcharge=0,atlas_surcharge=0,authorized_signer=owner)
    meta['forwarders']={op['hints']['aggregator']:[{'address':'0x'+'56'*20,'bundlers':['0x'+'67'*20]}]}
    plan=(bytes(32),b'',1,[],1,1,1,100,100)
    class RPC:
        def request(self,method,args):
            return '0x'+encode(['uint256[4][]','bytes[]'],[[(0,0,172500,2**256-1),(0,100000,287500,2**256-1)],[b'\x12\x34\x56\x78',b'']]).hex()
    values=atlas.measure_charges(RPC(),meta,op,owner,executor,[plan,plan],1800000,6000000,{}, {})
    assert values==[(0,0,None),(0,287500,None)]


def test_high_price_expired_trade_cancels_within_original_reservation(market):
    rpc,c,contract,signer,directory,index,candidate,head,_=market
    candidate['cost_budget'].update(max_fee_per_gas=120_000_000,priority_fee=120_000_000)
    candidate['cost_budget']['total_fee_wei']=candidate['cost_budget']['gas_limit']*120_000_000+int(c['policy']['l1_fee_allowance_wei'])
    c['policy']['final_validation_max_age_ms']=2000
    executor=Executor(rpc,c,contract,signer,directory/'high-price-cancel');rpc.fail_send=True
    try:
        with pytest.raises(TimeoutError):
            executor.execute(candidate,index.routes[candidate['route_key']],index.states,head,time.monotonic_ns()+5_000_000_000)
        for _ in range(3):rpc.request('evm_mine',[])
        executor.reconcile()
        intent=json.loads(executor.db.execute('SELECT payload FROM intents').fetchone()[0])
        assert intent['transaction']['maxFeePerGas']>100_000_000
        assert intent['recovery_status']=='cancellation prepared within original fee reservation'
        rpc.request('evm_mine',[]);executor.reconcile()
        state,actual,outcome=executor.db.execute('SELECT state,actual,outcome FROM attempts JOIN intents ON attempts.id=intents.attempt').fetchone()
        assert state=='failed' and json.loads(outcome)['cancelled']
        assert int(actual)<=intent['reserved_fee_wei']
    finally:executor.close()


def test_long_liquidation_payload_tip_uses_its_actual_extra_fee(structural_market,monkeypatch):
    from dataclasses import replace
    rpc,c,old,target,signer,directory,route,_=structural_market
    # ABI decoding permits trailing bytes. This keeps the same real liquidation
    # while exceeding the cached 2 KiB fee envelope.
    route=replace(route,arguments=route.arguments+bytes(2048))
    c['policy']['profit_aware_bidding']=True
    c['policy']['final_validation_max_age_ms']=2000
    executor=Executor(rpc,c,old,signer,directory/'large-payload-fee',structural_contract=target)
    try:
        h=executor.context['head'];clock=simulation_context(h,executor.context['pending_head'])
        candidate,_=complete_quote(rpc,c,target,route,3030001,h,clock,executor.context['nonce'])
        assert candidate['gross_surplus']==300000
        executor.context['observed_priority_fee']=500_000_000
        extra=2*10**13
        assert extra>executor.context['extra_fee']
        monkeypatch.setattr(executor.fees,'maximum_fee',lambda payload,gas,price:gas*price+extra)
        result=executor.execute(candidate,route,{},h,time.monotonic_ns()+5_000_000_000,submit=False)
        assert result['status']=='execution_preflight_passed'
        assert result['simulated_gross_profit']-result['fee_in_settlement']>=100000
        assert result['reserved_fee_wei']<=4*10**13
        assert not executor.db.execute('SELECT 1 FROM attempts').fetchone()
    finally:executor.close()
