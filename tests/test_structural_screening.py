import copy
import time
from types import SimpleNamespace
import pytest
from eth_abi import encode, decode
from searcher.core import load_config, unhex
from searcher.structural import psm_route, PLAN
from searcher.structural_runner import complete_quote
from searcher.structural_screening import ProbeSchedule, UnprofitableQuote
from searcher.context import simulation_context
from searcher.execution import Executor
from test_structural import structural_market
from test_execution import market


def test_recorded_micro_profit_is_economic_result_and_never_a_candidate():
    # Mainnet block 52668236: each attempted PSM size earned 162 raw USDC.
    config=load_config();route=psm_route('0x4c9f68e780523feb4c9bb1aad2e5cc3b6476892b')
    head={'number':hex(52668236),'timestamp':hex(1792125819),'hash':'0x'+'12'*32,'baseFeePerGas':hex(5000000)}
    calls=[]
    class MeasurementRPC:
        def request(self, method, args):
            assert method=='eth_call'
            calls.append(args)
            return '0x'+encode(['uint256','uint256'],[162,487224]).hex()
    with pytest.raises(UnprofitableQuote) as caught:
        complete_quote(MeasurementRPC(),config,'0x'+'31'*20,route,803327369,head,simulation_context(head),1)
    evidence=caught.value.evidence()
    assert evidence['gross_surplus']==162 and evidence['net_after_fee_reservation']<0
    assert evidence['required_gross_surplus']>=100000 and evidence['submitted'] is False
    assert evidence['status']=='not_profitable'
    measured_plan=decode([PLAN],unhex(calls[0][0]['data'])[4:])[0]
    assert measured_plan[3]==1  # This is only the discarded measurement.


def test_sizes_are_independent_and_changed_state_bypasses_backoff():
    now=[1.];schedule=ProbeSchedule(clock=lambda:now[0]);route=psm_route('0x'+'11'*20)
    first=('price-a','inventory-a')
    assert schedule.reject(route,100,first,economic=True)==5
    assert not schedule.ready(route,100,first)
    assert schedule.ready(route,50,first)
    assert schedule.ready(route,100,('price-b','inventory-a'))
    assert schedule.ready(route,100,first)


def test_backoff_is_bounded_and_never_a_permanent_blacklist():
    now=[0.];schedule=ProbeSchedule(clock=lambda:now[0]);route=psm_route('0x'+'11'*20)
    for expected in (5,10,20,30,30):
        assert schedule.reject(route,100,'same',economic=True)==expected
        assert not schedule.ready(route,100,'same')
        now[0]+=expected
        assert schedule.ready(route,100,'same')
    schedule.clear(route,100)
    assert schedule.ready(route,100,'same') and not schedule.entries


def test_transport_and_expired_context_do_not_inherit_economic_backoff():
    schedule=ProbeSchedule(clock=lambda:0);route=psm_route('0x'+'11'*20)
    schedule.reject(route,100,None,economic=True)
    assert schedule.reject(route,100,None)==2
    assert schedule.reject(route,100,None,refresh=True)==.05


def test_schedule_memory_bound_does_not_exclude_new_routes():
    schedule=ProbeSchedule(clock=lambda:0,capacity=3)
    route=psm_route('0x'+'11'*20)
    for amount in range(10):schedule.reject(route,amount,'state',economic=True)
    assert len(schedule.entries)==3 and schedule.ready(route,0,'state')


def test_psm_snapshot_is_exact_and_optional(monkeypatch):
    from searcher.psm_planner import discover
    from test_psm_planner import state
    pool='0x'+'11'*20;values=[state(99,100)]
    class RPC:
        def request(self,*args):return '0x'+encode(['uint256[6][]'],[values]).hex()
    captured={}
    assert discover(RPC(),[pool],10**12,100000,on_snapshot=captured.update)
    assert tuple(captured[pool])==values[0]
    assert discover(RPC(),[pool],10**12,100000)


def test_native_partial_fill_compares_actual_payment_and_removes_micro_profit():
    from searcher.psm_planner import native_screen
    pool='0x'+'11'*20;route=psm_route(pool)
    class RPC:
        multicall_verified=True
        def request(self,method,args):
            assert method=='eth_call'
            state=[1,10000,10**18,10**27,10**12,10**24]
            raw=[(True,encode(['uint256','uint256'],[1620,1782*10**12])),
                 (True,encode(['uint256[6][]'],[[state]]))]
            return '0x'+encode(['(bool,bytes)[]'],[raw]).hex()
    rows,states,evidence=native_screen(RPC(),[(route,803327369,700000)],100000)
    assert rows==[]
    assert evidence[0]['gross_surplus']==162 and evidence[0]['actual_input']==1620
    assert evidence[0]['status']=='not_profitable' and not evidence[0]['full_execution_checked']


@pytest.mark.parametrize('native_ok,returned,expected_status',[
    (False,b'','quote_unknown'),
    (True,encode(['uint256','uint256'],[0,0]),'quote_unknown'),
    (True,encode(['uint256','uint256'],[1000000,1200000*10**12]),'native_quote_admitted'),
])
def test_native_failure_keeps_fallback_and_real_margin_reaches_full_check(native_ok,returned,expected_status):
    from searcher.psm_planner import native_screen
    route=psm_route('0x'+'11'*20)
    class RPC:
        multicall_verified=True
        def request(self,*args):
            state=[1,500,10**18,10**27,10**12,10**24]
            return '0x'+encode(['(bool,bytes)[]'],[[(native_ok,returned),
                (True,encode(['uint256[6][]'],[[state]]))]]).hex()
    original=[(route,1000000,700000)]
    rows,_,evidence=native_screen(RPC(),original,100000)
    assert rows==original and evidence[0]['status']==expected_status


def test_reverse_direction_is_not_discarded_by_forward_only_screen():
    from searcher.psm_planner import native_screen
    route=psm_route('0x'+'11'*20,True)
    class RPC:
        multicall_verified=True
        def request(self,*args):
            return '0x'+encode(['(bool,bytes)[]'],[[(True,encode(['uint256[6][]'],[[[1,500,10**18,10**27,10**12,10**24]]]))]]).hex()
    original=[(route,1000000,700000)]
    rows,_,evidence=native_screen(RPC(),original,100000)
    assert rows==original and evidence[0]['status']=='quote_unknown'


def test_native_screen_pages_past_bad_top_ranked_sizes(monkeypatch):
    import searcher.psm_planner as planner
    from test_psm_planner import state
    pool='0x'+'11'*20;routes=[(psm_route(pool),i+1,700000) for i in range(17)]
    monkeypatch.setattr(planner,'proposals',lambda *args:routes)
    seen=set()
    def screen(rpc,rows,*args):
        seen.update(amount for _,amount,_ in rows)
        assert len(rows)<=8
        return [],{pool:state()},[]
    monkeypatch.setattr(planner,'native_screen',screen)
    class RPC:
        def request(self,*args):return '0x'+encode(['uint256[6][]'],[[state()]]).hex()
    for start in (0,8,16):planner.discover(RPC(),[pool],10**12,100000,native=True,screen_start=start)
    assert seen==set(range(1,18))


def test_low_profit_measurement_never_reaches_ledger_or_signing(structural_market):
    rpc,config,old,target,signer,directory,route,send=structural_market
    config=copy.deepcopy(config);config['policy']['min_net_profit_usdc_raw']=1000000
    executor=Executor(rpc,config,old,signer,directory,structural_contract=target)
    try:
        head=executor.context['head'];clock=simulation_context(head,executor.context['pending_head'])
        with pytest.raises(UnprofitableQuote) as caught:
            complete_quote(rpc,config,target,route,3030001,head,clock,executor.context['nonce'])
        assert caught.value.gross==300000
        assert caught.value.required>=1000000
        assert not executor.db.execute('SELECT 1 FROM attempts').fetchone()
    finally:executor.close()


def test_profitable_measurement_retains_policy_floor_and_can_execute(structural_market):
    rpc,config,old,target,signer,directory,route,send=structural_market
    executor=Executor(rpc,config,old,signer,directory,structural_contract=target)
    try:
        head=executor.context['head'];clock=simulation_context(head,executor.context['pending_head'])
        candidate,_=complete_quote(rpc,config,target,route,3030001,head,clock,executor.context['nonce'])
        assert candidate['minimum_gross_surplus']>=int(config['policy']['min_net_profit_usdc_raw'])
        result=executor.execute(candidate,route,{},head,time.monotonic_ns()+5_000_000_000)
        assert result['status']=='submitted'
        rpc.request('evm_mine',[]);executor.reconcile()
        assert executor.db.execute('SELECT state FROM attempts').fetchone()[0]=='success'
    finally:executor.close()


def test_running_lane_records_economics_defers_duplicate_and_never_sends(structural_market,monkeypatch,tmp_path):
    import json
    import threading
    from searcher import structural_runner as module
    rpc,config,old,target,signer,directory,route,send=structural_market
    config=copy.deepcopy(config);config['policy']['min_net_profit_usdc_raw']=1000000
    actors=['0x'+'11'*20,'0x'+'22'*20]
    class Identities:
        def __init__(self,config,book,*args):
            book.add(actors)
            self.thread=threading.Thread(target=lambda:None)
    class Access:
        def __init__(self,*args,**kwargs):self.thread=threading.Thread(target=lambda:None)
    class Reader:
        def __init__(self,*args,**kwargs):self.url=rpc.url
        def __getattr__(self,key):return getattr(rpc,key)
        def close(self):pass
    def inspect(reader,borrowers,block,clock):
        rows=[dict(borrower=a,shares=(1 if a==actors[0] else 2)*1000000,
                   debt=(1 if a==actors[0] else 2)*1000000,
                   collateral=10**12,boundary=2*10**36) for a in borrowers]
        return 10**36,int(clock['time'],16),rows
    import searcher.strategy_access as access
    monkeypatch.setattr(module,'ROOT',tmp_path)
    monkeypatch.setattr(module,'RPC',Reader)
    monkeypatch.setattr(module,'SeedWorker',Identities)
    monkeypatch.setattr(module,'inspect',inspect)
    monkeypatch.setattr(module,'sized_requests',lambda group:iter([group]))
    monkeypatch.setattr(access,'AccessWorker',Access)
    executor=Executor(rpc,config,old,signer,directory,structural_contract=target)
    try:
        result=module.run(config,seconds=1.6,executor=executor)
        assert result['counts']['economic_rejections']==1
        assert result['counts']['deferred_quotes']>=1
        assert result['counts']['recheck_failures']==0
        assert result['counts']['submitted']==0
        assert not executor.db.execute('SELECT 1 FROM attempts').fetchone()
        decisions=[json.loads(p.read_text()) for p in __import__('pathlib').Path(result['session']).glob('pipeline/decision-*.json')]
        assert any(o['status']=='not_profitable' for d in decisions for o in d['outcomes'])
    finally:executor.close()
