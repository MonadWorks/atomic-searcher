import threading
import time
from contextlib import nullcontext
from types import SimpleNamespace

import pytest
from searcher.core import USDC, WETH, RPCError, data
from searcher.dex_feedback import DexFeedback, route_for
from searcher.dex_screen import Scheduler
from searcher.transfer_feedback import TransferSample
from searcher.strategy_suite import Suite
from searcher.morpho_universe import Pool as MetadataPool, optional_views
from searcher.pools import Pool
from searcher.routes import Route
from searcher.costs import cost_budget, quote_profit_floor
from searcher.engine import probe_cycle
from test_execution import market
from test_dex_screen import address, pool, row, settlements

OWNER=address(10);EXECUTOR=address(11);STRUCTURAL=address(12)
BAD={'revert_name':'BadCallback','revert_selector':data('BadCallback()')[:10]}


def feedback(tmp_path, clock):
    return DexFeedback(OWNER, {'AtomicCycle':EXECUTOR,'StructuralExecutor':STRUCTURAL}, tmp_path, clock)


def reject(f,path):
    return f.rejected(path, 100, BAD, block=100, gross=5, floor=1)


def record_tax(f,path):
    sample=TransferSample(path[0][2],path[0][0],path[1][0],100)
    f.transfers['AtomicCycle'].record(sample, dict(token=sample.token,source=sample.source,
        destination=sample.destination,executor=EXECUTOR,amount=100,funded=96,debited=96,
        credited=92,canonical=True,block=100,block_hash='0x'+'11'*32))


def test_native_positive_and_price_change_do_not_clear_whole_call_failure(tmp_path):
    now=[1.];f=feedback(tmp_path,lambda:now[0]);s=Scheduler(settlements(),4,lambda:now[0],feedback=f)
    a,b=pool(1000,USDC,address(100)),pool(1001,USDC,address(100))
    path=(a.hop(USDC),b.hop(address(100)))
    s.sync({p.address:p for p in (a,b)});s.record([a,b],[row(a,USDC,2),row(b,USDC)],1)
    assert any(x[1]==path for x in s.ranked())
    assert reject(f,path)==30
    s.native_results([(path,100,settlements()[0])],[120],[0],[True])
    s.record([a],[row(a,USDC,3)],2)
    assert not f.allowed(path) and all(x[1]!=path for x in s.ranked())
    assert all(p!=path for p,_,_ in s.quotes())
    now[0]+=31
    assert f.allowed(path) and reject(f,path)==60
    f.passed(path,100,block=100,gross=10,floor=1)
    assert f.allowed(path) and reject(f,path)==30


def test_shared_direction_is_scoped_to_executor_and_does_not_remove_other_good_paths(tmp_path):
    now=[1.];f=feedback(tmp_path,lambda:now[0]);s=Scheduler(settlements(),4,lambda:now[0],feedback=f)
    bad,good=address(100),address(101)
    ps=[pool(1000,USDC,bad),pool(1001,USDC,bad),pool(1002,USDC,bad),
        pool(1003,USDC,good),pool(1004,USDC,good)]
    path=(ps[0].hop(USDC),ps[1].hop(bad));record_tax(f,path)
    # Both the same output transfer and the same input transfer are shared.
    assert not f.allowed((ps[0].hop(USDC),ps[2].hop(bad)))
    assert not f.allowed((ps[2].hop(USDC),ps[1].hop(bad)))
    assert f.allowed((ps[1].hop(USDC),ps[0].hop(bad)))
    structural=((ps[0].address,USDC,bad,4),ps[1].hop(bad))
    assert f.allowed(structural)
    s.sync({p.address:p for p in ps})
    s.record(ps,[row(p,USDC,2 if i in (0,3) else 1) for i,p in enumerate(ps)],1)
    choices=s.ranked()
    assert any(good in [h[1] for h in x[1]] for x in choices)
    assert all(path[0] not in x[1] for x in choices)
    # The shared retry is available again, then a real rejection consumes it.
    now[0]+=31
    assert f.admit(path)
    assert not f.allowed((ps[0].hop(USDC),ps[2].hop(bad)))
    reject(f,path)
    now[0]+=31
    assert not f.allowed((ps[0].hop(USDC),ps[2].hop(bad)))
    now[0]+=30
    assert f.allowed(path)


@pytest.mark.parametrize('failure',[
    {'cause':'read timed out'}, {'revert_name':'InsufficientProfit'},
    dict(BAD,validation_deadline_reached=True),dict(BAD,anchor_mismatch={ 'block':100})])
def test_transient_and_economic_failures_do_not_poison_routes(tmp_path,failure):
    f=feedback(tmp_path,time.monotonic);path=((address(20),USDC,WETH,3),(address(21),WETH,USDC,3))
    assert f.rejected(path,100,failure,block=100,gross=2,floor=1)==0
    assert f.allowed(path) and not f.pending


def test_failed_profit_conversion_does_not_reject_an_alternative_conversion(tmp_path):
    f=feedback(tmp_path,time.monotonic);path=((address(20),WETH,USDC,4),(address(21),USDC,WETH,3))
    f.rejected(path,100,BAD,block=100,gross=2,floor=1,execution_key='cycle:bad-conversion')
    assert f.allowed(path) and not f.allowed(path,'cycle:bad-conversion')
    assert f.admit(path,'cycle:other-conversion')


def test_concurrent_publish_serializes_diagnostic_and_scan_writes(tmp_path,monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    import searcher.dex_feedback as module
    f=feedback(tmp_path,time.monotonic);active=[]
    def write(path,value):
        assert not active
        active.append(1);time.sleep(.005);active.pop()
    monkeypatch.setattr(module,'atomic_json',write)
    with ThreadPoolExecutor(max_workers=2) as workers:
        list(workers.map(lambda _:f.publish(),range(20)))


def test_complete_conversion_below_cost_still_clears_transfer_suspicion(tmp_path,monkeypatch):
    from searcher.core import load_config
    from searcher.structural_screening import UnprofitableQuote
    import searcher.structural_runner as module
    config=load_config()
    suite=Suite(config,SimpleNamespace(),{'StructuralExecutor':STRUCTURAL},lambda:None,
                threading.Event(),tmp_path,'unused')
    suite.report('dex',state='starting')
    monkeypatch.setattr(suite,'current',lambda:({'nonce':1},{'number':'0x64'},{}))
    path=((address(20),USDC,WETH,4),(address(21),WETH,USDC,1))
    def quote(*args):raise UnprofitableQuote({'gross_surplus':100,'model_fee_raw':200,'minimum_gross_surplus':300})
    monkeypatch.setattr(module,'complete_quote',quote)
    suite.conversion_cycle_quote(SimpleNamespace(read_slot=lambda:nullcontext()),path,1000,100)
    assert suite.dex_feedback.counts['complete_calls_passed']==1
    assert suite.health['dex']['complete_quotes']==0 and suite.mailbox.take() is None


def test_real_taxed_native_positive_gets_one_full_check_then_learns_and_recovers(market,monkeypatch):
    import searcher.core as core
    import searcher.morpho_universe as mu
    monkeypatch.setattr(mu,'MULTICALL_HASH',core.MULTICALL_HASH)
    rpc,c,target,signer,directory,index,candidate,head,send=market
    now=[1.];route=index.routes[candidate['route_key']]
    path=tuple((p,a,b,1) for p,a,b in zip(route.pools,route.tokens,route.tokens[1:]))
    metadata={p.address:MetadataPool(p.address,p.token0,p.token1,p.factory,1,p.fee) for p in index.states.values()}
    def context():return dict(requested_ns=time.monotonic_ns(),head=rpc.header(),pending_head=None,nonce=1)
    suite=Suite(c,SimpleNamespace(),{'AtomicCycle':target},context,threading.Event(),directory,rpc.url)
    suite.dex_feedback=DexFeedback(c['owner'],{'AtomicCycle':target},directory,lambda:now[0])
    suite.report('dex',state='starting');rpc.read_slot=lambda:nullcontext()
    calls=[]
    def probe(*a,**kw):calls.append(1);return probe_cycle(*a,**kw)
    def quote():
        suite.dex_quote(rpc,[(path,n,c['settlement_tokens'][0]) for n in (10**16,2*10**16,3*10**16)],
            metadata,Pool,Route,cost_budget,quote_profit_floor,optional_views,probe)
    send(USDC,data('setBad(bool,uint256)',['bool','uint256'],[False,400]))
    quote()
    assert suite.health['dex']['native_positive']==3 and len(calls)==1
    assert suite.health['dex']['complete_quotes']==0 and suite.mailbox.take() is None
    assert suite.dex_feedback.diagnose_one(rpc)
    learned=suite.dex_feedback.transfers['AtomicCycle'].summary()
    assert learned['recent_samples'][0]['receipt_shortfall'] is True
    assert learned['recent_samples'][0]['credited']*100==learned['recent_samples'][0]['funded']*96
    quote();assert len(calls)==1
    # A different first-pool path still shares the actual outgoing tax leg.
    alternate=((address(999),*path[0][1:]),path[1])
    assert not suite.dex_feedback.allowed(alternate)
    send(USDC,data('setBad(bool,uint256)',['bool','uint256'],[False,0]))
    now[0]+=31
    quote()
    assert suite.health['dex']['complete_quotes']>0 and suite.mailbox.take() is not None
    assert suite.dex_feedback.allowed(alternate)
