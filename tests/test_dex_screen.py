import math
import threading
import time
from contextlib import nullcontext

import pytest
from eth_abi import encode
from searcher.core import USDC,WETH,load_config,data
from searcher.dex_screen import Scheduler,edges_for,read_markets
from searcher.morpho_universe import Pool,Universe,ConversionGraph
from searcher.morpho_worker import UniverseWorker,AERO_CREATE
from searcher.strategy_suite import Suite
from searcher.execution import Executor
from searcher.context import simulation_context
from test_funded_conversion_cycle import setup_cycle,Q,P,C,F,AERO
from test_strategy_suite import protocol_market
from test_structural import structural_market
from test_execution import market

def address(i):return '0x'+format(i,'040x')
def pool(i,a,b):return Pool(address(i),*sorted((a,b)),F,3,0)
def row(p,start,ratio=1,capacity=10**15):
    a,b=(capacity,int(capacity*ratio)) if start==p.token0 else (int(capacity*ratio),capacity)
    return (True,0,0,a,b,0,0,3000)
def settlements():return [{'address':USDC,'decimals':6,'max_input_raw':str(10**15)},
                           {'address':WETH,'decimals':18,'max_input_raw':str(10**24)}]

def test_new_pool_enters_price_search_without_finishing_old_paths():
    now=[1.];s=Scheduler(settlements(),4,lambda:now[0]);c=address(100)
    pools=[pool(i+1000,USDC,c) for i in range(300)]
    s.sync({p.address:p for p in pools});s.record(pools,[row(p,USDC) for p in pools],1)
    s._explore(USDC) # Old combinatorial traversal is deliberately still active.
    good=pool(5000,USDC,c);s.sync({**s.pools,good.address:good})
    assert good in s.page()
    s.record([good],[row(good,USDC,2)],2)
    ranked=s.ranked();assert any(good.address in [h[0] for h in x[1]] for x in ranked)
    assert s.last_search['edge_visits']<10000

def test_price_change_bypasses_same_state_retry_delay_and_stale_prices_expire():
    now=[10.];s=Scheduler(settlements(),4,lambda:now[0]);c=address(100)
    a,b=pool(1000,USDC,c),pool(1001,USDC,c);s.sync({p.address:p for p in (a,b)})
    s.record([a,b],[row(a,USDC,2),row(b,USDC)],1)
    assert s.ranked();s.quotes();assert not s.ranked()
    s.record([a],[row(a,USDC,3)],2);assert s.ranked()
    now[0]+=46;assert not s.ranked()
    assert s.quotes() # Unpriced/failing screening retains bounded exploration.

def test_hot_queue_cannot_starve_cold_or_unknown_pools():
    s=Scheduler(settlements(),4);pools=[pool(i+1000,USDC,address(100+i)) for i in range(200)]
    s.sync({p.address:p for p in pools});seen=set()
    for _ in range(10):
        s.hot.extend([p.address for p in pools[:32]]);page=s.page();seen.update(p.address for p in page)
    assert seen==set(s.pools)
    assert len(page)==len({p.address for p in page})<=96

def test_both_funding_assets_get_priority_and_cycles_are_simple():
    s=Scheduler(settlements(),4);pools=[]
    for i,start in enumerate((USDC,WETH)):
        a,b=pool(1000+2*i,start,address(100+i)),pool(1001+2*i,start,address(100+i));pools.extend((a,b))
    s.sync({p.address:p for p in pools})
    s.record(pools,[row(p,start,2 if j==0 else 1,capacity=10**15 if start==USDC else 10**22) for start in (USDC,WETH) for j,p in enumerate(pools[0:2] if start==USDC else pools[2:])],1)
    ranked=s.ranked();assert {x[4]['address'] for x in ranked[:2]}=={USDC,WETH}
    for _,path,_,_,_ in ranked:
        assert 2<=len(path)<=4 and len({h[0] for h in path})==len(path)
        assert len({h[1] for h in path})==len(path) and path[-1][2]==path[0][1]

def test_native_stable_quotes_and_full_width_capacity_are_used():
    p=Pool(address(1000),*sorted((USDC,C)),AERO,4,1)
    r=(True,0,0,1<<130,1<<130,10**30,10**29,0)
    es=edges_for(p,r);assert len(es)==2
    assert es[0].rate==pytest.approx(math.log(r[5])-math.log(r[3]//10**6))
    assert not edges_for(p,(False,*r[1:]))

def test_dust_extreme_price_cannot_evict_usable_capacity():
    s=Scheduler(settlements(),4);a=address(100)
    ps=[pool(i+1000,USDC,a) for i in range(4)];s.sync({p.address:p for p in ps})
    rows=[row(ps[0],USDC,2),row(ps[1],USDC,1),row(ps[2],USDC,10**12,capacity=100),row(ps[3],USDC,1,capacity=100)]
    s.record(ps,rows,1);ranked=s.ranked();assert ranked
    assert all(h[0] not in {ps[2].address,ps[3].address} for x in ranked for h in x[1])

def test_exploration_rotates_first_pool_and_zero_quotes_are_temporary():
    now=[1.];s=Scheduler(settlements(),4,lambda:now[0]);a=address(100)
    ps=[pool(i+1000,USDC,a) for i in range(8)];s.sync({p.address:p for p in ps})
    seen={s._explore(USDC)[0][0] for _ in range(8)};assert seen==set(s.pools)
    s.record(ps,[row(p,USDC,2 if i==0 else 1) for i,p in enumerate(ps)],1)
    selected=s.quotes();s.native_results(selected,[0]*len(selected));assert s.native_failures
    now[0]+=6;assert s.ranked()

def test_disconnected_factory_pool_survives_until_later_bridge(tmp_path,monkeypatch):
    config=load_config();u=Universe(config);w=UniverseWorker(config,u,threading.Event(),tmp_path,'https://mainnet.base.org')
    a,b=address(200),address(201)
    def authenticate(self,rpc,pools,block='latest'):
        for p in pools:self.add(p)
    monkeypatch.setattr(ConversionGraph,'authenticate',authenticate)
    def create(p,t0,t1):
        w.apply_log(None,{'address':AERO,'topics':[AERO_CREATE,'0x'+encode(['address'],[t0]).hex(),
             '0x'+encode(['address'],[t1]).hex(),'0x'+encode(['bool'],[False]).hex()],
             'data':'0x'+encode(['address','uint256'],[p,1]).hex()})
    create(address(1000),a,b)
    assert address(1000) in u.graph.pools
    create(address(1001),b,USDC)
    assert u.graph.has_path(a,USDC)

def test_real_screen_to_native_quote_to_repayment_receipt(protocol_market):
    rpc,c,old,target,signer,directory,contracts,_,send=protocol_market
    _,path=setup_cycle(protocol_market,stable=True)
    u=Universe(c)
    u.graph.factories.update({AERO:{'address':AERO,'kind':'aerodrome_v2'},F:{'address':F,'kind':'v3','keys':[500]}})
    for p in (Pool(Q,C,USDC,AERO,4,1),Pool(P,C,USDC,F,1,500)):u.graph.add(p)
    ex=Executor(rpc,c,old,signer,directory,structural_contract=target)
    suite=Suite(c,u,{**contracts,'StructuralExecutor':target},lambda:ex.context,threading.Event(),directory,rpc.url)
    suite.report('dex',state='starting');rpc.read_slot=lambda:nullcontext()
    try:
        # No hand-selected route enters the scheduler: it must find the cycle
        # from authenticated pool metadata and actual local EVM price reads.
        suite.dex(rpc,threading.Event())
        assert suite.health['dex']['complete_quotes']>0
        result=suite.consume(ex);assert result['status']=='submitted'
        rpc.request('evm_mine',[]);ex.reconcile()
        assert ex.db.execute('select state from attempts').fetchone()[0]=='success'
    finally:ex.close()

def test_screen_one_bad_pool_does_not_hide_healthy_native_pool(protocol_market):
    rpc,c,old,target,signer,directory,contracts,_,send=protocol_market
    setup_cycle(protocol_market,stable=True)
    h=rpc.header();clock=simulation_context(h)
    pools=[Pool(address(9000),C,USDC,F,1,500),Pool(Q,C,USDC,AERO,4,1)]
    rows,_=read_markets(rpc,pools,clock)
    assert not rows[0][0] and rows[1][0] and rows[1][5]>0

def test_failed_shared_direction_rotates_but_smaller_success_and_reverse_survive():
    now=[1.];s=Scheduler(settlements(),4,lambda:now[0]);a=address(100)
    ps=[pool(i+1000,USDC,a) for i in range(3)];s.sync({p.address:p for p in ps})
    s.record(ps,[row(p,USDC,2 if i==0 else 1) for i,p in enumerate(ps)],1)
    path=(ps[0].hop(USDC),ps[1].hop(a));q=(path,100,settlements()[0])
    s.native_results([q],[0],[1]);edge=(ps[0].address,USDC)
    assert edge in s.edge_failures and (ps[0].address,a) not in s.edge_failures
    assert all(path[0] not in candidate[1] for candidate in s.ranked())
    now[0]+=6;assert any(path[0] in candidate[1] for candidate in s.ranked())
    s.native_results([q,q],[0,120],[1,0]);assert edge not in s.edge_failures
    s.native_results([q],[0],[255]);assert edge not in s.edge_failures

def test_stale_account_clock_preserves_new_pools_and_does_not_shrink_page(tmp_path,monkeypatch):
    from searcher.core import Reject
    from types import SimpleNamespace
    import searcher.dex_screen as ds
    config=load_config();u=Universe(config)
    p=Pool(address(1000),*sorted((USDC,address(100))),F,1,500)
    u.graph.add(p)
    suite=Suite(config,u,{},lambda:None,threading.Event(),tmp_path,'unused')
    rpc=SimpleNamespace(read_slot=lambda:nullcontext());scope=threading.Event()
    for _ in range(5):
        with pytest.raises(Reject,match='account context refreshing'):suite.dex(rpc,scope)
    scheduler=suite.dex_scheduler
    assert scheduler.page_size==96 and scheduler.cursor==0
    assert list(scheduler.new)==[p.address] and not scheduler.samples
    monkeypatch.setattr(suite,'current',lambda:({}, {}, {}))
    def failed(*args):raise TimeoutError('actual read timed out')
    monkeypatch.setattr(ds,'read_markets',failed)
    with pytest.raises(TimeoutError):suite.dex(rpc,scope)
    assert scheduler.page_size==48

def test_repeated_failure_backoff_counts_batches_and_independent_exploration_survives():
    now=[1.];s=Scheduler(settlements(),4,lambda:now[0]);a=address(100)
    ps=[pool(i+1000,USDC,a) for i in range(2)];s.sync({p.address:p for p in ps})
    path=(ps[0].hop(USDC),ps[1].hop(a));q=(path,100,settlements()[0]);edge=(ps[0].address,USDC)
    for strike,delay in enumerate((5,15,45,120,300,300),1):
        s.native_results([q,q,q],[0,0,0],[1,1,1])
        assert s.edge_failures[edge]==now[0]+delay
        assert s.edge_failure_history[edge][0]==strike
        # This graph has only two-hop cycles; the explorer also deliberately
        # tries three- and four-hop lengths before rotating back.
        assert any(s._explore(USDC) is not None for _ in range(12))
        now[0]+=delay+1
    now[0]+=901;s.native_results([],[])
    assert not s.edge_failure_history and not s.edge_failures

def test_only_observed_market_change_or_success_resets_repeated_direction_failure():
    now=[1.];s=Scheduler(settlements(),4,lambda:now[0]);a=address(100)
    ps=[pool(i+1000,USDC,a) for i in range(2)];s.sync({p.address:p for p in ps})
    rows=[row(ps[0],USDC,2),row(ps[1],USDC,1)];s.record(ps,rows,1)
    path=(ps[0].hop(USDC),ps[1].hop(a));q=(path,100,settlements()[0]);edge=(ps[0].address,USDC)
    s.native_results([q],[0],[1]);now[0]+=6;s.native_results([q],[0],[1])
    s.record(ps,rows,2);assert s.edge_failure_history[edge][0]==2
    s.record([ps[1]],[row(ps[1],USDC,3)],3);assert edge in s.edge_failures
    s.record([ps[0]],[(False,*rows[0][1:])],4);assert edge in s.edge_failures
    s.record([ps[0]],[rows[0]],5);assert edge in s.edge_failures
    s.record([ps[0]],[row(ps[0],USDC,3)],5);assert edge not in s.edge_failures and edge not in s.edge_failure_history
    s.native_results([q],[0],[1]);s.native_results([q,q],[0,120],[1,0])
    assert edge not in s.edge_failures and edge not in s.edge_failure_history


def test_losing_small_completion_cannot_reset_repeated_large_failure():
    now=[1.];s=Scheduler(settlements(),4,lambda:now[0]);a=address(100)
    ps=[pool(i+1000,USDC,a) for i in range(3)];s.sync({p.address:p for p in ps})
    s.record(ps,[row(p,USDC,2 if i==0 else 1) for i,p in enumerate(ps)],1)
    edge=(ps[0].address,USDC)
    # Two cycles share the failing venue but change their closing pool.
    # Small native swaps complete at a loss, large ones fail. Previously each
    # small completion erased the failure, starving unrelated combinations.
    for turn,delay in enumerate((5,15,45)):
        path=(ps[0].hop(USDC),ps[1+turn%2].hop(a))
        quotes=[(path,100,settlements()[0]),(path,2500,settlements()[0])]
        s.native_results(quotes,[1,0],[0,1],[False,False])
        assert s.edge_failure_history[edge][0]==turn+1
        assert s.edge_failures[edge]==now[0]+delay
        assert all(path[0] not in candidate[1] for candidate in s.ranked())
        now[0]+=delay+1
    # A tiny positive output that cannot pay costs still leaves priority to
    # other routes; a genuinely promising smaller quote immediately reopens.
    s.native_results(quotes,[101,0],[0,1],[False,False])
    assert s.edge_failure_history[edge][0]==4
    s.native_results(quotes,[150,0],[0,1],[True,False])
    assert edge not in s.edge_failure_history and edge not in s.edge_failures


def test_successful_prefix_of_failed_cycle_does_not_reset_prior_failure():
    s=Scheduler(settlements(),4);a=address(100)
    ps=[pool(i+1000,USDC,a) for i in range(2)];s.sync({p.address:p for p in ps})
    path=(ps[0].hop(USDC),ps[1].hop(a));q=(path,100,settlements()[0]);edge=(ps[0].address,USDC)
    s.native_results([q],[0],[1],[False])
    s.native_results([q],[0],[2],[False])
    assert edge in s.edge_failures
    # Unknown outer failures do not assign a new guilty pool.
    before=dict(s.edge_failure_history);s.native_results([q],[0],[255],[False])
    assert s.edge_failure_history==before

@pytest.mark.parametrize('broken',[1,2,255])
def test_native_detailed_failure_identifies_actual_hop_and_amount(protocol_market,broken):
    from eth_abi import decode
    from searcher.core import unhex
    from searcher.structural import HOPS,artifact
    from searcher.dex_screen import LENS
    rpc,c,old,target,signer,directory,contracts,_,send=protocol_market
    _,path=setup_cycle(protocol_market,stable=True)
    if broken==255:path=(path[0],path[0])
    else:rpc.request('anvil_setCode',[path[broken-1][0],'0x'])
    clock=simulation_context(rpc.header())
    call={'to':LENS,'gas':hex(15_000_000),'data':data('quoteExecutableCyclesDetailed('+HOPS+'[],uint256[],uint256)',
          [HOPS+'[]','uint256[]','uint256'],[[path],[10**7],1_500_000])}
    raw=rpc.request('eth_call',[call,'pending',{LENS:{'code':artifact('CycleQuoteLens')['deployedBytecode']['object']}},clock])
    outputs,inputs,failed,amounts=decode(['uint256[]','uint256[]','uint8[]','uint256[]'],unhex(raw))
    assert outputs==(0,) and inputs==(10**7,) and failed==(broken,)
    if broken==1:assert amounts==(10**7,)
    elif broken==2:assert 0<amounts[0]<10**7
    else:assert amounts==(0,)
