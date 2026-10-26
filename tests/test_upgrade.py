import copy
from collections import defaultdict
import pytest
from searcher.core import WETH,USDC,load_config,Incomplete,Reject
from searcher.routes import optimize,input_frontiers,route_capacity,RouteIndex
from searcher.universe import CyclePages,swap_activity
from searcher.costs import competitive_bid
from test_searcher import _liquidity_frontier_route,pool,A,B,C,H0

@pytest.mark.parametrize('zero',[True,False])
@pytest.mark.parametrize('empty',[True,False])
def test_optimizer_finds_profitable_far_liquidity_despite_huge_input_ceiling(zero,empty):
    route,states,*_ = _liquidity_frontier_route(zero,empty)
    cap,limited=route_capacity(route,states,(1<<255)-1)
    result=optimize(route,states,cap,lambda ticks:10*ticks)
    out,crossings=route.quote(states,1_000_000)
    assert result['net_after_modelled_cost'] >= out-1_000_000-10*crossings > 0
    assert result['amount_in'] > 1_000_000
    assert result['sizing_search_maximum']==cap and limited
    for p,token in zip(route.pools,route.tokens):
        capacity,_,unknown = input_frontiers(states[p],token)
        assert unknown and capacity>0
        states[p].quote(token,capacity)
        with pytest.raises(Incomplete):states[p].quote(token,capacity+1)


def test_cycle_pages_preserve_quiet_return_leg_and_resume_across_refresh():
    ps=[]
    for i in range(30):
        other='0x'+f'{1000+i:040x}'
        ps.extend([pool('0x'+f'{100+i*2:040x}',A,other),pool('0x'+f'{101+i*2:040x}',A,other)])
    pages=CyclePages(ps,[A],3)
    seen=set()
    for i in range(50):
        selected=pages.take(9,group_limit=4)
        seen.update(g for g in pages.selected_groups)
        for group in pages.selected_groups:assert set(group).issubset({p.address for p in selected})
        replacement=CyclePages(ps,[A],3,groups=pages.groups)
        replacement.resume(pages);pages=replacement
    assert seen==set(pages.groups)
    # Index only explicitly scheduled complete groups, not all O(n^3) combinations.
    chosen=pages.take(9,group_limit=4)
    index=RouteIndex(chosen,[A],3,groups=pages.selected_groups)
    assert {tuple(sorted(r.pools)) for r in index.routes.values()}==set(pages.selected_groups)


def test_filtered_discovery_deduplicates_and_rejects_wrong_anchor():
    from searcher.events import V3_SWAP
    log={'address':A,'blockNumber':'0xb','blockHash':H0,'transactionHash':H0,'logIndex':'0x0','topics':[V3_SWAP]}
    class RPC:
        def request(self,method,params):
            assert method=='eth_getLogs' and params[0]['topics'][0][0]==V3_SWAP
            return [log,log]
    activity,seen=swap_activity(RPC(),11,11,H0)
    assert activity[A]==1 and seen[A]==11
    with pytest.raises(Reject,match='anchor'):swap_activity(RPC(),11,11,'0x'+'ff'*32)

@pytest.mark.parametrize('token',[WETH,USDC])
def test_competitive_bid_grows_only_when_profit_supports_it(token):
    p=load_config()['policy'];p['bid_profit_share_bps']=2500
    context={'observed_priority_fee':50_000_000,'extra_fee':10**12,'balance':10**18,
             'usdc_at_native_cap':500_000,'native_cap':10**14}
    small=5*10**12 if token==WETH else 25_000
    big=10**18 if token==WETH else 1_000_000
    low,_=competitive_bid(p,token,context,small,600_000,5_000_000,1_000_000,0)
    high,meta=competitive_bid(p,token,context,big,600_000,5_000_000,1_000_000,0)
    assert low<high<=55_000_000
    assert (10_000_000+high)*600_000+context['extra_fee']<=int(p['max_total_fee_wei'])
    assert meta['selected_priority_fee_wei']==high

def test_optimizer_refines_an_interior_peak_even_when_many_boundaries_rank_higher():
    from searcher.routes import Route
    p=pool(A,tick=1000,liquidity=10**8);q=pool(B,tick=0,liquidity=10**8)
    for state in (p,q):state.fee=0;state.bitmap={i:0 for i in range(-8,9)};state.ticks={}
    for t in (520,521,522):p.ticks[t]=(2,0);p.bitmap[t>>8]|=1<<(t&255)
    r=Route((A,B,A),(A,B));states={A:p,B:q}
    cap,_=route_capacity(r,states)
    out,ticks=r.quote(states,2_377_840)
    result=optimize(r,states,cap,lambda c:25*c)
    assert result['net_after_modelled_cost']>=out-2_377_840-25*ticks


def test_route_fairness_survives_page_switches(monkeypatch):
    from searcher.pipeline import PendingStrategy
    from searcher.statefeed import StateFrame
    from test_pipeline import RPCStub,saved,header,H1
    import time
    c=load_config();c['settlement_tokens']=[{'address':A}]
    strategy=PendingStrategy(RPCStub(),c,saved())
    seen=[]
    def slow(index,config,h,changed):
        key=index.work_queue.popleft();index.queued_routes.remove(key)
        index.last_evaluated_keys=[key];seen.append(key)
        return {'candidates':[]}
    monkeypatch.setattr('searcher.pipeline.evaluate',slow)
    # Force a page rebuild between observations, simulating discovery rotation.
    for _ in range(2):
        now=time.monotonic_ns()
        f=StateFrame(header(2,H1,H0),header(2,H1,H0),saved()['pools'],now,now,time.time(),0,100)
        strategy.last_groups='force-page-change'
        strategy.on_snapshot(f)
    assert seen[0]!=seen[1]

def test_newer_snapshot_cannot_silently_drop_an_unconsumed_page():
    from searcher.universe import PageProgress
    gate=PageProgress()
    advance,first=gate.begin();assert advance
    # Faster reader only overwrites this SAME page until slower consumer reads.
    for _ in range(8):assert gate.begin()==(False,first)
    gate.consumed(first)
    advance,second=gate.begin();assert advance and second!=first
    gate.consumed(first) # stale duplicate of the preceding page cannot skip this one
    assert gate.begin()==(False,second)
    gate.consumed(second)
    assert gate.begin()[0]

def test_stationary_candidates_bound_work_for_large_integer_tick_segments():
    from searcher.routes import Route
    p=pool(A,A,B,tick=1000,liquidity=10**18)
    q=pool(B,B,C,tick=0,liquidity=10**18)
    r=pool(C,A,C,tick=0,liquidity=10**18)
    for state,indices in [(p,range(680,1000,20)),(q,range(40,340,40)),(r,range(20,340,20))]:
        state.fee=0;state.bitmap={i:0 for i in range(-4,9)};state.ticks={}
        for t in indices:state.ticks[t]=(2,0);state.bitmap[t>>8]|=1<<(t&255)
    route=Route((A,B,C,A),(A,B,C));states={A:p,B:q,C:r}
    cap,_=route_capacity(route,states)
    out=optimize(route,states,cap,lambda n:(780000+25000*n)*1_000_000)
    assert out['complete'] and out['net_after_modelled_cost']>0
    assert out['quote_count']<1000 # amount precision must not imply thousands of swap replays


def test_public_node_pacing_counts_rpc_units_and_reserves_submission_capacity(monkeypatch):
    from searcher.core import PublicRateLimiter
    import searcher.core as core
    clock=[0.]
    monkeypatch.setattr(core.time,'monotonic',lambda:clock[0])
    monkeypatch.setattr(core.time,'sleep',lambda seconds:clock.__setitem__(0,clock[0]+seconds))
    gate=PublicRateLimiter();gate.acquire(21,critical=True)
    gate.acquire(1,submission=True)
    assert clock[0]==0 # signed tx is not held behind ordinary reads
    gate.acquire(1)
    assert clock[0]>=1.05
