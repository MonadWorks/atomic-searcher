import time
import threading
from dataclasses import replace
import pytest
from searcher.core import USDC,WETH,load_config,PublicRateLimiter,PublicProviderBudget,RPCUnavailable
from searcher.morpho_universe import Universe,Pool
from searcher.route_preparation import RouteBook,bucket
from searcher.auction_planner import packages,trials_for
from searcher.structural import GENERIC
from eth_abi import decode
from test_morpho_universe import make_market
from test_atlas import notice

def universe():
    cfg=load_config();u=Universe(cfg);m=make_market();u.add([m]);f=cfg['factories'][0]['address']
    u.graph.add(Pool('0x'+'31'*20,USDC,m.collateral,f,1,500))
    for i in range(15):u.graph.add(Pool('0x'+format(i+200,'040x'),WETH,USDC,f,1,500))
    rb=RouteBook(u);u.route_book=rb
    rb.explore(m.collateral,USDC);rb.explore(USDC,WETH)
    return cfg,u,m,rb

def test_quote_size_changes_best_route_and_late_pool_is_not_excluded():
    cfg,u,m,rb=universe();paths=rb.paths_for(USDC,WETH);small,large=paths[0],paths[-1]
    for p in paths:
        rb.record(p,100,100);rb.record(p,10000,10000)
    rb.record(small,100,120);rb.record(large,100,115)
    rb.record(small,10000,7000);rb.record(large,10000,11000)
    assert rb.ranked(USDC,WETH,100)[0]==small
    assert rb.ranked(USDC,WETH,10000)[0]==large
    assert rb.estimate(large,10**9) is None

def test_stale_curve_does_not_permanently_hide_an_alternative():
    cfg,u,m,rb=universe();paths=rb.paths_for(USDC,WETH)
    for p in paths:rb.record(p,100,100,now=time.monotonic()-60)
    seen={rb.ranked(USDC,WETH,100)[0] for _ in range(len(paths))}
    assert seen==set(paths)

def test_bad_route_reopens_after_cooldown_and_other_routes_remain_usable(monkeypatch):
    cfg,u,m,rb=universe();paths=rb.paths_for(USDC,WETH)
    for p in paths:rb.record(p,100,100)
    rb.record(paths[0],100,200);rb.reject(paths[0],100,seconds=1)
    assert paths[0] not in rb.ranked(USDC,WETH,100)
    rb.failed[(paths[0],bucket(100))]=time.monotonic()-1
    assert rb.ranked(USDC,WETH,100)[0]==paths[0]

def test_path_exploration_progress_survives_new_pools_and_bounded_dead_ends():
    cfg,u,m,rb=universe();f=cfg['factories'][0]['address'];bridge='0x'+'dd'*20
    # Make one late two-hop route after many dead ends.
    for i in range(50):
        t='0x'+format(1000+i,'040x');u.graph.add(Pool('0x'+format(5000+i,'040x'),min(m.collateral,t),max(m.collateral,t),f,1,500))
    u.graph.add(Pool('0x'+'51'*20,bridge,m.collateral,f,1,500));u.graph.add(Pool('0x'+'52'*20,USDC,bridge,f,1,500))
    seen=[]
    for i in range(100):
        seen+=u.graph.path_page(m.collateral,USDC,work=8,limit=2)
        if any(len(p)==2 for p in seen):break
    assert any(len(p)==2 for p in seen)
    assert u.graph.has_path(m.collateral,USDC)

def test_packages_include_combined_single_and_partial_debts():
    rows=[dict(borrower='0x'+format(i+1,'040x'),shares=1000000,debt=1000000,collateral=1200000,boundary=2*10**36) for i in range(3)]
    plans=packages(rows,10**36,10**30,860000000000000000,6)
    assert any(len(p)==3 for p in plans) and any(len(p)==1 for p in plans)
    assert any(len(p)==3 and p[0][1]<1000000 for p in plans)
    assert all(sum(r[2] for r in p)<=10**30 for p in plans)

def test_package_planner_can_select_best_conversion_and_compares_multiple_markets():
    cfg,u,m,rb=universe();n=notice()['partial_user_operation'];second=make_market(lltv=770000000000000000);u.add([second])
    for p in rb.paths_for(USDC,WETH):rb.record(p,65536,10**12)
    winner=rb.paths_for(USDC,WETH)[-1];rb.record(winner,65536,10**14)
    rows=[dict(borrower='0x'+format(i+1,'040x'),shares=1000000,debt=1000000,collateral=1200000,boundary=2*10**36) for i in range(2)]
    inspected={m.id:(10**36,0,rows),second.id:(10**36,0,rows)}
    trials=trials_for(u,cfg,n,inspected,[m,second],1,10**14)
    assert len(trials)<=8 and {t['market'] for t in trials}=={m.id,second.id}
    assert any(t['borrowers']==2 and t['conversion']==winner for t in trials)
    assert any(t['borrowers']==1 for t in trials)

def test_market_read_failure_backs_off_only_affected_market():
    cfg,u,m,rb=universe();other=make_market(lltv=770000000000000000);u.add([other])
    u.read_failed(m.id)
    assert all(u.choose()[0].id==other.id for _ in range(5))
    u.read_succeeded(m.id)
    assert m.id in {u.choose()[0].id for _ in range(20)}

@pytest.mark.parametrize('kind',['public','provider'])
def test_expired_pacing_ticket_is_removed_and_next_request_can_progress(kind):
    limiter=PublicRateLimiter() if kind=='public' else PublicProviderBudget()
    kwargs={'critical':True} if kind=='public' else {}
    with pytest.raises(RPCUnavailable,match='deadline'):
        limiter.acquire(1,deadline_ns=time.monotonic_ns()-1,**kwargs)
    if kind=='public':assert limiter.critical_waiters==0 and not limiter.critical_queue
    else:assert not limiter.queue and limiter.active is None
    ticket=limiter.acquire(1,**kwargs)
    if kind=='provider':limiter.finish(ticket,200)


def test_new_direct_pool_can_be_used_before_quote_worker_visits_market():
    cfg=load_config();u=Universe(cfg);m=make_market();u.add([m]);rb=RouteBook(u);u.route_book=rb
    assert rb.ranked(m.collateral,USDC,10**18)==[]
    f=cfg['factories'][0]['address'];pool=Pool('0x'+'31'*20,USDC,m.collateral,f,1,500);u.graph.add(pool)
    assert rb.ranked(m.collateral,USDC,10**18)==[(pool.hop(m.collateral),)]
