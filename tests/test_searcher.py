import copy
import json
from types import SimpleNamespace

import pytest
from eth_abi import encode

from searcher.core import RPC, Reject, Incomplete, WETH, USDC, load_config
from searcher.costs import cost_budget, expected_value
from searcher.events import StateBook, V3_SWAP, MINT, BURN
from searcher.ledger import RiskLedger
from searcher.math import Q96, MIN_TICK, MAX_TICK, MIN_SQRT, MAX_SQRT, sqrt_at_tick, next_tick, swap_step
from searcher.pools import Pool
from searcher.recorder import Recorder, decode_payload
from searcher.routes import RouteIndex, Route, marginal_profitable, optimize, gross_upper_bound

A, B, C = ("0x"+f"{x:040x}" for x in (1, 2, 3))
H0, H1 = "0x"+"11"*32, "0x"+"22"*32


def pool(addr=A, t0=A, t1=B, kind="v3", tick=0, liquidity=10**12):
    return Pool(addr, C, kind, t0, t1, 1, 3000, sqrt_at_tick(tick), tick, liquidity,
                {-2:0,-1:0,0:0,1:0,2:0}, {}, 1, H0)


def log(position=0, kind=V3_SWAP, address=A, raw=None, topics=None, blockhash=H1):
    return {"address":address,"blockHash":blockhash,"transactionHash":"0x"+f"{position+100:064x}",
            "transactionIndex":hex(position),"logIndex":hex(position),"topics":topics or [kind],
            "data":"0x"+(raw or encode(["int256","int256","uint160","uint128","int24"],[10,-9,sqrt_at_tick(1),10**12,1])).hex()}


def book(p=None):
    b=StateBook([p or pool()], {"number":"0x1","hash":H0})
    b.begin({"number":"0x2","hash":H1,"parentHash":H0})
    return b


def test_tickmath_reference_boundaries():
    assert sqrt_at_tick(MIN_TICK)==MIN_SQRT
    assert sqrt_at_tick(MAX_TICK)==MAX_SQRT
    assert sqrt_at_tick(0)==Q96
    assert sqrt_at_tick(-1)==79224201403219477170569942574
    assert sqrt_at_tick(1)==79232123823359799118286999568


@pytest.mark.parametrize('batched', [False, True])
@pytest.mark.parametrize('status', [401, 403])
def test_rpc_access_denial_is_permanent_and_redacts_provider_credentials(monkeypatch, batched, status):
    import httpx
    from searcher.core import RPCAccessDenied
    rpc = RPC('http://127.0.0.1:1')
    sent = []
    def post(payload):
        sent.append(payload)
        return httpx.Response(status, request=httpx.Request('POST', 'https://example.invalid/private-token'),
                              json={'error': {'message': 'Archive requests require a personal token. Get one at: https://example.invalid/private-token'}},
                              extensions={'headers_elapsed_ms': 0})
    monkeypatch.setattr(rpc, 'post', post)
    try:
        with pytest.raises(RPCAccessDenied, match='历史交易查询需要个人令牌') as caught:
            if batched: rpc.batch([('eth_getTransactionReceipt', ['0x'+'11'*32])])
            else: rpc.request('eth_getTransactionReceipt', ['0x'+'11'*32])
        assert str(status) in str(caught.value)
        assert 'private-token' not in str(caught.value) and 'example.invalid' not in str(caught.value)
        assert len(sent) == 1
    finally: rpc.close()


def test_negative_compression_and_empty_word_boundary():
    assert next_tick(-1,10,{-1:1<<255},True)==(-10,True)
    assert next_tick(-1,10,{0:1},False)==(0,True)
    assert next_tick(12,1,{0:0},True)==(0,False)
    assert next_tick(12,1,{0:0},False)==(255,False)
    with pytest.raises(Incomplete):next_tick(-1,1,{},True)


@pytest.mark.parametrize("zero",[True,False])
def test_swap_conservation_rounding_and_price_direction(zero):
    for amount in (1,2,999,10000,10**9):
        target=sqrt_at_tick(-100 if zero else 100)
        new,used,out,fee=swap_step(Q96,target,10**12,amount,3000)
        assert used+fee<=amount and out>=0
        assert target<=new<=Q96 if zero else Q96<=new<=target
        if new!=target:assert used+fee==amount


def test_incomplete_tick_data_never_becomes_zero_profit():
    p=pool();p.bitmap={}
    with pytest.raises(Incomplete):p.quote(A,100)
    p=pool();p.bitmap[0]=1;p.ticks={}
    with pytest.raises(Incomplete):p.quote(A,100)


def test_same_pool_not_reused_and_three_hop_cycles_indexed():
    ps=[pool(A,A,B),pool(B,B,C),pool(C,A,C)]
    idx=RouteIndex(ps,[A],3)
    assert len(idx.routes)==2
    assert all(len(r.pools)==3 and len(set(r.pools))==3 for r in idx.routes.values())
    assert len(idx.affected([A]))==2


def test_mixed_block_route_rejected():
    p,q=pool(A),pool(B);q.block_hash=H1
    with pytest.raises(Reject):Route((A,B,A),(A,B)).quote({A:p,B:q},100)


def test_no_marginal_edge_with_equal_prices_and_fees():
    ps={A:pool(A),B:pool(B)}
    assert not marginal_profitable(Route((A,B,A),(A,B)),ps)


@pytest.mark.parametrize("cached_only", [False, True])
def test_liquidity_ceiling_bound_dominates_exact_tick_crossing_quotes(cached_only):
    def concentrated(addr, t0, t1, tick):
        p = pool(addr, t0, t1, tick=tick)
        ranges = [(-100, 100, 100_000), (-20, 20, 300_000), (40, 200, 200_000)]
        p.liquidity = sum(liq for low, high, liq in ranges if low <= tick < high)
        for low, high, liq in ranges:
            for t, net in [(low, liq), (high, -liq)]:
                old = p.ticks.get(t, (0, 0)); p.ticks[t] = (old[0]+liq, old[1]+net)
                p.bitmap[t >> 8] |= 1 << (t & 255)
        return p
    states = {A:concentrated(A, A, B, 50), B:concentrated(B, A, B, -50)}
    route = Route((A, B, A), (A, B)); bound = gross_upper_bound(route, states, cached_only=cached_only)
    complete = 0
    for amount in range(1, 4000, 3):
        try: output, _ = route.quote(states, amount)
        except Incomplete: continue
        assert output-amount <= bound
        complete += 1
    assert complete > 100
    # Event-driven state mutations must invalidate the directional bound cache.
    states[A].liquidity *= 10
    states[A].ticks = {t:(g*10, n*10) for t,(g,n) in states[A].ticks.items()}
    after = gross_upper_bound(route, states)
    fresh = copy.deepcopy(states)
    for p in fresh.values(): p.__dict__.pop("_directional_upper", None)
    assert after == gross_upper_bound(route, fresh)


def _liquidity_frontier_route(zero_for_one=True, zero_liquidity=False):
    """Small nearby liquidity followed by a large, fifth initialized tick."""
    p = pool(A, tick=1000 if zero_for_one else 0, liquidity=0 if zero_liquidity else 1000)
    q = pool(B, tick=0 if zero_for_one else 1000, liquidity=10**10)
    for state in (p, q):
        state.fee = 0
        state.bitmap = {i:0 for i in range(-8, 9)}
        state.ticks = {}
    nearby = (999, 998, 997, 996) if zero_for_one else (1, 2, 3, 4)
    frontier = 900 if zero_for_one else 100
    if not zero_liquidity:
        # Net-zero boundaries can be formed by equal-sized positions ending
        # and starting there; they do not change active liquidity.
        for tick in nearby: p.ticks[tick] = (2, 0)
    if zero_for_one:
        p.ticks.update({-1000:(10**9+p.liquidity, 10**9+p.liquidity),
                        frontier:(10**9, -10**9), 2000:(p.liquidity, -p.liquidity)})
    else:
        p.ticks.update({-1000:(p.liquidity, p.liquidity),
                        frontier:(10**9, 10**9), 2000:(10**9+p.liquidity, -10**9-p.liquidity)})
    q.ticks.update({-2000:(10**10, 10**10), 2000:(10**10, -10**10)})
    for state in (p, q):
        for tick, (gross, _) in state.ticks.items():
            if gross: state.bitmap[tick >> 8] |= 1 << (tick & 255)
    route = Route((A, B, A) if zero_for_one else (B, A, B), (A, B))
    return route, {A:p, B:q}, frontier


@pytest.mark.parametrize("zero_for_one", [True, False])
@pytest.mark.parametrize("zero_liquidity", [True, False])
def test_missing_liquidity_frontier_never_becomes_a_small_profit_bound(zero_for_one, zero_liquidity):
    route, states, frontier = _liquidity_frontier_route(zero_for_one, zero_liquidity)
    output, _ = route.quote(states, 10**6)
    assert output-10**6 > 90_000
    full_bound = gross_upper_bound(route, states)
    assert full_bound is not None and output-10**6 <= full_bound
    assert gross_upper_bound(route, states, cached_only=True) == full_bound
    # Previously this returned 3 (or 0 for zero active liquidity), incorrectly
    # ignoring a real, large opportunity behind the fifth initialized tick.
    missing = states[A].ticks.pop(frontier)
    with pytest.raises(Incomplete, match="missing initialized tick"):
        route.quote(states, 10**6)
    assert gross_upper_bound(route, states) is None
    local_bound = gross_upper_bound(route, states, cached_only=True)
    assert local_bound == (0 if zero_liquidity else 3)
    # Local sizing may be skipped, but the default answer remains unknown even
    # after the same directional cache has been used by the local-only mode.
    assert gross_upper_bound(route, states) is None
    for amount in range(1, 50):
        try: small_output, _ = route.quote(states, amount)
        except Incomplete: continue
        assert small_output-amount <= local_bound
    states[A].ticks[frontier] = missing
    assert gross_upper_bound(route, states) == full_bound
    assert gross_upper_bound(route, states, cached_only=True) == full_bound
    assert route.quote(states, 10**6)[0]-10**6 > 90_000


def test_unknown_bitmap_frontier_and_bitmap_only_changes_invalidate_bound():
    p, q = pool(A, tick=1000, liquidity=1000), pool(B, tick=0, liquidity=10**10)
    p.fee = q.fee = 0
    q.bitmap = {i:0 for i in range(-8, 9)}
    p.bitmap = {3:0}
    route, states = Route((A, B, A), (A, B)), {A:p, B:q}
    assert gross_upper_bound(route, states) is None  # unknown word before tick 0
    assert gross_upper_bound(route, states, cached_only=True) == 3
    assert gross_upper_bound(route, states) is None
    p.bitmap.update({2:0, 1:0, 0:0})
    assert gross_upper_bound(route, states) is not None
    # The tick cache is unchanged, but the bitmap now exposes missing liquidity.
    p.bitmap[3] = 1 << (900 & 255)
    assert gross_upper_bound(route, states) is None
    p.bitmap[3] = 0
    assert gross_upper_bound(route, states) is not None
    del p.bitmap[2]
    assert gross_upper_bound(route, states) is None


@pytest.mark.parametrize("zero_for_one", [True, False])
def test_unknown_tick_at_current_boundary_is_checked_before_zero_cost_cross(zero_for_one):
    p = pool(A, tick=1000 if zero_for_one else 0, liquidity=1000)
    q = pool(B, tick=0 if zero_for_one else 1000, liquidity=10**10)
    p.fee = q.fee = 0
    for state in (p, q): state.bitmap = {i:0 for i in range(-8, 9)}
    boundary = p.tick
    if not zero_for_one: p.tick -= 1  # price at upper boundary after a downward swap
    p.bitmap[boundary >> 8] |= 1 << (boundary & 255)
    route = Route((A, B, A) if zero_for_one else (B, A, B), (A, B))
    assert gross_upper_bound(route, {A:p, B:q}) is None


@pytest.mark.parametrize("zero_for_one", [True, False])
def test_missing_tick_after_marginal_edge_is_exhausted_does_not_disable_bound(zero_for_one):
    p = pool(A, tick=1000 if zero_for_one else 0, liquidity=1000)
    q = pool(B, tick=0 if zero_for_one else 1000, liquidity=10**10)
    p.fee = q.fee = 0
    for state in (p, q): state.bitmap = {i:0 for i in range(-8, 9)}
    # Equality already makes the cycle's continuous profit non-increasing.
    boundary = 0 if zero_for_one else 1000
    p.bitmap[boundary >> 8] |= 1 << (boundary & 255)
    route = Route((A, B, A) if zero_for_one else (B, A, B), (A, B))
    bound = gross_upper_bound(route, {A:p, B:q})
    assert bound is not None and bound > 0
    for amount in range(1, 30):
        try: output, _ = route.quote({A:p, B:q}, amount)
        except Incomplete: continue
        assert output-amount <= bound


def test_optimizer_against_exhaustive_curve_and_incomplete_large_inputs():
    class Curve:
        def quote(self,_,amount):
            if amount>800:raise Incomplete("uncached tick")
            return amount+900-(amount-317)**2//10,0
    result=optimize(Curve(),{},1000,lambda _:30)
    brute=max(Curve().quote({},x)[0]-x-30 for x in range(1,801))
    assert result["net_after_modelled_cost"]==brute
    assert result["coverage_limited"] and not result["global_optimum_proven"]


def test_batch_updates_once_after_all_visible_swaps_and_deduplicates():
    b=book();l1=log(0);l2=log(1,raw=encode(["int256","int256","uint160","uint128","int24"],[2,-1,sqrt_at_tick(2),10**12,2]))
    assert b.apply_visible_batch([l2,l1])=={A}
    assert b.pools[A].tick==2
    assert b.apply_visible_batch([l1,l2])==set()


@pytest.mark.parametrize("case",["reorg","gap","removed","wrong_hash","out_of_order"])
def test_visibility_errors_invalidate_state(case):
    b=book()
    with pytest.raises(Incomplete):
        if case=="reorg":b.begin({"number":"0x3","hash":H0,"parentHash":H0})
        elif case=="gap":b.begin({"number":"0x4","hash":H0,"parentHash":H1})
        elif case=="removed":b.apply_visible_batch([{**log(),"removed":True}])
        elif case=="wrong_hash":b.apply_visible_batch([log(blockhash=H0)])
        else:b.apply_visible_batch([log(2)]);b.apply_visible_batch([log(1)])
    assert not b.pools[A].valid


def test_dynamic_fee_invalidation_preserves_prior_missing_state():
    p=pool(kind="slipstream");p.valid=False
    b=StateBook([p],{"number":"0x1","hash":H0});b.invalid_reasons[A]="missing ticks"
    b.begin({"number":"0x2","hash":H1,"parentHash":H0});b.apply_visible_batch([log()])
    assert b.invalid_reasons[A]=="missing ticks" and not p.valid
    assert not book(pool(kind="slipstream")).pools[A].valid


def test_mint_burn_keep_bitmap_net_and_active_liquidity_consistent():
    b=book();p=b.pools[A];before=p.liquidity
    topics=lambda kind:[kind,"0x"+"00"*32,"0x"+encode(["int24"],[-10]).hex(),"0x"+encode(["int24"],[10]).hex()]
    raw=encode(["address","uint128","uint256","uint256"],[A,1000,1,1])
    b.apply_visible_batch([log(0,MINT,raw=raw,topics=topics(MINT))])
    assert p.ticks[-10]==(1000,1000) and p.ticks[10]==(1000,-1000)
    assert p.liquidity==before+1000
    raw=encode(["uint128","uint256","uint256"],[1000,1,1])
    b.apply_visible_batch([log(1,BURN,raw=raw,topics=topics(BURN))])
    assert p.liquidity==before and not p.ticks and p.bitmap[-1]==p.bitmap[0]==0


def test_costs_include_l1_and_ceiling_and_explicit_failure_probability():
    c=load_config();p=c["policy"]
    eth=cost_budget(p,WETH,5_000_000,2)
    usdc=cost_budget(p,USDC,5_000_000,2)
    assert eth.total_fee_wei==eth.gas_limit*eth.max_fee_per_gas+int(p["l1_fee_allowance_wei"])
    assert usdc.settlement_cost_raw*10**12>=eth.total_fee_wei*p["eth_usdc_ceiling"]
    with pytest.raises(Reject):cost_budget(p,WETH,10**10,2)
    assert expected_value(100,20,10,0)==-10
    assert expected_value(100,20,10,1_000_000)==80


def test_transport_cannot_broadcast():
    with pytest.raises(Reject):RPC().request("eth_sendRawTransaction",["0x"])


def test_ledger_survives_restart_and_accounts_failed_gas(tmp_path):
    path=tmp_path/"risk.sqlite";r=RiskLedger(path,100,2)
    r.reserve("one",60,now=10);r.close()
    r=RiskLedger(path,100,2)
    with pytest.raises(Reject):r.reserve("two",30,now=10)
    r.submitted("one","0x1")
    with pytest.raises(Reject):r.settle("one",False,40,False)
    r.settle("one",False,40,True,now=11)
    with pytest.raises(Reject):r.reserve("two",61,now=12)
    r.reserve("two",60,now=12);r.submitted("two","0x2");r.settle("two",False,30,True,now=13)
    with pytest.raises(Reject):r.reserve("three",10,now=14)
    r.close()


def test_recorder_first_seen_is_immutable(tmp_path):
    r=Recorder(tmp_path/"feed.sqlite")
    first=r.append(1,10,"test",{"receipts":[{"transactionHash":"0x1"}]},100)
    r.append(10,20,"test",{"receipts":[{"transactionHash":"0x1"}]},200)
    assert r.db.execute("SELECT frame_id,received_utc FROM arrivals").fetchone()==(first,100)
    assert r.summary()["first_seen_transactions"]==1
    assert decode_payload(r.db.execute("SELECT payload FROM frames LIMIT 1").fetchone()[0])["receipts"][0]["transactionHash"]=="0x1"
    r.close()


def test_skipped_blocks_are_one_observation_and_end_at_final_context():
    p=pool();b=StateBook([p],{"number":"0x1","hash":H0})
    l1={**log(0),"blockNumber":"0x2"}
    l2={**log(1,blockhash="0x"+"33"*32),"blockNumber":"0x3"}
    final={"number":"0x4","hash":"0x"+"44"*32}
    assert b.apply_observed_range([l2,l1],final)=={A}
    assert b.header==final and p.block==4 and p.block_hash==final["hash"]


def test_fee_on_next_day_counts_on_receipt_day(tmp_path):
    r=RiskLedger(tmp_path/"risk.sqlite",100,3)
    r.reserve("late",90,now=86399);r.submitted("late","0x1");r.settle("late",True,90,True,now=86401)
    with pytest.raises(Reject):r.reserve("new",11,now=86402)
    r.close()


def test_fee_overrun_stops_even_after_restart_and_day_change(tmp_path):
    path=tmp_path/"risk.sqlite";r=RiskLedger(path,1000,3)
    r.reserve("one",10,now=1);r.submitted("one","0x1");r.settle("one",True,11,True,now=2);r.close()
    r=RiskLedger(path,1000,3)
    with pytest.raises(Reject):r.reserve("two",10,now=86402)
    r.close()
