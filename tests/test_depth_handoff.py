"""Interrupted search and paid-for depth must reach their intended routes."""
import copy
import time
from collections import deque

import pytest

from searcher.core import WETH, load_config
from searcher.engine import evaluate
from searcher.pipeline import PendingStrategy
from searcher.routes import RouteIndex, optimize
from searcher.statefeed import DeepReadScheduler, StateFeed, StateFrame
from test_pipeline import ClockRPC, clock_config, saved, header, H1, H0
from test_receipt_eligibility import pools, HEADER
from test_searcher import pool, A


def test_sizing_slices_resume_exact_quotes_until_same_result_as_full_search(monkeypatch):
    clock = [0]
    calls = []
    class Curve:
        def quote(self, states, amount):
            clock[0] += 1
            calls.append(amount)
            return amount + max(0, 1000-(amount-137)**2), 0
    monkeypatch.setattr('searcher.routes.time.monotonic_ns', lambda:clock[0])
    curve = Curve()
    expected = optimize(curve, {}, 1000)
    calls.clear()
    work = {}
    for _ in range(100):
        result = optimize(curve, {}, 1000, deadline_ns=clock[0]+5, continuation=work)
        if not result['sizing_time_limited']: break
    assert not result['sizing_time_limited']
    assert result['amount_in'] == expected['amount_in']
    assert result['amount_out'] == expected['amount_out']
    assert len(calls) == len(set(calls)), 'a resumed slice redid already cached quotes'


def test_real_multitick_resume_skips_completed_segments_under_deterministic_load(monkeypatch):
    import searcher.routes as routes
    from searcher.pools import Pool
    from test_searcher import B
    ps=[pool(A,tick=1000,liquidity=10**12),pool(B,tick=0,liquidity=10**12)]
    for p in ps:
        p.fee=0;p.bitmap={i:0 for i in range(-8,9)}
    for tick in range(950,1000):
        ps[0].ticks[tick]=(2,0);ps[0].bitmap[tick>>8] |= 1<<(tick&255)
    route=routes.Route((A,B,A),(A,B));states={p.address:p for p in ps}
    clock=[0];quotes=Pool.quote;stationary=routes.stationary_seed
    def charged_quote(self,*args):clock[0]+=100_000;return quotes(self,*args)
    def charged_seed(*args):clock[0]+=2_000_000;return stationary(*args)
    monkeypatch.setattr(Pool,'quote',charged_quote)
    monkeypatch.setattr(routes,'stationary_seed',charged_seed)
    monkeypatch.setattr(routes.time,'monotonic_ns',lambda:clock[0])
    expected=optimize(route,states,10**10)
    work={}
    for _ in range(100):
        result=optimize(route,states,10**10,deadline_ns=clock[0]+20_000_000,continuation=work)
        if not result['sizing_time_limited']:break
    assert not result['sizing_time_limited']
    assert result==expected
    assert len(work['completed_segments'])>20


def test_regular_search_sets_deadline_and_invalidates_resume_on_price_or_fee_change(monkeypatch):
    config = load_config()
    config['policy']['sizing_mode'] = 'fixed'
    config['settlement_tokens'] = [dict(config['settlement_tokens'][0], max_input_raw=str(10**18))]
    ix = RouteIndex(pools()[2:], [WETH], 2)
    seen = []
    def interrupted(route, states, maximum, fee, **kwargs):
        assert time.monotonic_ns() <= kwargs['deadline_ns']
        work = kwargs['continuation']
        seen.append(bool(work))
        work['sentinel'] = True
        return {'complete':False, 'sizing_time_limited':True}
    monkeypatch.setattr('searcher.engine.optimize', interrupted)
    first = evaluate(ix, config, HEADER)
    assert first['rejected']['sizing_budget_exhausted'] == 1
    assert 'below_configured_net_floor' not in first['rejected']
    assert ix.interrupted_keys and set(ix.interrupted_keys) <= set(ix.work_queue)
    evaluate(ix, config, HEADER)
    ix.states[pools()[2].address].reserve1 += 1
    evaluate(ix, config, HEADER)
    evaluate(ix, config, dict(HEADER, baseFeePerGas='0x2'))
    assert seen == [False, True, False, False]


def test_more_interrupted_routes_than_cache_slots_eventually_finish_without_eviction(monkeypatch):
    from searcher.pools import Pool
    config=load_config();config['policy'].update(sizing_mode='fixed',max_quote_ms=100000)
    config['settlement_tokens']=[dict(config['settlement_tokens'][0],max_input_raw=str(10**18))]
    ps=[]
    for i in range(40):
        token='0x'+f'{1000+i:040x}'
        for j,mult in enumerate((2,1)):
            ps.append(Pool('0x'+f'{100+i*2+j:040x}','0x'+'22'*20,'v2',WETH,token,0,3000,
                           block=100,block_hash=HEADER['hash'],reserve0=10**20,reserve1=mult*10**20))
    ix=RouteIndex(ps,[WETH],2);finished=set()
    def interrupted(route,states,maximum,fee,**kw):
        work=kw['continuation'];work['steps']=work.get('steps',0)+1
        if work['steps']<5:return {'complete':False,'sizing_time_limited':True}
        finished.add(route.key)
        return {'complete':False,'sizing_time_limited':False}
    monkeypatch.setattr('searcher.engine.optimize',interrupted)
    for _ in range(12):
        evaluate(ix,config,HEADER)
        assert len(ix.sizing_progress)<=32
        if len(finished)==40:break
    assert len(finished)==40


def test_unfinished_selection_bypasses_retry_delay_and_preserves_original_age():
    q = DeepReadScheduler()
    q.add([{'pools':['a','b'], 'score':100}], 1)
    assert q.take({'a','b'}, 2) == {'a','b'}
    original = q.selected_requests
    q.add([['a','b']], 3)
    assert not q.pending  # Ordinary completed-read retry stays throttled.
    q.resume(original, 3)
    assert q.take({'a','b'}, 3) == {'a','b'}
    assert q.selected_requests[0]['since'] == 1
    assert q.selected_deep_pools == {'a','b'}
    q.resume([{'pools':['c','d'], 'since':1, 'deep':False}], 3)
    assert q.take({'c','d'}, 3) == {'c','d'}
    assert not q.selected_deep_pools  # Ordinary continuation stays shallow.
    q.add([['c','d']], 4)
    assert q.pending  # A shallow continuation has not used the depth cooldown.


def test_unfinished_batch_finishes_before_new_depth_batch_and_removed_pools_expire():
    q=DeepReadScheduler()
    q.resume([{'pools':[f'r{i}a',f'r{i}b'], 'since':1, 'deep':False} for i in range(20)],1)
    q.add([{'pools':['new-a','new-b'], 'score':100}],2)
    available={'new-a','new-b'} | {f'r{i}{side}' for i in range(20) for side in 'ab'}
    selected=q.take(available,2)
    assert {'r0a','r0b'} <= selected and 'new-a' not in selected
    second=q.take(available,3)
    assert not q.resuming
    assert {'new-a','new-b'} <= second
    assert [r['pools'] for r in q.selected_requests[:8]] == [
        (f'r{i}a',f'r{i}b') for i in range(12,20)]
    assert q.selected_requests[-1]['pools']==('new-a','new-b')
    q.resume([{'pools':['gone-a','gone-b']}],5)
    q.take(set(),3)
    assert not q.resuming and not q.pending


def test_full_continuation_queue_returns_unadmitted_work_until_capacity_frees():
    q=DeepReadScheduler()
    rows=[{'pools':[f'{i}a',f'{i}b'], 'since':1,'deep':False} for i in range(1024)]
    assert not q.resume(rows,1)
    last={'pools':['last-a','last-b'],'since':2,'deep':True}
    waiting=q.resume([last],2)
    assert len(q.resuming)==1024 and len(waiting)==1
    available={a for row in rows+[last] for a in row['pools']}
    q.take(available,3)
    assert not q.resume(waiting,3)
    assert q.resuming[('last-a','last-b')]['deep']


def test_requested_groups_do_not_expand_into_all_pool_combinations():
    config=clock_config()
    config['policy']['cycle_pages']=True
    feed=StateFeed(ClockRPC([header(2,H1,H0)]),config,saved())
    target=tuple(p.address for p in feed.pools)
    # A trap detects the legacy all-combinations RouteIndex construction.
    import searcher.routes as routes
    original=routes.RouteIndex
    def forbidden(*args,**kwargs):raise AssertionError('expanded requested pool union')
    routes.RouteIndex=forbidden
    try:
        frame=feed.poll(deep_pools=target,requested_groups=[{'pools':target,'deep':True,'since':1,'score':1}])
    finally:routes.RouteIndex=original
    assert frame.transport['requested_groups'][0]['pools']==target
    assert {tuple(x) for x in frame.transport['route_groups']}=={tuple(sorted(target))}


def test_page_retains_unvisited_targets_then_yields_with_explicit_continuation(monkeypatch):
    config=load_config();config['settlement_tokens']=[{'address':A}]
    groups=[tuple('0x'+f'{100+i*2+j:040x}' for j in range(2)) for i in range(4)]
    ps=[pool(a).json() for g in groups for a in g]
    strategy=PendingStrategy(None,config,{'header':HEADER,'pools':ps})
    seen=[]
    def one_per_pass(index,config,h,changed):
        key=index.work_queue.popleft();index.queued_routes.remove(key)
        seen.append(tuple(sorted(index.routes[key].pools)))
        index.last_evaluated_keys=[key];index.interrupted_keys=set()
        # A candidate retry must not count as unvisited page work.
        index.work_queue.append(key);index.queued_routes.add(key)
        return {'candidates':[]}
    monkeypatch.setattr('searcher.pipeline.evaluate',one_per_pass)
    def frame(page,rows):
        now=time.monotonic_ns()
        return StateFrame(HEADER,HEADER,rows,now,now,time.time(),100,200,
            transport={'page_id':page,'route_groups':groups,'deep_pools':list(groups[-1]),
                       'requested_groups':[{'pools':g,'deep':g==groups[-1],'since':1,'score':10} for g in groups[1:]]})
    first=strategy.on_snapshot(frame(1,ps))
    assert seen[0] in groups[1:] and not first['page_advance_ready']
    # Fresh prices on the same page do not reset completed work.
    ps2=copy.deepcopy(ps);ps2[0]['liquidity']+=1
    second=strategy.on_snapshot(frame(1,ps2))
    third=strategy.on_snapshot(frame(1,ps2))
    assert second['page_unfinished_routes'] < first['page_unfinished_routes']
    # The three-pass depth allowance cannot acknowledge untouched ordinary
    # directions. Finish those in-place, then hand back only explicit requests.
    assert not third['page_advance_ready'] and third['page_unfinished_ordinary_routes']==2
    assert not third['continuation_requests']
    fourth=strategy.on_snapshot(frame(1,ps2))
    fifth=strategy.on_snapshot(frame(1,ps2))
    assert fourth['page_unfinished_ordinary_routes']==1
    assert fifth['page_advance_ready'] and not fifth['page_scan_complete']
    assert fifth['page_unfinished_ordinary_routes']==0 and fifth['continuation_requests']
    assert all(not row['deep'] for row in fifth['continuation_requests'] if tuple(row['pools'])!=groups[-1])
    assert all(tuple(row['pools'])!=groups[0] for row in fifth['continuation_requests'])
    assert set(k for row in fifth['continuation_requests'] for k in row['route_keys'])==strategy.page_pending


def test_target_without_any_sizing_attempt_is_not_acknowledged_as_delivered(monkeypatch):
    config=load_config();config['settlement_tokens']=[{'address':A}]
    strategy=PendingStrategy(None,config,saved())
    def unfinished(index,*args):
        keys=set(index.routes)
        index.last_evaluated_keys=list(keys);index.unstarted_keys=keys
        return {'candidates':[]}
    monkeypatch.setattr('searcher.pipeline.evaluate',unfinished)
    now=time.monotonic_ns()
    f=StateFrame(HEADER,HEADER,saved()['pools'],now,now,time.time(),100,200,
                 transport={'page_id':1,'requested_groups':[{'pools':[p['address'] for p in saved()['pools']],'deep':True}]})
    result=strategy.on_snapshot(f)
    assert not result['page_scan_complete'] and not result['page_advance_ready']


def test_page_ack_transfers_unvisited_directions_once_and_retains_depth():
    from searcher.universe import PageProgress
    gate=PageProgress();_,page=gate.begin()
    row={'pools':['a','b'],'route_keys':['direction-2'],'deep':True,'since':1}
    gate.consumed(page,[row])
    advanced,next_page=gate.begin()
    assert advanced
    carried=gate.take_continuation();assert carried==[row]
    assert gate.take_continuation()==[]
    gate.consumed(page,[row])
    assert gate.begin()==(False,next_page) and gate.take_continuation()==[]
    scheduler=DeepReadScheduler();scheduler.resume(carried,2);scheduler.take({'a','b'},2)
    assert scheduler.selected_requests[0]['route_keys']==['direction-2']


def test_bounded_sizing_attempt_delivers_depth_without_waiting_for_global_optimum(monkeypatch):
    config=load_config();config['settlement_tokens']=[{'address':A}]
    strategy=PendingStrategy(None,config,saved())
    def partial(index,*args):
        index.last_evaluated_keys=list(index.routes);index.interrupted_keys=set(index.routes)
        index.unstarted_keys=set()
        return {'candidates':[], 'rejected':{'sizing_budget_exhausted':2}}
    monkeypatch.setattr('searcher.pipeline.evaluate',partial)
    now=time.monotonic_ns()
    frame=StateFrame(HEADER,HEADER,saved()['pools'],now,now,time.time(),100,200,
        transport={'page_id':1,'requested_groups':[{'pools':[p['address'] for p in saved()['pools']],'deep':True}]})
    report=strategy.on_snapshot(frame)
    assert report['page_scan_complete'] and report['page_advance_ready']
    assert not report['continuation_requests']
