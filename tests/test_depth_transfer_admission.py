"""A queued deep read must honor later directed-transfer feedback."""
import pytest

from searcher.routes import Route, RouteIndex
from searcher.statefeed import DeepReadScheduler
from searcher.transfer_feedback import TransferFeedback
from test_hot_transfer_filter import A, B, C, EXECUTOR, OWNER, address, pool, incoming_shortfall


def market():
    d = address(4)
    ps = [pool(11,A,B), pool(12,B,C), pool(13,C,d), pool(14,A,d)]
    # The production route universe admits four hops only with a V2 leg.
    ps[-1].kind, ps[-1].fee = 'v2', 3000
    states = {p.address:p for p in ps}
    group = tuple(states)
    route = Route((A,B,C,d,A), group)
    clock = [0.0]
    feedback = TransferFeedback(EXECUTOR,OWNER,clock=lambda:clock[0])
    return clock, feedback, states, group, route


def check_request(feedback, states):
    return lambda row:feedback.group_deferred(row['pools'],states,[A],row.get('route_keys'))


@pytest.mark.parametrize('resume', [False, True])
def test_known_failed_request_does_not_borrow_eligibility_from_unrequested_reverse(resume):
    clock, feedback, states, group, route = market()
    scheduler = DeepReadScheduler()
    request = {'pools':group,'score':100,'route_keys':[route.key]}
    scheduler.add([request],0)
    if resume:
        assert scheduler.take(set(states),0) == set(group)
        assert scheduler.resume(scheduler.selected_requests,0) == []
    incoming_shortfall(feedback,B,group[0],group[1])
    assert not feedback.group_deferred(group,states,[A]), 'reverse remains eligible for its own scan'
    assert feedback.group_deferred(group,states,[A],[route.key])
    assert scheduler.take(set(states),1,check_request(feedback,states)) == set()
    assert not scheduler.selected_requests and not scheduler.selected_deep_pools
    assert group in (scheduler.resuming if resume else scheduler.pending)
    clock[0] = 30
    assert scheduler.take(set(states),30,check_request(feedback,states)) == set(group)
    assert scheduler.selected_requests[0]['route_keys'] == [route.key]


def test_deferred_resume_cannot_starve_unrelated_pending_depth():
    clock,feedback,states,group,route = market()
    scheduler = DeepReadScheduler()
    scheduler.resume([{'pools':group,'deep':True,'route_keys':[route.key]}],0)
    incoming_shortfall(feedback,B,group[0],group[1])
    clean = [pool(15,A,address(5)),pool(16,A,address(5))]
    states.update((p.address,p) for p in clean)
    clean_group = tuple(p.address for p in clean)
    scheduler.add([{'pools':clean_group,'score':1}],1)
    assert scheduler.take(set(states),2,check_request(feedback,states)) == set(clean_group)
    assert group in scheduler.resuming


@pytest.mark.parametrize('resume', [False, True])
def test_merging_directional_requests_retains_eligible_reverse(resume):
    clock,feedback,states,group,route = market()
    other = next(r for r in RouteIndex(list(states.values()),[A],4).routes.values() if r.key != route.key)
    scheduler = DeepReadScheduler()
    if resume:
        scheduler.resume([{'pools':group,'deep':True,'route_keys':[route.key]}],0)
    else:
        scheduler.add([{'pools':group,'route_keys':[route.key]}],0)
    scheduler.add([{'pools':group,'route_keys':[other.key]}],1)
    incoming_shortfall(feedback,B,group[0],group[1])
    assert scheduler.take(set(states),2,check_request(feedback,states)) == set(group)
    assert set(scheduler.selected_requests[0]['route_keys']) == {route.key,other.key}


@pytest.mark.parametrize('legacy_first', [False, True])
def test_legacy_unscoped_requests_never_shrink_to_a_single_direction(legacy_first):
    clock,feedback,states,group,route = market()
    scheduler = DeepReadScheduler()
    directional = {'pools':group,'route_keys':[route.key]}
    legacy = {'pools':group}
    scheduler.add([legacy if legacy_first else directional],0)
    scheduler.add([directional if legacy_first else legacy],1)
    incoming_shortfall(feedback,B,group[0],group[1])
    assert scheduler.take(set(states),2,check_request(feedback,states)) == set(group)
    assert 'route_keys' not in scheduler.selected_requests[0]


def test_unknown_direction_expiry_and_real_pass_keep_recovery_available():
    clock,feedback,states,group,route = market()
    incoming_shortfall(feedback,B,group[0],group[1])
    assert not feedback.group_deferred(group,states,[A],['unknown'])
    assert not feedback.group_deferred(group,states,[A],[])
    assert not feedback.group_deferred(group,states,[A],[route.key,'unknown'])
    feedback.passed(route)
    assert not feedback.group_deferred(group,states,[A],[route.key])
    clock[0] = 301
    incoming_shortfall(feedback,B,group[0],group[1])
    assert feedback.group_deferred(group,states,[A],[route.key])
    clock[0] = 602
    assert not feedback.group_deferred(group,states,[A],[route.key])


def test_engine_keeps_every_direction_that_requested_depth(monkeypatch):
    from searcher import engine
    from searcher.core import WETH, load_config
    from test_receipt_eligibility import pools, HEADER
    config=load_config()
    config['policy'].update(max_quote_ms=500,sizing_mode='fixed')
    config['settlement_tokens']=[dict(config['settlement_tokens'][0],max_input_raw=str(10**18))]
    index=RouteIndex(pools()[2:],[WETH],2)
    monkeypatch.setattr(engine,'marginal_profitable',lambda *args:True)
    monkeypatch.setattr(engine,'gross_upper_bound',lambda *args,**kwargs:None)
    monkeypatch.setattr(engine,'optimize',lambda *args,**kwargs:{'complete':False,
        'sizing_time_limited':False,'state_coverage_limited':True,'gross_surplus':0})
    report=engine.evaluate(index,config,HEADER)
    assert len(report['deep_read_requests'])==1
    assert set(report['deep_read_requests'][0]['route_keys'])==set(index.routes)


@pytest.mark.parametrize('scope', [[], ['obsolete-request-key']])
def test_invalid_scope_still_bounds_topology_cache(scope):
    clock,feedback,states,group,route=market()
    feedback.capacity=2
    incoming_shortfall(feedback,B,group[0],group[1])
    for n in range(50):
        ps=[pool(100+n*2,A,B),pool(101+n*2,A,B)]
        part={p.address:p for p in ps}
        assert not feedback.group_deferred(tuple(part),part,[A],scope)
        assert len(feedback.group_routes)<=2


@pytest.mark.parametrize('occupied', [1012,1020,1023,1024])
def test_pending_dispatch_reserves_capacity_for_every_possible_continuation(occupied):
    scheduler=DeepReadScheduler()
    old=[{'pools':[f'old-{i}-a',f'old-{i}-b'],'deep':True} for i in range(occupied)]
    new=[{'pools':[f'new-{i}-a',f'new-{i}-b']} for i in range(12)]
    assert not scheduler.resume(old,0)
    scheduler.add(new,1)
    available={a for row in old+new for a in row['pools']}
    scheduler.take(available,2,lambda row:row['pools'][0].startswith('old-'))
    selected=list(scheduler.selected_requests)
    assert len(selected)==1024-occupied
    if occupied==1024:
        assert not selected and not scheduler.selected_deep_pools
        assert len(scheduler.pending)==12
    # Failed polling may return the entire batch before an ACK returns a
    # subset. Both paths merge without dropping work or exceeding capacity.
    assert not scheduler.resume(selected,3)
    assert not scheduler.resume(selected[:2],4)
    assert len(scheduler.resuming)==1024
    assert all(tuple(row['pools']) in scheduler.resuming for row in selected)
    # Retry eligibility later frees its own slots, so full capacity is not a
    # permanent deadlock. A whole selected batch can be returned again.
    assert scheduler.take(available,5,lambda row:False)
    assert not scheduler.resume(scheduler.selected_requests,6)
    assert len(scheduler.resuming)<=1024
