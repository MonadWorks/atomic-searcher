"""Failure evidence controls work admission, never permission to spend."""
from eth_abi import encode
import pytest

from searcher.core import RPCError
from searcher.execution import execution_failure_details, verified_call_gas_budget
from searcher.pipeline import candidate_attempt_order
from searcher.route_feedback import RouteFeedback
from searcher.routes import RouteIndex
from searcher.statefeed import DeepReadScheduler
from test_hot_transfer_filter import setup, incoming_shortfall, A, B


def v2_error(reason='UniswapV2: TRANSFER_FAILED'):
    return RPCError('eth_call', {'code':3,'message':'execution reverted',
        'data':'0x08c379a0'+encode(['string'],[reason]).hex()})


def test_repeated_real_string_keeps_gas_escalation_then_earns_cooldown():
    calls = []
    class FailingRPC:
        def request(self,method,args):
            calls.append(int(args[0]['gas'],16))
            raise v2_error()
    import time
    with pytest.raises(RPCError) as caught:
        verified_call_gas_budget(FailingRPC(),{}, {},100000,400000,time.monotonic_ns()+10**9)
    assert calls == [100000,200000,400000]
    failure = execution_failure_details(caught.value)
    assert failure['revert_name'] == 'V2TransferFailed'
    f = RouteFeedback()
    assert f.record('forward',failure) == 30
    assert f.blocked('forward') and not f.blocked('reverse')
    assert f.record('single',execution_failure_details(v2_error())) == 0
    other = v2_error('some other token condition');other.previous_gas_error = v2_error()
    other.gas_attempts = calls
    assert f.record('different_reason',execution_failure_details(other)) == 0


def test_same_route_failures_back_off_but_reopen_and_success_clears_history():
    clock=[0];f=RouteFeedback(clock=lambda:clock[0])
    failure={'revert_name':'BadCallback','revert_selector':'0x6c25f7f8'}
    for delay in [30,60,120,240,300,300]:
        assert f.record('bad',failure)==delay
        assert f.blocked('bad') and f.priority('bad')==1
        clock[0]+=delay
        assert not f.blocked('bad')
    f.passed('bad')
    assert f.priority('bad')==0 and f.record('bad',failure)==30
    clock[0]+=3601
    assert f.priority('bad')==0 and f.record('bad',failure)==30


@pytest.mark.parametrize('extra',[{'validation_deadline_reached':True},
    {'validation_outcome':'call_succeeded_after_deadline'}, {'anchor_mismatch':{'number':1}}])
def test_timing_failure_does_not_build_blacklist(extra):
    f=RouteFeedback()
    assert f.record('route',dict(revert_name='BadCallback',**extra))==0
    assert not f.blocked('route') and f.priority('route')==0


def test_failed_routes_are_filtered_before_expensive_depth_and_retry_recovers():
    clock,transfers,pools,group=setup()
    feedback=RouteFeedback(clock=lambda:clock[0])
    directions=list(RouteIndex(list(pools.values()),[A],2).routes.values())
    incoming_shortfall(transfers,B,directions[0].pools[0],directions[0].pools[1])
    feedback.record(directions[1].key,{'revert_name':'BadCallback'})
    assert feedback.group_deferred(group,pools,[A],transfers=transfers)
    deep=DeepReadScheduler()
    deep.add([dict(pools=group,score=10000,route_keys=[r.key for r in directions])],0)
    blocked=lambda row:feedback.group_deferred(row['pools'],pools,[A],row.get('route_keys'),transfers)
    assert deep.take(set(pools),0,blocked)==set()
    assert group in deep.pending
    clock[0]=31
    assert deep.take(set(pools),31,blocked)==set(group)
    assert len(deep.selected_requests)==1


def test_quality_is_applied_before_per_asset_limit_and_keeps_large_healthy_first():
    candidates=[dict(tokens=[A],id='bad'+str(i),net_after_modelled_cost=10000-i,
        execution_revalidation_priority=1) for i in range(4)]
    candidates += [dict(tokens=[A],id='small',net_after_modelled_cost=10),
                   dict(tokens=[A],id='large',net_after_modelled_cost=100)]
    selected=candidate_attempt_order(candidates,[A])
    assert [c['id'] for c in selected]==['large','small','bad0']


def test_history_and_topology_are_bounded_and_direction_specific():
    clock,_,pools,group=setup()
    feedback=RouteFeedback(clock=lambda:clock[0],limit=2)
    routes=list(RouteIndex(list(pools.values()),[A],2).routes.values())
    for r in routes: feedback.record(r.key,{'revert_name':'BadCallback'})
    assert feedback.group_deferred(group,pools,[A])
    assert not feedback.group_deferred(group,pools,[A],['unknown'])
    feedback.passed(routes[0].key)
    assert not feedback.group_deferred(group,pools,[A])
    for i in range(10):feedback.record(str(i),{'revert_name':'BadCallback'})
    assert len(feedback.history)==len(feedback.failures)==2
