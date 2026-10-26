"""Independent bounded quality admission tests; no public RPC or signing."""
from dataclasses import replace
import threading
import time

import pytest
from eth_abi import encode

from searcher import execution
from searcher.pipeline import candidate_attempt_order
from searcher.route_feedback import RouteFeedback
from searcher.routes import RouteIndex
from searcher.statefeed import DeepReadScheduler
from test_hot_transfer_filter import setup, incoming_shortfall, A, B, C, address, pool


def failure(reason='UniswapV2: TRANSFER_FAILED', gases=(980000,1960000)):
    prior = None
    for i,gas in enumerate(gases):
        raw='0x08c379a0'+encode(['string'],[reason]).hex()
        error=RuntimeError("RPC eth_call: "+repr({'code':3,'message':'execution reverted','data':raw}))
        error.gas_attempts=list(gases[:i+1])
        if prior is not None:error.previous_gas_error=prior
        prior=error
    return execution.execution_failure_details(prior)


def two_directions():
    clock,tf,states,group=setup()
    rf=RouteFeedback(clock=lambda:clock[0])
    routes=list(RouteIndex(list(states.values()),[A],2).routes.values())
    assert len(routes)==2
    return clock,tf,rf,states,group,routes


def test_mixed_direction_exclusions_close_group_but_either_recovery_reopens():
    clock,tf,rf,states,group,routes=two_directions()
    incoming_shortfall(tf,B,group[0],group[1])
    reverse=next(r for r in routes if not tf.deferred(r))
    rf.record(reverse.key,{'revert_name':'BadCallback'})
    assert not tf.group_deferred(group,states,[A])
    assert not rf.group_deferred(group,states,[A])
    assert rf.group_deferred(group,states,[A],transfers=tf)
    rf.passed(reverse.key)
    assert not rf.group_deferred(group,states,[A],transfers=tf)
    rf.record(reverse.key,{'revert_name':'BadCallback'})
    clock[0]=30
    assert not rf.group_deferred(group,states,[A],transfers=tf)


def test_directional_scope_and_unknown_topology_never_borrow_other_direction_failure():
    _,tf,rf,states,group,routes=two_directions()
    rf.record(routes[0].key,{'revert_name':'InvalidPool'})
    assert not rf.group_deferred(group,states,[A],transfers=tf)
    assert rf.group_deferred(group,states,[A],[routes[0].key],tf)
    assert not rf.group_deferred(group,states,[A],[routes[1].key],tf)
    for scope in ([],['unknown'],[routes[0].key,'unknown']):
        assert not rf.group_deferred(group,states,[A],scope,tf)
    for bad in ((),group*2,(group[0],address(999))):
        assert not rf.group_deferred(bad,states,[A],transfers=tf)
    changed={**states};new=replace(states[group[1]],address=address(111))
    changed.pop(group[1]);changed[new.address]=new
    assert not rf.group_deferred(tuple(changed),changed,[A],transfers=tf)


def test_four_pool_mixed_cycle_requires_all_directions_to_be_blocked():
    D=address(4)
    ps=[pool(20,A,B),replace(pool(21,B,C),kind='v2',fee=3000,spacing=0),
        pool(22,C,D),pool(23,A,D)]
    states={p.address:p for p in ps};group=tuple(states)
    routes=list(RouteIndex(ps,[A],4).routes.values())
    assert len(routes)==2
    rf=RouteFeedback()
    rf.record(routes[0].key,{'revert_name':'BadCallback'})
    assert not rf.group_deferred(group,states,[A])
    rf.record(routes[1].key,{'revert_name':'BadCallback'})
    assert rf.group_deferred(group,states,[A])


def test_deep_scheduler_defers_without_losing_work_and_rechecks_expiry():
    clock,tf,rf,states,group,routes=two_directions()
    for r in routes:rf.record(r.key,{'revert_name':'BadCallback'})
    scheduler=DeepReadScheduler()
    scheduler.add([{'pools':group,'route_keys':[r.key for r in routes],'score':100}],0)
    gate=lambda row:rf.group_deferred(row['pools'],states,[A],row.get('route_keys'),tf)
    assert scheduler.take(set(states),0,gate)==set()
    assert tuple(sorted(group)) in scheduler.pending
    clock[0]=30
    assert scheduler.take(set(states),30,gate)==set(group)
    assert scheduler.selected_requests


def test_stable_error_history_is_bounded_and_has_real_reentry_windows():
    clock=[0.0];rf=RouteFeedback(clock=lambda:clock[0],limit=2)
    signature={'revert_name':'BadCallback','revert_selector':'0x6c25f7f8'}
    for delay in (30,60,120,240,300,300):
        assert rf.record('route',signature)==delay
        assert rf.blocked('route')
        clock[0]+=delay
        assert not rf.blocked('route') and rf.priority('route')==1
    assert rf.record('route',{'revert_name':'InvalidPool','revert_selector':'0x2083cd40'})==30
    rf.passed('route');assert not rf.blocked('route') and rf.priority('route')==0
    for key in ('a','b','c'):rf.record(key,signature)
    assert len(rf.history)==len(rf.failures)==2
    clock[0]+=3601
    assert rf.priority('c')==0


@pytest.mark.parametrize('extra',[{'validation_deadline_reached':True},
    {'validation_outcome':'call_succeeded_after_deadline'},{'anchor_mismatch':{'candidate':1}}])
def test_noncompatibility_outcome_does_not_increment_progressive_failure(extra):
    rf=RouteFeedback()
    assert rf.record('route',dict(failure(),**extra))==0
    assert not rf.failures and not rf.history


def test_archived_8233_error_shape_recognized_without_blanket_error_string_block():
    # Minimal public failure chain from archived opportunity-8233: same exact
    # reason at four budgets. No transaction calldata, sender or signature.
    f=failure(gases=(980000,1960000,3920000,5217391))
    assert f['revert_name']=='V2TransferFailed'
    assert f['previous_gas_failure']['revert_name']=='V2TransferFailed'
    rf=RouteFeedback()
    assert rf.record('archived-direction',f)==30
    assert not rf.blocked('reverse-direction')
    assert rf.record('single',failure(gases=(980000,)))==0
    assert rf.record('other-reason',failure('some other token error'))==0
    f['previous_gas_failure']=failure('some other token error',gases=(980000,))
    assert rf.record('different-reasons-same-selector',f)==0


def test_v2_transfer_error_still_gets_more_gas_and_can_succeed():
    class RPC:
        def __init__(self):self.budgets=[]
        def request(self,method,params):
            self.budgets.append(int(params[0]['gas'],16))
            if len(self.budgets)==1:
                raw='0x08c379a0'+encode(['string'],['UniswapV2: TRANSFER_FAILED']).hex()
                raise RuntimeError("RPC eth_call: "+repr({'code':3,'message':'execution reverted','data':raw}))
            return '0x'+encode(['uint256'],[1]).hex()
    rpc=RPC()
    assert execution.verified_call_gas_budget(rpc,{}, {},980000,3920000,time.monotonic_ns()+2_000_000_000)==1960000
    assert rpc.budgets==[980000,1960000]


def test_known_failures_do_not_fill_three_candidate_slots_before_healthy_alternative():
    candidates=[{'route_key':str(i),'tokens':[A],'net_after_modelled_cost':1000-i,
                 'execution_revalidation_priority':1} for i in range(4)]
    healthy={'route_key':'healthy','tokens':[A],'net_after_modelled_cost':1}
    chosen=candidate_attempt_order(candidates+[healthy],[A])
    assert chosen[0] is healthy and len(chosen)==3


def test_shared_feedback_caches_are_safe_under_concurrent_record_read_and_recovery():
    _,tf,rf,states,group,routes=two_directions()
    errors=[];barrier=threading.Barrier(3)
    def exercise(action):
        try:
            barrier.wait()
            for _ in range(300):action()
        except BaseException as exc:errors.append(exc)
    actions=[lambda:rf.record(routes[0].key,{'revert_name':'BadCallback'}),
             lambda:rf.group_deferred(group,states,[A],transfers=tf),
             lambda:(rf.passed(routes[0].key),rf.priority(routes[0].key))]
    threads=[threading.Thread(target=exercise,args=(action,),daemon=True) for action in actions]
    for thread in threads:thread.start()
    for thread in threads:thread.join(3)
    assert not any(thread.is_alive() for thread in threads)
    assert errors==[] and len(rf.failures)<=rf.limit and len(rf.history)<=rf.limit


def test_full_continuation_capacity_cannot_be_reused_while_selected_work_may_return():
    scheduler=DeepReadScheduler()
    old=[{'pools':[f'a{i:04}',f'b{i:04}'],'since':i,'deep':i%2==0} for i in range(1024)]
    new={'pools':['new-a','new-b'],'score':10**9}
    assert scheduler.resume(old,0)==[]
    scheduler.add([new],2000)
    available={p for row in old+[new] for p in row['pools']}
    eligible={tuple(row['pools']) for row in old[:6]}
    scheduler.take(available,2001,lambda row:row['pools'][0]!='new-a' and tuple(row['pools']) not in eligible)
    selected=list(scheduler.selected_requests)
    assert {row['pools'] for row in selected}==eligible
    assert tuple(new['pools']) in scheduler.pending
    assert scheduler.resume(selected,2002)==[]
    assert len(scheduler.resuming)==1024
    # If the selected work actually completes, only then its return slots free.
    scheduler.take(available,2003,lambda row:row['pools'][0]!='new-a' and tuple(row['pools']) not in eligible)
    scheduler.take(available,2004,lambda row:row['pools'][0]!='new-a')
    assert [row['pools'] for row in scheduler.selected_requests]==[tuple(new['pools'])]
    assert scheduler.resume(scheduler.selected_requests,2005)==[]
    assert len(scheduler.resuming)==1019


def test_fill_skips_a_group_that_does_not_fit_without_losing_it_or_exceeding_pool_budget():
    scheduler=DeepReadScheduler()
    old=[{'pools':[f'old-{i}-{j}' for j in range(4)],'since':i,'deep':False} for i in range(5)]
    old.append({'pools':['old-last-a','old-last-b'],'since':5,'deep':True})
    too_wide={'pools':['wide-a','wide-b','wide-c','wide-d'],'score':10**9,'route_keys':['wide-direction']}
    fits={'pools':['fit-a','fit-b'],'score':1,'route_keys':['fit-direction']}
    scheduler.resume(old,0);scheduler.add([too_wide,fits],10)
    available={p for row in old+[too_wide,fits] for p in row['pools']}
    assert len(scheduler.take(available,11))==24
    rows=scheduler.selected_requests
    assert len(rows)==7 and rows[-1]['pools']==tuple(fits['pools'])
    assert rows[-1]['route_keys']==['fit-direction']
    assert tuple(too_wide['pools']) in scheduler.pending
    assert scheduler.selected_deep_pools==set(old[-1]['pools']+fits['pools'])
    assert scheduler.resume(rows,12)==[]
    assert scheduler.pending[tuple(too_wide['pools'])]['route_keys']==['wide-direction']


def test_repeated_v2_transfer_error_needs_fresh_matching_helper_and_preserves_reverse():
    from searcher.transfer_feedback import TransferSample
    from test_hot_transfer_filter import EXECUTOR
    clock,tf,_,states,group,routes=two_directions()
    sample=TransferSample(B,group[0],group[1],100)
    result=dict(token=B,source=group[0],destination=group[1],executor=EXECUTOR,
                amount=100,funded=0,debited=0,credited=0,canonical=True,
                failure_stage=1,failure_selector='0x12345678',block=100,block_hash='0x'+'11'*32)
    tf.record(sample,result)
    forward=next(r for r in routes if tf.deferred(r))
    reverse=next(r for r in routes if r.key!=forward.key)
    clock[0]=31
    assert tf.claim_probe(forward)
    assert tf.rejected(forward,failure())==60
    assert tf.deferred(forward) and not tf.deferred(reverse)
    tf.passed(forward)
    assert tf.priority(forward)==0 and not tf.deferred(forward)
    assert tf.record(sample,result)==[]  # An in-flight helper cannot re-close a successful route.
