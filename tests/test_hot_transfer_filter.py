"""Before-read transfer feedback must preserve every still-eligible direction."""
from dataclasses import replace
import threading
import time

import pytest

from searcher.pools import Pool
from searcher.routes import Route, RouteIndex
from searcher.route_feedback import RouteFeedback
from searcher.transfer_feedback import TransferFeedback, TransferSample


def address(n): return '0x'+f'{n:040x}'


A, B, C = map(address, (1, 2, 3))
EXECUTOR, OWNER = address(90), address(91)


def pool(n, a, b):
    return Pool(address(n), address(99), 'v3', a, b, 1, 100)


def setup(triangle=False, capacity=2048):
    ps = ([pool(11,A,B), pool(12,B,C), pool(13,A,C)] if triangle else
          [pool(11,A,B), pool(12,A,B)])
    states = {p.address:p for p in ps}
    clock = [0.0]
    feedback = TransferFeedback(EXECUTOR, OWNER, clock=lambda:clock[0], capacity=capacity)
    return clock, feedback, states, tuple(states)


def incoming_shortfall(feedback, token, source, destination):
    sample = TransferSample(token, source, destination, 100)
    result = dict(token=token, source=source, destination=destination, executor=EXECUTOR,
                  amount=100, funded=96, debited=96, credited=96, canonical=True,
                  failure_stage=0, failure_selector='0x00000000', block=100, block_hash='0x'+'11'*32)
    assert feedback.record(sample, result) == [(token, source, EXECUTOR)]


def block_both(feedback, states):
    ps = list(states)
    incoming_shortfall(feedback, B, ps[0], ps[1])
    if len(ps)==2:
        incoming_shortfall(feedback, B, ps[1], ps[0])
    else:
        incoming_shortfall(feedback, C, ps[2], ps[1])


@pytest.mark.parametrize('triangle', [False, True])
def test_one_available_direction_keeps_the_complete_group(triangle):
    clock, feedback, states, group = setup(triangle)
    incoming_shortfall(feedback, B, group[0], group[1])
    directions = list(RouteIndex(list(states.values()), [A], 3).routes.values())
    assert any(feedback.deferred(r) for r in directions)
    assert any(not feedback.deferred(r) for r in directions)
    assert not feedback.group_deferred(group, states, [A])
    block_both(feedback, states)
    assert all(feedback.deferred(r) for r in directions)
    assert feedback.group_deferred(group, states, [A])


@pytest.mark.parametrize('stage', [1,2])
def test_identified_call_failure_preserves_reverse_then_defers_both(stage):
    _,feedback,states,group=setup()
    for source,destination in (group,group[::-1]):
        sample=TransferSample(B,source,destination,100)
        result=dict(token=B,source=source,destination=destination,executor=EXECUTOR,
                    amount=100,funded=0 if stage==1 else 100,debited=0,credited=0,
                    canonical=True,failure_stage=stage,failure_selector='0x12345678',
                    block=100,block_hash='0x'+'11'*32)
        feedback.record(sample,result)
        if source==group[0]:
            assert not feedback.group_deferred(group,states,[A])
    assert feedback.group_deferred(group,states,[A])
    assert {item['sample_kind'] for item in feedback.edges.values()}=={'call_failure'}


@pytest.mark.parametrize('triangle', [False, True])
def test_all_settlement_rotations_have_the_same_eligibility_as_production_routes(triangle):
    _, feedback, states, group = setup(triangle)
    block_both(feedback, states)
    starts = [A,B,C] if triangle else [A,B]
    directions = list(RouteIndex(list(states.values()), starts, 3).routes.values())
    assert len(directions) == (6 if triangle else 4)
    assert feedback.group_deferred(group, states, starts) == all(feedback.deferred(r) for r in directions)
    feedback.passed(directions[-1])
    assert not feedback.group_deferred(group, states, starts)
    assert any(not feedback.deferred(r) for r in directions)


def test_retry_deadline_reopens_hot_read_before_helper_ttl_and_consumption_closes_it():
    clock, feedback, states, group = setup()
    incoming_shortfall(feedback, B, group[0], group[1])
    clock[0] = 10
    incoming_shortfall(feedback, B, group[1], group[0])
    assert feedback.group_deferred(group, states, [A])
    clock[0] = 30
    assert not feedback.group_deferred(group, states, [A])
    forward = Route((A,B,A), group)
    assert feedback.claim_probe(forward)
    assert feedback.rejected(forward, {'revert_name':'BadCallback'}) == 60
    assert feedback.group_deferred(group, states, [A])
    clock[0] = 40
    assert not feedback.group_deferred(group, states, [A])


def test_expired_evidence_preserves_existing_retry_then_reopens_hot_group():
    clock, feedback, states, group = setup()
    block_both(feedback, states)
    routes = list(RouteIndex(list(states.values()), [A], 2).routes.values())
    for when in (31,92,213):
        clock[0] = when
        for route in routes:
            assert feedback.claim_probe(route)
            feedback.rejected(route, {'revert_name':'BadCallback'})
    assert feedback.group_deferred(group, states, [A])
    clock[0] = 301
    assert feedback.history
    assert feedback.group_deferred(group, states, [A])
    assert all(item['until'] <= clock[0] for item in feedback.edges.values())
    clock[0] = 454
    assert not feedback.group_deferred(group, states, [A])


def test_real_whole_call_success_immediately_reopens_a_cached_group():
    _, feedback, states, group = setup(True)
    block_both(feedback, states)
    assert feedback.group_deferred(group, states, [A])
    forward = Route((A,B,C,A), group)
    feedback.passed(forward)
    assert not feedback.group_deferred(group, states, [A])
    assert not feedback.deferred(forward)


def test_unknown_incomplete_or_oversized_groups_are_kept_not_inferred_blocked():
    _, feedback, states, group = setup()
    block_both(feedback, states)
    for unknown in ((), group[:1], group*2, (group[0],group[0]), (group[0],address(999))):
        assert not feedback.group_deferred(unknown, states, [A])
    assert not feedback.group_deferred(group, states, [C])
    third = pool(13,A,B)
    # Three parallel pools contain two-hop subsets, but not a complete
    # three-pool cycle; subsets must not justify dropping an unknown group.
    states[third.address] = third
    assert not feedback.group_deferred(tuple(states), states, [A])


def test_cache_keeps_topology_only_and_invalidates_on_token_identity_change(monkeypatch):
    import searcher.routes as routes
    clock, feedback, states, group = setup()
    block_both(feedback, states)
    original = routes.iter_cycles
    enumerations = []
    def traced(*args):
        enumerations.append(1)
        yield from original(*args)
    monkeypatch.setattr(routes,'iter_cycles',traced)
    assert feedback.group_deferred(group, states, [A])
    for p in states.values(): p.sqrt_price = 2**96
    clock[0] = 31
    assert not feedback.group_deferred(group, states, [A])
    assert len(enumerations) == 1
    clock[0] = 0
    changed = {a:replace(p, token1=C) for a,p in states.items()}
    assert not feedback.group_deferred(group, changed, [A])
    assert len(enumerations) == 2


def test_topology_cache_is_bounded_even_for_many_unrelated_groups():
    _, feedback, states, group = setup(capacity=2)
    block_both(feedback, states)
    for i in range(50):
        ps = [pool(100+i*2,A,C), pool(101+i*2,A,C)]
        other = {p.address:p for p in ps}
        assert not feedback.group_deferred(tuple(other), other, [A])
        assert len(feedback.group_routes) <= 2


def test_filter_does_not_consume_probe_slots_or_mutate_sample_lifetime():
    _, feedback, states, group = setup()
    block_both(feedback, states)
    before = {edge:(item['until'],item['next_probe'],item.get('probe_pending'))
              for edge,item in feedback.edges.items()}
    for _ in range(20): assert feedback.group_deferred(group, states, [A])
    assert before == {edge:(item['until'],item['next_probe'],item.get('probe_pending'))
                      for edge,item in feedback.edges.items()}


def test_hot_selector_skips_only_blocked_group_and_restores_on_retry():
    from searcher.fastlane import HotRoutes
    from test_fastlane import config
    clock, feedback, states, group = setup()
    block_both(feedback, states)
    healthy = [pool(14,C,address(4)), pool(15,C,address(4))]
    all_states = dict(states, **{p.address:p for p in healthy})
    c = config()
    c['settlement_tokens'] = [{'address':A},{'address':C}]
    selector = HotRoutes({'pools':[p.json() for p in all_states.values()]}, c,
                         clock=lambda:clock[0], group_allowed=lambda g,ps:not feedback.group_deferred(g,ps,[A,C]))
    selector.add([(p,0) for p in all_states])
    selected = selector.take()
    assert selected['groups'] == [tuple(p.address for p in healthy)]
    assert {p.address for p in selected['pools']} == {p.address for p in healthy}
    assert selected['selection_summary']['transfer_deferred_groups'] > 0
    selector.acknowledge(selected)

    # The feedback deadline and fresh activity, without metadata or selector
    # replacement, restore the previously excluded group for rechecking.
    clock[0] = 30
    selector.add([(p,30) for p in all_states])
    recovered = selector.take()
    assert group in recovered['groups']


def test_hot_selector_reads_group_when_reverse_direction_is_available():
    from searcher.fastlane import HotRoutes
    from test_fastlane import config
    clock, feedback, states, group = setup(True)
    incoming_shortfall(feedback,B,group[0],group[1])
    c = config();c['settlement_tokens']=[{'address':A}]
    selector=HotRoutes({'pools':[p.json() for p in states.values()]},c,
                       clock=lambda:clock[0],group_allowed=lambda g,ps:not feedback.group_deferred(g,ps,[A]))
    selector.add([(p,0) for p in group])
    selected=selector.take()
    assert selected['groups']==[tuple(sorted(group))]
    assert len(selected['pools'])==3


def test_hot_reader_never_requests_depth_of_all_direction_deferred_group(tmp_path):
    from searcher.activity import ActivityInbox
    from searcher.fastlane import FrameSequence, run_hot_reader
    from searcher.statefeed import StateFrame
    from test_fastlane import config
    from test_pipeline import header,H0,H1
    from test_runtime import FakeRPC
    from searcher.route_feedback import RouteFeedback

    _,feedback,states,group=setup()
    block_both(feedback,states)
    healthy=[pool(14,C,address(4)),pool(15,C,address(4))]
    all_states=dict(states,**{p.address:p for p in healthy})
    c=config();c['settlement_tokens']=[{'address':A},{'address':C}]
    stop=threading.Event();inbox=ActivityInbox();reads=[];frames=[]
    now=time.monotonic();inbox.publish(list(all_states),now)
    class Feed:
        def __init__(self,rpc,local,saved):
            self.pools=[Pool.from_json(p) for p in saved['pools']]
        def poll(self):
            reads.append({p.address for p in self.pools})
            ns=time.monotonic_ns()
            return StateFrame(header(2,H1,H0),header(2,H1,H0),[p.json() for p in self.pools],ns,ns,time.time(),3,106)
    def publish(lane, frame):
        frames.append(frame[1]);stop.set()
    run_hot_reader(FakeRPC,c,lambda:(0,{'pools':[p.json() for p in all_states.values()]}),
                   inbox,stop,now+2,tmp_path,FrameSequence(),publish,lambda *a,**kw:None,
                   feed_factory=Feed,group_allowed=lambda g,ps:not feedback.group_deferred(g,ps,[A,C]))
    assert reads==[{p.address for p in healthy}]
    assert len(frames)==1
    assert frames[0].transport['route_groups']==[tuple(p.address for p in healthy)]
    assert frames[0].transport['hot_selection']['transfer_deferred_groups']>0


@pytest.mark.parametrize('with_feedback', [False, True])
def test_pipeline_passes_live_shared_feedback_filter_to_hot_reader(tmp_path, monkeypatch, with_feedback):
    import searcher.activity as activity
    import searcher.fastlane as fastlane
    import searcher.pipeline as pipeline
    import searcher.transfer_feedback as transfers
    from searcher.statefeed import StateFrame
    from test_fastlane import config
    from test_pipeline import saved, header, H0, H1
    from test_runtime import FakeRPC

    c = config()
    if with_feedback: c['_diagnostic_executor'] = EXECUTOR
    else: c.pop('_diagnostic_executor',None)
    instances, route_feedbacks = [], []
    invoked = threading.Event()

    class Feedback(TransferFeedback):
        def __init__(self,*args,**kwargs):
            super().__init__(*args,**kwargs);instances.append(self)
        def run(self, url, chain, stop, finish, path, interval=10):
            stop.wait(max(0,finish-time.monotonic()))

    def hot_reader(**kwargs):
        callback=kwargs['group_allowed']
        assert callback is not None
        _,_,pools,group=setup()
        starts=tuple(t['address'] for t in c['settlement_tokens'])
        # Match authenticated metadata to the configured settlement asset.
        pools={a:replace(p,token0=starts[0]) for a,p in pools.items()}
        routes=list(RouteIndex(list(pools.values()),starts,2).routes.values())
        assert len(routes)==2 and callback(group,pools)
        route_feedbacks[0].record(routes[0].key,{'revert_name':'BadCallback'})
        assert callback(group,pools)  # Reverse direction stays available.
        if with_feedback:
            assert len(instances)==1
            reverse=routes[1]
            incoming_shortfall(instances[0],reverse.tokens[1],reverse.pools[0],reverse.pools[1])
            assert not callback(group,pools)
            instances[0].edges.clear()
            assert callback(group,pools)
        else:
            assert not instances
            route_feedbacks[0].record(routes[1].key,{'revert_name':'BadCallback'})
            assert not callback(group,pools)
        route_feedbacks[0].passed(routes[0].key)
        assert callback(group,pools)
        invoked.set()

    class Feed:
        def __init__(self,*args): pass
        def poll(self):
            now=time.monotonic_ns()
            return StateFrame(header(2,H1,H0),header(2,H1,H0),saved()['pools'],now,now,time.time(),3,106)

    class Strategy:
        def __init__(self,*args):
            self.execution_feedback=RouteFeedback();self.sizing_progress={}
            route_feedbacks.append(self.execution_feedback)
        def on_frame(self,frame): return {'block':2,'decision_age_ms':0,'candidates':[]}

    monkeypatch.setattr(transfers,'TransferFeedback',Feedback)
    monkeypatch.setattr(fastlane,'run_hot_reader',hot_reader)
    monkeypatch.setattr(fastlane,'endpoint_url',lambda:None)
    monkeypatch.setattr(pipeline,'RPC',FakeRPC)
    monkeypatch.setattr(pipeline,'StateFeed',Feed)
    monkeypatch.setattr(pipeline,'PendingStrategy',Strategy)
    monkeypatch.setattr(activity,'websocket_url',lambda:None)
    monkeypatch.setattr(activity,'ROOT',tmp_path)
    monkeypatch.setattr(FakeRPC,'request',lambda *args:[],raising=False)
    result=pipeline.run_pending(FakeRPC(),c,saved(),.08,tmp_path)
    assert invoked.is_set() and result['hot_reader_stopped']
