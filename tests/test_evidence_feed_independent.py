"""Independent evidence ordering and whole-page delivery boundaries.

Synthetic identities and deterministic local RPC only. No production state,
wallet, broadcasting, or historical profitability assertions.
"""
import copy

import pytest

from searcher.quarantine import DirectionQuarantine
from searcher.transfer_feedback import TransferFeedback, TransferSample
from test_candidate_quarantine_independent import (
    E, O, B, P, Q, EDGE, IDENTITY, ROUTE, evidence, failure,
)


def feedback_case(tmp_path):
    now = [1000.0]
    quarantine = DirectionQuarantine(tmp_path/'directions.json', IDENTITY, clock=lambda:now[0])
    feedback = TransferFeedback(E,O,clock=lambda:now[0],quarantine=quarantine)
    return now,quarantine,feedback,TransferSample(B,P,Q,100)


def test_later_helper_cannot_turn_completed_old_calls_into_witnesses(tmp_path):
    now,q,feedback,sample=feedback_case(tmp_path)
    for stamp in (1000.0,1700.0,2400.0):
        now[0]=stamp
        assert feedback.claim_probe(ROUTE)
        feedback.rejected(ROUTE,failure(stamp))
        now[0]=stamp+10
        feedback.record(sample,evidence(block=int(stamp)))
        assert not q.records, 'A later state must not retroactively authenticate an earlier call.'
    # A new, timely call after the actual sample is an admissible first witness.
    now[0]=2441
    assert feedback.claim_probe(ROUTE)
    feedback.rejected(ROUTE,failure(2441))
    assert q.summary()['watching']==1 and not q.blocked([EDGE])
    assert len(q.records[EDGE]['witnesses'])==1


@pytest.mark.parametrize('terminal', ['deadline','transport','succeeded_late'])
def test_fresh_diagnostic_does_not_upgrade_non_evidence_failure(tmp_path,terminal):
    now,q,feedback,sample=feedback_case(tmp_path)
    feedback.record(sample,evidence())
    now[0]=1031
    assert feedback.claim_probe(ROUTE)
    rejected=failure(1031)
    if terminal=='deadline': rejected['validation_deadline_reached']=True
    elif terminal=='transport': rejected={'cause':'RPC total request deadline exceeded'}
    else: rejected['validation_outcome']='call_succeeded_after_deadline'
    feedback.rejected(ROUTE,rejected)
    assert not q.records


def test_success_between_inflight_helper_and_completion_wins(tmp_path):
    now,q,feedback,sample=feedback_case(tmp_path)
    # Mark only an in-flight diagnostic, then simulate the real whole-call pass.
    feedback.in_flight[sample.key]=now[0]
    now[0]=1001
    feedback.passed(ROUTE)
    now[0]=1002
    assert feedback.record(sample,evidence())==[]
    feedback.finish_sample(sample,True)
    assert not q.records and not feedback.deferred(ROUTE)
    assert not feedback.edges and not feedback.in_flight


class IdentityQuote:
    def __init__(self, output=None): self.output=output
    def quote(self, token, amount): return (amount if self.output is None else self.output),0


def exact_states(output=None): return {P:IdentityQuote(output),Q:IdentityQuote()}


def test_exact_amount_sample_survives_other_amount_refresh(tmp_path):
    now,q,feedback,sample=feedback_case(tmp_path)
    feedback.record(sample,evidence())
    other=TransferSample(B,P,Q,200)
    feedback.record(other,dict(evidence(),amount=200,funded=190,debited=190,credited=190))
    now[0]=1031
    assert feedback.claim_probe(ROUTE,amount=100,states=exact_states(),settlements=[ROUTE.tokens[0]])
    feedback.rejected(ROUTE,failure(1031),amount=100)
    assert q.summary()['watching']==1, 'Another amount must not erase an exact fresh sample already available for this call.'
    assert len(q.records[EDGE]['witnesses'])==1


def test_post_claim_helper_cannot_become_evidence_for_already_started_call(tmp_path):
    now,q,feedback,sample=feedback_case(tmp_path)
    feedback.record(sample,evidence(clean=True))
    now[0]=1031
    assert feedback.claim_probe(ROUTE,amount=100,states=exact_states(),settlements=[ROUTE.tokens[0]])
    now[0]=1032
    feedback.record(sample,evidence(block=101))
    feedback.rejected(ROUTE,failure(1031),amount=100)
    assert not q.records


def test_pass_after_claim_invalidates_pinned_old_failure(tmp_path):
    now,q,feedback,sample=feedback_case(tmp_path)
    feedback.record(sample,evidence())
    now[0]=1031
    assert feedback.claim_probe(ROUTE,amount=100,states=exact_states(),settlements=[ROUTE.tokens[0]])
    now[0]=1032
    feedback.passed(ROUTE)
    feedback.rejected(ROUTE,failure(1031),amount=100)
    assert not q.records and not feedback.claimed_samples


def test_prewarm_reads_without_candidate_and_new_call_becomes_first_witness(tmp_path):
    now,q,feedback,sample=feedback_case(tmp_path)
    assert feedback.schedule_revalidation(ROUTE,exact_states(),100,[ROUTE.tokens[0]],failure(1000))
    assert feedback.take() is None
    now[0]=1031
    queued=feedback.take()
    assert queued==sample
    feedback.record(queued,evidence())
    feedback.finish_sample(queued,True)
    assert not q.records, 'Scheduling failure is not retrospectively upgraded.'
    now[0]=1062
    request=feedback.revalidation_requests()
    assert len(request)==1 and request[0]['deep'] is False
    assert request[0]['route_keys']==[ROUTE.key] and set(request[0]['pools'])==set(ROUTE.pools)
    assert feedback.claim_probe(ROUTE,amount=100,states=exact_states(),settlements=[ROUTE.tokens[0]])
    feedback.rejected(ROUTE,failure(1062),amount=100)
    assert q.summary()['watching']==1
    assert len(q.records[EDGE]['witnesses'])==1
    assert feedback.schedule_revalidation(ROUTE,exact_states(),100,[ROUTE.tokens[0]],failure(1062),60)
    now[0]=1123
    assert feedback.revalidation_requests()
    assert feedback.claim_probe(ROUTE,amount=100,states=exact_states(),settlements=[ROUTE.tokens[0]])
    feedback.rejected(ROUTE,failure(1123),amount=100)
    assert q.blocked([EDGE]) and len(q.records[EDGE]['witnesses'])==2
    assert not feedback.schedule_revalidation(ROUTE,exact_states(),100,[ROUTE.tokens[0]],failure(1123))


def test_repriced_intermediate_amount_requires_its_own_diagnostic(tmp_path):
    now,q,feedback,sample=feedback_case(tmp_path)
    assert feedback.schedule_revalidation(ROUTE,exact_states(),100,[ROUTE.tokens[0]],failure(1000))
    feedback.record(sample,evidence())
    now[0]=1031
    assert not feedback.claim_probe(ROUTE,amount=100,states=exact_states(200),settlements=[ROUTE.tokens[0]])
    replacement=feedback.take()
    assert replacement==TransferSample(B,P,Q,200)
    assert not q.records and not feedback.claimed_samples


class SliceRPC:
    """Decode the actual requested pool list and return only that atomic state."""
    def __init__(self): self.calls=[];self.fail_next=False;self.change_heads=False;self.number=2
    def batch(self,calls):
        from eth_abi import decode,encode
        from eth_utils import keccak
        from searcher.core import unhex
        from searcher.statefeed import STATE
        from test_pipeline import header,H0,H1,ZERO
        from searcher.math import sqrt_at_tick
        if calls[0][0]=='eth_getBlockByNumber':
            return [header(self.number,H1,H0),header(self.number+1,ZERO,H1)]
        params=calls[0][1]
        raw_call=unhex(params[0]['data'])
        depths='uint8[]' if raw_call[:4]==keccak(text='readWithDepths(address[],int24[],uint8,uint8[])')[:4] else 'uint8'
        addresses=decode(['address[]','int24[]','uint8',depths],raw_call[4:])[0]
        self.calls.append(tuple(addresses))
        assert len(addresses)<=24
        if self.fail_next:
            self.fail_next=False
            raise TimeoutError('synthetic read deadline')
        context=params[3];block=int(context['number'],16);stamp=int(context['time'],16)
        rows=[(a,True,sqrt_at_tick(0),0,10**12,3000,[-1,0,1],[0,0,0],[]) for a in addresses]
        raw='0x'+encode([STATE+'[]','uint256','uint256'],[rows,block,stamp]).hex()
        if self.change_heads:self.number+=1
        h=header(self.number,('0x'+f'{self.number:064x}') if self.change_heads else H1,H0)
        return [raw,h,header(self.number+1,ZERO,h['hash'])]


def slice_case():
    from test_pipeline import clock_config,header,H0,ZERO
    from test_searcher import pool
    from searcher.statefeed import StateFeed
    from searcher.pipeline import PendingStrategy
    from searcher.routes import RouteIndex
    addr=lambda n:'0x'+f'{n:040x}'
    config=clock_config()
    config['policy'].update(cycle_pages=True,max_hops=2,max_pools=64,route_groups_per_page=96)
    start=config['settlement_tokens'][0]['address']
    ps=[pool(addr(200+2*i+j),start,addr(100+i)) for i in range(20) for j in range(2)]
    snapshot={'header':header(1,H0,ZERO),'pools':[p.json() for p in ps]}
    rpc=SliceRPC();feed=StateFeed(rpc,config,snapshot)
    return feed,rpc,PendingStrategy(None,config,snapshot),set(RouteIndex(ps,[start],2).routes)


def test_bounded_slices_deliver_every_direction_before_new_economic_page(monkeypatch):
    from searcher.universe import PageProgress
    feed,rpc,strategy,expected=slice_case();progress=PageProgress();seen=[]
    def one(index,config,header,changed):
        key=index.work_queue.popleft();index.queued_routes.remove(key)
        index.last_evaluated_keys=[key];index.unstarted_keys=set();seen.append(key)
        return {'candidates':[],'routes_considered':1}
    monkeypatch.setattr('searcher.pipeline.evaluate',one)
    slices=[]
    while True:
        advance,page=progress.begin()
        assert advance
        frame=feed.poll(advance_page=advance);frame.transport['page_id']=page
        slices.append(copy.deepcopy(frame.transport['page_slice']))
        assert slices[-1]['logical_page']==1 and len(frame.snapshot_pools)<=24
        groups=frame.transport['route_groups']
        assert all(set(g)<=set(rpc.calls[-1]) for g in groups)
        while True:
            report=strategy.on_snapshot(frame)
            if report['page_advance_ready']:
                progress.consumed(page,report['continuation_requests']);break
            assert progress.begin()==(False,page), 'Unprocessed directions must not release the slice.'
        if not feed.has_pending_slices:break
    assert len(slices)>1 and {r['index'] for r in slices}==set(range(1,len(slices)+1))
    assert set(seen)==expected and len(seen)==len(expected)
    advance,page=progress.begin();assert advance
    assert feed.poll(advance_page=True).transport['page_slice']['logical_page']==2


def test_failed_slice_and_unacknowledged_rereads_keep_current_and_tail():
    feed,rpc,_,_=slice_case();rpc.fail_next=True
    with pytest.raises(TimeoutError):feed.poll()
    failed=rpc.calls[-1]
    frame=feed.poll(advance_page=False)
    first=copy.deepcopy(frame.transport['page_slice'])
    assert rpc.calls[-1]==failed and first['index']==1 and first['remaining_slices']>0
    again=feed.poll(advance_page=False)
    assert rpc.calls[-1]==failed and again.transport['page_slice']==first
    following=feed.poll(advance_page=True)
    assert following.transport['page_slice']['logical_page']==1
    assert following.transport['page_slice']['index']==2


def test_slicing_never_accepts_two_conflicting_pending_attempts():
    from searcher.core import Reject
    feed,rpc,_,_=slice_case();rpc.change_heads=True
    with pytest.raises(Reject,match='snapshot anchor changed'):feed.poll()
    assert len(rpc.calls)==2 and rpc.calls[0]==rpc.calls[1]
    assert feed._slice_number==1 and feed.has_pending_slices


def test_new_prewarm_cannot_steal_return_capacity_from_outstanding_depth():
    from searcher.statefeed import DeepReadScheduler
    addr=lambda n:'0x'+f'{n:040x}'
    old=[dict(pools=(addr(10000+2*i),addr(10001+2*i)),route_keys=[f'old-{i}'],deep=True) for i in range(1024)]
    scheduler=DeepReadScheduler()
    assert not scheduler.resume(old,0)
    available={a for row in old for a in row['pools']}
    scheduler.take(available,1)
    outstanding=copy.deepcopy(scheduler.selected_requests)
    assert len(outstanding)==12 and len(scheduler.resuming)==1012
    warm=[dict(pools=(addr(50000+2*i),addr(50001+2*i)),route_keys=[f'warm-{i}'],deep=False) for i in range(64)]
    scheduler.add(warm,2)
    assert len(scheduler.resuming)==1012, 'New requests do not own unfinished-return capacity.'
    assert not scheduler.resume(outstanding,3)
    assert len(scheduler.resuming)==1024 and len(scheduler.pending)==64
    assert all(not row['deep'] for row in scheduler.pending.values())
    scheduler.resuming.clear()  # Model the old workload being fully consumed.
    scheduler.take({a for row in warm for a in row['pools']},4)
    assert len(scheduler.selected_requests)==12 and not scheduler.selected_deep_pools
    assert all(not row['deep'] for row in scheduler.selected_requests)


def test_requested_direction_scope_and_depth_stay_with_each_slice():
    feed,rpc,_,_=slice_case()
    groups=list(feed.pages.groups)
    targets=(tuple(groups[0]),tuple(groups[-1]))
    requested=[dict(pools=g,route_keys=[f'precise-{i}'],deep=True) for i,g in enumerate(targets)]
    frame=feed.poll(deep_pools={a for g in targets for a in g},requested_groups=requested)
    collected=[];count=0
    while True:
        count+=1
        current=copy.deepcopy(frame.transport['requested_groups'])
        assert all(set(row['pools'])<=set(rpc.calls[-1]) for row in current)
        collected.extend(current)
        if current:
            old_groups=copy.deepcopy(frame.transport['route_groups'])
            shallow=feed.poll(advance_page=False,retry_shallow=True)
            assert shallow.transport['route_groups']==old_groups
            assert shallow.transport['requested_groups']==current
            assert not shallow.transport['deep_pools']
        if not feed.has_pending_slices:break
        frame=feed.poll(advance_page=True)
        expected={a for row in frame.transport['requested_groups'] for a in row['pools']}
        assert set(frame.transport['deep_pools'])==expected
    assert count>1
    assert sorted((tuple(r['pools']),tuple(r['route_keys'])) for r in collected)==sorted((tuple(r['pools']),tuple(r['route_keys'])) for r in requested)


def test_pipeline_refresh_waits_for_last_slice_actual_direction_ack(monkeypatch,tmp_path):
    import threading
    import searcher.pipeline as pipeline
    import searcher.discovery_reads as discovery_reads
    from searcher.statefeed import StateFeed
    from test_pipeline import header,H0,ZERO
    base,_,_,expected=slice_case()
    config=copy.deepcopy(base.config)
    config['v4']={'enabled':False}
    config.pop('_diagnostic_executor',None)
    config['policy'].update(hot_lane_enabled=False,activity_priority=False,
        pending_poll_interval_seconds=.001,universe_refresh_seconds=.001,max_quote_ms=1000)
    saved={'header':header(1,H0,ZERO),'pools':[p.json() for p in base.pages.pools.values()]}
    changed=copy.deepcopy(saved);changed.update(discovery_cursor=2,marker='refreshed')
    first_read,published,stop=threading.Event(),threading.Event(),threading.Event()
    seen=set();instances=[]
    class LocalRPC(SliceRPC):
        def __init__(self,*args,**kwargs):super().__init__();self.url='http://local.invalid';self.chain_id=8453
        def verify_chain(self):pass
        def close(self):pass
    class Feed(StateFeed):
        def __init__(self,rpc,cfg,state):
            super().__init__(rpc,cfg,state);instances.append(self)
            if state.get('marker')=='refreshed':
                assert seen==expected, 'Universe refresh discarded the first logical page tail.'
                stop.set()
        def poll(self,*args,**kwargs):
            frame=super().poll(*args,**kwargs)
            if not first_read.is_set():
                first_read.set()
                assert published.wait(3), 'Background refresh was not queued during the first slice.'
            return frame
    class Strategy(pipeline.PendingStrategy):
        def on_frame(self,frame):
            report=super().on_frame(frame)
            if report is not None:seen.update(set(self.index.last_evaluated_keys)-self.index.unstarted_keys)
            return report
    class Discovery:
        def __init__(self,*args,**kwargs):pass
        def prepare(self,*args,**kwargs):
            assert first_read.wait(3)
            return changed
        def close(self):pass
    original=pipeline.atomic_json
    def record(path,value):
        original(path,value)
        if path.name=='discovery-health.json' and value.get('state')=='updated':published.set()
    monkeypatch.setattr(pipeline,'RPC',LocalRPC)
    monkeypatch.setattr(pipeline,'StateFeed',Feed)
    monkeypatch.setattr(pipeline,'PendingStrategy',Strategy)
    monkeypatch.setattr(pipeline,'atomic_json',record)
    monkeypatch.setattr(discovery_reads,'DiscoveryReads',Discovery)
    pipeline.run_pending(LocalRPC(),config,saved,5,tmp_path/'pipeline',stop_event=stop)
    assert stop.is_set() and len(instances)>=2 and seen==expected


def test_helper_only_pipeline_success_cannot_clear_executor_quarantine(monkeypatch,tmp_path):
    import threading,time,json
    from types import SimpleNamespace
    import searcher.pipeline as pipeline
    import searcher.discovery_reads as discovery_reads
    import searcher.transfer_feedback as transfer_module
    import searcher.quarantine_runtime as quarantine_runtime
    from searcher.route_feedback import RouteFeedback
    from searcher.statefeed import StateFrame
    from test_pipeline import clock_config,saved,header,H0,H1
    from test_runtime import FakeRPC
    config=clock_config();config['v4']={'enabled':False};config['_diagnostic_executor']=E
    config['policy'].update(hot_lane_enabled=False,activity_priority=False,universe_refresh_seconds=30)
    snapshot=saved();route=ROUTE;config['settlement_tokens']=[{'address':route.tokens[0]}]
    stop=threading.Event();passed=[];probed=[]
    class Feedback:
        def __init__(self,*args,**kwargs):pass
        def run(self,*args,**kwargs):pass
        def revalidation_requests(self):return []
        def offer(self,*args):return 0
        def deferred(self,*args):return False
        def claim_probe(self,*args,**kwargs):return True
        def passed(self,*args):passed.append(args)
        def prewarm_summary(self):return {}
    class Feed:
        pages=None
        def __init__(self,*args):pass
        def poll(self,*args,**kwargs):
            now=time.monotonic_ns();h=header(2,H1,H0)
            return StateFrame(h,h,snapshot['pools'],now,now,time.time(),3,106)
    class Strategy:
        def __init__(self,*args):
            self.execution_feedback=RouteFeedback()
            self.index=SimpleNamespace(routes={route.key:route},states=exact_states())
        def on_frame(self,frame):
            return {'block':2,'decision_age_ms':0,'anchor':frame.header,'candidates':[{
                'route_key':route.key,'tokens':route.tokens,'pools':route.pools,'amount_in':100,
                'minimum_gross_surplus':1,'cost_budget':{'gas_limit':500000}}]}
    class Discovery:
        def __init__(self,*args,**kwargs):pass
        def close(self):pass
    def helper(*args,**kwargs):
        probed.append(True);stop.set()
        return {'first_transaction_gas_budget':100000}
    monkeypatch.setattr(pipeline,'RPC',FakeRPC)
    monkeypatch.setattr(pipeline,'StateFeed',Feed)
    monkeypatch.setattr(pipeline,'PendingStrategy',Strategy)
    monkeypatch.setattr(pipeline,'probe_cycle',helper)
    monkeypatch.setattr(discovery_reads,'DiscoveryReads',Discovery)
    monkeypatch.setattr(transfer_module,'TransferFeedback',Feedback)
    monkeypatch.setattr(quarantine_runtime,'open_quarantine',lambda *args,**kwargs:object())
    pipeline.run_pending(FakeRPC(),config,snapshot,5,tmp_path/'helper',stop_event=stop)
    assert probed==[True]
    records=[json.loads(p.read_text()) for p in (tmp_path/'helper').glob('decision-*.json')]
    assert any(c['status']=='whole_cycle_passed' for r in records for c in r['candidates'])
    assert not passed, 'CycleProbe success is not a call by the actual deployed executor.'
