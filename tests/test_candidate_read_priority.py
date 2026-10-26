"""Candidate rereads use reserved read quota; all HTTP/EVM data are synthetic."""
import copy
import json
import threading
import time
from types import SimpleNamespace

import pytest

from searcher.candidate_refresh import refresh_candidate, CandidateRefreshFailure, prioritized_candidate_read
from searcher.context import SharedClockHints
from searcher.core import PublicRateLimiter, WETH, Reject, load_config
from searcher.engine import evaluate
from searcher.routes import RouteIndex
from searcher.statefeed import StateFeed, StateFrame
from test_receipt_eligibility import pools, HEADER
from test_rpc_timing import transport, Session
from test_snapshot_freshness import SnapshotSession
from test_shared_clock import pair


def market():
    config=load_config()
    config['policy'].update(max_quote_ms=100, sizing_mode='fixed', cycle_pages=False,
                            activity_priority=False, hot_lane_enabled=False,
                            universe_refresh_seconds=100, pending_poll_interval_seconds=.01)
    config['settlement_tokens']=[dict(config['settlement_tokens'][0],max_input_raw=str(10**18))]
    ps=pools()[2:];ix=RouteIndex(ps,[WETH],2)
    previous=evaluate(ix,config,HEADER)['candidates'][0]
    return config,ix,ix.routes[previous['route_key']],previous


def cache_for(rpc):
    cache=SharedClockHints();now=time.monotonic_ns()
    assert cache.publish(rpc.url,rpc.chain_id,*pair(),requested_ns=now-1,received_ns=now)
    return cache


@pytest.mark.parametrize('candidate_priority',[False,True])
def test_saturated_ordinary_quota_does_not_block_reserved_candidate_triplet(transport,candidate_priority):
    rpc,clock=transport;config,index,route,previous=market()
    limiter=rpc.public_rate_limiter=PublicRateLimiter()
    limiter.acquire(18)
    cache=cache_for(rpc)
    rpc.session=SnapshotSession(clock,pools=[p.json() for p in index.states.values()],changed=False,network_ms=10)
    if candidate_priority:
        candidate,_,_,deadline,evidence=refresh_candidate(rpc,config,route,index.states,
            previous_candidate=previous,clock_hints=cache)
        assert candidate is not None and deadline>time.monotonic_ns()
        timing=evidence['frame']['transport']
        assert evidence['read_priority']=='critical'
        assert not hasattr(rpc,'clock_hints') and rpc.critical_reads is False
        assert len(limiter.sent)==21, 'Candidate triplet uses reserved read headroom, not submission slots.'
        assert timing['poll_public_limiter_wait_ms']==0
    else:
        rpc.clock_hints=cache
        frame=StateFeed(rpc,config,{'pools':[p.json() for p in index.states.values()]}).poll()
        timing=frame.transport
        assert timing['poll_public_limiter_wait_ms']>=1050-1e-5
    assert timing['shared_clock_used'] and timing['clock_bootstraps']==0
    assert len(rpc.session.payloads)==1 and len(rpc.session.payloads[0])==3
    assert timing['clock_attempts']==1


@pytest.mark.parametrize('original_priority',[False,True])
@pytest.mark.parametrize('existing_hints',[False,True])
def test_read_exception_restores_rpc_class_and_cache_and_preserves_failed_batch(transport,original_priority,existing_hints):
    rpc,clock=transport;config,index,route,previous=market()
    rpc.critical_reads=original_priority
    prior=object()
    if existing_hints:rpc.clock_hints=prior
    cache=cache_for(rpc)
    rpc.session=Session(clock,failure='headers')
    with pytest.raises(CandidateRefreshFailure) as caught:
        refresh_candidate(rpc,config,route,index.states,previous_candidate=previous,clock_hints=cache)
    evidence=caught.value.evidence
    assert evidence['stage']=='atomic_state_read' and evidence['preflight_reached'] is False
    assert evidence['error_type']=='ReadTimeout' and 'secret' not in evidence['error']
    assert rpc.critical_reads is original_priority
    assert (rpc.clock_hints is prior) if existing_hints else not hasattr(rpc,'clock_hints')
    row=evidence['transport']['rpc_batches'][0]
    assert row['role']=='atomic_snapshot' and row['succeeded'] is False
    assert row['methods']==['eth_call','eth_getBlockByNumber','eth_getBlockByNumber']
    assert row['network_headers_ms']==pytest.approx(11)
    assert evidence['started_ns']<=evidence['read_started_ns']<=evidence['read_finished_ns']<=evidence['failed_ns']
    assert len(rpc.session.payloads)==1, 'The patch adds no retry or longer timeout.'


def test_cancelled_candidate_read_leaves_no_limiter_or_priority_ownership(transport):
    rpc,clock=transport;config,index,route,previous=market()
    rpc.public_rate_limiter=PublicRateLimiter();rpc.cancel_event=threading.Event();rpc.cancel_event.set()
    with pytest.raises(CandidateRefreshFailure,match='cancelled'):
        refresh_candidate(rpc,config,route,index.states,previous_candidate=previous,clock_hints=cache_for(rpc))
    assert rpc.critical_reads is False and rpc.public_rate_limiter.critical_waiters==0
    assert not hasattr(rpc,'clock_hints') and not rpc.session.payloads


def test_critical_scope_restores_on_base_exception_and_does_not_create_legacy_flag():
    old=object();rpc=SimpleNamespace(critical_reads=False,clock_hints=old)
    with pytest.raises(KeyboardInterrupt):
        with prioritized_candidate_read(rpc,object()):
            assert rpc.critical_reads is True
            raise KeyboardInterrupt
    assert rpc.critical_reads is False and rpc.clock_hints is old
    legacy=SimpleNamespace()
    with prioritized_candidate_read(legacy):assert not hasattr(legacy,'critical_reads')
    assert not hasattr(legacy,'critical_reads')


def test_context_clock_is_not_discarded_when_lifecycle_cache_is_empty(transport):
    rpc,clock=transport;config,index,route,previous=market()
    cache=SharedClockHints();head,pending=pair()
    context={'requested_ns':time.monotonic_ns(),'head':head,'pending_head':pending}
    rpc.session=SnapshotSession(clock,pools=[p.json() for p in index.states.values()],changed=False,network_ms=10)
    _,_,_,_,evidence=refresh_candidate(rpc,config,route,index.states,context,
                                      previous_candidate=previous,clock_hints=cache)
    assert evidence['frame']['transport']['clock_bootstraps']==0
    assert not evidence['frame']['transport']['shared_clock_used']
    assert len(rpc.session.payloads)==1


@pytest.mark.parametrize('different_endpoint',[False,True])
def test_peer_hints_remain_endpoint_scoped_and_full_state_still_checked(transport,different_endpoint):
    rpc,clock=transport;config,index,route,previous=market()
    cache=SharedClockHints();now=time.monotonic_ns()
    assert cache.publish(rpc.url+'/other' if different_endpoint else rpc.url,rpc.chain_id,*pair(),
                         requested_ns=now-1,received_ns=now)
    rpc.session=SnapshotSession(clock,pools=[p.json() for p in index.states.values()],changed=False,corrupt='clock',network_ms=10)
    with pytest.raises(CandidateRefreshFailure,match='clock mismatch') as caught:
        refresh_candidate(rpc,config,route,index.states,previous_candidate=previous,clock_hints=cache)
    roles=[r['role'] for r in caught.value.evidence['transport']['rpc_batches']]
    assert roles==(['clock_bootstrap','atomic_snapshot'] if different_endpoint else ['atomic_snapshot'])
    assert rpc.critical_reads is False and not hasattr(rpc,'clock_hints')


def test_repricing_error_is_distinct_from_atomic_read_error(transport):
    rpc,clock=transport;config,index,route,previous=market()
    rpc.session=SnapshotSession(clock,pools=[p.json() for p in index.states.values()],changed=False,network_ms=10)
    previous['route_key']='other-route'
    with pytest.raises(CandidateRefreshFailure,match='route mismatch') as caught:
        refresh_candidate(rpc,config,route,index.states,previous_candidate=previous,clock_hints=cache_for(rpc))
    assert caught.value.evidence['stage']=='repricing'
    assert caught.value.evidence['transport']['rpc_batches'][-1]['succeeded']
    assert rpc.critical_reads is False


def test_pipeline_records_read_failure_and_never_calls_executor_or_execution_feedback(transport,monkeypatch,tmp_path):
    import searcher.pipeline as pipeline
    from test_runtime import FakeRPC
    rpc,clock=transport;config,index,route,previous=market()
    rpc.session=Session(clock,failure='headers')
    stop=threading.Event();calls=[];feedback=[]
    class DiscoveryFeed:
        def __init__(self,*args):pass
        def poll(self):
            now=time.monotonic_ns()
            return StateFrame(HEADER,HEADER,[p.json() for p in index.states.values()],
                              now-3_000_000_000,now,time.time(),101,202)
    head,pending=pair()
    class Executor:
        context={'requested_ns':time.monotonic_ns(),'head':head,'pending_head':pending}
        def bind_stop_event(self,event):pass
        def start_context_worker(self):pass
        def reconcile(self):
            if rpc.session.payloads:stop.set()
        def execute(self,*args):calls.append(args);raise AssertionError('preflight reached after failed state read')
    monkeypatch.setattr(pipeline,'RPC',FakeRPC)
    monkeypatch.setattr(pipeline,'StateFeed',DiscoveryFeed)
    monkeypatch.setattr('searcher.route_feedback.RouteFeedback.record',lambda *args:feedback.append(args))
    result=pipeline.run_pending(rpc,config,{'header':HEADER,'pools':[p.json() for p in index.states.values()]},1,tmp_path,
                                executor=Executor(),stop_event=stop)
    assert not result['worker_errors'] and result['counts']['candidate_recheck_failures']==1
    assert not calls and not feedback and rpc.critical_reads is False
    candidates=[c for p in tmp_path.glob('decision-*.json') for c in json.loads(p.read_text())['candidates']]
    failed=next(c for c in candidates if c['status']=='recheck_failed')
    assert failed['failure_stage']=='candidate_refresh' and 'execution_failure' not in failed
    evidence=failed['execution_recheck_failure']
    assert evidence['stage']=='atomic_state_read' and evidence['preflight_reached'] is False
    assert evidence['transport']['rpc_batches'][-1]['role']=='atomic_snapshot'
    assert evidence['transport']['rpc_batches'][-1]['succeeded'] is False
    assert not hasattr(rpc,'clock_hints')
