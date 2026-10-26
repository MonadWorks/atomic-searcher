"""Independent candidate-read boundaries; memory-only transports and fixtures."""
import copy
import json
import threading
import time

import pytest

from searcher.candidate_refresh import (
    CandidateRefreshFailure, prioritized_candidate_read, refresh_candidate,
)
from searcher.context import SharedClockHints
from searcher.core import RPCError, Reject
from searcher.statefeed import StateFrame
from test_candidate_read_priority import market, cache_for
from test_receipt_eligibility import HEADER
from test_rpc_timing import transport, Session, Limiter
from test_shared_clock import pair
from test_snapshot_freshness import SnapshotSession


def test_failure_diagnostics_filter_nested_extensions_and_provider_secrets(transport, monkeypatch):
    rpc, clock = transport
    config, index, route, previous = market()
    secret = 'PRIVATE_API_KEY_AND_RAW_TX_SENTINEL'
    row = dict(role='atomic_snapshot', method='batch', outcome='error', succeeded=False,
               elapsed_ms=12.5, http_status=403, attempts=1, batch_request_digest='a'*64,
               methods=['eth_call', 'eth_getBlockByNumber', 'eth_getBlockByNumber'],
               params=secret, headers={'Authorization':secret}, endpoint=secret,
               attempt_timings=[{'network_total_ms':2, 'error':secret, 'request':secret}]*12)
    class Feed:
        def __init__(self, rpc, local, saved):
            self.pools = saved['pools']
            self.last_transport = {'rpc_batches':[row]*10, 'credentials':secret}
        def poll(self, **kwargs):
            raise RPCError('eth_call', {'code':-32001, 'message':secret, 'data':'0x12'})
    monkeypatch.setattr('searcher.candidate_refresh.StateFeed', Feed)
    with pytest.raises(CandidateRefreshFailure) as caught:
        refresh_candidate(rpc, config, route, index.states, previous_candidate=previous,
                          clock_hints=cache_for(rpc))
    evidence = caught.value.evidence
    assert secret not in json.dumps(evidence) and secret not in str(caught.value)
    assert evidence['error']=='RPCError' and evidence['rpc_error_code']==-32001
    assert evidence['stage']=='atomic_state_read' and evidence['preflight_reached'] is False
    assert len(evidence['transport']['rpc_batches'])==4
    batch = evidence['transport']['rpc_batches'][0]
    assert len(batch['attempt_timings'])==8 and batch['elapsed_ms']==12.5
    assert batch['attempt_timings']==[{'network_total_ms':2}]*8
    assert rpc.critical_reads is False and not hasattr(rpc, 'clock_hints')


def test_nested_scope_and_next_ordinary_batch_restore_priority_and_original_hints(transport):
    rpc, clock = transport
    original, outer, inner = object(), object(), object()
    rpc.clock_hints = original
    limiter = rpc.public_rate_limiter = Limiter(clock, delay=0)
    rpc.session = Session(clock, statuses=(200,200))
    with prioritized_candidate_read(rpc, outer):
        with pytest.raises(Reject):
            with prioritized_candidate_read(rpc, inner):
                assert rpc.clock_hints is inner and rpc.critical_reads is True
                rpc.batch([('eth_call', ['first'])])
                raise Reject('synthetic local rejection')
        assert rpc.clock_hints is outer and rpc.critical_reads is True
    assert rpc.clock_hints is original and rpc.critical_reads is False
    rpc.batch([('eth_call', ['ordinary'])])
    assert limiter.calls==[(1,False,True),(1,False,False)]


@pytest.mark.parametrize('invalid_hint', ['expired','other_chain','other_endpoint'])
def test_candidate_does_not_borrow_expired_or_wrong_scope_clock(transport, invalid_hint):
    rpc, clock = transport
    config, index, route, previous = market()
    cache = SharedClockHints()
    now = time.monotonic_ns()
    assert cache.publish(rpc.url+'/other' if invalid_hint=='other_endpoint' else rpc.url,
                         1 if invalid_hint=='other_chain' else rpc.chain_id,
                         *pair(), requested_ns=now-1, received_ns=now)
    if invalid_hint=='expired': clock.advance(1.500001)
    rpc.session = SnapshotSession(clock, pools=[p.json() for p in index.states.values()],
                                  changed=False, network_ms=10)
    candidate, _, _, deadline, evidence = refresh_candidate(
        rpc, config, route, index.states, previous_candidate=previous, clock_hints=cache)
    assert candidate is not None and deadline>time.monotonic_ns()
    data = evidence['frame']['transport']
    assert data['clock_bootstraps']==1 and not data['shared_clock_used']
    assert [p[0]['method'] for p in rpc.session.payloads]==['eth_getBlockByNumber','eth_call']
    assert rpc.critical_reads is False and not hasattr(rpc, 'clock_hints')


@pytest.mark.parametrize('offset', [-1_500_000_001, 1])
def test_expired_or_future_supplied_context_requires_bootstrap(transport, offset):
    rpc, clock = transport
    config, index, route, previous = market()
    head, pending = pair()
    context={'requested_ns':time.monotonic_ns()+offset,'head':head,'pending_head':pending}
    rpc.session = SnapshotSession(clock, pools=[p.json() for p in index.states.values()],
                                  changed=False, network_ms=10)
    _, _, _, _, evidence = refresh_candidate(rpc, config, route, index.states, context,
        previous_candidate=previous, clock_hints=SharedClockHints())
    assert evidence['frame']['transport']['clock_bootstraps']==1


def test_failed_read_recovers_but_slow_complete_reread_retains_original_deadline(transport):
    rpc, clock = transport
    config, index, route, previous = market()
    original_config = copy.deepcopy(config)
    rpc.session = Session(clock, failure='headers')
    with pytest.raises(CandidateRefreshFailure):
        refresh_candidate(rpc, config, route, index.states, previous_candidate=previous,
                          clock_hints=cache_for(rpc))
    rpc.session = SnapshotSession(clock, pools=[p.json() for p in index.states.values()],
                                  changed=False, network_ms=2100)
    candidate, _, _, deadline, evidence = refresh_candidate(
        rpc, config, route, index.states, previous_candidate=previous, clock_hints=cache_for(rpc))
    assert candidate is None and deadline<time.monotonic_ns()
    assert deadline==evidence['observation_started_ns']+int(config['policy']['snapshot_max_age_ms']*1e6)
    assert evidence['decision']['search_time_limited']
    assert config==original_config and rpc.critical_reads is False
    assert len(rpc.session.payloads)==1 and not hasattr(rpc,'clock_hints')


def test_pipeline_repricing_failure_does_not_execute_or_penalize_route(transport, monkeypatch, tmp_path):
    import searcher.pipeline as pipeline
    import searcher.candidate_refresh as refresh_module
    from test_runtime import FakeRPC
    rpc, clock = transport
    config, index, route, previous = market()
    rpc.session = SnapshotSession(clock, pools=[p.json() for p in index.states.values()],
                                  changed=False, network_ms=10)
    stop=threading.Event(); executed=[]; feedback=[]
    class DiscoveryFeed:
        def __init__(self,*args): pass
        def poll(self):
            now=time.monotonic_ns()
            return StateFrame(HEADER,HEADER,[p.json() for p in index.states.values()],
                              now-3_000_000_000,now,time.time(),101,202)
    class Executor:
        context=None
        def bind_stop_event(self,event): pass
        def start_context_worker(self): pass
        def reconcile(self):
            if rpc.session.payloads: stop.set()
        def execute(self,*args):
            executed.append(args)
            raise AssertionError('repricing failure reached execute')
    def broken_index(*args,**kwargs): raise ValueError('raw provider credential=PRIVATE_REPRICE_SECRET')
    monkeypatch.setattr(pipeline,'RPC',FakeRPC)
    monkeypatch.setattr(pipeline,'StateFeed',DiscoveryFeed)
    monkeypatch.setattr(refresh_module,'RouteIndex',broken_index)
    monkeypatch.setattr('searcher.route_feedback.RouteFeedback.record',lambda *args:feedback.append(args))
    result=pipeline.run_pending(rpc,config,{'header':HEADER,'pools':[p.json() for p in index.states.values()]},
                               1,tmp_path,executor=Executor(),stop_event=stop)
    assert not result['worker_errors'] and result['counts']['candidate_recheck_failures']==1
    assert not executed and not feedback and rpc.critical_reads is False
    records=[json.loads(p.read_text()) for p in tmp_path.glob('decision-*.json')]
    failed=next(c for d in records for c in d['candidates'] if c['status']=='recheck_failed')
    assert failed['execution_recheck_failure']['stage']=='repricing'
    assert failed['execution_recheck_failure']['transport']['rpc_batches'][-1]['succeeded'] is True
    assert 'execution_failure' not in failed and 'execution' not in failed
    assert 'PRIVATE_REPRICE_SECRET' not in json.dumps(records)
