"""Independent freshness propagation counterexamples; synthetic local data only."""
import copy
import json
import threading
import time

import pytest

from searcher.core import Reject, load_config
from searcher.execution import Executor
from searcher.pipeline import PendingStrategy
from searcher.statefeed import StateFrame, StateFeed, observation_start_ns, complete_batch_observation_start
from test_pipeline import clock_config, saved
from test_receipt_eligibility import pools, HEADER
from test_rpc_timing import transport
from test_snapshot_freshness import read_frame


@pytest.mark.parametrize('field', ['pool_calldata', 'override_clock', 'read_tag', 'gas'])
def test_same_methods_different_snapshot_request_cannot_borrow_timing(transport, field):
    rpc, clock = transport
    frame = read_frame(transport)
    calls = [(r['method'], copy.deepcopy(r['params'])) for r in rpc.session.payloads[-1]]
    row = rpc.measurements[-1]
    assert complete_batch_observation_start(row, calls, row['batch_started_ns'], frame.received_ns)
    if field == 'pool_calldata': calls[0][1][0]['data'] += '00'
    elif field == 'override_clock': calls[0][1][3]['time'] = '0xffff'
    elif field == 'read_tag': calls[0][1][1] = 'latest'
    elif field == 'gas': calls[0][1][0]['gas'] = '0x01'
    assert complete_batch_observation_start(row, calls, row['batch_started_ns'], frame.received_ns) is None


def test_missing_timing_in_later_poll_does_not_reuse_validated_previous_poll(transport):
    rpc, clock = transport
    read_frame(transport, changed=False)
    feed = StateFeed(rpc, clock_config(), saved())
    first = feed.poll()
    assert first._observation_validated
    original_batch = rpc.batch
    def without_timing(calls):
        values = original_batch(calls)
        rpc.measurements[-1].pop('batch_request_digest')
        return values
    rpc.batch = without_timing
    second = feed.poll()
    assert not second._observation_validated
    assert second.observation_started_ns is None
    assert observation_start_ns(second) == second.requested_ns
    assert observation_start_ns(second) != first.observation_started_ns


@pytest.mark.parametrize('delay_phase', ['decode', 'consumer_queue'])
def test_time_after_network_still_expires_before_executor_work(transport, monkeypatch, delay_phase):
    rpc, clock = transport
    if delay_phase == 'decode':
        import searcher.statefeed as statefeed
        original_decode = statefeed.decode
        def slow_decode(*args, **kwargs):
            result = original_decode(*args, **kwargs)
            clock.advance(2.01)
            return result
        monkeypatch.setattr(statefeed, 'decode', slow_decode)
    frame = read_frame(transport)
    if delay_phase == 'consumer_queue': clock.advance(2.01)
    assert frame.received_ns-observation_start_ns(frame) == 350_000_000
    config = clock_config()
    report = PendingStrategy(rpc, config, saved()).on_snapshot(frame)
    assert report['observation_age_ms'] == pytest.approx(2360)
    executor = Executor.__new__(Executor)
    executor.config, executor.rpc = config, rpc
    requests_before = len(rpc.session.payloads)
    with pytest.raises(Reject, match='observation expired'):
        executor.execute({}, None, {}, frame.latest, observation_start_ns(frame)+2_000_000_000)
    assert len(rpc.session.payloads) == requests_before


@pytest.mark.parametrize('serialized', [False, True])
def test_real_pipeline_uses_live_origin_but_does_not_retrust_serialized_origin(monkeypatch, tmp_path, serialized):
    """Consumer integration; producer provenance is checked by real RPC tests above."""
    import searcher.pipeline as pipeline
    from test_runtime import FakeRPC
    config = load_config()
    config['policy'].update(max_quote_ms=500, cycle_pages=False, activity_priority=False,
                            universe_refresh_seconds=100, pending_poll_interval_seconds=.01)
    config['settlement_tokens'] = [dict(config['settlement_tokens'][0], max_input_raw=str(10**18))]
    ps = pools()[2:]
    saved_pools = {'header':HEADER, 'pools':[p.json() for p in ps]}
    stop = threading.Event()
    executed, refreshed, frames = [], [], []
    class Feed:
        def __init__(self, *args): pass
        def poll(self):
            now = time.monotonic_ns()
            frame = StateFrame(HEADER, HEADER, saved_pools['pools'], now-3_000_000_000,
                               now, time.time(), 101, 202, observation_started_ns=now-100_000_000)
            # Stand-in for the live producer. Serialized constructor inputs
            # deliberately have no corresponding validation marker.
            if not serialized: frame._observation_validated = True
            frames.append(frame)
            return frame
    class LocalExecutor:
        context = None
        def bind_stop_event(self, event): pass
        def start_context_worker(self): pass
        def reconcile(self): pass
        def execute(self, candidate, route, states, anchor, deadline):
            executed.append((candidate, route, states, anchor, deadline))
            stop.set()
            return {'tx_hash':'synthetic-local-no-broadcast'}
    def refresh(*args, **kwargs):
        refreshed.append(1)
        assert serialized, 'Live 100 ms state was treated as an old 3 second poll.'
        stop.set()
        return None, {}, HEADER, time.monotonic_ns()-1, {'decision':{'search_time_limited':True}}
    monkeypatch.setattr(pipeline, 'RPC', FakeRPC)
    monkeypatch.setattr(pipeline, 'StateFeed', Feed)
    monkeypatch.setattr('searcher.candidate_refresh.refresh_candidate', refresh)
    # End on the observed consumer action, not a 120 ms scheduling race.
    # The deadline only bounds a broken reader/consumer; assertions stay exact.
    result = pipeline.run_pending(FakeRPC(), config, saved_pools, 5, tmp_path,
                                  executor=LocalExecutor(), stop_event=stop)
    assert not result['worker_errors']
    decisions = [json.loads(p.read_text()) for p in tmp_path.glob('decision-*.json')]
    assert decisions and all(d['decision_age_ms'] >= 3000 for d in decisions)
    if serialized:
        assert refreshed and not executed
        assert all(not d['fresh_for_submission'] for d in decisions)
        assert all(c['status'] == 'expired_before_probe' for d in decisions for c in d['candidates'])
    else:
        assert len(executed) == 1 and not refreshed
        candidate, route, states, anchor, deadline = executed[0]
        assert any(deadline == f.observation_started_ns+2_000_000_000 for f in frames)
        assert anchor == HEADER and route.key == candidate['route_key']
        assert states[ps[0].address].reserve1 == ps[0].reserve1
        assert any(d['fresh_for_submission'] and d['observation_age_ms'] < 1000 for d in decisions)
