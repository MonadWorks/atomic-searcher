"""Recover the live 'fresh quote, changed preparation anchor' failure once.

Synthetic pipeline scenarios exercise repricing, rejection and cancellation;
loopback integration below verifies the typed failure precedes any signing.
"""
import copy
import json
import threading
import time

import pytest

from searcher.core import Reject, WETH, load_config
from searcher.execution import ExecutionAnchorChanged, execution_failure_details
from searcher.statefeed import StateFrame
from test_receipt_eligibility import pools, HEADER
from test_runtime import FakeRPC
from test_execution import market


@pytest.mark.parametrize('outcome', [
    'unchanged', 'profitable', 'disappeared', 'expired', 'read_failed', 'changed_again',
    'generic_error', 'contract_revert', 'cancelled', 'age_already_refreshed',
])
def test_pipeline_recovers_only_typed_preparation_failure_once(monkeypatch, tmp_path, outcome):
    import searcher.pipeline as pipeline
    config = load_config()
    config['policy'].update(max_quote_ms=500, cycle_pages=False, activity_priority=False,
        hot_lane_enabled=False, universe_refresh_seconds=100, pending_poll_interval_seconds=.01)
    config['settlement_tokens'] = [dict(config['settlement_tokens'][0], max_input_raw=str(10**18))]
    original = pools()[2:]
    fresh = copy.deepcopy(original)
    fresh[0].reserve1 = (1 if outcome == 'disappeared' else 3)*10**20
    new_header = dict(HEADER, number='0x65', hash='0x'+'22'*32)
    stop = threading.Event()
    calls, reads, submissions = [], [], []

    class Feed:
        def __init__(self, *args): pass
        def poll(self):
            now = time.monotonic_ns()
            age = 3_000_000_000 if outcome == 'age_already_refreshed' else 0
            return StateFrame(HEADER, HEADER, [p.json() for p in original],
                now-age, now, time.time(), 101, 202)

    class RefreshFeed:
        def __init__(self, rpc, local, saved): self.pools = saved['pools']
        def poll(self, **kwargs):
            reads.append(1)
            if outcome == 'read_failed': raise TimeoutError('synthetic read timeout')
            if outcome == 'cancelled': stop.set()
            now = time.monotonic_ns()
            age = 3_000_000_000 if outcome == 'expired' else 0
            return StateFrame(new_header, new_header, [p.json() for p in fresh],
                now-age, now, time.time(), 102, 204)

    class Executor:
        context = None
        def bind_stop_event(self, event): pass
        def start_context_worker(self): pass
        def reconcile(self):
            if calls or reads: stop.set()
        def execute(self, candidate, route, states, anchor, deadline):
            calls.append((copy.deepcopy(candidate), states, anchor, deadline))
            if outcome == 'generic_error': raise Reject('anchor expired during preparation')
            if outcome == 'contract_revert': raise Reject('BadCallback')
            if (len(calls) == 1 and outcome != 'unchanged') or outcome == 'changed_again':
                raise ExecutionAnchorChanged(anchor, new_header)
            assert not stop.is_set()
            assert anchor == (HEADER if outcome == 'unchanged' else new_header) and deadline > time.monotonic_ns()
            assert states[fresh[0].address].reserve1 == (2 if outcome == 'unchanged' else 3)*10**20
            submissions.append(candidate)
            stop.set()
            return {'tx_hash':'0x'+'11'*32, 'scope':'synthetic test only'}

    monkeypatch.setattr(pipeline, 'RPC', FakeRPC)
    monkeypatch.setattr(pipeline, 'StateFeed', Feed)
    monkeypatch.setattr('searcher.candidate_refresh.StateFeed', RefreshFeed)
    result = pipeline.run_pending(FakeRPC(), config,
        {'header':HEADER, 'pools':[p.json() for p in original]}, 1, tmp_path,
        executor=Executor(), stop_event=stop)
    assert not result['worker_errors']
    assert len(reads) == (0 if outcome in ('unchanged','generic_error','contract_revert') else 1)
    assert len(calls) == (2 if outcome in ('profitable','changed_again') else 1)
    assert bool(submissions) == (outcome in ('unchanged','profitable'))
    candidates = [c for f in tmp_path.glob('decision-*.json')
                  for c in json.loads(f.read_text())['candidates']]
    candidate = next(c for c in candidates if c.get('status') != 'quoted_only') if outcome != 'cancelled' else candidates[0]
    expected = {'unchanged':'submitted', 'profitable':'submitted', 'disappeared':'recheck_below_floor',
        'expired':'expired_before_probe', 'read_failed':'recheck_failed',
        'cancelled':'quoted_only'}
    assert candidate['status'] == expected.get(outcome, 'execution_rejected')
    if outcome not in ('unchanged','generic_error','contract_revert'):
        assert candidate['execution_recheck_trigger'] == (
            'observation_age' if outcome == 'age_already_refreshed' else 'execution_anchor_changed')
        assert candidate['execution_anchor_mismatch']['preflight_reached'] is False
    if outcome == 'profitable':
        assert submissions[0]['gross_surplus'] != candidate['gross_surplus']
        assert json.loads(json.dumps(submissions[0])) == candidate['execution_recheck']['decision']['candidates'][0]
    elif outcome == 'unchanged':
        assert 'execution_recheck' not in candidate and 'execution_anchor_mismatch' not in candidate
    else:
        assert 'execution' not in candidate


def test_real_executor_anchor_failure_precedes_probe_signing_and_reservation(market, monkeypatch):
    from searcher.execution import Executor
    rpc, config, contract, signer, directory, index, candidate, head, send = market
    executor = Executor(rpc, config, contract, signer, directory)
    try:
        anchor = dict(executor.context['head'], hash='0x'+'55'*32)
        monkeypatch.setattr(signer, 'sign_transaction', lambda tx:pytest.fail('anchor mismatch must not sign'))
        monkeypatch.setattr('searcher.execution.capture_execution_probe',
                            lambda *args:pytest.fail('anchor mismatch must not reach whole-call probe'))
        with pytest.raises(ExecutionAnchorChanged) as caught:
            executor.execute(candidate, index.routes[candidate['route_key']], index.states,
                             anchor, time.monotonic_ns()+2_000_000_000)
        details = execution_failure_details(caught.value)
        assert details['anchor_mismatch']['candidate'] == {k:anchor[k] for k in ('number','hash')}
        assert details['anchor_mismatch']['preflight_reached'] is False
        assert 'execution_probe' not in details
        assert executor.db.execute('SELECT count(*) FROM attempts').fetchone()[0] == 0
        assert executor.db.execute('SELECT count(*) FROM intents').fetchone()[0] == 0
    finally:
        executor.close()


@pytest.mark.parametrize('pool_rejects', [False, True])
def test_complete_recovery_pipeline_to_local_receipt_or_actual_contract_rejection(market, monkeypatch, pool_rejects):
    """Synthetic captured frames, real local executor, signing and receipts.

    This demonstrates state-machine wiring, not historical mainnet capture or
    live profit. No wallet or endpoint outside the temporary Anvil is used.
    """
    import searcher.pipeline as pipeline
    from searcher.core import number
    from searcher.execution import Executor
    rpc, config, contract, signer, directory, index, prepared, head, send = market
    config['policy'].update(cycle_pages=False, hot_lane_enabled=False, activity_priority=False,
        universe_refresh_seconds=100, pending_poll_interval_seconds=.01, max_quote_ms=500)
    route = index.routes[prepared['route_key']]
    if pool_rejects: rpc.request('anvil_setCode',[route.pools[1],'0x5f5ffd'])
    current = rpc.header()
    old = rpc.header(number(current['number'])-1)
    stop = threading.Event()
    reads, attempts = [], []
    states = [p.json() for p in index.states.values()]

    class Feed:
        def __init__(self,*args): pass
        def poll(self):
            now = time.monotonic_ns()
            return StateFrame(old,old,states,now,now,time.time(),
                number(old['number'])+1,number(old['timestamp'])+2)

    class RefreshFeed:
        def __init__(self,rpc,local,saved): self.pools=saved['pools']
        def poll(self,**kwargs):
            reads.append(1)
            now=time.monotonic_ns()
            return StateFrame(current,current,states,now,now,time.time(),
                number(current['number'])+1,number(current['timestamp'])+2)

    class RecordedExecutor(Executor):
        def start_context_worker(self): pass  # Explicit fixed local clock for this scenario.
        def reconcile(self):
            if attempts: stop.set()
            else: super().reconcile()
        def execute(self,*args):
            attempts.append(args[3]['hash'])
            try:
                result=super().execute(*args)
                stop.set()
                return result
            except ExecutionAnchorChanged:
                raise
            except Exception:
                stop.set()
                raise

    executor=RecordedExecutor(rpc,config,contract,signer,directory/'account')
    monkeypatch.setattr(pipeline,'RPC',FakeRPC)
    monkeypatch.setattr(pipeline,'StateFeed',Feed)
    monkeypatch.setattr('searcher.candidate_refresh.StateFeed',RefreshFeed)
    try:
        result=pipeline.run_pending(rpc,config,{'header':old,'pools':states},2,directory/'pipeline',
                                    executor=executor,stop_event=stop)
        assert not result['worker_errors'] and len(reads)==1 and attempts==[old['hash'],current['hash']]
        records=[c for p in (directory/'pipeline').glob('decision-*.json')
                 for c in json.loads(p.read_text())['candidates']]
        candidate=next(c for c in records if c.get('execution_recheck_trigger')=='execution_anchor_changed')
        if pool_rejects:
            assert candidate['status']=='execution_rejected'
            expected_stage = 'gas_estimate' if executor.gas_estimate_context_verified else 'initial_call'
            assert candidate['execution_failure']['execution_probe']['stage']==expected_stage
            assert executor.db.execute('SELECT count(*) FROM attempts').fetchone()[0]==0
        else:
            assert candidate['status']=='submitted'
            stop.clear()
            rpc.request('evm_mine',[])
            Executor.reconcile(executor)
            assert executor.db.execute('SELECT state FROM attempts').fetchone()[0]=='success'
            outcome=json.loads(executor.db.execute('SELECT outcome FROM intents').fetchone()[0])
            assert outcome['net_profit_raw']>0
    finally:
        executor.close()
