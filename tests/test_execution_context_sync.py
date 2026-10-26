"""Whole account-context synchronization; loopback-only executor integration."""
import copy
import json
import threading
import time
from types import SimpleNamespace

import pytest
from eth_abi import encode

import searcher.execution as execution
from searcher.core import Reject, number
from searcher.execution import Executor, ExecutionAnchorChanged, ORACLE
from test_execution import market
from test_context_freshness_timing import values_for


def empty_ledger(executor):
    assert executor.db.execute('SELECT count(*) FROM attempts').fetchone()[0] == 0
    assert executor.db.execute('SELECT count(*) FROM intents').fetchone()[0] == 0


def controlled_worker(monkeypatch, executor, fault=None):
    """First full read publishes; later full reads wait for a test event."""
    original = execution.RPC
    release, entered = threading.Event(), threading.Event()
    instances = []
    class WorkerRPC(original):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.reads = 0
            instances.append(self)

        def batch(self, calls):
            self.reads += 1
            if self.reads > 1:
                entered.set()
                assert release.wait(3), 'test did not release account read'
            result = super().batch(calls)
            if self.reads > 1 and fault:
                for i, (method, params) in enumerate(calls):
                    if fault == 'nonce' and method == 'eth_getTransactionCount' and params[1] == 'pending':
                        result[i] = hex(number(result[i]) + 1)
                    elif fault == 'balance' and method == 'eth_getBalance':
                        result[i] = '0x0'
                    elif fault == 'fees' and method == 'eth_call' and params[0].get('to') == ORACLE:
                        result[i] = '0x' + encode(['uint256'], [10**30]).hex()
            return result
    monkeypatch.setattr(execution, 'RPC', WorkerRPC)
    executor.start_context_worker()
    with executor._context_condition():
        assert executor.context_changed.wait_for(lambda: executor.context_published_sequence >= 2, 3)
    return release, entered, instances


def release_on_kick(executor, release):
    seen = []
    def observe():
        with executor._context_condition():
            ready = executor.context_changed.wait_for(lambda: executor.context_kick_sequence >= 1, 3)
            seen.append(ready)
        release.set()
    thread = threading.Thread(target=observe, daemon=True)
    thread.start()
    return thread, seen


def test_real_worker_catches_up_and_original_executor_signs_once_to_local_receipt(market, monkeypatch):
    rpc, config, contract, signer, directory, index, original_candidate, old_head, _ = market
    config['policy']['aggregate_execution_context'] = False
    candidate = copy.deepcopy(original_candidate)
    executor = Executor(rpc, config, contract, signer, directory)
    release, _, instances = controlled_worker(monkeypatch, executor)
    sign = signer.sign_transaction
    signed = []
    monkeypatch.setattr(signer, 'sign_transaction', lambda tx: (signed.append(dict(tx)), sign(tx))[1])
    try:
        rpc.request('evm_mine', [])
        anchor = rpc.header()
        assert number(anchor['number']) == number(old_head['number']) + 1
        assert executor.context['head']['hash'] == old_head['hash']
        unlocker, kicked = release_on_kick(executor, release)
        result = executor.execute(candidate, index.routes[candidate['route_key']], index.states,
                                  anchor, time.monotonic_ns() + 2_000_000_000)
        unlocker.join(3)
        assert kicked == [True]
        assert result['status'] == 'submitted' and len(signed) == 1
        assert executor.context_kick_sequence == 1
        assert candidate['execution_context_sync']['outcome'] == 'caught_up'
        assert candidate['execution_context_sync']['wait_ms'] < 500
        assert instances[0].reads >= 2
        rpc.request('evm_mine', [])
        executor.reconcile()
        assert executor.db.execute('SELECT state FROM attempts').fetchone()[0] == 'success'
    finally:
        release.set()
        executor.close()


@pytest.mark.parametrize('fault, reason', [('nonce', 'account already has a pending transaction'),
                                         ('balance', 'insufficient ETH fee balance'),
                                         ('fees', 'attempt fee cap exceeded')])
def test_new_complete_context_rechecks_nonce_balance_and_fees_before_probe(market, monkeypatch, fault, reason):
    rpc, config, contract, signer, directory, index, original_candidate, _, _ = market
    config['policy']['aggregate_execution_context'] = False
    candidate = copy.deepcopy(original_candidate)
    executor = Executor(rpc, config, contract, signer, directory)
    release, _, _ = controlled_worker(monkeypatch, executor, fault)
    monkeypatch.setattr(signer, 'sign_transaction', lambda tx: pytest.fail('new context rejection signed'))
    monkeypatch.setattr(execution, 'capture_execution_probe', lambda *args: pytest.fail('invalid account context reached probe'))
    try:
        rpc.request('evm_mine', [])
        anchor = rpc.header()
        unlocker, kicked = release_on_kick(executor, release)
        with pytest.raises(Reject, match=reason):
            executor.execute(candidate, index.routes[candidate['route_key']], index.states,
                             anchor, time.monotonic_ns() + 2_000_000_000)
        unlocker.join(3)
        assert kicked == [True] and executor.context_kick_sequence == 1
        assert candidate['execution_context_sync']['outcome'] == 'caught_up'
        empty_ledger(executor)
    finally:
        release.set()
        executor.close()


@pytest.mark.parametrize('kind', ['behind_without_worker', 'ahead', 'same_height_other_hash'])
def test_direct_or_non_lagging_mismatch_keeps_typed_immediate_rejection(market, monkeypatch, kind):
    rpc, config, contract, signer, directory, index, candidate, anchor, _ = market
    executor = Executor(rpc, config, contract, signer, directory)
    try:
        altered = copy.deepcopy(executor.context)
        height = number(anchor['number'])
        altered['head'] = dict(anchor, number=hex(height + (1 if kind == 'ahead' else -1 if kind == 'behind_without_worker' else 0)), hash='0x'+'33'*32)
        executor.context = altered
        # An alive worker for the two ineligible directions must not be kicked.
        if kind != 'behind_without_worker':
            executor.context_thread = threading.Thread(target=executor.context_stop.wait, daemon=True)
            executor.context_thread.start()
        monkeypatch.setattr(signer, 'sign_transaction', lambda tx: pytest.fail('mismatch signed'))
        monkeypatch.setattr(rpc, 'batch', lambda *args: pytest.fail('mismatch initiated synchronous RPC'))
        monkeypatch.setattr(rpc, 'request', lambda *args: pytest.fail('mismatch initiated synchronous RPC'))
        with pytest.raises(ExecutionAnchorChanged):
            executor.execute(candidate, index.routes[candidate['route_key']], index.states,
                             anchor, time.monotonic_ns()+2_000_000_000)
        assert executor.context_kick_sequence == 0
        assert 'execution_context_sync' not in candidate
        empty_ledger(executor)
    finally: executor.close()


def bare_executor(context):
    executor = Executor.__new__(Executor)
    executor.config = {'policy': {'max_gas': 100, 'max_total_fee_wei': 100,
                                  'aggregate_execution_context': False}, 'settlement_tokens': [], 'factories': []}
    executor.owner = '0x' + '11' * 20
    executor.context, executor.context_error = context, None
    executor.context_stop = threading.Event()
    executor.context_thread = None
    executor.rpc = SimpleNamespace(check_cancelled=lambda: None)
    return executor


def test_late_older_full_refresh_cannot_overwrite_new_account_generation():
    executor = bare_executor(None)
    entered, release = threading.Event(), threading.Event()
    class Source:
        def __init__(self, old): self.old = old
        def batch(self, calls):
            values = values_for(calls)
            if self.old:
                entered.set()
                assert release.wait(3)
            for i, (method, params) in enumerate(calls):
                if method == 'eth_getTransactionCount': values[i] = hex(1 if self.old else 2)
                if method == 'eth_getBalance': values[i] = hex(10 if self.old else 20)
            return values
    old = threading.Thread(target=lambda: executor.refresh_context(Source(True)), daemon=True)
    old.start()
    assert entered.wait(3)
    executor.refresh_context(Source(False))
    fresh = executor.context
    release.set()
    old.join(3)
    assert not old.is_alive() and executor.context is fresh
    assert fresh['nonce'] == fresh['pending_nonce'] == 2 and fresh['balance'] == 20
    assert fresh['refresh_generation'] == 2


def test_recorded_52595179_context_can_catch_52595180_anchor_without_price_refresh():
    anchor = {'number': hex(52595180), 'hash': '0x5f8b5925e3a646ace69ed3381a1d47ba8957ea68c00420964028932e558fc87e'}
    old = {'requested_ns': time.monotonic_ns()-380_261_708,
           'head': {'number': hex(52595179), 'hash': '0x9ce6d7dec5c5ce1d74efe4ac03b0f3f30e5da2907fdc2a121084f0ade13fd2a3'},
           'nonce': 7, 'pending_nonce': 7, 'balance': 1000, 'extra_fee': 100}
    executor = bare_executor(old)
    full = dict(old, requested_ns=time.monotonic_ns(), head=anchor, nonce=8, pending_nonce=8,
                balance=900, extra_fee=110)
    def publisher():
        with executor._context_condition():
            assert executor.context_changed.wait_for(lambda: getattr(executor, 'context_kick_sequence', 0) == 1, 3)
            executor.context = full
            executor.context_changed.notify_all()
    executor.context_thread = threading.Thread(target=publisher, daemon=True)
    executor.context_thread.start()
    candidate = {}
    deadline = old['requested_ns'] + 2_000_000_000
    context = executor._context_for_anchor(candidate, anchor, deadline)
    executor.context_thread.join(3)
    assert context is full and context['nonce'] == 8 and context['extra_fee'] == 110
    assert candidate['execution_context_sync']['outcome'] == 'caught_up'
    assert deadline == old['requested_ns'] + 2_000_000_000


@pytest.mark.parametrize('end', ['observation_deadline', 'maximum_wait', 'stop'])
def test_wait_is_bounded_by_original_deadline_and_stop_without_synchronous_reads(end):
    old = {'head': {'number': '0x1', 'hash': '0x'+'11'*32}}
    anchor = {'number': '0x2', 'hash': '0x'+'22'*32}
    executor, candidate = bare_executor(old), {}
    def worker():
        if end == 'stop':
            with executor._context_condition():
                assert executor.context_changed.wait_for(lambda: getattr(executor, 'context_kick_sequence', 0) == 1, 3)
                executor.context_stop.set()
                executor.context_changed.notify_all()
        else:
            executor.context_stop.wait(3)
    executor.context_thread = threading.Thread(target=worker, daemon=True)
    executor.context_thread.start()
    start = time.monotonic_ns()
    deadline = start + (80_000_000 if end == 'observation_deadline' else 2_000_000_000)
    try:
        if end == 'maximum_wait':
            assert executor._context_for_anchor(candidate, anchor, deadline) is old
            assert candidate['execution_context_sync']['outcome'] == 'timeout'
        else:
            with pytest.raises(Reject, match='expired|stopped'):
                executor._context_for_anchor(candidate, anchor, deadline)
        elapsed = (time.monotonic_ns()-start)/1e9
        assert elapsed < (.8 if end == 'maximum_wait' else .4)
        assert executor.context_kick_sequence == 1
    finally:
        executor.context_stop.set()
        executor.context_thread.join(3)


@pytest.mark.parametrize('outcome', ['submitted', 'contract_revert', 'still_behind'])
def test_repriced_candidate_context_sync_diagnostics_survive_pipeline_exit(monkeypatch, tmp_path, outcome):
    import searcher.pipeline as pipeline
    from searcher.core import load_config
    from searcher.statefeed import StateFrame
    from test_receipt_eligibility import pools, HEADER
    from test_runtime import FakeRPC
    config = load_config()
    config['policy'].update(max_quote_ms=500, cycle_pages=False, activity_priority=False,
        hot_lane_enabled=False, universe_refresh_seconds=100, pending_poll_interval_seconds=.01)
    config['settlement_tokens'] = [dict(config['settlement_tokens'][0], max_input_raw=str(10**18))]
    original = pools()[2:]
    fresh = copy.deepcopy(original)
    fresh[0].reserve1 = 3*10**20
    new_header = dict(HEADER, number='0x65', hash='0x'+'22'*32)
    stop, reads, calls = threading.Event(), [], []
    diagnostic = {'candidate_block':101, 'initial_context_block':100, 'refresh_requested':True,
                  'outcome':'timeout' if outcome == 'still_behind' else 'caught_up', 'wait_ms':25.0}
    class Feed:
        def __init__(self, *args): pass
        def poll(self):
            now = time.monotonic_ns()
            return StateFrame(HEADER, HEADER, [p.json() for p in original],
                              now-3_000_000_000, now, time.time(), 101, 202)
    class RefreshFeed:
        def __init__(self, rpc, local, saved): self.pools = saved['pools']
        def poll(self, **kwargs):
            reads.append(1)
            now = time.monotonic_ns()
            return StateFrame(new_header, new_header, [p.json() for p in fresh],
                              now, now, time.time(), 102, 204)
    class FakeExecutor:
        context = None
        def bind_stop_event(self, event): pass
        def start_context_worker(self): pass
        def reconcile(self):
            if calls: stop.set()
        def execute(self, candidate, route, states, anchor, deadline):
            calls.append(candidate)
            candidate['execution_context_sync'] = dict(diagnostic)
            if outcome == 'contract_revert': raise Reject('BadCallback')
            if outcome == 'still_behind': raise ExecutionAnchorChanged(anchor, HEADER)
            stop.set()
            return {'tx_hash':'0x'+'44'*32}
    monkeypatch.setattr(pipeline, 'RPC', FakeRPC)
    monkeypatch.setattr(pipeline, 'StateFeed', Feed)
    monkeypatch.setattr('searcher.candidate_refresh.StateFeed', RefreshFeed)
    result = pipeline.run_pending(FakeRPC(), config,
        {'header':HEADER, 'pools':[p.json() for p in original]}, 1, tmp_path,
        executor=FakeExecutor(), stop_event=stop)
    assert not result['worker_errors'] and len(reads) == len(calls) == 1
    rows = [row for path in tmp_path.glob('decision-*.json')
            for row in json.loads(path.read_text())['candidates']]
    row = next(row for row in rows if 'execution_context_sync' in row)
    assert row['execution_context_sync'] == diagnostic
    assert row['execution_recheck_trigger'] == 'observation_age'
    assert row['status'] == ('submitted' if outcome == 'submitted' else 'execution_rejected')
