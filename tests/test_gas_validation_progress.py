"""Real completed failures can advance validation without authorizing a send."""
from types import SimpleNamespace
import time

import pytest
from eth_abi import encode

import searcher.execution as execution
from searcher.core import RPCError, Reject, data
from searcher.gas_progress import GasValidationProgress
from searcher.route_feedback import RouteFeedback
from test_execution_context_sync_independent import handoff, head


OWNER, TARGET = '0x'+'11'*20, '0x'+'22'*20
ROUTE = 'route/forward'


def typed(name='V2TransferFailed'):
    raw = ('0x08c379a0'+encode(['string'],['UniswapV2: TRANSFER_FAILED']).hex()
           if name == 'V2TransferFailed' else data(name+'()'))
    return RPCError('eth_call', {'code':3,'message':'execution reverted','data':raw})


def clocked(monkeypatch):
    clock = [time.monotonic_ns()]
    monkeypatch.setattr(execution,'time',SimpleNamespace(monotonic_ns=lambda:clock[0],time=time.time))
    progress = GasValidationProgress(8453, OWNER, clock=lambda:clock[0]/1e9)
    return clock,progress,progress.key(TARGET,ROUTE,123)


def run(rpc,progress,key,clock,initial=580_000,maximum=5_217_391,duration=2_000_000_000):
    start,evidence = progress.start(key,initial,maximum)
    return execution.verified_call_gas_budget(rpc,{'from':OWNER,'to':TARGET}, {},start,maximum,
        clock[0]+duration,on_failed_call=lambda gas,name:progress.record(key,gas,name),
        on_success=lambda:progress.clear(key))


def test_next_attempt_retains_only_completed_typed_progress_and_uses_real_current_attempts(monkeypatch):
    clock,progress,key = clocked(monkeypatch)
    calls = []
    class RPC:
        def request(self,method,params):
            calls.append(int(params[0]['gas'],16))
            clock[0] += 200_000_000
            raise typed()
    with pytest.raises(execution.GasValidationExpired) as caught:
        run(RPC(),progress,key,clock,duration=650_000_000)
    assert calls == [580_000,1_160_000,2_320_000,4_640_000]
    assert progress.records[key]['gas'] == 2_320_000, 'late fourth failure is not a fresh completed hint'
    assert execution.execution_failure_details(caught.value)['validation_deadline_reached']
    calls.clear()
    with pytest.raises(RPCError) as caught:
        run(RPC(),progress,key,clock,duration=650_000_000)
    failure = execution.execution_failure_details(caught.value)
    assert calls == failure['gas_attempts'] == [2_608_696,5_217_391]
    assert failure['previous_gas_failure']['gas_attempts'] == calls[:1]
    assert 'validation_deadline_reached' not in failure
    assert RouteFeedback().record(ROUTE,failure) == 30


def test_odd_maximum_preserves_exactly_two_real_ceiling_attempts(monkeypatch):
    clock,progress,key = clocked(monkeypatch)
    progress.record(key,4_640_000,'V2TransferFailed')
    calls = []
    class RPC:
        def request(self,method,params):
            calls.append(int(params[0]['gas'],16))
            raise typed()
    with pytest.raises(RPCError) as caught: run(RPC(),progress,key,clock)
    assert calls == [2_608_696,5_217_391]
    assert execution.execution_failure_details(caught.value)['gas_attempts'] == calls


@pytest.mark.parametrize('fault', ['provider','timeout','oog','unknown','legacy_text','terminal'])
def test_transport_unknown_and_nonprogress_reverts_do_not_create_hints(monkeypatch,fault):
    clock,progress,key = clocked(monkeypatch)
    class RPC:
        def request(self,method,params):
            if fault == 'provider': raise RPCError(method,{'code':-32005,'message':'rate limit'})
            if fault == 'timeout': raise TimeoutError('read timed out')
            if fault == 'oog': raise RPCError(method,{'code':3,'message':'out of gas','data':'0x'})
            if fault == 'legacy_text': raise RuntimeError(str(typed()))
            raise typed('Unknown') if fault == 'unknown' else typed('BadCallback')
    with pytest.raises(Exception): run(RPC(),progress,key,clock)
    assert not progress.records


@pytest.mark.parametrize('late', [False,True])
def test_success_and_late_success_clear_hint_without_allowing_late_authority(monkeypatch,late):
    clock,progress,key = clocked(monkeypatch)
    progress.record(key,4_640_000,'TransferFailed')
    calls = []
    class RPC:
        def request(self,method,params):
            calls.append(int(params[0]['gas'],16))
            if late: clock[0] += 3_000_000_000
            return '0x'+encode(['uint256'],[1]).hex()
    if late:
        with pytest.raises(Reject) as caught: run(RPC(),progress,key,clock)
        assert execution.execution_failure_details(caught.value)['validation_outcome'] == 'call_succeeded_after_deadline'
    else: assert run(RPC(),progress,key,clock) == 2_608_696
    assert calls == [2_608_696] and not progress.records


@pytest.mark.parametrize('name', ['V2TransferFailed','TransferFailed','BadCallback'])
def test_late_final_ceiling_and_terminal_revert_are_not_fresh_quarantine_witnesses(monkeypatch,name):
    clock,progress,key = clocked(monkeypatch)
    class RPC:
        def request(self,method,params):
            clock[0] += 11
            raise typed(name)
    with pytest.raises(execution.GasValidationExpired) as caught:
        run(RPC(),progress,key,clock,initial=100,maximum=100,duration=10)
    failure = execution.execution_failure_details(caught.value)
    assert failure['validation_deadline_reached'] and failure['gas_attempts'] == [100]
    assert failure['revert_name'] == name and not progress.records
    assert RouteFeedback().record(ROUTE,failure) == 0


def test_hint_identity_ttl_capacity_and_changed_typed_failure_are_scoped():
    clock = [0.]
    progress = GasValidationProgress(8453,OWNER,ttl=30,capacity=2,clock=lambda:clock[0])
    key = progress.key(TARGET,ROUTE,123)
    progress.record(key,900,'TransferFailed')
    assert progress.start(key,100,1001)[0] == 501
    for other in [progress.key(TARGET,ROUTE,124), progress.key(TARGET,ROUTE+'reverse',123),
                  progress.key('0x'+'33'*20,ROUTE,123),
                  GasValidationProgress(1,OWNER).key(TARGET,ROUTE,123),
                  GasValidationProgress(8453,'0x'+'44'*20).key(TARGET,ROUTE,123)]:
        assert progress.start(other,100,1001)[0] == 100
    progress.record(key,200,'V2TransferFailed')
    assert progress.start(key,100,1001)[0] == 400, 'different typed failure starts after its own observed maximum'
    clock[0] = 30
    assert progress.start(key,100,1001)[0] == 100 and not progress.records
    for amount in range(3): progress.record(progress.key(TARGET,ROUTE,amount),200,'TransferFailed')
    assert len(progress.records) == 2
    assert progress.key(TARGET,ROUTE,0) not in progress.records


def test_existing_hint_does_not_extend_on_provider_failure_or_change_transaction_bounds(monkeypatch):
    clock,progress,key = clocked(monkeypatch)
    progress.record(key,4_640_000,'TransferFailed')
    original = dict(progress.records[key])
    class RPC:
        def request(self,method,params):
            clock[0] += 1000
            raise TimeoutError('provider')
    with pytest.raises(TimeoutError): run(RPC(),progress,key,clock)
    assert progress.records[key] == original
    assert progress.start(key,580_000,900_000)[0] == 580_000
    assert progress.start(key,580_000,100_000)[0] == 100_000


def test_stop_before_first_call_preserves_no_request_or_new_progress(monkeypatch):
    clock,progress,key = clocked(monkeypatch)
    class RPC:
        def request(self,*args): pytest.fail('stopped validation must not call')
    def cancelled(): raise Reject('stopped')
    with pytest.raises(Reject,match='stopped'):
        execution.verified_call_gas_budget(RPC(),{}, {},100,1000,clock[0]+10,
            before_first_call=cancelled,on_failed_call=lambda g,n:progress.record(key,g,n))
    assert not progress.records


def test_real_executor_rechecks_original_fee_cap_after_resumed_ceiling(handoff,monkeypatch):
    ex,rpc,candidate,route,states = handoff
    rpc.height = 101
    ex.refresh_context()
    key = ex.gas_progress.key(ex.contract,route.key,candidate['amount_in'])
    ex.gas_progress.record(key,4_640_000,'V2TransferFailed')
    ex.config['policy']['max_total_fee_wei'] = '20000000000000'
    before = dict(candidate['cost_budget'])
    with pytest.raises(Reject,match='attempt fee cap exceeded after gas estimate'):
        ex.execute(candidate,route,states,head(101),time.monotonic_ns()+2_000_000_000,submit=False)
    assert int(rpc.requests[-1][1][0]['gas'],16) == 2_608_696
    assert candidate['cost_budget'] == before
    assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
    assert not ex.db.execute('SELECT 1 FROM intents').fetchone()
    assert key not in ex.gas_progress.records, 'the real complete successful call clears the hint'


def test_real_executor_reuses_same_amount_progress_across_two_execute_calls(handoff,monkeypatch):
    ex,rpc,candidate,route,states = handoff
    rpc.height = 101
    ex.refresh_context()
    clock = [time.monotonic_ns()]
    monkeypatch.setattr(execution,'time',SimpleNamespace(monotonic_ns=lambda:clock[0],time=time.time))
    ex.gas_progress.clock = lambda:clock[0]/1e9
    calls = []
    def failing(method,params):
        assert method == 'eth_call' and params[0]['to'].lower() == ex.contract
        calls.append(int(params[0]['gas'],16))
        clock[0] += 200_000_000
        raise typed()
    monkeypatch.setattr(rpc,'request',failing)
    original_budget = dict(candidate['cost_budget'])
    with pytest.raises(execution.GasValidationExpired) as first:
        ex.execute(candidate,route,states,head(101),clock[0]+650_000_000,submit=False)
    assert calls == [580_000,1_160_000,2_320_000,4_640_000]
    first_failure = execution.execution_failure_details(first.value)
    assert first_failure['execution_gas_progress']['validation_start_gas'] == 580_000
    assert first_failure['validation_deadline_reached']
    assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
    # A new full account context and new original deadline belong to the next
    # attempt; no old pool state, pending clock or authorization is cached.
    ex.refresh_context()
    calls.clear()
    with pytest.raises(RPCError) as second:
        ex.execute(candidate,route,states,head(101),clock[0]+650_000_000,submit=False)
    failure = execution.execution_failure_details(second.value)
    assert calls == failure['gas_attempts'] == [2_608_696,5_217_391]
    assert failure['execution_gas_progress']['reused'] is True
    assert failure['execution_gas_progress']['remembered_failed_gas'] == 2_320_000
    assert failure['execution_gas_progress']['validation_start_gas'] == calls[0]
    assert 'validation_deadline_reached' not in failure
    assert candidate['cost_budget'] == original_budget
    assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
    assert not ex.db.execute('SELECT 1 FROM intents').fetchone()


def test_one_timely_failure_per_window_still_advances_to_the_next_unresolved_budget(monkeypatch):
    clock,progress,key = clocked(monkeypatch)
    starts = []
    for expected in [100,200,400,500]:
        calls = []
        class RPC:
            def request(self,method,params):
                calls.append(int(params[0]['gas'],16))
                clock[0] += 400_000_000 if len(calls)==1 else 900_000_000
                raise typed()
        with pytest.raises(execution.GasValidationExpired):
            run(RPC(),progress,key,clock,initial=100,maximum=1000,duration=1_000_000_000)
        starts.append(calls[0])
        assert calls[0] == expected and len(calls) == 2
        assert progress.records[key]['gas'] == expected
    assert starts == [100,200,400,500]
    # The final ceiling must still finish on time before it can be evidence.
    assert progress.start(key,100,1000)[0] == 500
