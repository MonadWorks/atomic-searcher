"""Independent deadline, identity and real Executor gas-progress regressions.

Pure-clock tests use no network. The integration fixture uses only temporary
loopback Anvil contracts; it proves wiring and accounting, not mainnet profit.
"""
import copy
import time

import pytest
from eth_abi import encode

from searcher.core import RPCError, Reject, data, number
from searcher.execution import (
    Executor, GasValidationExpired, execution_failure_details,
    verified_call_gas_budget,
)
from searcher.gas_progress import GasValidationProgress
from test_execution import market


V2_REVERT = '0x08c379a0' + encode(['string'], ['UniswapV2: TRANSFER_FAILED']).hex()


def reverted(name='V2TransferFailed'):
    raw = V2_REVERT if name == 'V2TransferFailed' else data(name + '()')
    return RPCError('eth_call', {'code': 3, 'message': 'execution reverted', 'data': raw})


class TimedReadRPC:
    def __init__(self, clock, outcomes):
        self.clock, self.outcomes, self.calls = clock, iter(outcomes), []

    def request(self, method, params):
        assert method == 'eth_call'
        assert params[1:3] == ['pending', {}]
        self.calls.append(number(params[0]['gas']))
        elapsed_ns, result = next(self.outcomes)
        self.clock[0] += elapsed_ns
        if isinstance(result, Exception):
            raise result
        return result


@pytest.mark.parametrize('name', ['V2TransferFailed', 'TransferFailed', 'BadCallback', 'InvalidPlan'])
def test_late_final_or_terminal_revert_is_never_fresh_quarantine_evidence(monkeypatch, name):
    clock = [0]
    monkeypatch.setattr('searcher.execution.time.monotonic_ns', lambda: clock[0])
    rpc = TimedReadRPC(clock, [(101, reverted(name))])
    remembered = []
    with pytest.raises(GasValidationExpired) as caught:
        verified_call_gas_budget(rpc, {}, {}, 100, 100, 100,
                                 on_failed_call=lambda *args: remembered.append(args))
    details = execution_failure_details(caught.value)
    assert details['validation_deadline_reached'] is True
    assert details['revert_name'] == name
    assert details['gas_attempts'] == [100]
    assert remembered == []


def test_two_fresh_observations_advance_without_borrowing_old_failure_attempts(monkeypatch):
    clock = [0]
    monkeypatch.setattr('searcher.execution.time.monotonic_ns', lambda: clock[0])
    progress = GasValidationProgress(8453, '0x' + '11' * 20, clock=lambda: clock[0] / 1e9)
    key = progress.key('0x' + '22' * 20, 'route', 123)
    histories = []
    for start_at, expected in [(0, [580000, 1160000, 2320000]),
                               (10_000_000_000, [2320000, 4640000, 5217391])]:
        clock[0] = start_at
        initial, _ = progress.start(key, 580000, 5217391)
        rpc = TimedReadRPC(clock, [(700_000_000, reverted()) for _ in range(3)])
        with pytest.raises(GasValidationExpired) as caught:
            verified_call_gas_budget(rpc, {}, {}, initial, 5217391, start_at + 2_000_000_000,
                on_failed_call=lambda gas, name: progress.record(key, gas, name),
                on_success=lambda: progress.clear(key))
        details = execution_failure_details(caught.value)
        assert rpc.calls == expected
        assert details['gas_attempts'] == expected
        assert details['previous_gas_failure']['gas_attempts'] == expected[:-1]
        histories.append(rpc.calls)
    assert histories[1][0] > histories[0][0]
    assert progress.records[key]['gas'] == 4640000  # Late maximum is not remembered.


def test_one_timely_failure_per_observation_advances_until_half_cap_without_false_completion(monkeypatch):
    clock = [0]
    monkeypatch.setattr('searcher.execution.time.monotonic_ns', lambda: clock[0])
    progress = GasValidationProgress(8453, '0x' + '11' * 20, clock=lambda: clock[0] / 1e9)
    key = progress.key('0x' + '22' * 20, 'route', 123)
    expected_starts = [580000, 1160000, 2320000, 2608696, 2608696]
    for turn, expected_start in enumerate(expected_starts):
        clock[0] = turn * 10_000_000_000
        deadline = clock[0] + 1_000_000_000
        initial, _ = progress.start(key, 580000, 5217391)
        assert initial == expected_start
        rpc = TimedReadRPC(clock, [(700_000_000, reverted()), (700_000_000, reverted())])
        with pytest.raises(GasValidationExpired) as caught:
            verified_call_gas_budget(rpc, {}, {}, initial, 5217391, deadline,
                on_failed_call=lambda gas, name: progress.record(key, gas, name))
        details = execution_failure_details(caught.value)
        assert details['validation_deadline_reached'] is True
        assert details['gas_attempts'] == [initial, min(initial * 2, 5217391)]
        assert progress.records[key]['gas'] == initial
    # Finite exploration progress does not manufacture a timely two-call result.
    assert details['gas_attempts'] == [2608696, 5217391]


def test_scope_ttl_and_odd_cap_keep_two_current_calls_available():
    clock = [0]
    progress = GasValidationProgress(8453, '0x' + '11' * 20, ttl=10, capacity=2, clock=lambda: clock[0])
    key = progress.key('0x' + '22' * 20, 'route', 123)
    progress.record(key, 5217391, 'V2TransferFailed')
    assert progress.start(key, 580000, 5217391)[0] == 2608696
    for changed in (progress.key('0x' + '33' * 20, 'route', 123),
                    progress.key('0x' + '22' * 20, 'reverse', 123),
                    progress.key('0x' + '22' * 20, 'route', 124),
                    (1, *key[1:]), (key[0], '0x' + '44' * 20, *key[2:])):
        assert progress.start(changed, 580000, 5217391)[0] == 580000
    progress.record(progress.key('0x' + '22' * 20, 'second', 123), 2000000, 'TransferFailed')
    progress.record(progress.key('0x' + '22' * 20, 'third', 123), 2000000, 'TransferFailed')
    assert len(progress.records) == 2 and key not in progress.records
    live_key = next(iter(progress.records))
    clock[0] = 10
    assert progress.start(live_key, 580000, 5217391)[0] == 580000


@pytest.mark.parametrize('late', [False, True])
def test_success_clears_hint_even_when_returned_after_deadline(monkeypatch, late):
    clock = [0]
    monkeypatch.setattr('searcher.execution.time.monotonic_ns', lambda: clock[0])
    progress = GasValidationProgress(8453, '0x' + '11' * 20)
    key = progress.key('0x' + '22' * 20, 'route', 123)
    progress.record(key, 100, 'V2TransferFailed')
    rpc = TimedReadRPC(clock, [(101 if late else 50, '0x' + '00' * 32)])
    kwargs = dict(on_success=lambda: progress.clear(key))
    if late:
        with pytest.raises(Reject) as caught:
            verified_call_gas_budget(rpc, {}, {}, 100, 200, 100, **kwargs)
        assert execution_failure_details(caught.value)['validation_outcome'] == 'call_succeeded_after_deadline'
    else:
        assert verified_call_gas_budget(rpc, {}, {}, 100, 200, 100, **kwargs) == 100
    assert key not in progress.records


@pytest.mark.parametrize('failure', [TimeoutError('transport timeout'),
    RPCError('eth_call', {'code': 429, 'message': 'rate limited'}),
    Reject("RPC eth_call: {'message': 'execution reverted', 'data': '" + V2_REVERT + "'}")])
def test_transport_or_untyped_error_cannot_create_gas_progress(monkeypatch, failure):
    clock = [0]
    monkeypatch.setattr('searcher.execution.time.monotonic_ns', lambda: clock[0])
    remembered = []
    rpc = TimedReadRPC(clock, [(1, failure)])
    with pytest.raises(Exception):
        verified_call_gas_budget(rpc, {}, {}, 100, 100, 100,
                                 on_failed_call=lambda *args: remembered.append(args))
    assert remembered == []


def test_executor_reuses_hint_then_real_call_reprices_and_never_signs_in_readonly_mode(market, monkeypatch):
    rpc, config, contract, signer, directory, index, original_candidate, head, _ = market
    candidate = copy.deepcopy(original_candidate)
    candidate['cost_budget']['gas_limit'] = 100000
    executor = Executor(rpc, config, contract, signer, directory)
    executor.gas_estimate_context_verified = False
    route = index.routes[candidate['route_key']]
    key = executor.gas_progress.key(contract, route.key, int(candidate['amount_in']))
    real_request = rpc.request
    real_monotonic_ns = time.monotonic_ns
    fake_ns = [real_monotonic_ns()]
    fail_phase = [True]
    first_calls, second_calls = [], []
    before_nonce = number(real_request('eth_getTransactionCount', [signer.address, 'latest']))

    def controlled_request(method, params):
        if method == 'eth_sendRawTransaction':
            raise AssertionError('read-only integration must not broadcast')
        if (method == 'eth_call' and len(params) == 4 and
                params[0].get('to', '').lower() == contract):
            gas = number(params[0]['gas'])
            if fail_phase[0]:
                first_calls.append(gas)
                fake_ns[0] += 700_000_000
                raise reverted()
            second_calls.append(gas)
        return real_request(method, params)

    class ReadOnlyIdentity:
        address = signer.address
        def sign_transaction(self, *args, **kwargs):
            raise AssertionError('read-only integration must not sign')

    executor.signer = ReadOnlyIdentity()
    monkeypatch.setattr(rpc, 'request', controlled_request)
    try:
        with monkeypatch.context() as clock_patch:
            clock_patch.setattr('searcher.execution.time.monotonic_ns', lambda: fake_ns[0])
            # Retain the real context content, with a fresh local request clock.
            executor.context['requested_ns'] = fake_ns[0]
            with pytest.raises(GasValidationExpired) as caught:
                executor.execute(candidate, route, index.states, head,
                                 fake_ns[0] + 2_000_000_000, submit=False)
            assert execution_failure_details(caught.value)['gas_attempts'] == [100000, 200000, 400000]
        assert first_calls == [100000, 200000, 400000]
        assert executor.gas_progress.records[key]['gas'] == 200000
        assert executor.db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0] == 0
        fail_phase[0] = False
        executor.refresh_context()  # New real account/header reads for the next observation.
        next_candidate = copy.deepcopy(original_candidate)
        next_candidate['cost_budget']['gas_limit'] = 100000
        result = executor.execute(next_candidate, route, index.states, head,
                                  real_monotonic_ns() + 2_000_000_000, submit=False)
        assert second_calls[0] == 400000
        assert result['status'] == 'execution_preflight_passed'
        assert result['execution_gas_progress']['reused'] is True
        assert not result['signed'] and not result['broadcast']
        assert key not in executor.gas_progress.records
        assert result['reserved_fee_wei'] >= second_calls[0] * next_candidate['cost_budget']['max_fee_per_gas']
        assert result['simulated_gross_profit'] >= result['fee_in_settlement'] * 2
        assert executor.db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0] == 0
        assert number(real_request('eth_getTransactionCount', [signer.address, 'latest'])) == before_nonce
    finally:
        executor.close()
