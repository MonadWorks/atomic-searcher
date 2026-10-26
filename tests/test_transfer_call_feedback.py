"""Independent directed-call failure and recovery regressions.

Helper failures affect bounded scheduling only. They never replace a complete
executor validation, and infrastructure failures create no direction evidence.
"""
import json
import time

import pytest
from eth_abi import encode

from searcher.core import RPCError, Reject, WETH, data
from searcher.execution import Executor, execution_failure_details
from searcher.routes import Route
from searcher.transfer_feedback import TransferFeedback, TransferSample, measure_sample
from test_execution import market
from test_receipt_eligibility import BAD, HEADER, pools
from test_transfer_feedback import EXECUTOR, OWNER, OTHER_POOL, sample_result


OTHER_TOKEN = '0x' + 'dd' * 20
TAIL = '0x' + 'ee' * 20
TAIL_OTHER = '0x' + 'ff' * 20
FAILURE_SELECTOR = '0x90b8ec18'


def observed_failure(sample, stage=2, selector='0x00000000'):
    result = sample_result(sample, funded=0 if stage == 1 else sample.amount, credited=0)
    result.update(debited=0, failure_stage=stage, failure_selector=selector)
    return result


def whole_failure(**extra):
    result = {'revert_name': 'TransferFailed', 'revert_selector': FAILURE_SELECTOR,
              'gas_attempts': [500_000, 1_000_000],
              'previous_gas_failure': {'revert_name': 'TransferFailed',
                                       'revert_selector': FAILURE_SELECTOR,
                                       'gas_attempts': [500_000]}}
    result.update(extra)
    return result


def fixture_registry(stage=2):
    clock = [0.0]
    feedback = TransferFeedback(EXECUTOR, OWNER, clock=lambda: clock[0])
    ps = pools()
    sample = TransferSample(BAD, ps[0].address, ps[1].address, 100)
    route = Route((WETH, BAD, WETH), (sample.source, sample.destination))
    feedback.record(sample, observed_failure(sample, stage))
    return clock, feedback, sample, route


@pytest.mark.parametrize('stage', [1, 2])
def test_failure_uses_only_its_actual_directed_edge(stage):
    _, feedback, sample, route = fixture_registry(stage)
    same_source = Route((WETH, BAD, WETH), (sample.source, OTHER_POOL))
    same_destination = Route((WETH, BAD, WETH), (OTHER_POOL, sample.destination))
    reverse = Route((WETH, BAD, WETH), (sample.destination, sample.source))
    expected = ((BAD, sample.source, EXECUTOR) if stage == 1 else
                (BAD, EXECUTOR, sample.destination))
    assert set(feedback.edges) == {expected}
    assert feedback.deferred(route)
    assert feedback.deferred(same_source) == (stage == 1)
    assert feedback.deferred(same_destination) == (stage == 2)
    assert not feedback.deferred(reverse)
    retained = feedback.summary()['recent_samples'][0]
    assert retained['failure_stage'] == stage and retained['transfer_call_failed']
    assert retained['token'] == sample.token and retained['source'] == sample.source
    assert retained['destination'] == sample.destination and retained['canonical']
    assert not retained['receipt_shortfall']


def test_same_failed_middle_edge_is_shared_across_tail_and_settlement_variants():
    clock, feedback, sample, _ = fixture_registry()
    routes = [
        Route((WETH, BAD, OTHER_TOKEN, WETH), (sample.source, sample.destination, tail))
        for tail in (TAIL, TAIL_OTHER)
    ]
    routes.append(Route((OTHER_TOKEN, WETH, BAD, OTHER_TOKEN),
                        (TAIL_OTHER, sample.source, sample.destination)))
    assert len({route.key for route in routes}) == 3
    assert all(feedback.deferred(route) for route in routes)
    clock[0] = 31
    assert feedback.claim_probe(routes[0])
    assert feedback.rejected(routes[0], whole_failure()) == 60
    assert all(feedback.deferred(route) for route in routes)
    assert feedback.rejected(routes[0], whole_failure()) == 0
    assert all(item['until'] == 300 for item in feedback.edges.values())


@pytest.mark.parametrize('stage', [1, 2])
def test_improved_transfer_after_expiry_removes_scheduling_suspicion(stage):
    clock, feedback, sample, route = fixture_registry(stage)
    clock[0] = 31
    assert feedback.claim_probe(route)
    assert feedback.rejected(route, whole_failure()) == 60
    clock[0] = 301
    ps = {pool.address: pool for pool in pools()}
    assert feedback.offer(route, ps, 10**15, [WETH]) == 1
    fresh = feedback.take()
    assert fresh is not None
    good = sample_result(fresh)
    good.update(failure_stage=0, failure_selector='0x00000000')
    assert feedback.record(fresh, good) == []
    feedback.finish_sample(fresh, True)
    assert not feedback.deferred(route) and feedback.claim_probe(route)
    assert feedback.history  # Failure history alone has no eligibility effect.
    assert not feedback.results[fresh.key]['transfer_call_failed']


def test_call_failure_history_never_renews_freshness_or_restarts_elapsed_retry():
    clock, feedback, sample, route = fixture_registry()
    for when, delay in [(31, 60), (92, 120), (213, 240)]:
        clock[0] = when
        assert feedback.claim_probe(route)
        assert feedback.rejected(route, whole_failure()) == delay
        assert all(item['until'] == 300 for item in feedback.edges.values())
    clock[0] = 301
    feedback.record(sample, observed_failure(sample))
    assert all(item['until'] == 601 and item['next_probe'] == 453
               for item in feedback.edges.values())
    clock[0] = 454
    assert feedback.claim_probe(route)
    assert feedback.rejected(route, whole_failure()) == 300
    clock[0] = 602
    feedback.record(sample, observed_failure(sample))
    assert all(item['until'] == 902 and item['next_probe'] == 754
               for item in feedback.edges.values())
    clock[0] = 755
    assert feedback.claim_probe(route)
    assert feedback.rejected(route, whole_failure()) == 300
    clock[0] = 903
    assert feedback.deferred(route) and not feedback.claim_probe(route)
    clock[0] = 1056
    assert not feedback.deferred(route) and feedback.claim_probe(route)


@pytest.mark.parametrize('failure', [
    whole_failure(gas_attempts=[]),
    whole_failure(gas_attempts=[500_000]),
    whole_failure(gas_attempts=[500_000, 500_000]),
    whole_failure(validation_deadline_reached=True),
    whole_failure(validation_outcome='call_succeeded_after_deadline'),
    {'cause': 'connection lost'},
    # More than one attempted budget is not evidence of the same failure at
    # more than one budget: the preceding call here ran out of gas instead.
    whole_failure(previous_gas_failure={'revert_name': 'unclassified',
                                       'revert_selector': '0x00000000',
                                       'gas_attempts': [500_000]}),
])
def test_inconclusive_or_single_semantic_failure_does_not_escalate_history(failure):
    clock, feedback, _, route = fixture_registry()
    clock[0] = 31
    assert feedback.claim_probe(route)
    assert feedback.rejected(route, failure) == 0
    assert not feedback.history
    assert all(item['repeated_reverts'] == 0 and item['until'] == 300
               for item in feedback.edges.values())
    clock[0] = 301
    assert feedback.claim_probe(route) and not feedback.deferred(route)


def test_receipt_shortfall_alone_is_not_call_failure_evidence():
    clock, feedback, sample, route = fixture_registry()
    feedback.edges.clear()
    feedback.record(sample, sample_result(sample, funded=96))
    clock[0] = 31
    assert feedback.claim_probe(route)
    assert feedback.rejected(route, whole_failure()) == 0
    assert not feedback.history


def test_mixed_incoming_shortfall_and_outgoing_failure_escalates_only_failed_call_edge():
    clock, feedback, sample, route = fixture_registry()
    feedback.edges.clear()
    result = observed_failure(sample)
    result['funded'] = 96
    feedback.record(sample, result)
    incoming = (BAD, sample.source, EXECUTOR)
    outgoing = (BAD, EXECUTOR, sample.destination)
    assert set(feedback.edges) == {incoming, outgoing}
    retained = feedback.summary()['recent_samples'][-1]
    assert retained['receipt_shortfall'] and retained['transfer_call_failed']
    assert feedback.edges[incoming]['sample_kind'] == 'receipt_shortfall'
    assert feedback.edges[outgoing]['sample_kind'] == 'call_failure'
    clock[0] = 31
    assert feedback.claim_probe(route)
    assert feedback.rejected(route, whole_failure()) == 60
    assert feedback.edges[outgoing]['repeated_reverts'] == 1
    assert feedback.edges[incoming]['repeated_reverts'] == 0
    assert incoming not in feedback.history
    # A route through the same funding source but a different destination has
    # only the normal probe spacing, not escalation from someone else's edge.
    alternate = Route((WETH, BAD, WETH), (sample.source, OTHER_POOL))
    clock[0] = 62
    assert feedback.claim_probe(alternate)
    assert feedback.deferred(route)


@pytest.mark.parametrize('stage', [1, 2])
def test_real_pass_outranks_call_failure_and_late_helper_result(stage):
    clock, feedback, sample, route = fixture_registry(stage)
    clock[0] = 31
    assert feedback.claim_probe(route)
    assert feedback.rejected(route, whole_failure()) == 60
    feedback.passed(route)
    assert not feedback.edges and not feedback.history
    assert feedback.record(sample, observed_failure(sample, stage)) == []
    assert feedback.priority(route) == 0 and not feedback.deferred(route)
    clock[0] += 301
    feedback.record(sample, observed_failure(sample, stage))
    assert all(item['repeated_reverts'] == 0 for item in feedback.edges.values())


@pytest.mark.parametrize('stage,values', [(1, [0, 0, 0]), (2, [100, 0, 0])])
def test_new_status_abi_preserves_failure_identity_without_overriding_token(stage, values):
    calls = []
    class RPC:
        def header(self, number=None): return HEADER
        def request(self, method, params):
            calls.append((method, params))
            return '0x' + encode(['uint256'] * 3 + ['uint8', 'bytes4'],
                                 values + [stage, bytes.fromhex('12345678')]).hex()
    _, _, sample, _ = fixture_registry()
    result = measure_sample(RPC(), sample, EXECUTOR, OWNER, '0x6000')
    assert result['failure_stage'] == stage and result['failure_selector'] == '0x12345678'
    method, params = calls[0]
    assert method == 'eth_call' and params[1] == HEADER['number']
    assert params[0]['data'].startswith(data('sampleTransferStatus(address,address,address,uint256)')[:10])
    assert set(params[2]) == {sample.source, EXECUTOR}
    assert all(set(override) == {'code'} for override in params[2].values())
    assert sample.token not in params[2]


@pytest.mark.parametrize('case', ['timeout', 'rpc_revert', 'reorg', 'invalid_stage', 'truncated_abi'])
def test_worker_infrastructure_or_unknown_failure_creates_no_direction_evidence(case, monkeypatch, tmp_path):
    import searcher.transfer_feedback as module
    class RPC:
        def __init__(self, *args, **kwargs): pass
        def verify_chain(self): pass
        def close(self): pass
        def header(self, number=None):
            return dict(HEADER, hash='0x' + '99' * 32) if case == 'reorg' and number is not None else HEADER
        def request(self, method, params):
            if case == 'timeout': raise TimeoutError('network deadline')
            if case == 'rpc_revert': raise RPCError(method, {'code': 3, 'message': 'execution reverted', 'data': FAILURE_SELECTOR})
            if case == 'truncated_abi': return '0x00'
            return '0x' + encode(['uint256'] * 3 + ['uint8', 'bytes4'],
                                 [100, 0, 0, 3 if case == 'invalid_stage' else 2, bytes(4)]).hex()
    class Stop:
        done = False
        def is_set(self): return self.done
        def wait(self, seconds): self.done = True
    artifact = tmp_path / 'out/CycleProbe.sol/CycleProbe.json'
    artifact.parent.mkdir(parents=True)
    artifact.write_text(json.dumps({'deployedBytecode': {'object': '0x6000'}}))
    monkeypatch.setattr(module, 'ROOT', tmp_path)
    monkeypatch.setattr(module, 'RPC', RPC)
    clock = [0]
    feedback = TransferFeedback(EXECUTOR, OWNER, clock=lambda: clock[0])
    ps = pools()
    route = Route((WETH, BAD, WETH), (ps[0].address, ps[1].address))
    assert feedback.offer(route, {p.address: p for p in ps}, 10**15, [WETH]) == 1
    feedback.run('https://example.invalid', 8453, Stop(), time.monotonic() + 2, tmp_path / 'health.json', interval=0)
    health = json.loads((tmp_path / 'health.json').read_text())
    assert health['state'] == 'retrying' and not health['recent_samples']
    assert not feedback.edges and not feedback.history and not feedback.in_flight
    assert feedback.claim_probe(route) and not feedback.deferred(route)


def test_executor_wires_repeated_transfer_failure_into_shared_feedback_before_signing(market, monkeypatch):
    rpc, config, contract, signer, directory, index, candidate, _, _ = market
    executor = Executor(rpc, config, contract, signer, directory)
    executor.gas_estimate_context_verified = False
    route = index.routes[candidate['route_key']]
    clock = [0]
    feedback = TransferFeedback(contract, signer.address, clock=lambda: clock[0])
    intermediate = index.states[route.pools[0]].quote(route.tokens[0], candidate['amount_in'])[0]
    sample = TransferSample(route.tokens[1], route.pools[0], route.pools[1], intermediate)
    result = observed_failure(sample)
    result['executor'] = contract
    feedback.record(sample, result)
    executor.transfer_feedback = feedback
    request = rpc.request
    calls = []
    def reject_call(method, params):
        if method == 'eth_call' and params[0].get('to', '').lower() == contract.lower():
            calls.append(int(params[0]['gas'], 16))
            raise RPCError(method, {'code': 3, 'message': 'execution reverted', 'data': FAILURE_SELECTOR})
        return request(method, params)
    clock[0] = 31
    try:
        monkeypatch.setattr(rpc, 'request', reject_call)
        with pytest.raises(RPCError) as caught:
            executor.execute(candidate, route, index.states, rpc.header(), time.monotonic_ns() + 2_000_000_000)
        failure = execution_failure_details(caught.value)
        assert len(set(calls)) >= 2 and failure['transfer_retry_after_seconds'] == 60
        assert feedback.deferred(route)
        assert executor.db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0] == 0
    finally:
        executor.close()


def test_successful_real_executor_overrides_a_prior_failed_call_sample(market):
    rpc, config, contract, signer, directory, index, candidate, _, _ = market
    executor = Executor(rpc, config, contract, signer, directory)
    executor.gas_estimate_context_verified = False
    route = index.routes[candidate['route_key']]
    clock = [0]
    feedback = TransferFeedback(contract, signer.address, clock=lambda: clock[0])
    sample = TransferSample(route.tokens[1], route.pools[0], route.pools[1], 100)
    observation = observed_failure(sample)
    observation['executor'] = contract
    feedback.record(sample, observation)
    executor.transfer_feedback = feedback
    clock[0] = 31
    try:
        result = executor.execute(candidate, route, index.states, rpc.header(),
                                  time.monotonic_ns() + 2_000_000_000)
        assert result['status'] == 'submitted'
        assert feedback.last_passed and not feedback.edges and not feedback.history
        assert feedback.record(sample, observation) == []
        assert feedback.claim_probe(route)
        rpc.request('evm_mine', [])
        executor.reconcile()
        assert executor.db.execute('SELECT state FROM attempts').fetchone()[0] == 'success'
    finally:
        executor.close()
