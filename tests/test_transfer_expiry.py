"""A whole-call revert cannot renew an unrelated helper observation.

These tests drive the production queue, transfer registry and quote eligibility
with a deterministic clock. They do not claim that a quote is a passed call.
"""
import pytest

from searcher.core import WETH, load_config
from searcher.engine import evaluate
from searcher.routes import Route, RouteIndex
from searcher.transfer_feedback import TransferFeedback
from test_receipt_eligibility import BAD, HEADER, pools
from test_transfer_feedback import EXECUTOR, OWNER, sample_result


def sampled_route():
    clock = [0.0]
    feedback = TransferFeedback(EXECUTOR, OWNER, clock=lambda: clock[0])
    states = {pool.address: pool for pool in pools()}
    route = Route((WETH, BAD, WETH), tuple(states)[:2])
    assert feedback.offer(route, states, 10**15, [WETH]) == 1
    sample = feedback.take()
    feedback.record(sample, sample_result(sample, funded=sample.amount * 96 // 100))
    feedback.finish_sample(sample, True)
    return clock, feedback, states, route, sample


def three_real_rejections(clock, feedback, route):
    for when, delay in [(31, 60), (92, 120), (213, 240)]:
        clock[0] = when
        assert feedback.claim_probe(route)
        assert feedback.rejected(route, {'revert_name': 'BadCallback'}) == delay


@pytest.mark.parametrize('clean_expired_edges', [False, True])
def test_generic_reverts_do_not_prevent_the_next_real_helper_sample(clean_expired_edges):
    clock, feedback, states, route, first = sampled_route()
    three_real_rejections(clock, feedback, route)
    assert all(item['until'] == 300 for item in feedback.edges.values())
    assert feedback.deferred(route)

    clock[0] = 301
    if clean_expired_edges:
        assert feedback.deferred(route) and not feedback.edges
    assert feedback.offer(route, states, 2 * 10**15, [WETH]) == 1
    fresh = feedback.take()
    assert fresh is not None and fresh.key == first.key
    assert fresh.amount == states[route.pools[0]].quote(WETH, 2 * 10**15)[0]
    assert fresh.key in feedback.in_flight


def test_normal_receipt_after_expiry_reenters_whole_call_validation_eligibility():
    clock, feedback, states, route, _ = sampled_route()
    three_real_rejections(clock, feedback, route)
    config = load_config()
    config['unsupported_transfer_tokens'] = {}
    config['policy'].update(max_quote_ms=500, sizing_mode='fixed')
    config['settlement_tokens'] = [dict(config['settlement_tokens'][0], max_input_raw=str(10**18))]
    index = RouteIndex(list(states.values()), [WETH], 3)
    index.transfer_feedback = feedback
    before = evaluate(index, config, HEADER)
    assert before['rejected']['sampled_transfer_backoff'] == 1
    assert all(BAD not in candidate['tokens'] for candidate in before['candidates'])

    clock[0] = 301
    assert feedback.offer(route, states, 10**15, [WETH]) == 1
    fresh = feedback.take()
    assert fresh is not None
    assert feedback.record(fresh, sample_result(fresh)) == []
    feedback.finish_sample(fresh, True)
    # The good helper is not a whole-executor pass. Preserve the existing
    # finite real-failure schedule, then allow a single shared revalidation.
    assert feedback.history and feedback.deferred(route)
    clock[0] = 454
    assert not feedback.deferred(route)
    after = evaluate(index, config, HEADER, set())
    assert any(candidate['route_key'] == route.key for candidate in after['candidates'])
    assert feedback.claim_probe(route)
    # This assertion is eligibility only; it is not a successful executor call.
    assert not feedback.last_passed


def test_fresh_shortfall_reuses_history_without_extending_evidence_to_retry_time():
    clock, feedback, states, route, _ = sampled_route()
    three_real_rejections(clock, feedback, route)
    history_until = next(iter(feedback.history.values()))['until']

    clock[0] = 301
    assert feedback.offer(route, states, 10**15, [WETH]) == 1
    fresh = feedback.take()
    feedback.record(fresh, sample_result(fresh, funded=fresh.amount * 96 // 100))
    feedback.finish_sample(fresh, True)
    assert all(item['repeated_reverts'] == 3 and item['until'] == 601
               and item['next_probe'] == 453 for item in feedback.edges.values())
    assert next(iter(feedback.history.values()))['until'] == history_until

    clock[0] = 542
    assert feedback.claim_probe(route)
    assert feedback.rejected(route, {'revert_name': 'BadCallback'}) == 300
    assert all(item['until'] == 601 and item['next_probe'] == 842
               for item in feedback.edges.values())
    clock[0] = 602
    assert feedback.offer(route, states, 10**15, [WETH]) == 1
    fresh = feedback.take()
    assert fresh is not None
    feedback.record(fresh, sample_result(fresh, funded=fresh.amount * 96 // 100))
    feedback.finish_sample(fresh, True)
    assert all(item['repeated_reverts'] == 4 and item['until'] == 902
               and item['next_probe'] == 842 for item in feedback.edges.values())
    # The existing retry is due before this sample expires.
    clock[0] = 902
    assert not feedback.deferred(route) and feedback.claim_probe(route)
    assert feedback.offer(route, states, 10**15, [WETH]) == 1
    assert feedback.take() is not None


def test_maximum_backoff_allows_whole_call_even_when_helper_refreshes_first():
    clock, feedback, states, route, _ = sampled_route()
    three_real_rejections(clock, feedback, route)
    clock[0] = 301
    assert feedback.offer(route, states, 10**15, [WETH]) == 1
    fresh = feedback.take()
    feedback.record(fresh, sample_result(fresh, funded=fresh.amount * 96 // 100))
    feedback.finish_sample(fresh, True)
    clock[0] = 542
    assert feedback.claim_probe(route)
    assert feedback.rejected(route, {'revert_name': 'BadCallback'}) == 300

    # Model the diagnostic worker completing before the candidate's whole-call
    # eligibility check on each expiry. Its fresh evidence may reconfirm a
    # shortfall, but must not restart the clock of an already scheduled retry.
    for sampled_at, probe_at in [(602, 843), (903, 1144), (1204, 1445)]:
        clock[0] = sampled_at
        assert feedback.offer(route, states, 10**15, [WETH]) == 1
        fresh = feedback.take()
        assert fresh is not None
        feedback.record(fresh, sample_result(fresh, funded=fresh.amount * 96 // 100))
        feedback.finish_sample(fresh, True)
        assert feedback.deferred(route)
        clock[0] = probe_at
        assert feedback.claim_probe(route), 'fresh helper must preserve the existing retry deadline'
        assert feedback.rejected(route, {'revert_name': 'BadCallback'}) == 300


@pytest.mark.parametrize('pass_time', [150, 301])
def test_real_executor_success_overrides_active_or_expired_sample_history(pass_time):
    clock, feedback, states, route, sample = sampled_route()
    for when, delay in [(31, 60), (92, 120)]:
        clock[0] = when
        assert feedback.claim_probe(route)
        assert feedback.rejected(route, {'revert_name': 'BadCallback'}) == delay
    clock[0] = pass_time
    feedback.passed(route)
    assert not feedback.edges and not feedback.history
    assert feedback.record(sample, sample_result(sample, funded=sample.amount * 96 // 100)) == []
    assert not feedback.deferred(route) and feedback.claim_probe(route)
    assert feedback.priority(route) == 0

    clock[0] += 301
    assert feedback.offer(route, states, 10**15, [WETH]) == 1
    fresh = feedback.take()
    assert fresh is not None
    feedback.record(fresh, sample_result(fresh, funded=fresh.amount * 96 // 100))
    assert all(item['repeated_reverts'] == 0 for item in feedback.edges.values())
