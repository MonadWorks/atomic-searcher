"""Page selection must not acknowledge ordinary directions never evaluated."""
import copy
import time
from collections import Counter, deque

import pytest

from searcher.core import Reject, load_config
from searcher.engine import evaluate
from searcher.pipeline import PendingStrategy
from searcher.statefeed import StateFrame
from page_ownership_evidence import saved_page
from test_searcher import A, pool
from test_receipt_eligibility import HEADER


def frame(rows, groups, page=1, requested=()):
    now = time.monotonic_ns()
    return StateFrame(HEADER, HEADER, rows, now, now, time.time(), 100, 200,
        transport={'page_id':page, 'route_groups':groups, 'requested_groups':list(requested)})


def case(groups=3):
    config = load_config(); config['settlement_tokens'] = [{'address':A}]
    groups = [tuple('0x'+f'{100+i*2+j:040x}' for j in range(2)) for i in range(groups)]
    rows = [pool(address).json() for group in groups for address in group]
    return config, rows, groups, PendingStrategy(None, config, {'header':HEADER, 'pools':rows})


def one_target(monkeypatch, *, unstarted=False):
    seen = []
    def one(index, config, header, changed):
        keys = []
        if index.work_queue:
            key = index.work_queue.popleft(); index.queued_routes.remove(key)
            keys.append(key); seen.append(key)
            # Retained profitable candidates cannot steal the unfinished page.
            index.work_queue.append(key); index.queued_routes.add(key)
        index.last_evaluated_keys = keys
        index.unstarted_keys = set(keys) if unstarted else set()
        return {'candidates':[], 'routes_considered':len(keys)}
    monkeypatch.setattr('searcher.pipeline.evaluate', one)
    return seen


@pytest.mark.parametrize('number', [517,559])
def test_saved_live_page_keeps_all_ordinary_work_until_real_evaluation(number, monkeypatch):
    evidence = saved_page(number)
    config = load_config()
    strategy = PendingStrategy(None, config,
        {'header':evidence['latest'], 'pools':evidence['snapshot_pools']})
    cap = evidence['source']['routes_considered']
    # A deterministic dispatch ceiling reproduces the observed incomplete
    # slice. Pricing/rejections inside every slice use the actual evaluator and
    # recorded public pool state, with no RPC or historical profit assertion.
    def bounded(index, config, header, changed):
        keys = list(index.work_queue); carry = keys[cap:]
        index.work_queue = deque(keys[:cap]); index.queued_routes = set(index.work_queue)
        report = evaluate(index, config, header, set())
        for key in carry:
            if key not in index.queued_routes:
                index.work_queue.append(key); index.queued_routes.add(key)
        report['routes_deferred'] = len(index.work_queue)
        return report
    monkeypatch.setattr('searcher.pipeline.evaluate', bounded)
    visited = Counter()
    for attempt in range(30):
        now = time.monotonic_ns()
        sample = StateFrame(evidence['header'], evidence['latest'], evidence['snapshot_pools'],
            now, now, time.time(), evidence['context_block'], evidence['context_time'],
            transport=evidence['transport'])
        report = strategy.on_snapshot(sample)
        considered = set(strategy.index.last_evaluated_keys)-strategy.index.unstarted_keys
        visited.update(considered)
        assert max(visited.values(), default=0) <= 1, 'completed targets consumed the same page again'
        if attempt == 0:
            assert len(strategy.index.routes) == evidence['source']['routes_considered']+evidence['source']['routes_deferred']
            assert report['routes_considered'] <= cap
            assert not report['page_scan_complete'] and not report['page_advance_ready']
            assert report['page_unfinished_ordinary_routes'] > 0
            assert not report['continuation_requests']
        if report['page_advance_ready']: break
    assert report['page_advance_ready'] and report['page_unfinished_ordinary_routes'] == 0
    carried = {key for row in report['continuation_requests'] for key in row['route_keys']}
    assert set(visited) | carried == set(strategy.index.routes)
    assert not (set(visited) & carried)
    requests = {tuple(sorted(row['pools'])) for row in evidence['transport']['requested_groups']}
    assert all(tuple(row['pools']) in requests for row in report['continuation_requests'])


def test_price_changes_cannot_revisit_completed_ordinary_targets_or_fill_depth_queue(monkeypatch):
    _, rows, groups, strategy = case()
    seen = one_target(monkeypatch)
    for attempt in range(6):
        changed = copy.deepcopy(rows); changed[0]['liquidity'] += attempt
        report = strategy.on_snapshot(frame(changed, groups))
        assert report['page_considered_routes'] == attempt+1
        assert report['page_newly_considered_routes'] == 1
        assert not report['continuation_requests']
        assert report['page_advance_ready'] == (attempt == 5)
    assert len(set(seen)) == 6
    complete = strategy.on_snapshot(frame(rows, groups))
    assert complete['page_scan_complete'] and complete['page_newly_considered_routes'] == 0
    assert len(seen) == 6
    strategy.on_snapshot(frame(rows, groups, page=2))
    assert len(seen) == 7, 'the next genuine page visit must reconsider fresh prices'


def test_same_page_topology_addition_is_owned_and_withdrawal_is_visible(monkeypatch):
    _, rows, groups, strategy = case(2)
    one_target(monkeypatch)
    first = strategy.on_snapshot(frame(rows[:2], groups[:1]))
    assert first['page_total_routes'] == 2
    second = strategy.on_snapshot(frame(rows, groups))
    assert second['page_total_routes'] == 4 and second['page_unfinished_routes'] == 2
    # Removing the still-unvisited group is explicit withdrawal, not coverage.
    final = strategy.on_snapshot(frame(rows[:2], groups[:1]))
    assert final['page_withdrawn_routes'] == 2
    assert final['page_total_routes'] == final['page_considered_routes'] == 2


def test_same_route_key_with_changed_pool_metadata_reopens_ownership(monkeypatch):
    _, rows, groups, strategy = case(1)
    seen = one_target(monkeypatch)
    strategy.on_snapshot(frame(rows, groups)); strategy.on_snapshot(frame(rows, groups))
    altered = copy.deepcopy(rows); altered[0]['factory'] = '0x'+'fa'*20
    report = strategy.on_snapshot(frame(altered, groups))
    assert len(seen) == 3 and report['page_unfinished_routes'] == 1
    assert not report['page_advance_ready']


def test_same_addresses_with_changed_intermediate_token_rebuild_routes(monkeypatch):
    _, rows, groups, strategy = case(1)
    one_target(monkeypatch)
    strategy.on_snapshot(frame(rows, groups)); strategy.on_snapshot(frame(rows, groups))
    token = '0x'+'ab'*20
    altered = copy.deepcopy(rows)
    for item in altered: item['token1'] = token
    report = strategy.on_snapshot(frame(altered, groups))
    assert all(route.tokens == (A,token,A) for route in strategy.index.routes.values())
    assert report['page_total_routes'] == 2 and report['page_unfinished_routes'] == 1
    assert not report['page_advance_ready']


def test_unpaged_hot_snapshot_reports_partial_actual_coverage(monkeypatch):
    _, rows, groups, strategy = case(2)
    one_target(monkeypatch)
    report = strategy.on_snapshot(frame(rows, groups, page=None))
    assert report['page_total_routes'] == 4 and report['page_considered_routes'] == 1
    assert report['page_unfinished_routes'] == 3 and not report['page_scan_complete']
    assert report['page_coverage_scope'] == 'this unpaged observation only'
    assert not report['continuation_requests']


def test_zero_budget_is_rejected_and_unstarted_stub_never_claims_progress(monkeypatch):
    config, rows, groups, strategy = case(1)
    config['policy']['max_quote_ms'] = 0
    with pytest.raises(Reject, match='quote budget must be positive'):
        PendingStrategy(None, config, {'header':HEADER, 'pools':rows})
    one_target(monkeypatch, unstarted=True)
    for attempt in range(5):
        report = strategy.on_snapshot(frame(rows, groups))
        assert report['page_newly_considered_routes'] == report['page_considered_routes'] == 0
        assert report['page_no_progress_passes'] == attempt+1
        assert not report['page_scan_complete'] and not report['page_advance_ready']
        assert not report['continuation_requests']


def test_small_positive_real_budget_makes_a_concrete_judgment(monkeypatch):
    config, rows, groups, _ = case(1)
    config['policy']['max_quote_ms'] = 1
    strategy = PendingStrategy(None, config, {'header':HEADER,'pools':rows})
    # Equal-price, fee-charging real pools have a concrete marginal rejection.
    clock = [time.monotonic_ns()]
    import searcher.engine as engine
    original = engine.marginal_profitable
    def charged(route, states):
        value = original(route, states); clock[0] += 1_000_000
        return value
    monkeypatch.setattr(engine, 'marginal_profitable', charged)
    monkeypatch.setattr(engine.time, 'monotonic_ns', lambda:clock[0])
    first = strategy.on_snapshot(frame(rows, groups))
    second = strategy.on_snapshot(frame(rows, groups))
    assert first['page_newly_considered_routes'] == second['page_newly_considered_routes'] == 1
    assert not first['page_advance_ready'] and second['page_scan_complete']
