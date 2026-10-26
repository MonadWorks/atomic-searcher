"""Independent runtime evidence: page selection is not route evaluation."""
import copy
import time

import pytest

from searcher.core import load_config
from searcher.pipeline import PendingStrategy
from searcher.statefeed import StateFrame
from test_receipt_eligibility import HEADER, pools


def snapshot_case(*, both_groups=False, coactive=True):
    config = load_config()
    config['policy'].update(max_quote_ms=1000, sizing_mode='fixed')
    config['settlement_tokens'] = [dict(config['settlement_tokens'][0],
                                       max_input_raw=str(10**18))]
    ps = pools() if both_groups else pools()[2:]
    rows = [p.json() for p in ps]
    groups = [tuple(p.address for p in ps[i:i+2]) for i in range(0, len(ps), 2)]
    target = groups[-1]
    strategy = PendingStrategy(None, config, {'header': HEADER, 'pools': rows})
    now = time.monotonic_ns()
    frame = StateFrame(HEADER, HEADER, rows, now, now, time.time(), 100, 200,
        transport={'page_id': 1, 'route_groups': groups,
                   'coactive_priority_groups': [target] if coactive else []})
    return strategy, frame, target


def test_fully_considered_coactive_group_counts_both_directions_without_claiming_execution():
    strategy, frame, target = snapshot_case()
    report = strategy.on_snapshot(frame)
    assert report['coactive_route_progress'] == [
        {'pools': list(target), 'directions_in_page': 2, 'directions_considered': 2}]
    # The reverse route is considered and discarded before sizing; this field
    # records evaluation reach, not profitable quotes or whole-call validation.
    assert report['routes_sized'] == 1
    assert len(report['candidates']) == 1
    assert report['candidates'][0]['status'] == 'quoted_only'
    assert not report['candidates'][0]['placement_proven']


@pytest.mark.parametrize('whole_page_budget_exhausted', [False, True])
def test_budget_that_prevents_first_quote_is_excluded_from_considered(monkeypatch, whole_page_budget_exhausted):
    strategy, frame, target = snapshot_case()
    clock = [time.monotonic_ns()]
    monkeypatch.setattr('searcher.engine.time.monotonic_ns', lambda: clock[0])
    def unstarted(*args, **kwargs):
        assert kwargs['deadline_ns'] > 0
        assert not kwargs['continuation'].get('quotes')
        if whole_page_budget_exhausted:
            clock[0] += 2_000_000_000
        return {'complete': False, 'sizing_time_limited': True}
    monkeypatch.setattr('searcher.engine.optimize', unstarted)
    report = strategy.on_snapshot(frame)
    assert report['rejected']['sizing_budget_exhausted'] == 1
    assert len(strategy.index.last_evaluated_keys) == (1 if whole_page_budget_exhausted else 2)
    assert len(strategy.index.unstarted_keys) == 1
    assert report['coactive_route_progress'] == [
        {'pools': list(target), 'directions_in_page': 2,
         'directions_considered': 0 if whole_page_budget_exhausted else 1}]
    assert not report['candidates']


def test_other_groups_read_and_considered_on_same_page_do_not_enter_coactive_evidence():
    strategy, frame, target = snapshot_case(both_groups=True)
    report = strategy.on_snapshot(frame)
    assert report['routes_considered'] == 4
    assert report['rejected']['unsupported_token_transfer'] == 2
    assert report['coactive_route_progress'] == [
        {'pools': list(target), 'directions_in_page': 2, 'directions_considered': 2}]
    assert tuple(frame.transport['route_groups'][0]) != target


def test_reporting_coactivity_does_not_change_candidates_or_execution_eligibility():
    baseline, plain, _ = snapshot_case(both_groups=True, coactive=False)
    observed, marked, _ = snapshot_case(both_groups=True, coactive=True)
    plain_before, marked_before = copy.deepcopy(plain.snapshot_pools), copy.deepcopy(marked.snapshot_pools)
    first, second = baseline.on_snapshot(plain), observed.on_snapshot(marked)
    assert first['candidates'] and first['candidates'] == second['candidates']
    for field in ('rejected', 'routes_considered', 'routes_sized', 'routes_deferred',
                  'deep_read_routes', 'deep_read_requests', 'continuation_requests',
                  'page_scan_complete', 'page_advance_ready'):
        assert first[field] == second[field]
    assert first['coactive_route_progress'] == []
    assert second['coactive_route_progress']
    assert plain.snapshot_pools == plain_before and marked.snapshot_pools == marked_before
