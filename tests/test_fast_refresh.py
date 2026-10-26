import copy
import time

import pytest

from searcher.core import WETH, Reject, load_config
from searcher.costs import cost_budget, quote_profit_floor
from searcher.engine import evaluate
from searcher.routes import Route, RouteIndex, optimize
from searcher.statefeed import StateFrame
from searcher.candidate_refresh import refresh_candidate
from test_receipt_eligibility import pools, HEADER


def market(monkeypatch, *, age_ms=0, basefee=5_000_000):
    config = load_config()
    config['policy'].update(max_quote_ms=100, sizing_mode='fixed')
    config['settlement_tokens'] = [dict(config['settlement_tokens'][0], max_input_raw=str(10**20))]
    ps = pools()[2:]
    index = RouteIndex(ps, [WETH], 2)
    previous = evaluate(index, config, HEADER)['candidates'][0]
    route = index.routes[previous['route_key']]
    fresh = copy.deepcopy(ps)
    fresh[0].reserve1 = 3*10**20
    header = dict(HEADER, number='0x65', hash='0x'+'22'*32, baseFeePerGas=hex(basefee))
    class Feed:
        def __init__(self, rpc, local, saved): self.pools = saved['pools']
        def poll(self, **kwargs):
            now = time.monotonic_ns()
            return StateFrame(header, header, [p.json() for p in fresh],
                              now-int(age_ms*1e6), now, time.time(), 102, 204)
    monkeypatch.setattr('searcher.candidate_refresh.StateFeed', Feed)
    return config, index, route, previous, fresh


def test_fast_path_reprices_output_and_current_fees_without_full_search(monkeypatch):
    config, index, route, previous, fresh = market(monkeypatch)
    previous['amount_out'] = 1 << 200  # An old or incorrect estimate grants nothing.
    def forbidden(*args, **kwargs): raise AssertionError('full search on profitable reused amount')
    monkeypatch.setattr('searcher.candidate_refresh.evaluate', forbidden)
    candidate, states, header, deadline, evidence = refresh_candidate(
        None, config, route, index.states, previous_candidate=previous)
    output, crossings = route.quote(states, previous['amount_in'])
    cost = cost_budget(config['policy'], WETH, 5_000_000, len(route.pools), crossings)
    assert candidate['amount_in'] == previous['amount_in']
    assert candidate['amount_out'] == output != previous['amount_out']
    assert candidate['net_after_modelled_cost'] == output-previous['amount_in']-cost.settlement_cost_raw
    assert candidate['cost_budget']['max_fee_per_gas'] == cost.max_fee_per_gas
    assert candidate['sizing_mode'] == 'previous_amount_repriced'
    assert candidate['minimum_gross_surplus'] >= cost.settlement_cost_raw+quote_profit_floor(config,config['settlement_tokens'][0],cost.settlement_cost_raw)
    assert all(p.block == 101 and p.block_hash == evidence['frame']['observation_id'] for p in states.values())
    assert time.monotonic_ns() < deadline


def test_smaller_profitable_amount_is_found_when_previous_size_loses(monkeypatch):
    config, index, route, previous, fresh = market(monkeypatch)
    previous['amount_in'] = 10**20
    assert route.quote({p.address:p for p in fresh}, previous['amount_in'])[0] < previous['amount_in']
    candidate, states, _, _, evidence = refresh_candidate(None, config, route, index.states,
                                                          previous_candidate=previous)
    assert candidate and candidate['amount_in'] < previous['amount_in']
    assert candidate['net_after_modelled_cost'] > 0
    assert candidate['amount_out'] == route.quote(states, candidate['amount_in'])[0]


@pytest.mark.parametrize('reason', ['price', 'profit_floor', 'fee_cap', 'amount_cap'])
def test_old_candidate_never_bypasses_new_price_and_policy(monkeypatch, reason):
    config, index, route, previous, fresh = market(monkeypatch)
    if reason == 'price': fresh[0].reserve1 = fresh[1].reserve1
    elif reason == 'profit_floor': config['settlement_tokens'][0]['min_net_raw'] = str(10**30)
    elif reason == 'fee_cap': config['policy']['max_total_fee_wei'] = '1'
    elif reason == 'amount_cap': config['settlement_tokens'][0]['max_input_raw'] = '1'
    candidate, *_ = refresh_candidate(None, config, route, index.states, previous_candidate=previous)
    assert candidate is None


def test_expired_snapshot_never_enters_sizing_or_quote(monkeypatch):
    config, index, route, previous, _ = market(monkeypatch, age_ms=3000)
    def forbidden(*args, **kwargs): raise AssertionError('expired snapshot used')
    monkeypatch.setattr(Route, 'quote', forbidden)
    monkeypatch.setattr('searcher.candidate_refresh.evaluate', forbidden)
    candidate, _, _, deadline, evidence = refresh_candidate(None, config, route, index.states,
                                                            previous_candidate=previous)
    assert candidate is None and time.monotonic_ns() > deadline
    assert evidence['decision']['search_time_limited']


def test_candidate_for_another_route_is_rejected(monkeypatch):
    config, index, route, previous, _ = market(monkeypatch)
    previous['route_key'] = 'wrong'
    with pytest.raises(Reject, match='route mismatch'):
        refresh_candidate(None, config, route, index.states, previous_candidate=previous)


def test_partial_sizing_keeps_only_actually_quoted_result(monkeypatch):
    class Curve:
        def quote(self, states, amount): return amount*2, 0
    tick = iter(range(100))
    monkeypatch.setattr('searcher.routes.time.monotonic_ns', lambda: next(tick))
    found = optimize(Curve(), {}, 1000, lambda _: 5, deadline_ns=4, initial_amounts=(150,))
    assert found['complete'] and found['sizing_time_limited']
    assert found['amount_in'] == 150 and found['amount_out'] == 300
    assert found['net_after_modelled_cost'] == 145
    assert not found['global_optimum_proven']


def test_expired_search_returns_unknown_without_quoting(monkeypatch):
    class Curve:
        def quote(self, states, amount): raise AssertionError('expired search')
    monkeypatch.setattr('searcher.routes.time.monotonic_ns', lambda: 100)
    found = optimize(Curve(), {}, 1000, deadline_ns=99)
    assert not found['complete'] and found['sizing_time_limited']


def test_timed_out_refresh_reports_unknown_instead_of_unprofitable(monkeypatch):
    config, index, route, previous, fresh = market(monkeypatch, age_ms=1200)
    fresh[0].reserve1 = fresh[1].reserve1
    def forbidden(*args, **kwargs): raise AssertionError('execution reserve spent searching')
    monkeypatch.setattr('searcher.candidate_refresh.evaluate', forbidden)
    candidate, _, _, deadline, evidence = refresh_candidate(None, config, route, index.states,
                                                            previous_candidate=previous)
    assert candidate is None and evidence['decision']['search_time_limited']
    assert time.monotonic_ns() < deadline
