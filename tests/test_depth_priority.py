"""A feedback cooldown must not strand an already-read depth target."""
import copy

from searcher import engine
from searcher.core import WETH, load_config
from searcher.pipeline import PendingStrategy
from searcher.statefeed import StateFrame
from test_receipt_eligibility import HEADER, pools


def test_transfer_priority_preserves_unvisited_depth_target_under_busy_page(monkeypatch):
    config = load_config()
    config['policy']['sizing_mode'] = 'fixed'
    config['settlement_tokens'] = [
        next(token for token in config['settlement_tokens'] if token['address'] == WETH)
    ]
    # Four ordinary groups provide enough work to exhaust every page slice.
    # The fifth has already been read deeply and needs its cooldown evaluated.
    states, groups = [], []
    for group_number in range(5):
        group = []
        for side, original in enumerate(pools()[2:]):
            state = copy.deepcopy(original)
            state.address = '0x' + f'{100 + group_number * 2 + side:040x}'
            states.append(state.json())
            group.append(state.address)
        groups.append(tuple(group))
    target = set(groups[-1])
    strategy = PendingStrategy(None, config, {'header': HEADER, 'pools': states})

    class Feedback:
        def priority(self, route):
            return int(set(route.pools) == target)

        def deferred(self, route):
            return set(route.pools) == target

    strategy.transfer_feedback = Feedback()
    clock = [1]
    monkeypatch.setattr(engine.time, 'monotonic_ns', lambda: clock[0])
    monkeypatch.setattr(engine, 'marginal_profitable', lambda *args: True)
    monkeypatch.setattr(engine, 'gross_upper_bound', lambda *args, **kwargs: 10**100)

    def fill_slice(route, states, maximum, fee, **kwargs):
        # Respect the actual per-route deadline; do not manufacture an overrun.
        clock[0] = kwargs['deadline_ns']
        kwargs['continuation']['quotes'] = {1: None}
        return {'complete': False, 'sizing_time_limited': True}

    monkeypatch.setattr(engine, 'optimize', fill_slice)
    reports, first_visited = [], set()
    for attempt in range(6):
        frame = StateFrame(
            HEADER, HEADER, states, clock[0], clock[0], 0, 100, 200,
            transport={
                'page_id': 1,
                'route_groups': groups,
                'deep_pools': list(groups[-1]),
                'requested_groups': [{'pools': groups[-1], 'deep': True, 'since': 1}],
            },
        )
        reports.append(strategy.on_snapshot(frame))
        if attempt == 0:
            first_visited = {
                key for key in strategy.index.last_evaluated_keys
                if set(strategy.index.routes[key].pools) == target
            }

    assert len(first_visited) == 2, 'ordinary sizing starved both requested directions'
    assert reports[0]['rejected']['sampled_transfer_backoff'] == 2
    # The requested target is delivered first, but that cannot claim coverage
    # of ordinary directions whose bounded sizing slices are still waiting.
    assert all(report['page_requested_complete'] for report in reports)
    assert not reports[0]['page_scan_complete'] and reports[0]['page_unfinished_ordinary_routes']>0
    assert reports[-1]['page_scan_complete']
    assert sum(report['page_newly_considered_routes'] for report in reports)==len(strategy.index.routes)
    assert all(not report['continuation_requests'] for report in reports)
