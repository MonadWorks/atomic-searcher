import threading
import time
from types import SimpleNamespace

from searcher.core import USDC, WETH
from searcher.route_preparation import RouteBook


def book(count=3):
    paths = [((f'0x{i + 1:040x}', USDC, WETH, 1),) for i in range(count)]
    universe = SimpleNamespace(lock=threading.RLock(),
                               graph=SimpleNamespace(direct=lambda a, b: paths))
    return RouteBook(universe), paths


def test_failed_quotes_do_not_fill_execution_slots():
    rb, paths = book()
    for path in paths:
        rb.record(path, 100, 0)
    assert rb.ranked(USDC, WETH, 100, 2) == []


def test_single_slot_explores_unknown_route_instead_of_known_failure():
    rb, paths = book()
    rb.record(paths[0], 100, 0)
    observed = {rb.ranked(USDC, WETH, 100, 1)[0] for _ in range(4)}
    assert observed == set(paths[1:])


def test_unpriced_exploration_keeps_best_measured_route():
    rb, paths = book(4)
    rb.record(paths[0], 100, 0)
    rb.record(paths[1], 100, 110)
    rb.record(paths[2], 100, 120)
    selected = rb.ranked(USDC, WETH, 100, 2)
    assert selected == [paths[2], paths[3]]
    assert rb.ranked(USDC, WETH, 100, 1) == [paths[2]]


def test_fresh_success_reopens_previously_failed_route_immediately():
    rb, paths = book(1)
    rb.record(paths[0], 100, 0)
    assert rb.ranked(USDC, WETH, 100) == []
    rb.record(paths[0], 100, 120)
    assert rb.ranked(USDC, WETH, 100) == paths


def test_expired_failure_returns_to_exploration():
    rb, paths = book(1)
    rb.record(paths[0], 100, 0, now=time.monotonic() - 46)
    assert rb.ranked(USDC, WETH, 100) == paths


def test_failure_at_one_size_does_not_rule_out_smaller_or_larger_trades():
    rb, paths = book(1)
    rb.record(paths[0], 100, 0)
    # A zero may be insufficient depth, rounding, or a failed bounded quote.
    # It cannot be interpolated into a measurement at another input amount.
    assert rb.estimate(paths[0], 50) is None
    assert rb.estimate(paths[0], 200) is None
    assert rb.ranked(USDC, WETH, 50) == paths
    assert rb.ranked(USDC, WETH, 200) == paths


def test_identity_conversion_needs_no_market_quote():
    rb, _ = book()
    assert rb.ranked(USDC, USDC, 100) == [()]
