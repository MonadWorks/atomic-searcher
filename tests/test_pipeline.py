import copy
import time

import pytest
from eth_abi import encode

from searcher.core import Reject, load_config
from searcher.feed import Frame, validate_frame
from searcher.pipeline import PendingStrategy
from searcher.events import V3_SWAP
from searcher.math import sqrt_at_tick
from searcher.pools import Pool
from searcher.statefeed import StateFrame, StateFeed, STATE

A, B, C = ["0x"+f"{n:040x}" for n in (1, 2, 3)]
H0, H1, H2 = ["0x"+f"{n:064x}" for n in (10, 11, 12)]
ZERO = "0x"+"00"*32
TX = "0x"+"22"*32


def header(n, h, parent, txs=()):
    return {"number": hex(n), "hash": h, "parentHash": parent, "transactions": list(txs),
            "timestamp": hex(100+n*2), "baseFeePerGas": "0x1"}


def receipt(n=2, h=H1, tx=TX, index=0):
    log = {"address": A, "blockHash": h, "blockNumber": hex(n), "transactionHash": tx,
           "transactionIndex": hex(index), "logIndex": hex(index), "topics": [V3_SWAP],
           "data": "0x"+encode(["int256", "int256", "uint160", "uint128", "int24"],
                                     [10, -9, sqrt_at_tick(10), 10**12, 10]).hex()}
    return {"transactionHash": tx, "transactionIndex": hex(index), "blockNumber": hex(n),
            "blockHash": h, "logs": [log]}


def saved():
    ps = [Pool(a, C, "v3", A, B, 1, 500, sqrt_at_tick(0), 0, 10**12,
               {-1: 0, 0: 0, 1: 0}, {}, 1, H0) for a in (A, B)]
    return {"header": header(1, H0, ZERO), "pools": [p.json() for p in ps]}


class RPCStub:
    def header(self, n):
        return header(n, H0 if n == 1 else H1, ZERO if n == 1 else H0, [TX] if n == 2 else [])
    def request(self, method, params): return [receipt()]
    def views(self, calls, block): return []


@pytest.mark.parametrize("mutation", ["wrong_hash", "missing", "wrong_index", "wrong_log_hash", "replaced"])
def test_reject_incoherent_prefix(mutation):
    h, base, r = header(2, H1, H0, [TX]), header(1, H0, ZERO), receipt()
    after = copy.deepcopy(h)
    if mutation == "wrong_hash": r["blockHash"] = H2
    elif mutation == "missing": h["transactions"].append(H2)
    elif mutation == "wrong_index": r["transactionIndex"] = "0x1"
    elif mutation == "wrong_log_hash": r["logs"][0]["blockHash"] = H2
    else: after["transactions"] = [H2]
    with pytest.raises(Reject): validate_frame(h, base, [r], after)


def test_pending_tag_alone_does_not_prove_early_data():
    h, r = header(2, ZERO, H0, [TX]), receipt(h=ZERO)
    f = Frame(h, header(2, H1, H0, [TX]), [r], 1, 2, 3)
    assert not f.preconfirmed
    assert Frame(h, header(1, H0, ZERO), [r], 1, 2, 3).preconfirmed


def test_cross_block_changes_and_complete_batch_drive_search(monkeypatch):
    calls = []
    def evaluate(index, config, h, changed):
        calls.append((index.states[A].tick, changed))
        return {"candidates": [], "computation_ms": 0}
    monkeypatch.setattr("searcher.pipeline.evaluate", evaluate)
    c = load_config(); c["settlement_tokens"] = [{"address": A}]
    s = PendingStrategy(RPCStub(), c, saved())
    now = time.monotonic_ns()
    f = Frame(header(3, ZERO, H1), header(2, H1, H0, [TX]), [], now-5_000_000_000, now, time.time())
    report = s.on_frame(f)
    assert calls == [(10, None)]  # Re-evaluate changes in the sealed tail.
    assert report["decision_age_ms"] >= 5000


def test_same_observation_applies_competitor_before_decision(monkeypatch):
    seen = []
    def evaluate(index, config, h, changed):
        seen.append(index.states[A].tick)
        return {"candidates": [], "computation_ms": 0}
    monkeypatch.setattr("searcher.pipeline.evaluate", evaluate)
    c = load_config(); c["settlement_tokens"] = [{"address": A}]
    s = PendingStrategy(RPCStub(), c, saved())
    first, competitor = receipt(h=ZERO), receipt(h=ZERO, tx=H2, index=1)
    competitor["logs"][0]["data"] = "0x"+encode(["int256", "int256", "uint160", "uint128", "int24"],
                                                 [10, -9, sqrt_at_tick(0), 10**12, 0]).hex()
    now = time.monotonic_ns()
    f = Frame(header(2, ZERO, H0, [TX, H2]), header(1, H0, ZERO), [first, competitor], now, now, time.time())
    s.on_frame(f)
    assert seen == [0]


def test_atomic_snapshot_same_identity_revisits_deferred_routes_without_invented_height(monkeypatch):
    seen = []
    def evaluate(index, config, h, changed):
        seen.append(changed)
        return {"candidates": [], "computation_ms": 0, "routes_deferred": 1}
    monkeypatch.setattr("searcher.pipeline.evaluate", evaluate)
    c = load_config(); c["settlement_tokens"] = [{"address": A}]
    s = PendingStrategy(RPCStub(), c, saved())
    now = time.monotonic_ns()
    f = StateFrame(header(2, H1, H0), header(2, H1, H0), saved()["pools"], now, now, time.time(), 0, 100)
    first, second = s.on_frame(f), s.on_frame(f)
    assert len(seen) == 2 and second["preconfirmed"] is None
    assert first["anchor_block"] == 2 and first["context_block"] == 0
    assert not first["state_height_proven"]


def test_deferred_routes_survive_new_changed_pool(monkeypatch):
    from searcher.engine import evaluate
    from searcher.routes import Route
    from types import SimpleNamespace
    routes = [Route((A, B, A), (str(i), str(i+3))) for i in range(3)]
    index = SimpleNamespace(routes={r.key:r for r in routes}, states={}, affected=lambda changed: [routes[0]])
    seen = []
    def profitable(route, states): seen.append(route.key); return False
    monkeypatch.setattr("searcher.engine.marginal_profitable", profitable)
    clock = iter(range(0, 100_000_000, 1_000_000))
    monkeypatch.setattr("searcher.engine.time.monotonic_ns", lambda: next(clock))
    c = load_config(); c["policy"]["max_quote_ms"] = 1
    evaluate(index, c, header(2, H1, H0))
    evaluate(index, c, header(2, H1, H0), {"busy-pool"})
    evaluate(index, c, header(2, H1, H0), {"busy-pool"})
    assert seen == [r.key for r in routes]


def clock_config():
    config = load_config()
    config['policy']['snapshot_encoding'] = 'abi'  # ClockRPC supplies legacy ABI.
    config['settlement_tokens'] = [{'address':A}]
    return config


class ClockRPC:
    """A pending overlay whose native clock lags its actual pool state."""
    def __init__(self, heads, ignore_override=False):
        self.heads, self.ignore_override, self.calls = list(heads), ignore_override, []

    def header(self): return header(2, H1, H0)

    def batch(self, calls):
        if calls[0][0] == 'eth_getBlockByNumber':
            return self.header(), header(3, ZERO, H1)
        self.calls.append(calls)
        params = calls[0][1]
        if params[1] == 'pending':
            context = params[3]
            block, stamp = int(context['number'], 16), int(context['time'], 16)
            if self.ignore_override: block, stamp = 0, 100
        else:
            assert len(params) == 3  # Historical reads retain their own clock.
            block, stamp = int(params[1], 16), 104
        fee = 1060 if stamp >= 106 else 100
        rows = [(a, True, sqrt_at_tick(0), 0, 10**12, fee, [-1, 0, 1], [0, 0, 0], []) for a in (A, B)]
        raw = '0x'+encode([STATE+'[]', 'uint256', 'uint256'], [rows, block, stamp]).hex()
        head = self.heads.pop(0)
        if params[1] != 'pending': return raw, head
        return raw, head, header(int(head['number'], 16)+1, ZERO, head['hash'])


def test_pending_quote_uses_execution_clock_for_dynamic_fee():
    rpc = ClockRPC([header(2, H1, H0)])
    frame = StateFeed(rpc, clock_config(), saved()).poll()
    assert (frame.context_block, frame.context_time) == (3, 106)
    assert all(p['fee'] == 1060 for p in frame.snapshot_pools)
    assert rpc.calls[0][0][1][3]['blockHash'] == {'2': H1}
    assert frame.header['hash'] == H1


def test_pending_quote_rejects_node_ignoring_execution_clock():
    rpc = ClockRPC([header(2, H1, H0)], ignore_override=True)
    with pytest.raises(Reject, match='clock mismatch'):
        StateFeed(rpc, clock_config(), saved()).poll()


@pytest.mark.parametrize('same_height_reorg', [False, True])
def test_quote_discards_changed_anchor_and_rereads_with_new_clock(same_height_reorg):
    changed = header(2 if same_height_reorg else 3, H2, H0 if same_height_reorg else H1)
    rpc = ClockRPC([changed, changed])
    frame = StateFeed(rpc, clock_config(), saved()).poll()
    assert len(rpc.calls) == 2
    assert frame.context_block == int(changed['number'], 16)+1
    assert rpc.calls[-1][0][1][3]['blockHash'] == {str(int(changed['number'], 16)): H2}
    assert frame.header['hash'] == H2


def test_quote_bounds_anchor_retries_and_keeps_historical_context():
    rpc = ClockRPC([header(3, H2, H1), header(4, TX, H2)])
    with pytest.raises(Reject, match='anchor changed'):
        StateFeed(rpc, clock_config(), saved()).poll()
    rpc = ClockRPC([header(2, H1, H0)])
    frame = StateFeed(rpc, clock_config(), saved()).poll(block=2)
    assert (frame.context_block, frame.context_time) == (2, 104)
    assert all(p['fee'] == 100 for p in frame.snapshot_pools)


def test_quote_rejects_same_clock_with_inconsistent_pending_parent():
    class ChangedParentRPC(ClockRPC):
        def batch(self, calls):
            values = super().batch(calls)
            if calls[0][0] == 'eth_call':
                values = list(values)
                values[2] = {**values[2], 'parentHash': H2}
            return values
    rpc = ChangedParentRPC([header(2, H1, H0), header(2, H1, H0)])
    with pytest.raises(Reject, match='pending parent differs'):
        StateFeed(rpc, clock_config(), saved()).poll()
