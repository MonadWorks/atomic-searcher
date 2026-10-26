"""Restored public manager state, synthetic prior metadata and local account.

This proves counterpart discovery -> actual Reader -> Executor integration.
It deliberately makes no claim about historical first observation or winning.
"""
import time
from searcher.core import USDC, number
from searcher.execution import Executor
from searcher.v4 import Reader, Route, evaluate
from searcher.v4_market import Key
from searcher.v4_peers import PeerFinder
from searcher.v4_runtime import Inventory
from test_v4_integration import v4market, event
from test_execution import market


def test_verified_counterpart_state_reaches_full_execution_preflight(v4market):
    rpc, config, cl, contract, signer, directory, prior, candidate, route, send = v4market
    inv = Inventory(directory/'single-known-pool.sqlite')
    head = rpc.header()
    # Only one pool in the target pair. A synthetic unrelated key contributes
    # the other fee/spacing template; that prior knowledge is explicit.
    template = Key(USDC, '0xff00000000000000000000000000000000000001', route.buy.fee, route.buy.spacing)
    initial = [route.sell, template]
    inv.accept([event(k, head) for k in initial], number(head['number']), number(head['number']),
               {number(head['number']): head['hash']})
    assert not inv.registry.pairs(USDC)
    touched = PeerFinder().step(inv, rpc, head)
    assert route.buy.id in touched
    assert route.buy.id in inv.registry.keys
    reader = Reader(rpc, config)
    keys = (route.sell, inv.registry.keys[route.buy.id])
    frame = reader.read(keys); report = evaluate(frame, config)
    for _ in range(4):
        if report['candidates']: break
        reader.request_missing(keys, report['missing_slots'])
        frame = reader.read(keys); report = evaluate(frame, config)
    assert report['candidates']
    ex = Executor(rpc, config, cl, signer, directory, v4_contract=contract)
    try:
        candidate = report['candidates'][0]
        route = Route(Key(*candidate['v4_keys'][0]), Key(*candidate['v4_keys'][1]))
        result = ex.execute(candidate, route, {}, frame.anchor, time.monotonic_ns()+5_000_000_000, submit=False)
        assert result['status'] == 'execution_preflight_passed'
        assert not result['signed'] and not result['broadcast']
        assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
    finally:
        ex.close(); inv.close()
