"""The refreshed amount must still pass the real executor on loopback Anvil."""
import json
import time

import pytest

from test_execution import market
from searcher.candidate_refresh import refresh_candidate
from searcher.core import Reject, data, number
from searcher.execution import Executor
from searcher.statefeed import StateFrame


@pytest.mark.parametrize('pool_rejects', [False, True])
def test_refreshed_candidate_uses_full_execution_and_receipt_guards(market, monkeypatch, pool_rejects):
    rpc, config, contract, signer, directory, index, old, head, send = market
    route = index.routes[old['route_key']]
    if pool_rejects:
        # The fresh quote cannot waive the contract's actual failure.
        rpc.request('anvil_setCode', [route.pools[1], '0x5f5ffd'])
    head = rpc.header()
    class FreshFeed:
        def __init__(self, rpc, config, saved): self.pools = saved['pools']
        def poll(self, **kwargs):
            now = time.monotonic_ns()
            return StateFrame(head, head, [p.json() for p in index.states.values()],
                              now, now, time.time(), number(head['number'])+1, number(head['timestamp'])+2)
    monkeypatch.setattr('searcher.candidate_refresh.StateFeed', FreshFeed)
    refreshed, states, anchor, deadline, evidence = refresh_candidate(
        rpc, config, route, index.states, previous_candidate=old)
    assert refreshed and refreshed['sizing_mode'] == 'previous_amount_repriced'
    executor = Executor(rpc, config, contract, signer, directory)
    try:
        if pool_rejects:
            with pytest.raises(Reject): executor.execute(refreshed, route, states, anchor, deadline)
            assert executor.db.execute('SELECT count(*) FROM attempts').fetchone()[0] == 0
        else:
            result = executor.execute(refreshed, route, states, anchor, deadline)
            assert result['status'] == 'submitted'
            rpc.request('evm_mine', [])
            executor.reconcile()
            assert executor.db.execute('SELECT state FROM attempts').fetchone()[0] == 'success'
            outcome = json.loads(executor.db.execute('SELECT outcome FROM intents').fetchone()[0])
            assert outcome['net_profit_raw'] > 0
    finally:
        executor.close()
