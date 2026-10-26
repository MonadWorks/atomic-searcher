import copy
import time
from types import SimpleNamespace

import pytest

from searcher.core import Reject
from searcher.execution import Executor
from test_execution import market


def hinted(candidate, gas):
    c=copy.deepcopy(candidate)
    c.update(execution_kind='mixed-v1', execution_gas_source='whole_cycle_probe', execution_gas_estimate=gas)
    return c


@pytest.mark.parametrize('gas', [0, -1, True, 6_000_001])
def test_invalid_gas_hint_is_rejected_without_signing(market, monkeypatch, gas):
    rpc,c,contract,signer,directory,index,candidate,head,_=market
    ex=Executor(rpc,c,contract,signer,directory)
    try:
        candidate=hinted(candidate,gas)
        monkeypatch.setattr(signer,'sign_transaction',lambda *a:pytest.fail('invalid hint signed'))
        with pytest.raises(Reject,match='invalid whole-cycle gas budget'):
            ex.execute(candidate,index.routes[candidate['route_key']],index.states,head,time.monotonic_ns()+2_000_000_000)
        assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
    finally:ex.close()


def test_underestimated_hint_still_fails_actual_deployed_call_before_reservation(market, monkeypatch):
    rpc,c,contract,signer,directory,index,candidate,head,_=market
    ex=Executor(rpc,c,contract,signer,directory)
    try:
        candidate=hinted(candidate,21000)
        candidate['cost_budget']['gas_limit']=21000
        monkeypatch.setattr(signer,'sign_transaction',lambda *a:pytest.fail('underestimated hint signed'))
        with pytest.raises(Reject):
            ex.execute(candidate,index.routes[candidate['route_key']],index.states,head,time.monotonic_ns()+2_000_000_000)
        assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
    finally:ex.close()


def test_gas_hint_does_not_bypass_transfer_probe_guard(market, monkeypatch):
    rpc,c,contract,signer,directory,index,candidate,head,_=market
    ex=Executor(rpc,c,contract,signer,directory)
    try:
        candidate=hinted(candidate,candidate['cost_budget']['gas_limit'])
        ex.transfer_feedback=SimpleNamespace(claim_probe=lambda *a,**kw:False)
        monkeypatch.setattr(signer,'sign_transaction',lambda *a:pytest.fail('blocked route signed'))
        with pytest.raises(Reject,match='sampled transfer retry'):
            ex.execute(candidate,index.routes[candidate['route_key']],index.states,head,time.monotonic_ns()+2_000_000_000)
        assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
    finally:ex.close()
