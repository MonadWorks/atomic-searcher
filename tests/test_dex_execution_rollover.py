import copy
import time

import pytest

from searcher.core import Reject, number
from searcher.execution import Executor, ExecutionAnchorChanged
from test_execution import market


def opted_in(candidate):
    value = copy.deepcopy(candidate)
    value.update(execution_kind='mixed-v1', allow_execution_anchor_rollover=True)
    return value


@pytest.mark.parametrize('delta', [-1, 0, 3])
def test_rollover_rejects_backward_reorg_or_distant_context(market, monkeypatch, delta):
    rpc,c,contract,signer,directory,index,original,head,_ = market
    ex = Executor(rpc,c,contract,signer,directory)
    try:
        candidate = opted_in(original)
        ex.context['head'] = dict(head, number=hex(number(head['number'])+delta), hash='0x'+'aa'*32)
        monkeypatch.setattr(signer,'sign_transaction',lambda *a:pytest.fail('invalid rollover signed'))
        with pytest.raises(ExecutionAnchorChanged):
            ex.execute(candidate,index.routes[candidate['route_key']],index.states,head,time.monotonic_ns()+2_000_000_000)
        assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
    finally: ex.close()


def test_rollover_does_not_refresh_expired_deadline(market, monkeypatch):
    rpc,c,contract,signer,directory,index,original,head,_ = market
    ex = Executor(rpc,c,contract,signer,directory)
    try:
        rpc.request('evm_mine',[]);ex.refresh_context()
        candidate = opted_in(original)
        monkeypatch.setattr(signer,'sign_transaction',lambda *a:pytest.fail('expired quote signed'))
        with pytest.raises(Reject,match='observation expired'):
            ex.execute(candidate,index.routes[candidate['route_key']],index.states,head,time.monotonic_ns()-1)
        assert 'execution_anchor_rollover' not in candidate
        assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
    finally: ex.close()


def test_new_anchor_still_requires_canonical_final_batch(market, monkeypatch):
    rpc,c,contract,signer,directory,index,original,head,_ = market
    ex = Executor(rpc,c,contract,signer,directory)
    try:
        rpc.request('evm_mine',[]);ex.refresh_context()
        candidate = opted_in(original)
        original_batch = rpc.batch
        def reorg(calls):
            result = original_batch(calls)
            if len(calls)==5 and calls[1][0]=='eth_getBlockByNumber':
                result[1] = dict(result[1],hash='0x'+'bb'*32)
            return result
        monkeypatch.setattr(rpc,'batch',reorg)
        monkeypatch.setattr(signer,'sign_transaction',lambda *a:pytest.fail('reorg signed'))
        with pytest.raises(Reject,match='not canonical'):
            ex.execute(candidate,index.routes[candidate['route_key']],index.states,head,time.monotonic_ns()+2_000_000_000)
        assert not candidate['execution_anchor_rollover']['final_validation_passed']
        assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
    finally: ex.close()


def test_second_rollover_does_not_repeat_preparation(market):
    rpc,c,contract,signer,directory,index,original,head,_ = market
    ex = Executor(rpc,c,contract,signer,directory)
    try:
        rpc.request('evm_mine',[]);ex.refresh_context()
        candidate = opted_in(original)
        candidate['execution_anchor_rollover']={'final_validation_passed':False}
        with pytest.raises(ExecutionAnchorChanged):
            ex.execute(candidate,index.routes[candidate['route_key']],index.states,head,time.monotonic_ns()+2_000_000_000)
        assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
    finally: ex.close()
