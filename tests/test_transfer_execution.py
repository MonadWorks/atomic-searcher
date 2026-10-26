"""Actual whole-executor retry through a temporary loopback-only market."""
import copy
import time

import pytest

from test_execution import market
from searcher.core import Reject
from searcher.execution import Executor
from searcher.transfer_feedback import TransferFeedback, TransferSample


def test_real_success_clears_helper_suspicion_and_precheck_does_not_spend_retry(market,monkeypatch):
    rpc,config,contract,signer,directory,index,candidate,head,_=market
    executor=Executor(rpc,config,contract,signer,directory)
    executor.gas_estimate_context_verified=False
    route=index.routes[candidate['route_key']]
    clock=[0];feedback=TransferFeedback(contract,signer.address,clock=lambda:clock[0])
    executor.transfer_feedback=feedback
    sample=TransferSample(route.tokens[1],route.pools[0],route.pools[1],100)
    observation=dict(token=sample.token,source=sample.source,destination=sample.destination,
        amount=100,funded=96,debited=96,credited=96,executor=contract,canonical=True,
        block=0,block_hash='0x'+'11'*32)
    feedback.record(sample,observation)
    clock[0]=31
    try:
        oversized=copy.deepcopy(candidate)
        oversized['amount_in']=int(config['settlement_tokens'][0]['max_input_raw'])+1
        with pytest.raises(Reject,match='input cap'):
            executor.execute(oversized,route,index.states,head,time.monotonic_ns()+2_000_000_000)
        assert not feedback.deferred(route) and feedback.priority(route)==1
        with monkeypatch.context() as m:
            requests=[]
            m.setattr(executor,'verify_settlements',lambda:time.sleep(.02))
            m.setattr(rpc,'request',lambda *args:requests.append(args))
            with pytest.raises(Reject,match='observation expired'):
                executor.execute(candidate,route,index.states,head,time.monotonic_ns()+5_000_000)
            assert not requests and not feedback.deferred(route) and feedback.priority(route)==1
        result=executor.execute(candidate,route,index.states,head,time.monotonic_ns()+2_000_000_000)
        assert result['status']=='submitted' and feedback.priority(route)==0
        # An older diagnostic completing later cannot restore this suspicion.
        assert feedback.record(sample,observation)==[]
        rpc.request('evm_mine',[]);executor.reconcile()
        assert executor.db.execute('SELECT state FROM attempts').fetchone()[0]=='success'
    finally:executor.close()
