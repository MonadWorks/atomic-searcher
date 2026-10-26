"""Final signing-age regression on an ephemeral, loopback-only Anvil chain."""
import copy
import time

import pytest

from searcher.core import Reject, load_config
from searcher.execution import Executor
from test_execution import market


def test_real_slow_final_validation_still_rejects_at_500ms_before_signing(market, monkeypatch):
    import searcher.execution as execution_module
    rpc, original_config, contract, signer, directory, index, candidate, head, _ = market
    # Fixture candidate preparation has its own search allowance; execution
    # consumers still receive the unmodified ordinary search policy.
    assert original_config['policy']['max_quote_ms']==load_config()['policy']['max_quote_ms']
    config=copy.deepcopy(original_config)
    # Isolate the final-validation guard from the earlier observation guard.
    # Only this test's synthetic observation deadline is made generous.
    config['policy']['snapshot_max_age_ms']=10_000
    assert config['policy']['final_validation_max_age_ms']==500
    assert execution_module.time is time
    executor=Executor(rpc,config,contract,signer,directory)
    batch=rpc.batch; delayed=[];signed=[]
    def slow_final_batch(calls):
        values=batch(calls)
        method,params=calls[0]
        if method=='eth_call' and params[0].get('to','').lower()==contract and len(params)==4:
            assert calls[1][0]=='eth_getBlockByNumber'
            started=time.monotonic_ns()
            time.sleep(.55)
            delayed.append(time.monotonic_ns()-started)
        return values
    def forbidden_sign(transaction):
        signed.append(transaction)
        pytest.fail('expired final validation must not reach signing')
    monkeypatch.setattr(rpc,'batch',slow_final_batch)
    monkeypatch.setattr(signer,'sign_transaction',forbidden_sign)
    try:
        with pytest.raises(Reject,match='final validation expired before signing'):
            executor.execute(candidate,index.routes[candidate['route_key']],index.states,head,
                             time.monotonic_ns()+10_000_000_000)
        assert len(delayed)==1 and delayed[0]>=550_000_000
        assert not signed
        assert executor.db.execute('SELECT count(*) FROM attempts').fetchone()[0]==0
        assert executor.db.execute('SELECT count(*) FROM intents').fetchone()[0]==0
        assert execution_module.time is time
    finally: executor.close()
