import time

import pytest

from searcher.core import RPCError, WETH, data, load_config
from searcher.engine import evaluate
from searcher.execution import Executor, execution_failure_details
from searcher.routes import Route, RouteIndex
from searcher.transfer_feedback import TransferFeedback, TransferSample
from test_execution import market
from test_receipt_eligibility import pools, HEADER, BAD
from test_transfer_feedback import EXECUTOR, OWNER, OTHER_POOL, sample_result


def registry(ttl=300):
    clock=[0]
    f=TransferFeedback(EXECUTOR,OWNER,clock=lambda:clock[0],ttl=ttl)
    sample=TransferSample(BAD,pools()[0].address,pools()[1].address,100)
    f.record(sample,sample_result(sample,funded=96))
    route=Route((WETH,BAD,WETH),(sample.source,sample.destination))
    return clock,f,sample,route


def test_shared_direction_retries_slow_down_and_leave_other_routes_available():
    clock,f,sample,route=registry(ttl=3600)
    alternate=Route((WETH,BAD,WETH),(sample.source,OTHER_POOL))
    reverse=Route((WETH,BAD,WETH),(sample.destination,sample.source))
    delays=[]
    for i in range(7):
        clock[0]=max(item['next_probe'] for item in f.edges.values())+.01
        selected=route if i%2==0 else alternate
        assert f.claim_probe(selected)
        delays.append(f.rejected(selected,{'revert_name':'BadCallback'}))
        assert f.deferred(route) and f.deferred(alternate)
        assert not f.deferred(reverse)
        # Duplicate error handling cannot double the delay again.
        assert f.rejected(selected,{'revert_name':'BadCallback'})==0
    assert delays==[60,120,240,300,300,300,300]
    c=load_config();c['unsupported_transfer_tokens']={}
    c['policy'].update(max_quote_ms=500,sizing_mode='fixed')
    c['settlement_tokens']=[dict(c['settlement_tokens'][0],max_input_raw=str(10**18))]
    index=RouteIndex(pools(),[WETH],3);index.transfer_feedback=f
    report=evaluate(index,c,HEADER)
    assert report['rejected']['sampled_transfer_backoff']==1
    assert report['candidates'] and all(BAD not in c['tokens'] for c in report['candidates'])
    assert all(sample.source not in group for group in report['deep_read_routes'])


@pytest.mark.parametrize('failure',[
    {'cause':'execution context stale'},
    {'revert_name':'TransferFailed'},
    {'revert_name':'BadCallback','validation_deadline_reached':True},
    {'revert_name':'BadCallback','validation_outcome':'call_succeeded_after_deadline'},
])
def test_inconclusive_failure_never_escalates_a_sample(failure):
    clock,f,sample,route=registry();clock[0]=31
    assert f.rejected(route,{'revert_name':'BadCallback'})==0  # no actual retry claimed
    assert f.claim_probe(route)
    assert f.rejected(route,failure)==0
    assert all(item.get('repeated_reverts',0)==0 for item in f.edges.values())
    clock[0]=62
    assert f.claim_probe(route)


def test_fresh_sample_preserves_retry_and_real_pass_clears_it():
    clock,f,sample,route=registry();clock[0]=31
    assert f.claim_probe(route)
    assert f.rejected(route,{'revert_name':'BadCallback'})==60
    clock[0]=40
    f.record(sample,sample_result(sample,funded=96))
    assert all(item['repeated_reverts']==1 and item['next_probe']==91 for item in f.edges.values())
    f.passed(route)
    assert not f.deferred(route) and not f.edges
    assert f.record(sample,sample_result(sample,funded=96))==[]


def test_real_tax_reverts_back_off_before_signing_and_recover_after_tax_removed(market):
    rpc,config,contract,signer,directory,index,candidate,_,send=market
    route=index.routes[candidate['route_key']]
    token=route.tokens[1]
    send(token,data('setBad(bool,uint256)',['bool','uint256'],[False,400]))
    executor=Executor(rpc,config,contract,signer,directory)
    executor.gas_estimate_context_verified=False
    clock=[0];f=TransferFeedback(contract,signer.address,clock=lambda:clock[0])
    intermediate=index.states[route.pools[0]].quote(route.tokens[0],candidate['amount_in'])[0]
    sample=TransferSample(token,route.pools[0],route.pools[1],intermediate)
    observation=dict(token=token,source=sample.source,destination=sample.destination,
        executor=contract,amount=intermediate,funded=intermediate*96//100,
        debited=intermediate*96//100,credited=intermediate*96//100*96//100,
        canonical=True,block=0,block_hash='0x'+'11'*32)
    f.record(sample,observation);executor.transfer_feedback=f
    try:
        for when,delay in [(31,60),(92,120)]:
            clock[0]=when;executor.refresh_context()
            with pytest.raises(RPCError) as caught:
                executor.execute(candidate,route,index.states,rpc.header(),time.monotonic_ns()+2_000_000_000)
            details=execution_failure_details(caught.value)
            assert details['revert_name']=='BadCallback'
            assert details['transfer_retry_after_seconds']==delay
            assert f.deferred(route)
            assert executor.db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0]==0
        send(token,data('setBad(bool,uint256)',['bool','uint256'],[False,0]))
        clock[0]=213;executor.refresh_context()
        result=executor.execute(candidate,route,index.states,rpc.header(),time.monotonic_ns()+2_000_000_000)
        assert result['status']=='submitted' and not f.edges
        rpc.request('evm_mine',[]);executor.reconcile()
        assert executor.db.execute('SELECT state FROM attempts').fetchone()[0]=='success'
    finally:
        executor.close()
