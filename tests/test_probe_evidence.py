import json
import time

import pytest

from searcher.core import RPCError, data
from searcher.execution import capture_execution_probe, execution_failure_details, verified_call_gas_budget, GasValidationExpired


def test_failure_preserves_exact_unsigned_probe_and_original_exception_without_retries():
    error=RPCError('eth_call',{'code':3,'message':'execution reverted','data':data('InsufficientProfit()')})
    calls=[]
    def operation():
        calls.append(1)
        raise error
    call={'from':'0x'+'11'*20,'to':'0x'+'22'*20,'data':'0x1234','gas':'0x12345',
          'endpoint':'https://example/secret','raw_transaction':'secret'}
    context={'number':'0x100','time':'0x200','blockHash':{'255':'0x'+'33'*32}}
    with pytest.raises(RPCError) as caught:
        capture_execution_probe(operation,call,context,'initial_call')
    assert caught.value is error and calls==[1]
    detail=execution_failure_details(caught.value)
    assert detail['revert_name']=='InsufficientProfit'
    probe=detail['execution_probe']
    assert probe['unsigned_call']=={k:call[k] for k in ('from','to','data','gas')}
    assert probe['block_context']==context and probe['elapsed_ms']>=0
    assert not probe['state_root_proven'] and 'secret' not in json.dumps(detail)
    context['number']='0x999'
    assert probe['block_context']['number']=='0x100'
    wrapped=execution_failure_details(GasValidationExpired(error,[74565]))
    assert wrapped['execution_probe']==probe and wrapped['validation_deadline_reached']


def test_actual_gas_probe_attaches_metadata_and_leaves_success_unchanged():
    calls=[]
    class RPC:
        def request(self,method,params):
            calls.append((method,params))
            raise RPCError(method,{'code':3,'message':'execution reverted','data':data('BadCallback()')})
    with pytest.raises(RPCError) as caught:
        verified_call_gas_budget(RPC(),{'to':'0x'+'22'*20},{'number':'0x1'},580000,2000000,time.monotonic_ns()+10**9)
    assert len(calls)==1
    assert execution_failure_details(caught.value)['execution_probe']['unsigned_call']['gas']==hex(580000)
    token=object()
    assert capture_execution_probe(lambda:token,{}, {},'initial_call') is token
