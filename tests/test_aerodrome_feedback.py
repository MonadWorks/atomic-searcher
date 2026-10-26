"""The observed live K() loop must diagnose, defer, and recover by direction."""
from types import SimpleNamespace
import time
import pytest
from searcher.core import RPCError, USDC, WETH
from searcher.execution import execution_failure_details, verified_call_gas_budget
from searcher.dex_feedback import DexFeedback
from searcher.transfer_feedback import TransferSample

OWNER='0x'+'11'*20
EXECUTOR='0x'+'22'*20
TOKEN='0x09f87f948c88848363b124c9099cbb58e4cc7cb6'
AERO='0x940181a94a35a4569e4529a3cdfb74e38fd98631'
PATH=(('0x9be74093980230702f766bc6f8234177e58237e4',WETH,AERO,2),
      ('0xe7ae73f6b49976d860d7b7eb9bdfcfb042b9201e',AERO,TOKEN,4),
      ('0xebe3e212ee559268c80fd16a518f078c04480157',TOKEN,WETH,4))

def error():return RPCError('eth_call',{'code':3,'message':'execution reverted','data':'0xa932492f'})

def test_actual_custom_invariant_is_terminal_for_gas_escalation():
    calls=[]
    def request(*args):calls.append(args);raise error()
    with pytest.raises(RPCError):
        verified_call_gas_budget(SimpleNamespace(request=request),{'from':OWNER,'to':EXECUTOR},
                                 {},700000,6000000,time.monotonic_ns()+10**9)
    assert len(calls)==1
    assert execution_failure_details(error())['revert_name']=='PoolInvariant'

def test_custom_invariant_enters_diagnostics_without_blacklisting_alternative_conversion(tmp_path):
    f=DexFeedback(OWNER,{'StructuralExecutor':EXECUTOR},tmp_path,lambda:1.)
    assert f.rejected(PATH,7871990050776073,execution_failure_details(error()),
                      block=52757503,gross=94783461354782,floor=None,execution_key='conversion-a')==30
    assert not f.allowed(PATH,'conversion-a')
    assert f.allowed(PATH,'conversion-b')
    assert len(f.pending)==2
    # A K() failure alone identifies neither a tax nor the failing hop.
    assert not f.transfers['StructuralExecutor'].edges

def test_measured_shared_loss_yields_across_routes_then_allows_recovery(tmp_path):
    now=[1.];f=DexFeedback(OWNER,{'StructuralExecutor':EXECUTOR},tmp_path,lambda:now[0])
    sample=TransferSample(TOKEN,PATH[1][0],PATH[2][0],40241699147894608986565)
    f.transfers['StructuralExecutor'].record(sample,dict(token=TOKEN,source=sample.source,
        destination=sample.destination,executor=EXECUTOR,amount=sample.amount,
        funded=38268365438466775868516,debited=38268365438466775868516,
        credited=36412418859810071267790,canonical=True,block=52757625,block_hash='0x'+'33'*32))
    alternate=(('0x'+'44'*20,*PATH[0][1:]),*PATH[1:])
    assert not f.allowed(PATH,'conversion-a') and not f.allowed(alternate,'conversion-b')
    reverse=tuple((pool,b,a,kind) for pool,a,b,kind in reversed(PATH))
    assert f.allowed(reverse)
    now[0]=32
    assert f.admit(PATH,'conversion-a')
    assert not f.allowed(alternate,'conversion-b')
    assert f.rejected(PATH,7871990050776073,execution_failure_details(error()),
                      block=52757503,gross=94783461354782,floor=None,execution_key='conversion-a')>=60
    now[0]=63
    assert not f.allowed(alternate,'conversion-b')
    now[0]=94
    assert f.admit(PATH,'conversion-a')
    f.passed(PATH,7871990050776073,block=52757505,gross=100000,floor=90000,execution_key='conversion-a')
    assert f.allowed(alternate,'conversion-b')

@pytest.mark.parametrize('extra',[{'validation_deadline_reached':True},{'anchor_mismatch':{'block':1}}])
def test_late_or_wrong_anchor_invariant_does_not_seed_transfer_diagnosis(tmp_path,extra):
    f=DexFeedback(OWNER,{'StructuralExecutor':EXECUTOR},tmp_path,lambda:1.)
    failure={**execution_failure_details(error()),**extra}
    assert f.rejected(PATH,100,failure,block=1,gross=2,floor=1)==0
    assert f.allowed(PATH) and not f.pending
