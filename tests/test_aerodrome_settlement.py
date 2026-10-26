"""Complete callback financing, final checking, broadcast and accounting locally."""
import time
import pytest
from eth_abi import decode,encode
from searcher.core import USDC,data,unhex
from searcher.protocol_execution import route,TYPES
from searcher.structural_runner import complete_quote
from searcher.execution import Executor
from searcher.context import simulation_context
from test_strategy_suite import protocol_market,C,P,protocol_market as _fixture
from test_structural import structural_market
from test_execution import market,artifact

AERO='0x420dd381b31aef6683db6b902084cb0ffece40da'

@pytest.mark.parametrize('strategy',[1,2,3,4])
@pytest.mark.parametrize('stable',[False,True])
def test_native_pool_through_full_protocol_execution(protocol_market,strategy,stable):
    rpc,c,old,structural,signer,directory,contracts,routes,send=protocol_market
    rpc.request('anvil_setCode',[AERO,artifact('AerodromeConversion.t.sol','AeroFactoryMock')['deployedBytecode']['object']])
    rpc.request('anvil_setCode',[P,artifact('AerodromeConversion.t.sol','AeroPoolMock')['deployedBytecode']['object']])
    send(P,data('initialize(address,address,address,bool,uint256)',['address','address','address','bool','uint256'],[AERO,C,USDC,stable,75]))
    send(AERO,data('add(address,address,bool,address)',['address','address','bool','address'],[C,USDC,stable,P]))
    original=routes[strategy-1]
    values=list(decode(['('+','.join(TYPES[strategy])+')'],original.arguments)[0]);values[-2]=[(P,C,USDC,4)]
    r=route(strategy,values,original.assets)
    ex=Executor(rpc,c,old,signer,directory,structural_contract=structural,protocol_contracts=contracts)
    try:
        h=ex.context['head'];clock=simulation_context(h,ex.context['pending_head'])
        candidate,_=complete_quote(rpc,c,contracts[r.contract_name],r,10**7,h,clock,ex.context['nonce'])
        expected=decode(['uint256'],rpc.call(P,data('getAmountOut(uint256,address)',['uint256','address'],[11000000,C]),'latest'))[0]-10**7
        assert candidate['gross_surplus']==expected and expected>100000
        result=ex.execute(candidate,r,{},h,time.monotonic_ns()+5_000_000_000)
        assert result['status']=='submitted'
        rpc.request('evm_mine',[]);ex.reconcile()
        assert ex.db.execute('SELECT state FROM attempts').fetchone()[0]=='success'
    finally:ex.close()
