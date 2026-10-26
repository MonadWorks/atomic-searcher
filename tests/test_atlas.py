import json,time
from types import SimpleNamespace
from eth_abi import encode,decode
from eth_account import Account
from eth_account.messages import encode_typed_data
import pytest
from searcher.atlas import *
from searcher.core import Reject

def notice():
    feed='0x'+'12'*20;stamp=123
    report=encode(['uint32','bytes32','int192[]','int192'],[stamp,bytes(32),[10,20,30],0])
    transmitted=data('transmitSecondary(bytes32[3],bytes,bytes32[],bytes32[],bytes32)',['bytes32[3]','bytes','bytes32[]','bytes32[]','bytes32'],[[bytes(32)]*3,report,[],[],bytes(32)])
    op={'chainId':8453,'to':ATLAS,'control':CONTROL,'dapp':CONTROL,'from':ZERO,'deadline':100,'gas':500000,
        'maxFeePerGas':100,'userOpHash':'0x'+'11'*32,'hints':{'aggregator':feed,'medianPrice':20,'rawReport':'0x'+report.hex(),
        'forwardData':data('forward(address,bytes)',['address','bytes'],[feed,unhex(transmitted)])}}
    return {'partial_user_operation':op,'received_at':10,'auction_id':'one'}

def test_full_report_binds_median_feed_and_domain():
    n=notice();assert validate_notice(n,99,now=10)[2]==20
    n['partial_user_operation']['hints']['medianPrice']=21
    with pytest.raises(Reject,match='median'):validate_notice(n,99,now=10)

def test_auction_time_window_is_shorter_than_chain_deadline():
    n=notice()
    with pytest.raises(Reject,match='window'):validate_notice(n,1,now=12)

def test_price_override_returns_correct_full_round_data(market):
    rpc,*_=market;feed='0x'+'ab'*20;overrides=report_override(feed,222,123)
    raw=rpc.request('eth_call',[{'to':feed,'data':data('latestRoundData()')},'latest',overrides])
    assert decode(['uint80','int256','uint256','uint256','uint80'],unhex(raw))==(1,222,123,123,1)

def test_full_simulation_accepts_official_enum_and_never_invents_success():
    class RPC:
        def __init__(self,success,result):self.success=success;self.result=result
        def request(self,method,args):return '0x'+encode(['bool','uint8','uint256'],[self.success,self.result,0]).hex()
    n=notice();op=n['partial_user_operation'];owner='0x'+'23'*20;contract='0x'+'34'*20
    meta=dict(simulator='0x'+'45'*20,call_config=41732,dapp_gas=2000000,solver_gas=6000000,surcharge=0,authorized_signer=owner,forwarders={op['hints']['aggregator']:[{'address':'0x'+'56'*20,'bundlers':['0x'+'67'*20]}]})
    plan=(unhex(op['userOpHash']),b'',1,[],1,1,1,100,100);s=solution(owner,contract,op,600000,1,plan);s['signature']='0x'+'00'*65
    assert full_simulation(RPC(True,6),meta,op,s)['full_simulation_passed']
    with pytest.raises(Reject):full_simulation(RPC(False,4),meta,op,s)

@pytest.fixture
def market(request):
    # Reuse the existing disposable local execution fixture.
    return request.getfixturevalue('base_market')

from test_execution import market as base_market
