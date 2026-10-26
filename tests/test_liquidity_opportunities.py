"""Liquidity changes reach the same bounded, fully validated search path."""
import json
from liquidity_fixture import RECORDED_LIQUIDITY
import pytest
from searcher.events import POOL_ACTIVITY_TOPICS, MINT, BURN, INITIALIZE
from searcher.universe import ActivityHints, swap_activity
from searcher.activity import ActivityInbox
from searcher.fastlane import HotRoutes
from test_fastlane import config
from test_pipeline import saved, A, B, H0, receipt


@pytest.mark.parametrize('event',[MINT,BURN,INITIALIZE])
def test_liquidity_event_arms_complete_route_without_waiting_for_swap(event):
    log=dict(receipt()['logs'][0],topics=[event])
    hints=ActivityHints()
    inbox=ActivityInbox()
    inbox.publish(hints.accept([log]),10)
    selector=HotRoutes(saved(),config(),clock=lambda:10)
    selector.add(inbox.drain(10))
    result=selector.take()
    assert result['groups']==[(A,B)]
    assert {p.address for p in result['pools']}=={A,B}
    assert all(p.block==1 for p in result['pools']) # hint, not executable state
    assert hints.accept([log])==set()


def test_recorded_launch_mints_are_discoverable_before_first_arbitrage_swap():
    case=RECORDED_LIQUIDITY
    assert case['logs'] and all(int(l['transactionIndex'],16)<case['winner_index'] for l in case['logs'])
    assert all(l['topics'][0]==MINT for l in case['logs'])
    got=[]
    class RPC:
        def request(self,method,params):
            assert method=='eth_getLogs'
            assert set(params[0]['topics'][0])==set(POOL_ACTIVITY_TOPICS)
            got.extend(params)
            return case['logs']
    activity,last=swap_activity(RPC(),case['block'],case['block'],case['logs'][0]['blockHash'])
    pool=case['logs'][0]['address']
    assert activity[pool]==3 and last[pool]==case['block']
    assert ActivityHints().accept(case['logs'])=={pool}


def test_removed_liquidity_and_unknown_emitters_never_arm_execution():
    log=dict(receipt()['logs'][0],topics=[MINT],removed=True)
    assert not ActivityHints().accept([log])
    log.pop('removed');log['address']='0x'+'ff'*20
    selector=HotRoutes(saved(),config(),clock=lambda:10)
    selector.add([(a,10) for a in ActivityHints().accept([log])])
    assert selector.take()['groups']==[]
