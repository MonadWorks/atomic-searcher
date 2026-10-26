from searcher.route_feedback import RouteFeedback
from searcher.core import load_config
from searcher.engine import evaluate
from searcher.routes import RouteIndex
from test_receipt_eligibility import pools, HEADER
from searcher.core import WETH


def test_only_execution_compatibility_failures_pause_a_specific_route():
    now=[100]
    feedback=RouteFeedback(clock=lambda:now[0],limit=2)
    for name in ('InsufficientProfit','InvalidPlan','TransferFailed','unclassified',None):
        assert feedback.record('route',{'revert_name':name})==0
        assert not feedback.blocked('route')
    assert feedback.record('route',{'revert_name':'UniswapV2Invariant'})==30
    assert feedback.blocked('route') and not feedback.blocked('reverse-route')
    now[0]=130
    assert not feedback.blocked('route')
    for key in ('a','b','c'): feedback.record(key,{'revert_name':'BadCallback'})
    assert len(feedback.failures)==2 and not feedback.blocked('a')
    feedback.passed('b');assert not feedback.blocked('b')


def test_cooldown_releases_unchanged_route_and_survives_page_rebuild():
    now=[100]; feedback=RouteFeedback(clock=lambda:now[0])
    config=load_config();config['policy']['max_quote_ms']=500
    config['settlement_tokens']=[dict(config['settlement_tokens'][0],max_input_raw=str(10**18))]
    index=RouteIndex(pools()[2:],[WETH],3);index.execution_feedback=feedback
    first=evaluate(index,config,HEADER)
    key=first['candidates'][0]['route_key']
    feedback.record(key,{'revert_name':'UniswapV2Invariant'})
    second=evaluate(index,config,HEADER,set())
    assert not second['candidates']
    assert key in index.queued_routes
    now[0]=131
    third=evaluate(index,config,HEADER,set())
    assert any(c['route_key']==key for c in third['candidates'])
    replacement=RouteIndex(pools()[2:],[WETH],3);replacement.execution_feedback=feedback
    assert not replacement.execution_feedback.blocked(key)


def test_repeated_wrapped_revert_yields_to_other_routes_without_token_exclusion():
    now=[0];feedback=RouteFeedback(clock=lambda:now[0])
    failure={'revert_name':'TransferFailed','revert_selector':'0x90b8ec18',
             'gas_attempts':[980000,1960000],
             'previous_gas_failure':{'revert_selector':'0x90b8ec18','gas_attempts':[980000]}}
    assert feedback.record('failed-direction',failure)==30
    assert feedback.blocked('failed-direction') and not feedback.blocked('other-direction')
    now[0]=31;assert not feedback.blocked('failed-direction')
    assert feedback.record('single',dict(failure,previous_gas_failure={}))==0
