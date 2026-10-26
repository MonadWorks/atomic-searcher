import json
import time

import pytest

from searcher.context import simulation_context
from searcher.core import Reject, data
from searcher.execution import Executor, ExecutionAnchorChanged, execution_failure_details
from searcher.structural import MORPHO, PSM_POOLS, SUSDS, psm_route
from searcher.core import USDC
from searcher.structural_screening import UnprofitableQuote
from searcher.structural_runner import complete_quote
from test_execution import market
from test_structural import structural_market
from test_strategy_suite import protocol_market
from test_venue import venue_market


def quote(ex, rpc, config, target, route, amount):
    anchor = ex.context['head']
    candidate, _ = complete_quote(rpc, config, target, route, amount, anchor,
        simulation_context(anchor, ex.context['pending_head']), ex.context['nonce'])
    return anchor, candidate


@pytest.mark.parametrize('submit', [False, True])
def test_new_head_requotes_same_funded_route_then_validates_and_accounts(structural_market, submit):
    rpc, c, old, target, signer, directory, route, send = structural_market
    ex = Executor(rpc, c, old, signer, directory, structural_contract=target)
    try:
        h, candidate = quote(ex, rpc, c, target, route, 3030001)
        rpc.request('evm_mine', []); ex.refresh_context()
        outcome = ex.execute(candidate, route, {}, h, time.monotonic_ns()+5_000_000_000, submit=submit)
        assert outcome['anchor_refresh']['attempts'] == 1
        assert outcome['anchor_refresh']['remeasured']
        assert candidate['gross_surplus'] == 300000
        if submit:
            assert outcome['status'] == 'submitted'
            rpc.request('evm_mine', []); ex.reconcile()
            assert ex.db.execute('SELECT state FROM attempts').fetchall() == [('success',)]
            settled = json.loads(ex.db.execute('SELECT outcome FROM intents').fetchone()[0])
            assert settled['net_after_reserved_fee_raw'] > 0
        else:
            assert outcome['status'] == 'execution_preflight_passed'
            assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
    finally: ex.close()


def test_competitor_takes_opportunity_before_refresh_no_sign_or_reservation(structural_market):
    rpc, c, old, target, signer, directory, route, send = structural_market
    ex = Executor(rpc, c, old, signer, directory, structural_contract=target)
    try:
        h, candidate = quote(ex, rpc, c, target, route, 3030001)
        for borrower in ('0x'+'11'*20, '0x'+'22'*20):
            send(MORPHO, data('setDebt(address,uint256)', ['address','uint256'], [borrower,0]))
        ex.refresh_context()
        with pytest.raises(Reject) as caught:
            ex.execute(candidate, route, {}, h, time.monotonic_ns()+5_000_000_000)
        assert execution_failure_details(caught.value)['anchor_refresh']['attempts'] == 1
        assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
    finally: ex.close()


def test_second_head_change_ends_attempt_without_another_requote(structural_market, monkeypatch):
    import searcher.structural_runner as runner
    rpc, c, old, target, signer, directory, route, send = structural_market
    ex = Executor(rpc, c, old, signer, directory, structural_contract=target)
    try:
        h, candidate = quote(ex, rpc, c, target, route, 3030001)
        rpc.request('evm_mine', []); ex.refresh_context(); calls=[]
        def changed_again(*args, **kwargs):
            calls.append(1); result=complete_quote(*args, **kwargs)
            rpc.request('evm_mine', []); ex.refresh_context(); return result
        monkeypatch.setattr(runner, 'complete_quote', changed_again)
        with pytest.raises(ExecutionAnchorChanged) as caught:
            ex.execute(candidate, route, {}, h, time.monotonic_ns()+5_000_000_000)
        assert calls == [1] and caught.value.anchor_refresh['remeasured']
        assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
    finally: ex.close()


@pytest.mark.parametrize('strategy', [1, 5])
def test_protocol_lanes_share_fresh_quote_and_same_account_guard(protocol_market, strategy):
    rpc,c,old,structural,signer,directory,contracts,routes,send=protocol_market
    ex=Executor(rpc,c,old,signer,directory,structural_contract=structural,protocol_contracts=contracts)
    try:
        route=routes[strategy-1];h,candidate=quote(ex,rpc,c,contracts[route.contract_name],route,10**7)
        rpc.request('evm_mine',[]);ex.refresh_context()
        result=ex.execute(candidate,route,{},h,time.monotonic_ns()+5_000_000_000)
        assert result['status']=='submitted' and result['anchor_refresh']['remeasured']
        rpc.request('evm_mine',[]);ex.reconcile()
        assert ex.db.execute('SELECT state FROM attempts').fetchall()==[('success',)]
    finally:ex.close()


def test_expired_input_is_not_revived_by_anchor_refresh(structural_market, monkeypatch):
    import searcher.structural_runner as runner
    rpc,c,old,target,signer,directory,route,send=structural_market
    ex=Executor(rpc,c,old,signer,directory,structural_contract=target)
    try:
        h,candidate=quote(ex,rpc,c,target,route,3030001)
        rpc.request('evm_mine',[]);ex.refresh_context()
        monkeypatch.setattr(runner,'complete_quote',lambda *a,**k:pytest.fail('expired input remeasured'))
        with pytest.raises(Reject,match='observation expired'):
            ex.execute(candidate,route,{},h,time.monotonic_ns()-1)
        assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
    finally:ex.close()


@pytest.mark.parametrize('profit_remains', [False, True])
def test_psm_opportunity_crossing_head_remeasures_real_profit(structural_market, profit_remains):
    rpc,c,old,target,signer,directory,_,send=structural_market
    ex=Executor(rpc,c,old,signer,directory,structural_contract=target)
    try:
        route=psm_route(PSM_POOLS[0]);h,candidate=quote(ex,rpc,c,target,route,10**7)
        if profit_remains:
            rpc.request('evm_mine',[])
        else:
            factory='0x33128a8fc17869897dce68ed026d694621f6fdfd'
            send(PSM_POOLS[0],data('initialize(address,address,address,uint256,uint256)',
                ['address']*3+['uint256']*2,[factory,SUSDS,USDC,100000,100005]))
        ex.refresh_context()
        if profit_remains:
            result=ex.execute(candidate,route,{},h,time.monotonic_ns()+5_000_000_000)
            assert result['status']=='submitted' and result['anchor_refresh']['remeasured']
            rpc.request('evm_mine',[]);ex.reconcile()
            assert ex.db.execute('SELECT state FROM attempts').fetchall()==[('success',)]
        else:
            with pytest.raises(UnprofitableQuote) as caught:
                ex.execute(candidate,route,{},h,time.monotonic_ns()+5_000_000_000)
            evidence=caught.value.evidence()
            assert evidence['anchor_refresh']['remeasured']
            assert candidate['gross_surplus']==evidence['gross_surplus']<candidate['minimum_gross_surplus']
            assert not ex.db.execute('SELECT 1 FROM attempts').fetchone()
    finally:ex.close()


def test_venue_mailbox_requotes_before_shared_executor_send(venue_market):
    import threading
    from types import SimpleNamespace
    from searcher.strategy_suite import Suite
    rpc,c,old,structural,signer,directory,target,route,send,_=venue_market
    ex=Executor(rpc,c,old,signer,directory,structural_contract=structural,protocol_contracts={'VenueExecutor':target})
    try:
        h,candidate=quote(ex,rpc,c,target,route,10**7)
        suite=Suite(c,SimpleNamespace(),{'VenueExecutor':target},lambda:ex.context,threading.Event(),directory,rpc.url)
        suite.report('venues',state='scanning');suite.offer('venues',candidate,route,{},h,time.monotonic_ns())
        rpc.request('evm_mine',[]);ex.refresh_context()
        result=suite.consume(ex)
        assert result['status']=='submitted' and result['anchor_refresh']['remeasured']
        rpc.request('evm_mine',[]);ex.reconcile()
        assert ex.db.execute('SELECT state FROM attempts').fetchall()==[('success',)]
    finally:ex.close()
