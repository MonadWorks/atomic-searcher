"""Independent context handoff checks; deterministic responses, no sockets/signing."""
import copy
import threading
import time
from types import SimpleNamespace

from eth_abi import decode, encode
import pytest

from searcher.core import RPCError, Reject, WETH, data, load_config, unhex
from searcher.execution import Executor, ExecutionAnchorChanged, ORACLE
from searcher.engine import PLAN_TYPE
from searcher.routes import RouteIndex
from test_receipt_eligibility import pools


def head(height, byte=None):
    return dict(number=hex(height), timestamp=hex(2*height),
                hash='0x'+(byte or f'{height:02x}')*32, baseFeePerGas='0x1')


class AccountRPC:
    """Returns complete account snapshots through the production decoder."""
    def __init__(self, height=100, *, nonce=7, pending_nonce=None, balance=10**18,
                 oracle_fee=100, cancel_event=None):
        self.height, self.nonce = height, nonce
        self.pending_nonce = nonce if pending_nonce is None else pending_nonce
        self.balance, self.oracle_fee = balance, oracle_fee
        self.cancel_event = cancel_event or threading.Event()
        self.url, self.chain_id = 'https://context-test.invalid', 8453
        self.batches, self.requests, self.closed = [], [], False
        self.target = head(101)
        self.final_nonce = nonce

    def check_cancelled(self):
        if self.cancel_event.is_set(): raise Reject('RPC cancelled')

    def batch(self, calls):
        self.batches.append(copy.deepcopy(calls))
        if calls[0][0] == 'eth_call':
            # Final full-call, canonical head, pending clock and nonce checks.
            anchor = self.target
            pending = dict(head(int(anchor['number'],16)+1), parentHash=anchor['hash'])
            return ['0x'+encode(['uint256'],[10**15]).hex(), anchor, pending,
                    '0x'+encode(['bool','uint256','uint256'],
                                [True,int(pending['number'],16),int(pending['timestamp'],16)]).hex(),
                    hex(self.final_nonce)]
        answers = []
        anchor = head(self.height)
        for method, params in calls:
            if method == 'eth_getTransactionCount':
                answers.append(hex(self.nonce if params[1]=='latest' else self.pending_nonce))
            elif method == 'eth_getBalance': answers.append(hex(self.balance))
            elif method == 'eth_getBlockByNumber':
                answers.append(anchor if params[0]=='latest' else
                    dict(head(self.height+1),parentHash=anchor['hash']))
            elif method == 'eth_call':
                assert params[0]['to'].lower() == ORACLE
                answers.append('0x'+encode(['uint256'],[self.oracle_fee]).hex())
            else: raise AssertionError(method)
        return answers

    def batch_contract_reads(self, *args):
        raise AssertionError('fixture deliberately exercises the ordinary context decoder')

    def request(self, method, params):
        assert method == 'eth_call', 'this independent fixture cannot broadcast'
        self.requests.append(copy.deepcopy((method,params)))
        return '0x'+encode(['uint256'],[10**15]).hex()

    def close(self): self.closed = True


@pytest.fixture
def handoff(monkeypatch, tmp_path):
    config=load_config()
    config['settlement_tokens']=[dict(config['settlement_tokens'][0],address=WETH,min_net_raw='1')]
    config['policy'].update(aggregate_execution_context=False,profit_aware_bidding=False,
                            min_net_profit_usdc_raw=0)
    rpc=AccountRPC()
    signer=SimpleNamespace(address=config['owner'],sign_transaction=lambda tx:pytest.fail('must not sign'))
    monkeypatch.setattr(Executor,'preflight',lambda self:None)
    ex=Executor(rpc,config,'0x'+'aa'*20,signer,tmp_path/'account')
    ex.gas_estimate_context_verified=False
    ps=pools()[2:]
    ix=RouteIndex(ps,[WETH],2)
    route=next(iter(ix.routes.values()))
    candidate=dict(route_key=route.key,pools=list(route.pools),tokens=list(route.tokens),
                   amount_in=100000,gross_surplus=10**15,minimum_gross_surplus=1,
                   cost_budget=dict(gas_limit=580000,max_fee_per_gas=11000000,
                                    priority_fee=1000000,total_fee_wei=7380000000000))
    try: yield ex,rpc,candidate,route,ix.states
    finally: ex.close()


def execute(handoff, *, anchor=None, deadline=None):
    ex,rpc,candidate,route,states=handoff
    return ex.execute(candidate,route,states,anchor or head(101),
                      deadline or time.monotonic_ns()+2_000_000_000,submit=False)


def test_no_worker_keeps_typed_mismatch_without_an_extra_request(handoff):
    ex,rpc,*_=handoff
    before=len(rpc.batches)
    with pytest.raises(ExecutionAnchorChanged) as caught: execute(handoff)
    assert caught.value.anchor_mismatch['preflight_reached'] is False
    assert len(rpc.batches)==before and not rpc.requests
    assert ex.db.execute('select count(*) from attempts').fetchone()[0]==0


def test_matching_fast_path_needs_no_account_refresh_and_keeps_final_validation(handoff):
    ex,rpc,*_=handoff
    rpc.height=101
    ex.refresh_context()
    before=len(rpc.batches)
    result=execute(handoff)
    assert result['status']=='execution_preflight_passed'
    assert result['signed'] is result['broadcast'] is False
    assert len(rpc.batches)==before+1
    assert len(rpc.requests)==1
    assert len(rpc.batches[-1])==5
    assert ex.db.execute('select count(*) from attempts').fetchone()[0]==0


class ObservedCondition(threading.Condition):
    def __init__(self, executor):
        super().__init__()
        self.executor, self.kicked = executor, threading.Event()

    def notify_all(self):
        if self.executor.context_kick_sequence:
            self.kicked.set()
        super().notify_all()


class BlockedAccountRPC(AccountRPC):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.entered, self.release = threading.Event(), threading.Event()

    def batch(self, calls):
        self.entered.set()
        while not self.release.wait(.01): self.check_cancelled()
        self.check_cancelled()
        return super().batch(calls)


def worker(handoff, monkeypatch, **fields):
    ex=handoff[0]
    ex.context_changed=ObservedCondition(ex)
    rpc=BlockedAccountRPC(height=101,cancel_event=ex.context_stop,**fields)
    monkeypatch.setattr('searcher.execution.RPC',lambda *args,**kwargs:rpc)
    ex.start_context_worker()
    assert rpc.entered.wait(1), 'real background refresh did not start'
    return rpc


def release_after_kick(ex, rpc, action=None):
    errors=[]
    def release():
        try:
            assert ex.context_changed.kicked.wait(1), 'execute never requested the existing worker'
            if action: action()
            rpc.release.set()
        except BaseException as exc:
            errors.append(exc)
            rpc.release.set()
    thread=threading.Thread(target=release)
    thread.start()
    return thread,errors


@pytest.mark.parametrize('changed,expected',[
    ({'nonce':8,'pending_nonce':9},'account already has a pending transaction'),
    ({'balance':0},'insufficient ETH fee balance'),
    ({'oracle_fee':10**18},'attempt fee cap exceeded'),
])
def test_caught_up_context_rechecks_new_nonce_balance_and_fees(handoff,monkeypatch,changed,expected):
    ex,main,candidate,*_=handoff
    rpc=worker(handoff,monkeypatch,**changed)
    controller,errors=release_after_kick(ex,rpc)
    try:
        with pytest.raises(Reject,match=expected): execute(handoff)
        assert candidate['execution_context_sync']['outcome']=='caught_up'
        assert ex.context_kick_sequence==1
        assert not main.requests
        assert ex.db.execute('select count(*) from attempts').fetchone()[0]==0
    finally:
        rpc.release.set();controller.join(1)
    assert not errors and not controller.is_alive()


def test_existing_worker_whole_context_reaches_original_full_call_checks(handoff,monkeypatch):
    ex,main,candidate,*_=handoff
    rpc=worker(handoff,monkeypatch,nonce=9,oracle_fee=2_000_000_000_000)
    main.final_nonce=9
    controller,errors=release_after_kick(ex,rpc)
    try:
        result=execute(handoff)
        assert result['status']=='execution_preflight_passed'
        assert candidate['execution_context_sync']['outcome']=='caught_up'
        assert ex.context_kick_sequence==1
        assert ex.context['nonce']==ex.context['pending_nonce']==9
        assert ex.context['extra_fee']==8_000_000_000_000
        assert result['reserved_fee_wei']>=8_000_000_000_000
        assert len(main.requests)==1
        plan=decode([PLAN_TYPE],unhex(main.requests[0][1][0]['data'])[4:])[0]
        assert plan[-1]==9
        assert main.batches[-1][0][0]=='eth_call' and len(main.batches[-1])==5
        assert ex.db.execute('select count(*) from attempts').fetchone()[0]==0
    finally:
        rpc.release.set();controller.join(1)
    assert not errors and not controller.is_alive()


@pytest.mark.parametrize('height,byte',[(102,None),(101,'ee')])
def test_ahead_or_same_height_reorg_never_kicks_worker(handoff,monkeypatch,height,byte):
    ex,main,candidate,*_=handoff
    rpc=worker(handoff,monkeypatch)
    ex.context=dict(ex.context,head=head(height,byte))
    try:
        with pytest.raises(ExecutionAnchorChanged): execute(handoff)
        assert ex.context_kick_sequence==0
        assert 'execution_context_sync' not in candidate
        assert not main.requests and not rpc.batches
    finally: rpc.release.set()


def test_short_original_deadline_is_not_reset_or_extended(handoff,monkeypatch):
    ex,main,candidate,*_=handoff
    rpc=worker(handoff,monkeypatch)
    deadline=time.monotonic_ns()+100_000_000
    try:
        with pytest.raises(Reject,match='observation expired'): execute(handoff,deadline=deadline)
        details=candidate['execution_context_sync']
        assert 0<details['max_wait_ms']<=100
        assert details['outcome']=='expired_or_cancelled'
        assert time.monotonic_ns()>=deadline
        assert ex.context_kick_sequence==1 and not main.requests
        assert ex.db.execute('select count(*) from attempts').fetchone()[0]==0
    finally: rpc.release.set()


def test_stop_interrupts_wait_and_prevents_late_context_publication(handoff,monkeypatch):
    ex,main,candidate,*_=handoff
    rpc=worker(handoff,monkeypatch)
    previous=ex.context
    controller,errors=release_after_kick(ex,rpc,ex.context_stop.set)
    try:
        with pytest.raises(Reject,match='stopped|cancelled'): execute(handoff)
        ex.context_thread.join(1)
        assert not ex.context_thread.is_alive() and rpc.closed
        assert ex.context is previous
        assert candidate['execution_context_sync']['outcome']=='stopped'
        assert not main.requests and ex.db.execute('select count(*) from attempts').fetchone()[0]==0
    finally:
        rpc.release.set();controller.join(1)
    assert not errors and not controller.is_alive()


def test_late_older_full_response_cannot_overwrite_newer_context(handoff):
    ex,*_=handoff
    old=BlockedAccountRPC(height=100,nonce=6)
    errors=[]
    def refresh_old():
        try: ex.refresh_context(old)
        except BaseException as error: errors.append(error)
    thread=threading.Thread(target=refresh_old)
    thread.start()
    try:
        assert old.entered.wait(1)
        ex.refresh_context(AccountRPC(height=101,nonce=9,balance=123456,oracle_fee=456))
        fresh=ex.context
        old.release.set();thread.join(1)
        assert ex.context is fresh
        assert ex.context['nonce']==9 and ex.context['balance']==123456
        assert ex.context['extra_fee']==1824
    finally: old.release.set();thread.join(1)
    assert not errors and not thread.is_alive()


def test_bounded_wait_times_out_without_blocking_for_worker_rpc(handoff,monkeypatch):
    ex,main,candidate,*_=handoff
    rpc=worker(handoff,monkeypatch)
    try:
        with pytest.raises(ExecutionAnchorChanged): execute(handoff)
        details=candidate['execution_context_sync']
        assert details['max_wait_ms']==500
        assert details['outcome']=='timeout'
        assert ex.context_kick_sequence==1
        assert rpc.entered.is_set() and not rpc.release.is_set()
        assert not main.requests and ex.db.execute('select count(*) from attempts').fetchone()[0]==0
    finally: rpc.release.set()


def test_matched_context_is_still_rejected_when_its_account_data_is_stale(handoff,monkeypatch):
    import searcher.execution as execution
    ex,main,candidate,*_=handoff
    real_time=execution.time
    offset=[0]
    class ShiftedClock:
        def monotonic_ns(self): return real_time.monotonic_ns()+offset[0]
        def __getattr__(self,name): return getattr(real_time,name)
    monkeypatch.setattr(execution,'time',ShiftedClock())
    rpc=worker(handoff,monkeypatch)
    # Hold any follow-up worker read: the already-inflight old request is the
    # specific generation whose age this regression needs to exercise.
    original_batch=rpc.batch
    def one_response(calls):
        if rpc.batches:
            while not ex.context_stop.wait(.01): pass
            rpc.check_cancelled()
        return original_batch(calls)
    monkeypatch.setattr(rpc,'batch',one_response)
    # The account request was already old before this candidate arrived;
    # synchronization itself is still allowed its unchanged bounded window.
    offset[0]=1_600_000_000
    controller,errors=release_after_kick(ex,rpc)
    try:
        with pytest.raises(Reject,match='execution context stale'):
            execute(handoff,deadline=execution.time.monotonic_ns()+2_000_000_000)
        assert ex.context['head']['hash']==head(101)['hash']
        assert candidate['execution_context_sync']['outcome']=='caught_up'
        assert not main.requests and ex.db.execute('select count(*) from attempts').fetchone()[0]==0
    finally:
        rpc.release.set();controller.join(1)
    assert not errors and not controller.is_alive()


def test_successful_context_handoff_never_retries_a_whole_call_revert(handoff,monkeypatch):
    ex,main,candidate,*_=handoff
    rpc=worker(handoff,monkeypatch)
    calls=[]
    def revert(method,params):
        calls.append(method)
        raise RPCError(method,{'code':3,'message':'execution reverted','data':data('BadCallback()')})
    monkeypatch.setattr(main,'request',revert)
    controller,errors=release_after_kick(ex,rpc)
    try:
        with pytest.raises(RPCError): execute(handoff)
        assert calls==['eth_call']
        assert ex.context_kick_sequence==1
        assert candidate['execution_context_sync']['outcome']=='caught_up'
        assert ex.db.execute('select count(*) from attempts').fetchone()[0]==0
    finally:
        rpc.release.set();controller.join(1)
    assert not errors and not controller.is_alive()
