"""Independent pacing/admission tests with in-memory sockets, no network."""
from collections import deque
import json

import pytest

from searcher.activity import stream_session
from searcher.core import Reject
from searcher.stream_usage import RequestBudget, StreamSelection, SubscriptionRoster, StreamPacingPause
from test_activity import log
from test_searcher import A, B


class Clock:
    def __init__(self): self.now = 1791936000.0; self.halted = False
    def __call__(self): return self.now
    def advance(self, seconds): self.now += max(0, seconds)
    def is_set(self): return self.halted
    def wait(self, seconds): self.advance(seconds); return self.halted
    def set(self): self.halted = True


class QuietSocket:
    def __init__(self, clock):
        self.clock = clock
        self.calls, self.inbox, self.subscribed = [], deque(), set()
        self.closed = False
    def __enter__(self): return self
    def __exit__(self, *args): self.closed = True
    def send(self, raw):
        call = json.loads(raw); self.calls.append(call)
        if call['method'] == 'eth_chainId': result = '0x2105'
        elif call['method'] == 'eth_unsubscribe': result = True
        else:
            self.subscribed.update(call['params'][1]['address'])
            result = 'subscription-'+str(call['id'])
        self.inbox.append(json.dumps({'id':call['id'], 'result':result}))
    def recv(self, timeout):
        if self.inbox: return self.inbox.popleft()
        self.clock.advance(timeout)
        raise TimeoutError


def test_every_received_kind_is_durable_without_explicit_save_and_restart_credit(tmp_path):
    clock = Clock(); path = tmp_path/'usage.json'
    budget = RequestBudget(path, 80_000, clock)
    for kind in ['control', 'notification', 'invalid', 'unknown']*5:
        budget.add(31, kind=kind)
        original = dict(budget.state)
        budget = RequestBudget(path, 80_000, clock)
        assert budget.state == original
    assert budget.state['units'] == 20
    assert budget.state['pace_credit'] == 108
    assert budget.state['kinds'] == dict(control=5,notification=5,invalid=5,unknown=5)


def test_clock_rollback_does_not_grant_credit_or_clear_used_units(tmp_path):
    clock = Clock(); path = tmp_path/'usage.json'
    budget = RequestBudget(path, 80_000, clock)
    for _ in range(10): budget.add(1)
    clock.now -= 86400
    restarted = RequestBudget(path, 80_000, clock)
    assert restarted.details()['pacing_credit'] == 118
    assert restarted.state['units'] == 10
    clock.now += 2*86400
    assert restarted.details()['pacing_credit'] == 128
    assert restarted.state['units'] == 0
    restarted.add(1)
    assert RequestBudget(path, 80_000, clock).state['pace_credit'] == 127


def test_control_reply_flood_is_metered_delivered_and_physically_closes(tmp_path, monkeypatch):
    import searcher.activity as activity
    clock = Clock()
    monkeypatch.setattr(activity.time, 'monotonic', clock)
    budget = RequestBudget(tmp_path/'usage.json', 80_000, clock)
    selector = StreamSelection(80_000, clock=clock, budget=budget)
    selector.select({A})
    delivered = []
    class Flood(QuietSocket):
        sid = None
        def send(self, raw):
            call = json.loads(raw)
            if call['method'] == 'eth_unsubscribe':
                self.calls.append(call)  # Intentionally no ACK: old stream continues.
                return
            super().send(raw)
            if call['method'] == 'eth_subscribe': self.sid = 'subscription-'+str(call['id'])
        def recv(self, timeout):
            if self.inbox: return self.inbox.popleft()
            self.clock.advance(.05)
            return json.dumps({'method':'eth_subscription', 'params':{'subscription':self.sid,'result':log(A)}})
    socket = Flood(clock)
    # Buffered notifications remain deliverable while unsubscribe is pending.
    # The request's acknowledgement deadline, rather than an arbitrary count of
    # notifications, closes a peer that never acknowledges the control message.
    with pytest.raises(Reject, match='event subscription timed out'):
        stream_session('wss://example.invalid', 8453, lambda:{A}, clock, clock()+60,
            lambda logs, source:delivered.extend(logs), lambda *a,**k:None, budget,
            selector=selector, connect=lambda *a,**k:socket)
    assert socket.closed and any(c['method']=='eth_unsubscribe' for c in socket.calls)
    assert budget.state['kinds']['notification'] == len(delivered)
    assert 12 < len(delivered) < 80
    assert budget.state['kinds']['control'] == 2
    assert RequestBudget(tmp_path/'usage.json',80_000,clock).state['units'] == len(delivered)+2
    assert 3 <= clock()-1791936000 < 4


def test_unmatched_control_replies_cannot_bypass_pacing_while_awaiting_id(tmp_path, monkeypatch):
    import searcher.activity as activity
    clock = Clock()
    monkeypatch.setattr(activity.time, 'monotonic', clock)
    budget = RequestBudget(tmp_path/'usage.json', 80_000, clock)
    class StaleReplies(QuietSocket):
        def send(self, raw): self.calls.append(json.loads(raw))
        def recv(self, timeout):
            self.clock.advance(.001)
            return json.dumps({'id':999999,'result':True})
    socket = StaleReplies(clock)
    with pytest.raises(StreamPacingPause):
        stream_session('wss://example.invalid',8453,lambda:{A},clock,clock()+5,
            lambda *a:None,lambda *a,**k:None,budget,connect=lambda *a,**k:socket)
    assert socket.closed
    assert budget.state['units'] <= 130
    assert budget.state['units'] == budget.state['kinds']['control']


def test_many_individual_filters_do_not_repeat_same_paid_prefix_after_reconnect(tmp_path, monkeypatch):
    import searcher.activity as activity
    clock = Clock()
    monkeypatch.setattr(activity.time, 'monotonic', clock)
    budget = RequestBudget(tmp_path/'usage.json', 80_000, clock)
    selector = StreamSelection(80_000, clock=clock, budget=budget)
    pools = {'0x'+f'{i:040x}' for i in range(1,161)}
    # These pools previously cooled and then graduated a quiet actual probe.
    # They retain individual filters but are all eligible again.
    selector.history.update((p,dict(strikes=1,rate=20,last_probe=clock()-3600)) for p in sorted(pools))
    roster = SubscriptionRoster(clock=clock)
    seen = set()
    for _ in range(3):
        socket = QuietSocket(clock)
        try:
            stream_session('wss://example.invalid',8453,lambda:pools,clock,clock()+20,
                lambda *a:None,lambda *a,**k:None,budget,selector=selector,roster=roster,
                connect=lambda *a,**k:socket)
        except (StreamPacingPause, InterruptedError): pass
        assert socket.subscribed-seen, 'Every paid reconnect repeats the same prefix.'
        seen.update(socket.subscribed)
        assert len(socket.subscribed) <= budget.filter_capacity()
        assert socket.closed
        selector.disconnected()
        budget.pause()
        for _ in range(4):
            delay=budget.pacing_delay()
            if not delay: break
            clock.advance(delay+1)
    # Full coverage respects the real finite filter window, rather than falsely
    # promising that a reconnect subscribes every historical individual pool.
    for _ in range(len(pools)):
        clock.advance(31)
        seen.update(roster.select(pools,individual=pools))
    assert seen == pools


def test_roster_denied_probe_has_no_started_lease_or_silent_graduation():
    clock = Clock()
    selector = StreamSelection(80_000,clock=clock)
    selector.history[A] = dict(strikes=1,rate=20,last_probe=clock()-3600)
    selector.cool_until[A] = clock()-1
    selector.last_probe = clock()-100
    roster = SubscriptionRoster(capacity=1,batch_size=1,rotation_seconds=30,clock=clock)
    assert roster.select({B}) == {B}
    desired=selector.select({A,B})
    assert A in desired and roster.select(desired)=={B}
    selector.subscribed({B})
    clock.advance(11)
    selector.select({A,B})
    assert selector.probes_started == 0 and A not in selector.live
    assert A in selector.cool_until and selector.details()['pending_probe_admission']
    clock.advance(20)
    assert A in roster.select(selector.select({A,B}))
    selector.subscribed({A})
    assert selector.probes_started == 1


@pytest.mark.parametrize('state,extra', [('budget_pacing',{}), ('connected',{'http_only_pools':1})])
def test_http_fallback_remains_active_for_pacing_pause_and_partial_coverage(tmp_path, monkeypatch, state, extra):
    import searcher.activity as activity
    import searcher.core as core
    from test_runtime import setup_runtime, FakeRPC
    module, _, config = setup_runtime(monkeypatch)
    config['policy'].update(activity_priority=True, activity_poll_seconds=.01, universe_refresh_seconds=100)
    monkeypatch.setattr(core,'ROOT',tmp_path)
    monkeypatch.setattr(activity,'websocket_url',lambda:'wss://base-mainnet.core.chainstack.com/test')
    def paused(url,chain,pools,stop,finish,publish,health,budget,selector=None):
        health(state,retry_at=1791937000,**extra)
        stop.wait(.25)
    monkeypatch.setattr(activity,'run_stream',paused)
    calls=[]
    class ReadRPC(FakeRPC):
        def request(self,method,params): calls.append(method); return [log()]
    monkeypatch.setattr(module,'RPC',ReadRPC)
    result=module.run_pending(ReadRPC(),config,{},.16,tmp_path/'pipeline')
    assert calls.count('eth_getLogs') > 1
    assert result['counts']['decisions'] > 0 and result['activity_stream_stopped']


def test_invalid_received_envelope_is_charged_before_session_rejects(tmp_path, monkeypatch):
    import searcher.activity as activity
    clock=Clock()
    monkeypatch.setattr(activity.time,'monotonic',clock)
    budget=RequestBudget(tmp_path/'usage.json',80_000,clock)
    class Invalid(QuietSocket):
        def recv(self,timeout): return '{'
    socket=Invalid(clock)
    with pytest.raises(json.JSONDecodeError):
        stream_session('wss://example.invalid',8453,lambda:{A},clock,clock()+5,
            lambda *a:None,lambda *a,**k:None,budget,connect=lambda *a,**k:socket)
    assert socket.closed
    assert RequestBudget(tmp_path/'usage.json',80_000,clock).state['kinds'] == {'invalid':1}


def test_mid_startup_transport_failure_rotates_confirmed_prefix_on_actual_reconnect(tmp_path, monkeypatch):
    import searcher.activity as activity
    clock=Clock()
    monkeypatch.setattr(activity.time,'monotonic',clock)
    budget=RequestBudget(tmp_path/'usage.json',80_000,clock)
    selector=StreamSelection(80_000,clock=clock,budget=budget)
    roster=SubscriptionRoster(clock=clock,confirm_admissions=True)
    pools={'0x'+f'{i:040x}' for i in range(1,257)}
    class DropSecondFilter(QuietSocket):
        def send(self,raw):
            call=json.loads(raw)
            if call['method']=='eth_subscribe' and self.subscribed:
                raise ConnectionError('synthetic lost connection before second filter')
            super().send(raw)
    seen=set()
    for _ in range(2):
        socket=DropSecondFilter(clock)
        with pytest.raises(ConnectionError):
            stream_session('wss://example.invalid',8453,lambda:pools,clock,clock()+10,
                lambda *a:None,lambda *a,**k:None,budget,selector=selector,roster=roster,
                connect=lambda *a,**k:socket)
        assert socket.closed and len(socket.subscribed)==128
        assert socket.subscribed-seen
        seen.update(socket.subscribed)
        selector.disconnected()
        clock.advance(2)
    assert seen==pools
    assert budget.state['kinds']['control']==4


def test_history_eviction_during_live_probe_cannot_break_reconnect_selection():
    clock=[0.]
    selector=StreamSelection(80_000,clock=lambda:clock[0])
    probe='0x'+f'{1:040x}'
    old={'0x'+f'{i:040x}' for i in range(2,8193)}
    new='0x'+f'{8193:040x}'
    universe={probe,new}|old
    selector.select(universe)
    selector.subscribed({probe})
    for _ in range(4): selector.observe(probe)
    selector.unsubscribed({probe})
    # Existing bounded history is accumulated before this actual probe. Only
    # one new pool needs four events while the probe is live to force eviction.
    for pool in sorted(old): selector._cool(pool,20)
    assert len(selector.history)==8192
    clock[0]=121.
    selector.select(universe)
    assert selector.probe_candidate==probe
    selector.subscribed({probe,new})
    for _ in range(4): selector.observe(new)
    assert selector.history_evicted > 0 and len(selector.history)<=8192
    selector.disconnected()
    clock[0]=242.
    selected=selector.select(universe)
    assert set(selector.cool_until) <= set(selector.history)
    assert probe in selected or probe in selector.cool_until
