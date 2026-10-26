"""Shared public dRPC coordination; real transport code, no network sockets."""
import asyncio
import copy
from email.utils import formatdate
import json
import threading
import time

import httpx
import pytest

from searcher.core import PublicProviderBudget, RPC, Reject, batch_request_digest
from test_rpc_timing import transport, Session


def provider_transport(transport, statuses=(200,), retry_after=None):
    rpc, clock = transport
    rpc.provider_budget = PublicProviderBudget()
    class Responses(Session):
        def stream(self, *args, **kwargs):
            stream = super().stream(*args, **kwargs)
            class Wrapped:
                async def __aenter__(self):
                    response = await stream.__aenter__()
                    if retry_after is not None: response.headers['Retry-After'] = str(retry_after)
                    return response
                async def __aexit__(self, *args): return await stream.__aexit__(*args)
            return Wrapped()
    rpc.session = Responses(clock, statuses=statuses)
    return rpc, clock


def test_public_hostname_instances_share_gate_but_other_endpoints_do_not():
    connections = [RPC(url) for url in ('https://base.drpc.org', 'https://base.drpc.org/',
                                      'https://mainnet.base.org', 'https://private.invalid')]
    try:
        a,b,main,other = connections
        assert a.provider_budget is b.provider_budget
        assert main.provider_budget is other.provider_budget is None
        assert main.public_rate_limiter is not None
        assert a.public_rate_limiter is b.public_rate_limiter is None
    finally:
        for rpc in connections: rpc.close()


def test_429_read_retry_uses_shared_cooldown_and_recovers_gradually(transport):
    rpc, clock = provider_transport(transport, statuses=(429,200)+((200,)*7))
    before = clock.now
    assert rpc.request('eth_chainId', []) == '0x2105'
    assert len(rpc.session.payloads) == 2 and rpc.measurements[-1]['attempts'] == 2
    assert rpc.measurements[-1]['retry_wait_ms'] == 0
    assert rpc.measurements[-1]['attempt_timings'][0]['provider_budget']['cooldown_seconds'] == pytest.approx(2)
    assert clock.now-before >= 2
    assert rpc.provider_budget.rate == 1 and rpc.provider_budget.throttles == 1
    for _ in range(7): rpc.request('eth_chainId', [])
    assert rpc.provider_budget.rate == 1.25 and rpc.provider_budget.throttles == 0
    assert rpc.provider_budget.snapshot()['quota_claim'] is False


@pytest.mark.parametrize('as_date', [False,True])
def test_long_retry_after_applies_to_new_requests_and_exceeds_thirty_seconds(transport, monkeypatch, as_date):
    rpc, clock = transport
    monkeypatch.setattr('searcher.core.time.time', lambda:1_800_000_000)
    value = formatdate(1_800_000_090,usegmt=True) if as_date else '90'
    rpc, clock = provider_transport(transport, statuses=(429,200), retry_after=value)
    assert rpc.request('eth_chainId', []) == '0x2105'
    attempts = rpc.measurements[-1]['attempt_timings']
    assert len(attempts) == 2 and attempts[0]['provider_budget']['cooldown_seconds'] == pytest.approx(90)
    assert attempts[1]['network_started_ns'] >= attempts[0]['network_finished_ns']+90_000_000_000
    assert rpc.measurements[-1]['public_limiter_wait_ms'] >= 90_000-1e-4


@pytest.mark.parametrize('value', ['bad','NaN','inf','-2'])
def test_invalid_retry_after_uses_finite_shared_backoff(transport, value):
    rpc, clock = provider_transport(transport, statuses=(429,)*4, retry_after=value)
    with pytest.raises(httpx.HTTPStatusError): rpc.request('eth_chainId', [])
    assert rpc.provider_budget.cooldown_until-clock.now == pytest.approx(16)
    assert len(rpc.session.payloads) == 4


def test_503_retry_after_is_also_shared(transport):
    rpc, clock = provider_transport(transport, statuses=(503,)*4, retry_after='7')
    with pytest.raises(httpx.HTTPStatusError): rpc.request('eth_chainId', [])
    assert len(rpc.session.payloads) == 4
    assert rpc.provider_budget.cooldown_until-clock.now == pytest.approx(16)


def test_incomplete_200_bodies_never_promote_provider_recovery(transport):
    rpc, clock = provider_transport(transport)
    budget = rpc.provider_budget
    ticket = budget.acquire(1); budget.finish(ticket,429)
    for _ in range(8):
        rpc.session = Session(clock,failure='body')
        with pytest.raises(httpx.ReadTimeout): rpc.request('eth_chainId',[])
        assert budget.successes == 0 and budget.rate == 1 and budget.throttles == 1
        assert budget.active is None


@pytest.mark.parametrize('status', [429,503])
def test_throttled_headers_keep_shared_deadline_even_when_body_fails(transport, status):
    rpc, clock = provider_transport(transport, statuses=(status,), retry_after='90')
    rpc.session.failure = 'body'
    with pytest.raises(httpx.ReadTimeout): rpc.request('eth_chainId',[])
    assert rpc.provider_budget.cooldown_until-clock.now == pytest.approx(90)
    assert rpc.provider_budget.throttles == 1 and rpc.provider_budget.active is None


def test_batch_is_split_by_methods_and_prepared_remains_one_bound_batch(transport):
    rpc, clock = provider_transport(transport, statuses=(200,)*4)
    calls = [('eth_call',[n]) for n in range(7)]
    assert rpc.batch(calls) == list(range(7))
    assert [len(p) for p in rpc.session.payloads] == [3,3,1]
    assert rpc.measurements[-1]['public_limiter_wait_ms'] == pytest.approx(2952)
    before = len(rpc.session.payloads)
    prepared = [('eth_call',['new']), ('eth_getBlockByNumber',['latest']), ('eth_getBlockByNumber',['pending'])]
    values, actual = rpc.batch_prepared([('eth_call',[]),('eth_getBlockByNumber',[]),('eth_getBlockByNumber',[])],
                                      lambda:prepared)
    assert len(rpc.session.payloads) == before+1 and actual == prepared
    assert values == ['new','latest','pending']
    assert rpc.measurements[-1]['batch_request_digest'] == batch_request_digest(actual)
    assert rpc.measurements[-1]['network_started_ns'] >= rpc.measurements[-1]['batch_started_ns']
    with pytest.raises(Reject, match='pacing capacity'):
        rpc.batch_prepared([('eth_call',[])]*4, lambda:[])


def test_batch_429_publishes_cooldown_without_retrying_remaining_chunks(transport):
    rpc, clock = provider_transport(transport, statuses=(429,)*4, retry_after='12')
    with pytest.raises(httpx.HTTPStatusError): rpc.batch([('eth_call',[])]*7)
    assert len(rpc.session.payloads) == 4
    assert all(len(payload) == 3 for payload in rpc.session.payloads)
    assert rpc.provider_budget.cooldown_until-clock.now == pytest.approx(16)
    assert rpc.provider_budget.active is None


@pytest.mark.parametrize('failure', ['prepare','headers','cancel_after_prepare'])
def test_ownership_released_on_prepare_network_failure_and_cancellation(transport, failure):
    rpc, clock = provider_transport(transport)
    if failure == 'headers': rpc.session = Session(clock,failure='headers')
    stop = rpc.cancel_event = threading.Event()
    def prepare():
        if failure == 'prepare': raise ValueError('synthetic preparation error')
        if failure == 'cancel_after_prepare': stop.set()
        return [('eth_call',[])]
    expected = ValueError if failure == 'prepare' else httpx.ReadTimeout if failure == 'headers' else Reject
    with pytest.raises(expected): rpc.batch_prepared([('eth_call',[])],prepare)
    assert rpc.provider_budget.active is None and not rpc.provider_budget.queue
    if failure != 'headers': assert not rpc.session.payloads


def test_long_cooldown_wait_and_queued_followers_cancel_promptly():
    budget = PublicProviderBudget()
    ticket = budget.acquire(1);budget.finish(ticket,429,'120')
    stops, errors, threads = [], [], []
    for _ in range(3):
        stop = threading.Event();stops.append(stop)
        def run(stop=stop):
            try: budget.acquire(1,stop)
            except Exception as exc: errors.append(exc)
        thread = threading.Thread(target=run);threads.append(thread);thread.start()
    deadline = time.monotonic()+1
    while len(budget.queue)<3 and time.monotonic()<deadline: time.sleep(.001)
    assert len(budget.queue) == 3
    started = time.monotonic()
    for stop in stops: stop.set()
    for thread in threads: thread.join(.5)
    assert all(not t.is_alive() for t in threads)
    assert time.monotonic()-started < .5
    assert not budget.queue and budget.active is None and len(errors) == 3
    assert all(isinstance(e,Reject) for e in errors)


@pytest.mark.parametrize('first_throttled', [False,True])
def test_three_real_rpc_workers_share_fifo_spacing_and_single_inflight(monkeypatch, first_throttled):
    lock = threading.Lock();events=[];active=[0];maximum=[0];errors=[]
    grants=[]
    class ObservedBudget(PublicProviderBudget):
        def acquire(self,count,cancelled=None):
            ticket=super().acquire(count,cancelled)
            with self.lock:
                assert self.active is ticket
                # next_at is set at admission under this lock. Measuring the
                # later async stream entry folds scheduler delay into spacing.
                grants.append(self.next_at-count/self.rate)
            return ticket
    budget = ObservedBudget(rate=50)
    monkeypatch.setattr('searcher.core._PUBLIC_DRPC_BUDGET', budget)
    class LocalSession:
        def __init__(self,label): self.label=label
        def stream(self, method, url, json):
            session=self
            class Stream:
                async def __aenter__(self):
                    with lock:
                        active[0]+=1;maximum[0]=max(maximum[0],active[0])
                        events.append((session.label,time.monotonic()))
                        status=429 if first_throttled and len(events)==1 else 200
                    await asyncio.sleep(.005)
                    return httpx.Response(status,json={'id':1,'result':'0x2105'},
                                          request=httpx.Request('POST','https://base.drpc.org'))
                async def __aexit__(self,*args):
                    with lock: active[0]-=1
            return Stream()
        async def aclose(self): pass
    conns=[]
    for i in range(3):
        rpc=RPC('https://base.drpc.org');rpc.loop.run_until_complete(rpc.session.aclose())
        rpc.session=LocalSession(i);conns.append(rpc)
    start=threading.Barrier(3)
    def worker(rpc):
        try:
            start.wait()
            for _ in range(3):
                assert rpc.request('eth_chainId',[])=='0x2105'
        except Exception as exc: errors.append(exc)
        finally: rpc.close()
    threads=[threading.Thread(target=worker,args=(rpc,)) for rpc in conns]
    for thread in threads: thread.start()
    for thread in threads: thread.join(3)
    assert not errors and all(not t.is_alive() for t in threads)
    assert len(events)==9+int(first_throttled) and maximum[0]==1 and not budget.queue and budget.active is None
    assert len(grants)==len(events)
    assert all(b-a>=.019 for a,b in zip(grants,grants[1:])), grants
    assert all(event[1]>=grant for event,grant in zip(events,grants))
    if first_throttled:
        assert events[1][1]-events[0][1]>=2
    assert set(label for label,_ in events[:3])=={0,1,2}, 'One worker monopolized the FIFO.'
