"""Transport timing with an in-memory async session; no sockets or credentials."""
import asyncio
import json
import threading

import httpx
import pytest

from searcher.core import RPC, RPCError, RPCUnavailable, Reject


class Clock:
    def __init__(self): self.now = 100.0
    def advance(self, seconds): self.now += seconds


class Session:
    def __init__(self, clock, statuses=(200,), headers=.011, body=.013, failure=None):
        self.clock, self.statuses = clock, iter(statuses)
        self.headers, self.body, self.failure = headers, body, failure
        self.payloads = []

    def stream(self, method, url, json):
        self.payloads.append(json)
        session = self
        class Stream:
            async def __aenter__(self):
                session.clock.advance(session.headers)
                if session.failure == 'headers':
                    raise httpx.ReadTimeout('synthetic failure https://example.invalid/secret')
                status = next(session.statuses)
                rows = json if isinstance(json, list) else [json]
                values = [{'jsonrpc':'2.0', 'id':r['id'], 'result':r['params'][0] if r['params'] else '0x2105'}
                          for r in reversed(rows)]
                class Response(httpx.Response):
                    async def aread(self):
                        session.clock.advance(session.body)
                        if session.failure == 'body':
                            raise httpx.ReadTimeout('synthetic body failure')
                        return self.content
                return Response(status, json=values if isinstance(json,list) else values[0],
                                request=httpx.Request('POST','https://example.invalid/secret'))
            async def __aexit__(self, *args): pass
        return Stream()

    async def aclose(self): pass


class Limiter:
    def __init__(self, clock, delay=.007): self.clock, self.delay, self.calls = clock, delay, []
    def acquire(self, count, cancelled=None, submission=False, critical=False):
        self.calls.append((count,submission,critical))
        self.clock.advance(self.delay)


def test_opportunity_deadline_aborts_network_and_does_not_retry(transport):
    rpc,clock=transport
    rpc.session=Session(clock,statuses=(200,),headers=.011,body=.050)
    rpc.deadline_ns=round((clock.now+.020)*1e9)
    with pytest.raises(RPCUnavailable,match='opportunity deadline'):
        rpc.request('eth_chainId',[])
    assert len(rpc.session.payloads)==1


def test_opportunity_rate_limit_returns_without_backoff_sleep(transport):
    rpc,clock=transport
    rpc.session=Session(clock,statuses=(429,200))
    rpc.deadline_ns=round((clock.now+.2)*1e9)
    with pytest.raises((RPCUnavailable,httpx.HTTPStatusError)):
        rpc.request('eth_chainId',[])
    assert len(rpc.session.payloads)==1 and clock.now<100.1


@pytest.fixture
def transport(monkeypatch):
    rpc = RPC('https://example.invalid/secret', timeout=15)
    rpc.loop.run_until_complete(rpc.session.aclose())
    clock = Clock()
    monkeypatch.setattr('searcher.core.time.monotonic', lambda:clock.now)
    monkeypatch.setattr('searcher.core.time.monotonic_ns', lambda:round(clock.now*10**9))
    monkeypatch.setattr('searcher.core.time.sleep', clock.advance)
    rpc.session = Session(clock)
    try: yield rpc,clock
    finally: rpc.close()


def test_batch_separates_interval_quota_headers_and_body(transport):
    rpc,clock = transport
    rpc.minimum_request_interval = .005
    rpc.last_post_at = clock.now
    limiter = rpc.public_rate_limiter = Limiter(clock)
    rpc.critical_reads = True
    assert rpc.batch([('eth_call',['first']),('eth_getBlockByNumber',['second'])]) == ['first','second']
    timing = rpc.measurements[-1]
    assert timing['minimum_interval_wait_ms'] == pytest.approx(5)
    assert timing['public_limiter_wait_ms'] == pytest.approx(7)
    assert timing['network_headers_ms'] == pytest.approx(11)
    assert timing['response_headers_ms'] == pytest.approx(11)
    assert timing['network_body_ms'] == pytest.approx(13)
    assert timing['network_total_ms'] == pytest.approx(24)
    assert [timing['post_total_ms'],timing['elapsed_ms']] == pytest.approx([36,36])
    assert timing['attempts'] == 1 and timing['timing_complete'] and timing['outcome'] == 'ok'
    assert limiter.calls == [(2,False,True)]
    assert 'secret' not in json.dumps(timing) and 'example.invalid' not in json.dumps(timing)


def test_request_sums_attempts_and_keeps_retry_sleep_out_of_network(transport):
    rpc,clock = transport
    rpc.session = Session(clock,statuses=(503,200))
    rpc.public_rate_limiter = Limiter(clock)
    assert rpc.request('eth_chainId',[]) == '0x2105'
    timing = rpc.measurements[-1]
    assert timing['attempts'] == 2
    assert timing['public_limiter_wait_ms'] == pytest.approx(14)
    assert timing['network_headers_ms'] == pytest.approx(22)
    assert timing['network_body_ms'] == pytest.approx(26)
    assert timing['post_total_ms'] == pytest.approx(62)
    assert timing['retry_wait_ms'] == pytest.approx(1000)
    assert timing['elapsed_ms'] == pytest.approx(1062)
    assert [t['http_status'] for t in timing['attempt_timings']] == [503,200]
    assert len(rpc.measurements) == 1


@pytest.mark.parametrize('batched',[False,True])
@pytest.mark.parametrize('headers_extension',[False,True])
def test_plain_monkeypatched_response_keeps_unknown_phases_and_no_stale_data(transport,monkeypatch,batched,headers_extension):
    rpc,clock = transport
    rpc.request('eth_chainId',[])  # Its real phase data must not leak into the fake.
    rpc.measurements.clear()
    def post(payload):
        clock.advance(.020)
        values = [{'id':r['id'],'result':'ok'} for r in payload] if batched else {'id':1,'result':'ok'}
        return httpx.Response(200,json=values,request=httpx.Request('POST','https://example.invalid'),
                              extensions={'headers_elapsed_ms':3} if headers_extension else {})
    monkeypatch.setattr(rpc,'post',post)
    result = rpc.batch([('eth_call',[])]) if batched else rpc.request('eth_call',[])
    assert result == (['ok'] if batched else 'ok')
    timing = rpc.measurements[-1]
    assert [timing['elapsed_ms'],timing['post_total_ms']] == pytest.approx([20,20])
    assert timing['network_headers_ms'] == (3 if headers_extension else None)
    for key in ('minimum_interval_wait_ms','public_limiter_wait_ms','network_body_ms','network_total_ms'):
        assert timing[key] is None
    assert not timing['timing_complete']


@pytest.mark.parametrize('phase',['headers','body'])
@pytest.mark.parametrize('batched',[False,True])
def test_network_failure_keeps_the_phase_spent_without_endpoint(transport,phase,batched):
    rpc,clock = transport
    rpc.session = Session(clock,failure=phase)
    with pytest.raises(httpx.ReadTimeout):
        if batched: rpc.batch([('eth_call',[])])
        else: rpc.request('eth_call',[])
    timing = rpc.measurements[-1]
    assert timing['outcome'] == 'error' and timing['attempts'] == 1
    assert timing['network_headers_ms'] == pytest.approx(11)
    assert timing['network_body_ms'] == pytest.approx(13 if phase=='body' else 0)
    elapsed = 24 if phase=='body' else 11
    assert [timing['network_total_ms'],timing['elapsed_ms']] == pytest.approx([elapsed,elapsed])
    assert 'secret' not in json.dumps(timing)


def test_cancel_during_local_wait_records_only_actual_wait_and_never_opens_transport(transport):
    rpc,clock = transport
    class Stop:
        cancelled = False
        def is_set(self): return self.cancelled
        def wait(self, seconds):
            clock.advance(.002); self.cancelled = True
    rpc.cancel_event = Stop()
    rpc.minimum_request_interval, rpc.last_post_at = .050, clock.now
    with pytest.raises(Reject,match='cancelled'): rpc.request('eth_chainId',[])
    timing = rpc.measurements[-1]
    assert [timing['minimum_interval_wait_ms'],timing['elapsed_ms']] == pytest.approx([2,2])
    assert timing['network_total_ms'] == timing['public_limiter_wait_ms'] == 0
    assert not rpc.session.payloads


def test_cancel_during_retry_records_partial_wait_not_requested_delay(transport):
    rpc,clock = transport
    class Stop:
        cancelled = False
        def is_set(self): return self.cancelled
        def wait(self, seconds):
            assert seconds == 1
            clock.advance(.006); self.cancelled = True
    rpc.cancel_event = Stop()
    rpc.session = Session(clock,statuses=(503,))
    with pytest.raises(Reject,match='cancelled'): rpc.request('eth_chainId',[])
    timing = rpc.measurements[-1]
    assert timing['retry_wait_ms'] == pytest.approx(6)
    assert timing['network_total_ms'] == pytest.approx(24)
    assert timing['elapsed_ms'] == pytest.approx(30)
    assert timing['attempts'] == 1


def test_cancel_during_public_quota_records_wait_without_network(transport):
    rpc,clock = transport
    class CancelledLimiter:
        def acquire(self,*args,**kwargs):
            clock.advance(.004)
            raise Reject('RPC cancelled')
    rpc.public_rate_limiter = CancelledLimiter()
    with pytest.raises(Reject,match='cancelled'): rpc.batch([('eth_call',[])])
    timing = rpc.measurements[-1]
    assert timing['public_limiter_wait_ms'] == pytest.approx(4)
    assert timing['network_total_ms'] == timing['minimum_interval_wait_ms'] == 0
    assert not rpc.session.payloads


@pytest.mark.parametrize('phase',['headers','body'])
@pytest.mark.parametrize('mode',['cancel','deadline'])
def test_in_memory_pending_io_cleans_up_and_records_elapsed_phase(phase,mode):
    stop = threading.Event()
    rpc = RPC('https://example.invalid/secret',timeout=.03 if mode=='deadline' else 1,cancel_event=stop)
    rpc.loop.run_until_complete(rpc.session.aclose())
    cleaned = []
    async def stall():
        if mode == 'cancel': asyncio.get_running_loop().call_later(.002,stop.set)
        try: await asyncio.Event().wait()
        finally: cleaned.append(True)
    class PendingSession:
        def stream(self,*args,**kwargs):
            class Stream:
                async def __aenter__(self):
                    if phase == 'headers': await stall()
                    class Body(httpx.Response):
                        async def aread(self): await stall()
                    return Body(200,request=httpx.Request('POST','https://example.invalid'))
                async def __aexit__(self,*args): pass
            return Stream()
        async def aclose(self): pass
    rpc.session = PendingSession()
    try:
        with pytest.raises(Reject if mode=='cancel' else RPCUnavailable,
                           match='cancelled' if mode=='cancel' else 'deadline'):
            rpc.request('eth_chainId',[])
        timing = rpc.measurements[-1]
        assert cleaned and timing['outcome'] == 'error'
        assert timing['network_total_ms'] > 0
        assert timing['network_body_ms' if phase=='body' else 'network_headers_ms'] > 0
        assert timing['minimum_interval_wait_ms'] == timing['public_limiter_wait_ms'] == 0
        assert not asyncio.all_tasks(rpc.loop)
    finally: rpc.close()


@pytest.mark.parametrize('critical,expected',[(False,[18,13]),(True,[21,10])])
def test_chunked_batch_has_one_whole_call_measurement_and_unchanged_order(transport,critical,expected):
    rpc,clock = transport
    rpc.critical_reads = critical
    limiter = rpc.public_rate_limiter = Limiter(clock)
    rpc.session = Session(clock,statuses=(200,200))
    assert rpc.batch([('eth_call',[i]) for i in range(31)]) == list(range(31))
    assert [len(p) for p in rpc.session.payloads] == expected
    assert limiter.calls == [(n,False,critical) for n in expected]
    assert len(rpc.measurements) == 1
    timing = rpc.measurements[-1]
    assert len(timing['methods']) == 31 and timing['attempts'] == 2
    assert timing['network_total_ms'] == pytest.approx(48)
    assert timing['public_limiter_wait_ms'] == pytest.approx(14)
    assert timing['elapsed_ms'] == pytest.approx(62)


def test_empty_measurements_pre_cancel_and_error_result_remain_compatible(transport,monkeypatch):
    rpc,clock = transport
    assert not rpc.measurements
    def post(payload):
        return httpx.Response(200,json={'id':1,'error':{'code':3,'message':'execution reverted','data':'0x12345678'}},
                              request=httpx.Request('POST','https://example.invalid'))
    monkeypatch.setattr(rpc,'post',post)
    with pytest.raises(RPCError) as caught: rpc.request('eth_call',[])
    assert caught.value.revert_data == '0x12345678'
    assert rpc.measurements[-1]['outcome'] == 'error'
    class Stop:
        def is_set(self): return True
    rpc.cancel_event = Stop()
    with pytest.raises(Reject,match='cancelled'): rpc.request('eth_chainId',[])
    timing = rpc.measurements[-1]
    assert timing['attempts'] == timing['network_total_ms'] == 0
    assert timing['elapsed_ms'] == 0 and timing['outcome'] == 'error'
