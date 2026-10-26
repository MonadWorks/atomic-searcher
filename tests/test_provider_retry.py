"""Bounded in-place provider retries use the shared gate and retain progress."""
import json

import httpx
import pytest

from searcher.core import RPC, RPCError, Reject, batch_request_digest
from searcher.statefeed import complete_batch_observation_start
from test_provider_budget import provider_transport
from test_rpc_timing import transport, Session


@pytest.mark.parametrize('status', [429,503])
def test_read_retries_same_call_through_shared_gate_without_second_sleep(transport, status):
    rpc, _ = provider_transport(transport,statuses=(status,200),retry_after='3')
    assert rpc.request('eth_getBlockByNumber',['0x123',False]) == '0x123'
    assert rpc.session.payloads[0] == rpc.session.payloads[1]
    timing = rpc.measurements[-1]
    assert timing['attempts'] == 2 and timing['retry_wait_ms'] == 0
    assert timing['public_limiter_wait_ms'] >= 3000-1e-4
    a,b = timing['attempt_timings']
    assert b['network_started_ns'] >= a['network_finished_ns']+3_000_000_000
    assert rpc.provider_budget.active is None and not rpc.provider_budget.queue


def test_middle_split_only_is_retried_and_successful_splits_are_retained(transport):
    rpc, _ = provider_transport(transport,statuses=(200,429,200,200))
    calls = [('eth_call',[n]) for n in range(7)]
    assert rpc.batch(calls) == list(range(7))
    sent = [[row['params'][0] for row in payload] for payload in rpc.session.payloads]
    assert sent == [[0,1,2],[3,4,5],[3,4,5],[6]]
    timing = rpc.measurements[-1]
    assert timing['attempts'] == 4 and timing['outcome'] == 'ok'
    assert timing['batch_request_digest'] == batch_request_digest(calls)
    assert [t['http_status'] for t in timing['attempt_timings']] == [200,429,200,200]
    assert timing['retry_wait_ms'] == 0


@pytest.mark.parametrize('batch', [False,True])
def test_four_attempt_exhaustion_retains_all_timings_and_releases_gate(transport,batch):
    rpc, _ = provider_transport(transport,statuses=(429,)*4)
    with pytest.raises(httpx.HTTPStatusError):
        if batch: rpc.batch([('eth_call',[n]) for n in range(7)])
        else: rpc.request('eth_getLogs',[{}])
    assert len(rpc.session.payloads) == 4
    if batch: assert all([r['params'][0] for r in p] == [0,1,2] for p in rpc.session.payloads)
    timing = rpc.measurements[-1]
    assert timing['attempts'] == len(timing['attempt_timings']) == 4
    assert timing['outcome'] == 'error' and timing['retry_wait_ms'] == 0
    assert all(t['http_status'] == 429 for t in timing['attempt_timings'])
    assert rpc.provider_budget.active is None and not rpc.provider_budget.queue


@pytest.mark.parametrize('batch', [False,True])
def test_extended_write_method_is_never_replayed(transport,batch):
    rpc, _ = provider_transport(transport,statuses=(429,200))
    rpc.METHODS = set(RPC.METHODS) | {'eth_sendRawTransaction'}
    with pytest.raises(httpx.HTTPStatusError):
        if batch: rpc.batch([('eth_chainId',[]),('eth_sendRawTransaction',['synthetic'])])
        else: rpc.request('eth_sendRawTransaction',['synthetic'])
    assert len(rpc.session.payloads) == 1


@pytest.mark.parametrize('status', [429,502,503,504])
@pytest.mark.parametrize('managed', [False,True])
def test_write_does_not_enter_generic_retry_for_any_transport(transport,status,managed):
    rpc, clock = provider_transport(transport,statuses=(status,200))
    if not managed: rpc.provider_budget = None
    rpc.METHODS = set(RPC.METHODS) | {'eth_sendRawTransaction'}
    with pytest.raises(httpx.HTTPStatusError): rpc.request('eth_sendRawTransaction',['synthetic'])
    assert len(rpc.session.payloads) == 1 and rpc.measurements[-1]['retry_wait_ms'] == 0


def test_any_write_in_whole_batch_disables_later_read_split_retry(transport):
    rpc, _ = provider_transport(transport,statuses=(200,429,200))
    rpc.METHODS = set(RPC.METHODS) | {'eth_sendRawTransaction'}
    calls = [('eth_sendRawTransaction',['synthetic'])]+[('eth_call',[n]) for n in range(4)]
    with pytest.raises(httpx.HTTPStatusError): rpc.batch(calls)
    assert len(rpc.session.payloads) == 2
    assert rpc.session.payloads[0][0]['method'] == 'eth_sendRawTransaction'
    assert [row['params'][0] for row in rpc.session.payloads[1]] == [2,3]


def test_prepared_retry_rebinds_each_payload_and_retains_earliest_age(transport):
    rpc, clock = provider_transport(transport,statuses=(429,200))
    calls = [('eth_call',[]),('eth_getBlockByNumber',[]),('eth_getBlockByNumber',[])]
    count = []
    def prepare():
        count.append(clock.now)
        return [('eth_call',['clock-'+str(len(count))]),('eth_getBlockByNumber',['latest']),
                ('eth_getBlockByNumber',['pending'])]
    start = round(clock.now*1e9)
    values, actual = rpc.batch_prepared(calls,prepare)
    received = round(clock.now*1e9)
    assert values == ['clock-2','latest','pending'] and len(count) == 2
    assert count[1]-count[0] >= 2
    sent = [[(r['method'],r['params']) for r in p] for p in rpc.session.payloads]
    assert sent[0] != sent[1] and actual == sent[1]
    timing = rpc.measurements[-1]
    assert timing['batch_request_digest'] == batch_request_digest(actual)
    assert [t['batch_request_digest'] for t in timing['attempt_timings']] == [batch_request_digest(p) for p in sent]
    observed = complete_batch_observation_start(timing,actual,start,received)
    assert observed == timing['attempt_timings'][0]['network_started_ns']
    assert observed < timing['attempt_timings'][1]['network_started_ns']
    assert received-observed > 2_000_000_000, 'Cooldown was erased from observation age.'
    assert complete_batch_observation_start(timing,sent[0],start,received) is None


@pytest.mark.parametrize('prepared', [False,True])
def test_cancelling_long_shared_retry_wait_sends_no_second_request(transport,prepared):
    rpc, clock = provider_transport(transport,statuses=(429,200),retry_after='90')
    class CancelOnWait:
        stopped = False
        def is_set(self): return self.stopped
        def wait(self, seconds): self.stopped = True
    rpc.cancel_event = CancelOnWait()
    prepared_calls=[]
    def prepare():
        prepared_calls.append(True)
        return [('eth_call',[])]
    with pytest.raises(Reject,match='RPC cancelled'):
        if prepared: rpc.batch_prepared([('eth_call',[])],prepare)
        else: rpc.request('eth_chainId',[])
    assert len(rpc.session.payloads) == 1
    assert len(prepared_calls) == int(prepared)
    assert rpc.provider_budget.active is None and not rpc.provider_budget.queue
    assert rpc.provider_budget.cooldown_until-clock.now == pytest.approx(90)


@pytest.mark.parametrize('failure', ['http503_without_retry_after','rpc_error','network'])
def test_batch_does_not_generalize_retry_to_other_failures(transport,failure):
    rpc, clock = provider_transport(transport,statuses=(503 if failure=='http503_without_retry_after' else 200,))
    expected = httpx.HTTPStatusError
    if failure == 'network':
        rpc.session = Session(clock,failure='headers');expected=httpx.ReadTimeout
    elif failure == 'rpc_error':
        post=rpc.post
        def semantic_error(payload):
            response=post(payload)
            response._content=json.dumps([{'id':0,'error':{'code':-32000,'message':'synthetic semantic error'}}]).encode()
            return response
        rpc.post=semantic_error;expected=RPCError
    with pytest.raises(expected): rpc.batch([('eth_call',[])])
    assert len(rpc.session.payloads) == 1


def test_prepared_retry_still_rejects_second_attempt_method_mutation(transport):
    rpc, _ = provider_transport(transport,statuses=(429,200))
    count=[]
    def prepare():
        count.append(True)
        return [('eth_call',[])] if len(count)==1 else [('eth_getBalance',[])]
    with pytest.raises(Reject,match='methods changed'):
        rpc.batch_prepared([('eth_call',[])],prepare)
    assert len(count)==2 and len(rpc.session.payloads)==1
    assert rpc.provider_budget.active is None
