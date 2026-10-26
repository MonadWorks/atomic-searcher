from collections import deque
import copy

from eth_abi import encode
import pytest

from searcher.core import load_config, data
from searcher.execution import Executor, ORACLE


def values_for(calls, malformed_rewards=False):
    result = []
    for method, params in calls:
        if method == 'eth_getTransactionCount': result.append('0x0')
        elif method == 'eth_getBalance': result.append(hex(10**18))
        elif method == 'eth_getBlockByNumber':
            result.append({'number':hex(101 if params[0]=='pending' else 100),
                'timestamp':hex(202 if params[0]=='pending' else 200), 'hash':'0x'+'11'*32,
                'parentHash':'0x'+'11'*32})
        elif method == 'eth_call':
            types = ['uint256'] if params[0]['to']==ORACLE else ['uint256','uint160','uint32','uint256']
            result.append('0x'+encode(types, [100]*len(types)).hex())
        elif method == 'eth_feeHistory': result.append({'reward':[] if malformed_rewards else [['0x1']]})
        else: raise AssertionError(method)
    return result


def executor_with_timing(monkeypatch, mode='actual', malformed_rewards=False):
    now = [10**12]
    monkeypatch.setattr('searcher.execution.time.monotonic_ns', lambda:now[0])
    class Transport:
        def __init__(self): self.measurements=deque()
        def batch(self, calls):
            start = now[0]
            now[0] += 1_600_000_000  # Local limiter wait, before queries are sent.
            send = now[0]
            now[0] += 200_000_000
            record = dict(method='batch', outcome='ok', timing_complete=True,
                          network_started_ns=send, network_finished_ns=now[0])
            if mode=='missing': record.pop('network_started_ns')
            elif mode=='future': record['network_started_ns']=now[0]+1
            elif mode=='incomplete': record['timing_complete']=False
            elif mode=='before_refresh': record['network_started_ns']=start-1
            elif mode=='wrong_method': record['method']='eth_call'
            if mode!='same_object': self.measurements.append(record)
            return values_for(calls,malformed_rewards)
        def batch_contract_reads(self,calls,approved): return self.batch(calls)
    e = Executor.__new__(Executor)
    e.config=load_config(); e.config['policy']['profit_aware_bidding']=True
    e.owner=e.config['owner']; e.rpc=Transport(); e.context={'old':True}; e.context_error='prior'
    return e,now


def test_context_age_starts_at_real_send_and_local_queue_time_remains_visible(monkeypatch):
    e, now=executor_with_timing(monkeypatch)
    e.refresh_context()
    c=e.context
    assert now[0]-c['requested_ns']==200_000_000
    assert c['requested_ns']-c['refresh_started_ns']==1_600_000_000
    assert c['freshness_origin']=='network_send'
    # Transport wait is excluded; actual on-wire age still grows without renewal.
    now[0]+=1_301_000_000
    assert (now[0]-c['requested_ns'])/1e6>1500


@pytest.mark.parametrize('mode',['missing','future','incomplete','before_refresh','wrong_method','same_object'])
def test_missing_unbound_or_impossible_measurement_uses_conservative_refresh_start(monkeypatch,mode):
    e,now=executor_with_timing(monkeypatch,mode)
    if mode=='same_object':
        e.rpc.measurements.append(dict(method='batch',outcome='ok',timing_complete=True,
            network_started_ns=now[0]+1_600_000_000,network_finished_ns=now[0]+1_800_000_000))
    e.refresh_context()
    assert e.context['requested_ns']==e.context['refresh_started_ns']
    assert e.context['freshness_origin']=='refresh_start'


def test_failed_reward_parsing_never_publishes_partially_checked_context(monkeypatch):
    e,now=executor_with_timing(monkeypatch,malformed_rewards=True)
    original=e.context
    with pytest.raises(Exception,match='fee history rewards'):
        e.refresh_context()
    assert e.context is original

from test_rpc_timing import transport, Limiter, Session


def test_whole_batch_start_uses_first_fragment_after_its_queue_wait(transport):
    rpc,clock=transport
    rpc.session=Session(clock,statuses=(200,200,200))
    rpc.public_rate_limiter=Limiter(clock,delay=.6)
    assert len(rpc.batch([('eth_call',[i]) for i in range(40)]))==40
    timing=rpc.measurements[-1]
    starts=[x['network_started_ns'] for x in timing['attempt_timings']]
    ends=[x['network_finished_ns'] for x in timing['attempt_timings']]
    assert len(starts)==3 and starts[0]<starts[1]<starts[2]
    assert timing['network_started_ns']==starts[0]==100_600_000_000
    assert timing['network_finished_ns']==ends[-1]
    assert (timing['network_finished_ns']-timing['network_started_ns'])/1e6>1200


def test_retry_does_not_reset_freshness_start_to_last_successful_response(transport):
    rpc,clock=transport
    rpc.session=Session(clock,statuses=(503,200))
    rpc.public_rate_limiter=Limiter(clock,delay=.1)
    rpc.request('eth_chainId',[])
    timing=rpc.measurements[-1]
    assert timing['network_started_ns']==timing['attempt_timings'][0]['network_started_ns']
    assert timing['network_started_ns']<timing['attempt_timings'][1]['network_started_ns']
