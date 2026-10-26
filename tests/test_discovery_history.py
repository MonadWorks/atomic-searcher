import copy
from collections import Counter
from types import SimpleNamespace

import pytest

from searcher import engine
from searcher.core import RPCUnavailable, Reject, load_config
from searcher.discovery_history import (plan_history, merge_ranges, pool_hint_queue,
                                        select_pool_hints, finish_pool_hints)
from searcher.events import POOL_ACTIVITY_TOPICS
from searcher.snapshots import read_universe, write_universe
from test_receipt_eligibility import pools


def h(n):
    return {'number': hex(n), 'hash': '0x'+f'{n:064x}', 'parentHash': '0x'+f'{n-1:064x}'}


def log(n, address):
    return {'blockNumber': hex(n), 'blockHash': h(n)['hash'],
            'transactionHash': '0x'+f'{n+1000:064x}', 'logIndex': '0x0',
            'address': address, 'topics': [next(iter(POOL_ACTIVITY_TOPICS))]}


@pytest.fixture
def harness(monkeypatch):
    existing, other = pools()[2:]
    candidate = copy.deepcopy(other)
    config = load_config()
    config['policy'].update(cycle_pages=False, discovery_mode='logs', max_pools=2)
    config['factories'] = [{'address':existing.factory, 'kind':existing.kind, 'keys':[]}]
    initial = {'known_pools': [existing.json()], 'discovery_cursor': 10,
               'last_seen': {existing.address: 98}}

    class RPC:
        height = 100
        history_failure = False
        reorg = False
        header_calls = Counter()
        ranges = []
        def verify_chain(self): pass
        def header(self, height=None):
            n = self.height if height is None else height
            self.header_calls[n] += 1
            header = h(n)
            if self.reorg and height == 20 and self.header_calls[n] > 1:
                header['hash'] = '0x'+'fe'*32
            return header
        def request(self, method, params):
            assert method == 'eth_getLogs'
            lo, hi = (int(params[0][key],16) for key in ('fromBlock','toBlock'))
            self.ranges.append([lo, hi])
            if lo < 69 and self.history_failure:
                raise RPCUnavailable('shared provider 429')
            return [log(n, address) for n,address in self.events if lo <= n <= hi]

    rpc = RPC()
    rpc.header_calls = Counter()
    rpc.ranges = []
    rpc.events = [(15,candidate.address), (20,existing.address), (100,existing.address)]
    validated = []
    def validate(rpc, config, addresses, block):
        validated.extend(addresses)
        return [candidate for key in addresses if key == candidate.address], []
    class Feed:
        def __init__(self,rpc,config,saved): self.saved=saved
        def poll(self,height): return SimpleNamespace(snapshot_pools=self.saved['pools'])
    monkeypatch.setattr(engine,'validate_pools',validate)
    monkeypatch.setattr(engine,'discover',lambda *args:([],[]))
    monkeypatch.setattr(engine,'discover_pairs',lambda *args:([],[]))
    monkeypatch.setattr('searcher.statefeed.StateFeed',Feed)
    return rpc, config, initial, existing, candidate, validated


def test_live_window_then_durable_oldest_page_and_restart(harness,tmp_path):
    rpc,config,initial,existing,candidate,validated=harness
    checkpoint=copy.deepcopy(initial)
    result=engine.prepare(rpc,config,previous=initial)
    assert rpc.ranges == [[69,78],[79,88],[89,98],[99,100],[11,20]]
    assert candidate.address in validated
    assert result['last_seen'][existing.address] == 100
    assert result['discovery_backlog'] == [[21,68]]
    assert result['discovery_recovered_range'] == [11,20]
    assert initial == checkpoint
    file=tmp_path/'universe.json';write_universe(file,result)
    result=read_universe(file)
    rpc.height=110
    again=engine.prepare(rpc,config,previous=result)
    assert again['discovery_fresh_range'] == [101,110]
    assert again['discovery_recovered_range'] == [21,30]
    assert again['discovery_backlog'] == [[31,68]]


@pytest.mark.parametrize('failure',['rate_limit','reorg'])
def test_failed_old_page_keeps_work_and_live_discovery(harness,failure):
    rpc,config,initial,existing,candidate,validated=harness
    rpc.history_failure=failure=='rate_limit';rpc.reorg=failure=='reorg'
    result=engine.prepare(rpc,config,previous=initial)
    assert result['discovery_cursor']==100
    assert result['discovery_backlog']==[[11,68]]
    assert result['discovery_recovered_range'] is None
    assert result['discovery_history_error']
    assert candidate.address not in validated and candidate.address not in result['last_seen']
    assert result['last_seen'][existing.address]==100


def test_wrong_historical_height_is_not_consumed(harness):
    rpc,config,initial,*_=harness
    old=rpc.header
    rpc.header=lambda n=None: h(19) if n==20 else old(n)
    result=engine.prepare(rpc,config,previous=initial)
    assert result['discovery_backlog']==[[11,68]]
    assert 'height mismatch' in result['discovery_history_error']


def test_failing_final_snapshot_does_not_commit_any_progress(harness,monkeypatch):
    rpc,config,initial,*_=harness
    old=copy.deepcopy(initial)
    class FailingFeed:
        def __init__(self,*args): pass
        def poll(self,*args): raise RPCUnavailable('snapshot failed')
    monkeypatch.setattr('searcher.statefeed.StateFeed',FailingFeed)
    with pytest.raises(RPCUnavailable): engine.prepare(rpc,config,previous=initial)
    assert initial==old


def test_legacy_gap_imported_once_and_ranges_bounded_without_loss():
    previous={'discovery_cursor':100,'offline_history_gap':[10,18]}
    fresh,gap,pending,chunk=plan_history(previous,110,6,32)
    assert pending==[[10,18]] and chunk==[10,18]
    previous['discovery_backlog']=[]
    assert plan_history(previous,110,6,32)[2]==[]
    ranges=[[n*3,n*3] for n in range(100)]
    bounded=merge_ranges(ranges)
    assert len(bounded)==64
    assert all(any(lo<=n*3<=hi for lo,hi in bounded) for n in range(100))


def test_full_hint_queue_preserves_unadmitted_history_range(harness,monkeypatch):
    rpc,config,initial,existing,candidate,validated=harness
    from searcher import discovery_history
    real=discovery_history.pool_hint_queue
    monkeypatch.setattr(discovery_history,'pool_hint_queue',
                        lambda *args:real(*args,capacity=0))
    result=engine.prepare(rpc,config,previous=initial)
    assert result['discovery_backlog']==[[11,68]]
    assert result['discovery_recovered_range'] is None
    assert not validated


def test_full_hint_queue_preserves_fresh_range(harness,monkeypatch):
    rpc,config,initial,existing,candidate,_=harness
    rpc.events=[(100,candidate.address)]
    from searcher import discovery_history
    real=discovery_history.pool_hint_queue
    monkeypatch.setattr(discovery_history,'pool_hint_queue',lambda *args:real(*args,capacity=0))
    result=engine.prepare(rpc,config,previous=initial)
    assert result['discovery_backlog']==[[21,100]]


def test_authentication_budget_overflow_and_transient_metadata_are_durable(harness,tmp_path,monkeypatch):
    rpc,config,initial,existing,candidate,_=harness
    config['policy']['max_pools']=1
    first='0x'+'55'*20
    rpc.events=[(100,first),(15,candidate.address)]
    def validate(rpc,config,addresses,block):
        return [],[{'pool':key,'reason':'metadata unavailable'} for key in addresses]
    monkeypatch.setattr(engine,'validate_pools',validate)
    result=engine.prepare(rpc,config,previous=initial)
    assert set(result['pending_discovery_pools'])=={first,candidate.address}
    assert result['discovery_backlog']==[[21,68]]
    file=tmp_path/'universe.json';write_universe(file,result)
    restored=read_universe(file)
    rpc.height=101;rpc.events=[]
    calls=[]
    def now_valid(rpc,config,addresses,block):
        calls.extend(addresses)
        return [],[{'pool':key,'reason':'metadata unavailable'} for key in addresses]
    monkeypatch.setattr(engine,'validate_pools',now_valid)
    engine.prepare(rpc,config,previous=restored)
    # The unattempted hint gets its turn even while the old failure backs off.
    attempted=next(key for key,row in restored['pending_discovery_pools'].items() if row['attempts'])
    assert calls and attempted not in calls


def test_fresh_priority_has_reserved_pending_progress_and_permanent_rejection_removed():
    queue,overflow,deferred=pool_hint_queue({},set(),{f'fresh{i}':100 for i in range(8)},{'old':10})
    selected=select_pool_hints(queue,Counter({f'fresh{i}':10 for i in range(8)}),100,4)
    assert selected[:3]==['fresh0','fresh1','fresh2'] and selected[3]=='old'
    finish_pool_hints(queue,selected,[],[
        {'pool':'fresh0','reason':'factory outside supported registry'},
        {'pool':'fresh1','reason':'factory reverse lookup unavailable'},
        {'pool':'fresh2','reason':'factory reverse lookup mismatch'}],100)
    assert 'fresh0' not in queue and 'fresh2' not in queue
    assert queue['fresh1']['retry_after_block']==102
    assert queue['old']['retry_after_block']==102
    assert 'old' not in select_pool_hints(queue,Counter(),101,100)
    assert 'old' in select_pool_hints(queue,Counter(),102,100)
