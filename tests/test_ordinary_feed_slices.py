"""Bound live-shaped reads without losing complete route/page ownership.

The topology/state fixture is an actual public observation. The deterministic
RPC model reproduces large-read clock drift; it is not a network speed or
historical profit measurement. No wallet, production state or socket is used.
"""
import copy
import hashlib
import json
from collections import Counter
from pathlib import Path

import pytest
from eth_abi import decode, encode

from searcher.core import Reject, load_config, number
from searcher.pipeline import PendingStrategy
from searcher.pools import Pool
from searcher.routes import RouteIndex
from searcher.statefeed import DeepReadScheduler, StateFeed
from searcher.universe import PageProgress


def evidence():
    return json.loads((Path(__file__).parent/'fixtures/ordinary-large-page.json').read_text())


def packed(rows, block, stamp):
    def word(value, width, signed=False): return int(value).to_bytes(width, 'big', signed=signed)
    result = b'\2'+word(block,32)+word(stamp,32)+word(len(rows),2)
    for pool in map(Pool.from_json, rows):
        result += bytes.fromhex(pool.address[2:])
        if not pool.valid: result += b'\0'; continue
        if pool.kind == 'v2':
            result += b'\2'+word(pool.reserve0,14)+word(pool.reserve1,14)+word(pool.fee,3)
            continue
        result += (b'\1'+word(pool.sqrt_price,20)+word(pool.tick,3,True)+word(pool.liquidity,16)+
                   word(pool.fee,3)+word(len(pool.bitmap),1))
        for key,value in pool.bitmap.items(): result += word(key,2,True)+word(value,32)
        result += word(len(pool.ticks),1)
        for tick,(gross,net) in pool.ticks.items(): result += word(tick,3,True)+word(gross,16)+word(net,16,True)
    return '0x'+encode(['bytes'], [result]).hex()


class RecordedRPC:
    def __init__(self, record):
        self.rows = {row['address']:row for row in record['snapshot_pools']}
        self.head = dict(record['header'])
        self.reads = []
        self.conflicts = 0
        self.fail = False

    def pending(self):
        return dict(self.head, number=hex(number(self.head['number'])+1),
                    timestamp=hex(number(self.head['timestamp'])+2), parentHash=self.head['hash'])

    def batch(self, calls):
        if calls[0][0] != 'eth_call': return [dict(self.head), self.pending()]
        params = calls[0][1]
        assert params[1] == 'pending' and len(params) == 4
        addresses,_,_,depths = decode(['address[]','int24[]','uint8','uint8[]'], bytes.fromhex(params[0]['data'][10:]))
        self.reads.append((tuple(addresses), tuple(depths)))
        if self.fail: raise TimeoutError('synthetic bounded transport timeout')
        context = params[3]
        raw = packed([self.rows[a] for a in addresses], number(context['number']), number(context['time']))
        # A large call crosses every header boundary in this deterministic
        # regression. The same complete <=24-pool reads have a stable clock.
        if len(addresses) > 24 or self.conflicts:
            self.conflicts = max(0,self.conflicts-1)
            self.head = dict(self.head, number=hex(number(self.head['number'])+1),
                timestamp=hex(number(self.head['timestamp'])+2), parentHash=self.head['hash'],
                hash='0x'+hashlib.sha256(self.head['hash'].encode()).hexdigest())
        return [raw, dict(self.head), self.pending()]


class ExactPage:
    """Replay exactly the selected public economic page, not a new discovery."""
    def __init__(self, record):
        self.pools = {row['address']:Pool.from_json(row) for row in record['snapshot_pools']}
        self.groups = [tuple(group) for group in record['transport']['route_groups']]
        self.covered = set(self.pools)
        self.cursor = 0
        self.calls = 0
        self.selection_summary = {'scope':'recorded selected page'}
        self.selected_priority = record['transport']['activity_priority_groups']
        self.selected_coactive = record['transport']['coactive_priority_groups']

    def take(self, *args, **kwargs):
        self.calls += 1
        self.cursor += 1
        self.selected_groups = list(self.groups)
        return list(self.pools.values())


def feed_case():
    record = evidence()
    config = load_config()
    config['policy']['snapshot_encoding'] = 'packed-v1'
    config['policy']['max_quote_ms'] = 1000
    saved = {'header':record['header'],'pools':record['snapshot_pools']}
    rpc = RecordedRPC(record)
    feed = StateFeed(rpc, config, saved)
    feed.pages = ExactPage(record)
    return record,config,saved,rpc,feed


def test_recorded_large_read_rejects_but_complete_slices_reach_every_real_direction(monkeypatch):
    record,config,saved,rpc,feed = feed_case()
    assert len(saved['pools']) == 194 and len(feed.pages.groups) == 105
    with monkeypatch.context() as m:
        m.setattr(StateFeed,'SNAPSHOT_POOL_LIMIT',300)
        with pytest.raises(Reject,match='snapshot anchor changed during read'): feed.poll()
    record,config,saved,rpc,feed = feed_case()
    expected = set(RouteIndex([Pool.from_json(p) for p in saved['pools']],
        [t['address'] for t in config['settlement_tokens']], config['policy']['max_hops'], groups=feed.pages.groups).routes)
    strategy,gate = PendingStrategy(None,config,saved),PageProgress()
    considered,delivered = Counter(),Counter()
    for _ in range(300):
        advance,page = gate.begin()
        frame = feed.poll(advance_page=advance)
        frame.transport['page_id'] = page
        assert frame.state_tag == 'pending' and frame.preconfirmed is None
        assert len(frame.snapshot_pools) <= 24
        assert frame.transport['page_slice']['logical_groups'] == 105
        report = strategy.on_snapshot(frame)
        considered.update(set(strategy.index.last_evaluated_keys)-strategy.index.unstarted_keys)
        assert report['page_withdrawn_routes'] == 0
        if report['page_advance_ready']:
            assert not report['continuation_requests']
            delivered.update(map(tuple,frame.transport['route_groups']))
            gate.consumed(page)
            if not feed.has_pending_slices: break
    else: pytest.fail('bounded ordinary page did not finish')
    assert feed.pages.calls == 1
    assert set(considered) == expected and max(considered.values()) == 1
    assert delivered == Counter(map(tuple,record['transport']['route_groups']))
    assert all(len(addresses)<=24 for addresses,_ in rpc.reads)


def test_failed_or_unacknowledged_slice_preserves_exact_tail_and_scoped_requests():
    record,_,_,rpc,feed = feed_case()
    groups = [feed.pages.groups[i] for i in (0,30,60,90)]
    requests = [dict(pools=group,route_keys=['direction-'+str(i)],deep=True,since=1,score=i)
                for i,group in enumerate(groups)]
    deep = {a for group in groups for a in group}
    first = feed.poll(deep_pools=deep,requested_groups=requests)
    original = copy.deepcopy(first.transport)
    tail = copy.deepcopy(list(feed._slices))
    rpc.conflicts = 2
    with pytest.raises(Reject,match='anchor changed'): feed.poll(advance_page=False)
    assert list(feed._slices) == tail
    rpc.fail = True
    with pytest.raises(TimeoutError): feed.poll(advance_page=False)
    rpc.fail = False
    refreshed = feed.poll(advance_page=False)
    assert refreshed.transport['page_slice'] == original['page_slice']
    assert refreshed.transport['route_groups'] == original['route_groups']
    assert refreshed.transport['requested_groups'] == original['requested_groups']
    assert list(feed._slices) == tail
    delivered = list(refreshed.transport['requested_groups'])
    while feed.has_pending_slices:
        frame = feed.poll()
        addresses = {row['address'] for row in frame.snapshot_pools}
        selected = set(map(tuple,frame.transport['route_groups']))
        assert all(set(group)<=addresses for group in selected)
        assert all(tuple(row['pools']) in selected for row in frame.transport['requested_groups'])
        delivered.extend(frame.transport['requested_groups'])
    assert sorted(delivered,key=lambda row:row['score']) == requests
    assert feed.pages.calls == 1
    feed.poll()
    assert feed.pages.calls == 2


def test_depth_fallback_changes_current_depth_only_not_future_request_ownership():
    _,_,_,_,feed = feed_case()
    groups = [feed.pages.groups[i] for i in (0,30,60,90)]
    requests = [dict(pools=group,route_keys=['scope-'+str(i)],deep=True,since=3) for i,group in enumerate(groups)]
    first = feed.poll(deep_pools={a for group in groups for a in group}, requested_groups=requests)
    tail = copy.deepcopy(list(feed._slices))
    shallow = feed.poll(advance_page=False,retry_shallow=True)
    assert shallow.transport['deep_pools'] == []
    assert shallow.transport['route_groups'] == first.transport['route_groups']
    assert shallow.transport['requested_groups'] == first.transport['requested_groups']
    assert list(feed._slices) == tail
    while feed.has_pending_slices:
        next_frame = feed.poll()
        wanted = {a for row in next_frame.transport['requested_groups'] for a in row['pools']}
        assert wanted <= set(next_frame.transport['deep_pools'])


def test_new_depth_work_cannot_replace_tail_and_priority_metadata_is_slice_local():
    _,_,_,_,feed = feed_case()
    first = feed.poll()
    pending = len(feed._slices)
    with pytest.raises(Reject,match='before page tail completed'):
        feed.poll(requested_groups=[{'pools':list(feed.pages.groups[-1])}])
    assert len(feed._slices) == pending and feed.pages.calls == 1
    while True:
        chosen = set(map(tuple,first.transport['route_groups']))
        assert set(map(tuple,first.transport['activity_priority_groups'])) <= chosen
        assert set(map(tuple,first.transport['coactive_priority_groups'])) <= chosen
        if not feed.has_pending_slices: break
        first = feed.poll()


def test_ready_shallow_requests_keep_bounded_admission_and_never_downgrade_depth():
    scheduler = DeepReadScheduler()
    shallow = dict(pools=['a','b'],deep=False,route_keys=['forward'])
    scheduler.last[('a','b')] = 2
    scheduler.add([shallow],3)
    scheduler.take({'a','b'},3)
    assert not scheduler.selected_deep_pools
    assert scheduler.selected_requests[0]['deep'] is False
    assert scheduler.resume(scheduler.selected_requests,4) == []
    scheduler.add([dict(shallow,deep=True,route_keys=['reverse'])],20)
    scheduler.add([shallow],21)
    scheduler.take({'a','b'},22)
    assert scheduler.selected_deep_pools == {'a','b'}
    assert scheduler.selected_requests[0]['route_keys'] == ['forward','reverse']
    # A new shallow task goes through pending admission, not the return queue.
    existing = [dict(pools=[f'{i}a',f'{i}b'],deep=False,since=0) for i in range(1024)]
    assert scheduler.resume(existing,23) == []
    scheduler.add([dict(pools=['new-a','new-b'],deep=False)],24)
    scheduler.take({'new-a','new-b'} | {p for row in existing for p in row['pools']},25,
                   deferred=lambda row: row['pools'][0] != 'new-a')
    assert scheduler.selected_requests == []
    assert ('new-a','new-b') in scheduler.pending and len(scheduler.resuming) == 1024
