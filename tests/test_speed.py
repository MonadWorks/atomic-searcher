"""Exact encoding, scheduling, and aggregated context regression checks."""
import copy
import json
import time
from pathlib import Path

import pytest
from eth_abi import encode, decode
from eth_utils import keccak

from searcher.core import ROOT, RPC, MULTICALL, Reject, data, load_config, unhex
from searcher.statefeed import StateFeed, decode_packed
from searcher.universe import CyclePages, ActivityHints
from searcher.events import V3_SWAP
from test_execution import market, artifact
from test_searcher import pool, A, B, C


def test_packed_real_evm_same_as_abi_including_negative_tick_and_invalid_pool(market):
    rpc, config, _, _, _, _, _, _, send = market
    runtime = artifact('StateLens.t.sol', 'LensPoolMock')['bytecode']['object']
    addr = send(calldata=runtime+encode(['int24','int24'], [-1,10]).hex())['contractAddress'].lower()
    for tick in (-2580,-20,-10,0,10,2580):
        send(addr, data('add(int24)', ['int24'], [tick]))
    saved = {'pools':[pool(addr).json(), pool('0x'+'00'*19+'ee').json()]}
    saved['pools'][0]['spacing'] = 10
    config = copy.deepcopy(config)
    config['policy']['cycle_pages'] = False
    config['policy']['snapshot_ticks_per_side'] = 4
    block = int(rpc.header()['number'],16)
    feeds, frames = [], []
    for encoding in ('abi','packed-v1'):
        config['policy']['snapshot_encoding'] = encoding
        feed = StateFeed(rpc,copy.deepcopy(config),saved)
        frames.append(feed.poll(block, deep_pools=[addr]))
    assert frames[0].snapshot_pools == frames[1].snapshot_pools
    assert frames[0].context_block == frames[1].context_block
    assert frames[0].context_time == frames[1].context_time
    assert frames[0].snapshot_pools[0]['valid'] and not frames[0].snapshot_pools[1]['valid']
    assert frames[1].transport['response_bytes'] < frames[0].transport['response_bytes']


def packed(value): return '0x'+encode(['bytes'],[value]).hex()


def test_packed_decoder_rejects_truncation_unknown_version_and_trailing_data():
    valid = b'\1'+(123).to_bytes(32,'big')+(456).to_bytes(32,'big')+b'\0\1'+bytes.fromhex(A[2:])+b'\0'
    assert decode_packed(packed(valid))[1:] == (123,456)
    for i in range(len(valid)):
        with pytest.raises(Reject): decode_packed(packed(valid[:i]))
    for invalid in (b'\3'+valid[1:], valid+b'\0', valid[:-1]+b'\2'):
        with pytest.raises(Reject): decode_packed(packed(invalid))


def test_activity_prioritizes_cold_complete_cycle_and_rotation_survives_busy_pool():
    pools=[]
    for i in range(60):
        token='0x'+f'{1000+i:040x}'
        pools.extend([pool('0x'+f'{100+i*2:040x}',A,token),pool('0x'+f'{101+i*2:040x}',A,token)])
    pages=CyclePages(pools,[A],3)
    cold=pages.groups[-1]
    pages.prioritize([cold[0]])
    chosen=pages.take(30,group_limit=8)
    assert cold in pages.selected_priority
    assert set(cold).issubset({p.address for p in chosen})
    seen=set(pages.selected_groups)
    for _ in range(40):
        pages.prioritize([cold[0]])
        pages.take(30,group_limit=8)
        seen.update(pages.selected_groups)
        replacement=CyclePages(pools,[A],3,groups=pages.groups)
        replacement.resume(pages);pages=replacement
    assert seen==set(pages.groups)
    pages.priority.clear()
    pages.prioritize([cold[0]], time.monotonic()-6)
    pages.take(30,group_limit=8)
    assert not pages.selected_priority


def test_activity_hints_deduplicate_pending_then_sealed_and_skip_malformed():
    hints=ActivityHints()
    entry={'address':A,'transactionHash':'0x'+'ab'*32,'logIndex':'0x0','topics':[V3_SWAP]}
    assert hints.accept([entry,entry,{},dict(entry,removed=True)])=={A}
    assert not hints.accept([dict(entry,blockHash='0x'+'cc'*32)])
    assert hints.accept([dict(entry,logIndex='0x1')])=={A}


def test_small_page_keeps_cold_rotation_under_constant_activity():
    pools=[]
    for i in range(6):
        token='0x'+f'{1000+i:040x}'
        pools.extend([pool('0x'+f'{100+i*2:040x}',A,token),pool('0x'+f'{101+i*2:040x}',A,token)])
    pages=CyclePages(pools,[A],3)
    seen=set()
    for _ in range(12):
        pages.prioritize(pages.groups[-1]);pages.take(3,group_limit=4)
        seen.update(pages.selected_groups)
    assert seen==set(pages.groups)


def test_aggregated_context_preserves_values_order_and_failures(market, monkeypatch):
    import searcher.core as core
    rpc, config, _, _, _, _, _, _, _ = market
    runtime=artifact('ContextAggregator.t.sol','ContextAggregatorMock')['deployedBytecode']['object']
    rpc.request('anvil_setCode',[MULTICALL,runtime])
    monkeypatch.setattr(core,'MULTICALL_HASH','0x'+keccak(unhex(runtime)).hex())
    oracle='0x420000000000000000000000000000000000000f'
    calls=[('eth_getBlockByNumber',['latest',False]),
           ('eth_call',[{'to':oracle,'data':data('getL1FeeUpperBound(uint256)',['uint256'],[2048])},'pending']),
           ('eth_getBalance',[config['owner'],'pending']),
           ('eth_call',[{'to':oracle,'data':data('getOperatorFee(uint256)',['uint256'],[10**6])},'pending'])]
    expected=rpc.batch(calls)
    approved=[(oracle, calls[i][1][0]['data'][:10]) for i in (1,3)]
    assert rpc.batch_contract_reads(calls,approved)==expected
    assert len(rpc.measurements[-1]['methods'])==3
    # Reverts in a subcall must not turn into a zero fee or a missing result.
    rpc.request('anvil_setCode',[oracle,'0x5f5ffd'])
    with pytest.raises(Reject,match='context contract read failed'):rpc.batch_contract_reads(calls,approved)


def test_context_aggregate_rejects_unverified_code(market):
    rpc, config, _, _, _, _, _, _, _ = market
    rpc.request('anvil_setCode',[MULTICALL,'0x'])
    calls=[('eth_call',[{'to':config['owner'],'data':'0x'},'pending'])]*2
    with pytest.raises(Reject,match='unverified Multicall'):rpc.batch_contract_reads(calls,[(config['owner'],'0x')])


def test_hot_quote_priority_keeps_cold_work_across_page_switches(monkeypatch):
    from searcher.pipeline import PendingStrategy
    from searcher.statefeed import StateFrame
    from test_pipeline import RPCStub,saved,header,H1,H0
    c=load_config();c['settlement_tokens']=[{'address':A}]
    strategy=PendingStrategy(RPCStub(),c,saved())
    hot=('0x'+f'{101:040x}','0x'+f'{102:040x}')
    cold_groups=[('0x'+f'{201:040x}','0x'+f'{202:040x}'),('0x'+f'{301:040x}','0x'+f'{302:040x}')]
    seen=[]
    def single_route_budget(index,config,h,changed):
        key=index.work_queue.popleft();index.queued_routes.remove(key)
        index.last_evaluated_keys=[key]
        seen.append(tuple(sorted(index.routes[key].pools)))
        return {'candidates':[]}
    monkeypatch.setattr('searcher.pipeline.evaluate',single_route_budget)
    for i in range(20):
        cold=cold_groups[i%2]
        ps=[pool(addr).json() for addr in hot+cold]
        now=time.monotonic_ns()
        frame=StateFrame(header(2,H1,H0),header(2,H1,H0),ps,now,now,time.time(),0,100,
            transport={'route_groups':[hot,cold],'activity_priority_groups':[hot]})
        strategy.on_snapshot(frame)
    assert set(cold_groups).issubset(set(seen)), f'only quoted {set(seen)}'

def test_new_activity_is_scheduled_without_rebuilding_same_page(monkeypatch):
    from searcher.pipeline import PendingStrategy
    from searcher.statefeed import StateFrame
    from test_pipeline import RPCStub,saved,header,H1,H0
    c=load_config();c['settlement_tokens']=[{'address':A}]
    strategy=PendingStrategy(RPCStub(),c,saved())
    groups=[('0x'+f'{101:040x}','0x'+f'{102:040x}'),('0x'+f'{201:040x}','0x'+f'{202:040x}')]
    seen=[]
    def single_route_budget(index,config,h,changed):
        key=index.work_queue.popleft();index.queued_routes.remove(key)
        index.last_evaluated_keys=[key]
        seen.append(tuple(sorted(index.routes[key].pools)))
        return {'candidates':[]}
    monkeypatch.setattr('searcher.pipeline.evaluate',single_route_budget)
    for urgent in ([],[groups[1]]):
        now=time.monotonic_ns()
        frame=StateFrame(header(2,H1,H0),header(2,H1,H0),[pool(addr).json() for g in groups for addr in g],now,now,time.time(),0,100,
            transport={'route_groups':groups,'activity_priority_groups':urgent})
        strategy.on_snapshot(frame)
    assert seen[1]==groups[1], f'new activity ignored on unchanged page: {seen}'
