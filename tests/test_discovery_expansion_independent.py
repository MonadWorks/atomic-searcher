"""Independent bounded-discovery progress checks using local aggregate replies."""
import copy
from collections import Counter
from itertools import combinations
from types import SimpleNamespace

import pytest
from eth_abi import decode, encode

from searcher.core import MULTICALL, RPC, RPCUnavailable, Reject, ZERO, data, load_config, unhex
from searcher.discovery_expansion import expansion_state, factory_registry_id, run_expansion
from searcher.pools import Pool
from searcher.snapshots import read_universe, write_universe


def addr(value): return '0x' + f'{value:040x}'


FACTORY, BLOCK = addr(1000), 100


def config(*, seeds=3, cap=2, slots=20):
    result = load_config()
    result['seed_tokens'] = [addr(index + 1) for index in range(seeds)]
    result['factories'] = [{'address': FACTORY, 'kind': 'v3', 'keys': list(range(500, 500 + slots))}]
    result['policy'].update(max_new_tokens_per_refresh=cap, cycle_pages=False, discovery_mode='logs')
    return result


def known(config, tokens=(101, 102, 103, 104)):
    pools = [Pool(addr(5000 + value), FACTORY, 'v3', config['seed_tokens'][0], addr(value),
                  10, 500, sqrt_price=1 << 96, liquidity=10**12, block=BLOCK,
                  block_hash='0x' + f'{BLOCK:064x}') for value in tokens]
    return {pool.address: pool for pool in pools}


def previous(config, pools):
    return {'factory_registry_id': factory_registry_id(config),
            'expanded_tokens': list(config['seed_tokens']),
            'known_pools': [pool.json() for pool in pools.values()], 'discovery_cursor': BLOCK}


class Aggregates(RPC):
    """Run real RPC.views encoding/chunking with no client or network socket."""
    def __init__(self):
        self.multicall_verified = True
        self.requests = []
        self.queries = []
        self.failure = None
        self.found = None

    def call(self, target, calldata, block):
        assert target == MULTICALL and block == BLOCK
        calls = decode(['(address,bool,bytes)[]'], unhex(calldata)[4:])[0]
        assert 0 < len(calls) <= 120
        self.requests.append([(address.lower(), '0x' + raw.hex()) for address, _, raw in calls])
        if self.failure == 'transport': raise RPCUnavailable('synthetic temporary failure')
        replies = []
        for index, (factory, allowed, raw) in enumerate(calls):
            assert allowed
            selector = '0x' + raw[:4].hex()
            if selector == data('getPair(address,address)'):
                token0, token1 = decode(['address', 'address'], raw[4:]); key = None
            elif selector == data('getPool(address,address,uint24)'):
                token0, token1, key = decode(['address', 'address', 'uint24'], raw[4:])
            elif selector == data('getPool(address,address,int24)'):
                token0, token1, key = decode(['address', 'address', 'int24'], raw[4:])
            else:
                raise AssertionError('Unexpected getter: ' + selector)
            self.queries.append((factory.lower(), token0.lower(), token1.lower(), key))
            value = self.found if self.found is not None and index == 0 else ZERO
            replies.append((not (self.failure == 'missing' and index == 0), encode(['address'], [value])))
        return encode(['(bool,bytes)[]'], [replies])


def checkpoint(result, pools):
    keys = ('factory_registry_id', 'expanded_tokens', 'pending_expansion_tokens', 'token_expansion')
    return {**{key: copy.deepcopy(result[key]) for key in keys},
            'known_pools': [pool.json() for pool in pools.values()], 'discovery_cursor': BLOCK}


def test_disk_resume_freezes_active_batch_and_new_hot_tokens_do_not_jump_old_fifo(tmp_path):
    cfg = config()
    pools = known(cfg)
    old = previous(cfg, pools)
    rpc = Aggregates()
    first = run_expansion(rpc, cfg, old, pools, BLOCK)
    assert first['getters_attempted'] == first['getters_completed'] == 120
    assert first['token_expansion'] is not None
    assert set(first['expanded_tokens']) == set(cfg['seed_tokens'])
    original_tokens = tuple(first['token_expansion']['tokens'])
    assert {addr(101), addr(102)} <= set(original_tokens)
    previous_before = copy.deepcopy(old)
    saved = checkpoint(first, pools)
    file = tmp_path / 'universe.json'
    write_universe(file, saved)
    saved = read_universe(file)
    newest = known(cfg, (105,))
    pools.update(newest)
    active_before = copy.deepcopy(saved['token_expansion'])
    resumed = expansion_state(cfg, saved, pools, {next(iter(newest)): 10**9})
    assert resumed['token_expansion'] == active_before
    assert resumed['pending_expansion_tokens'] == [addr(103), addr(104), addr(105)]
    completions = []
    for _ in range(12):
        before = copy.deepcopy(saved)
        result = run_expansion(rpc, cfg, saved, pools, BLOCK, {next(iter(newest)): 10**9})
        assert saved == before
        assert result['getters_attempted'] <= 120
        added = set(result['expanded_tokens']) - set(saved['expanded_tokens'])
        if added: completions.append(added - set(cfg['seed_tokens']))
        saved = checkpoint(result, pools)
        write_universe(file, saved); saved = read_universe(file)
        if {addr(n) for n in range(101, 106)} <= set(saved['expanded_tokens']): break
    assert completions == [{addr(101), addr(102)}, {addr(103), addr(104)}, {addr(105)}]
    expected_first = {(FACTORY, a, b, key) for a, b in combinations(original_tokens, 2)
                      for key in cfg['factories'][0]['keys']}
    assert expected_first <= set(rpc.queries)
    assert old == previous_before
    assert not saved['pending_expansion_tokens'] and saved['token_expansion'] is None


def test_one_pair_with_more_than_120_factory_keys_resumes_inside_the_pair(tmp_path):
    cfg = config(seeds=1, cap=1, slots=121)
    pools = known(cfg, (101,))
    rpc = Aggregates()
    result = run_expansion(rpc, cfg, previous(cfg, pools), pools, BLOCK)
    assert result['getters_completed'] == 120
    assert result['token_expansion']['factory_offset'] == 120
    assert addr(101) not in result['expanded_tokens']
    file = tmp_path / 'universe.json'
    write_universe(file, checkpoint(result, pools))
    resumed = run_expansion(rpc, cfg, read_universe(file), pools, BLOCK)
    assert resumed['getters_completed'] == 1 and resumed['token_expansion'] is None
    assert addr(101) in resumed['expanded_tokens']
    expected = [(FACTORY, cfg['seed_tokens'][0], addr(101), key) for key in cfg['factories'][0]['keys']]
    assert rpc.queries == expected and [len(rows) for rows in rpc.requests] == [120, 1]


@pytest.mark.parametrize('failure', ['transport', 'missing'])
def test_failed_chunk_keeps_every_lookup_for_retry_and_does_not_mutate_previous(failure):
    cfg = config(); pools = known(cfg); old = previous(cfg, pools)
    before = copy.deepcopy(old); rpc = Aggregates(); rpc.failure = failure
    result = run_expansion(rpc, cfg, old, pools, BLOCK)
    initial = expansion_state(cfg, old, pools)
    assert result['getters_completed'] == 0 and result['error']
    assert result['token_expansion'] == initial['token_expansion'] and old == before
    rpc.failure = None
    resumed = run_expansion(rpc, cfg, checkpoint(result, pools), pools, BLOCK)
    assert resumed['getters_completed'] == 120
    assert rpc.requests[0] == rpc.requests[1]


@pytest.mark.parametrize('reason, advances', [
    ('metadata unavailable', False), ('CL metadata unavailable', False),
    ('factory reverse lookup unavailable', False), ('undecodable reply', False),
    ('factory outside supported registry', True), ('invalid fee/spacing', True),
    ('invalid token order', True), ('factory reverse lookup mismatch', True),
])
def test_identity_failures_only_advance_for_explicit_terminal_rejection(reason, advances):
    cfg = config(); pools = known(cfg); old = previous(cfg, pools)
    rpc = Aggregates(); rpc.found = addr(7000)
    def validator(rpc, config, addresses, block):
        assert addresses == [addr(7000)] and block == BLOCK
        return [], [{'pool': addr(7000), 'reason': reason}]
    result = run_expansion(rpc, cfg, old, pools, BLOCK, validator=validator)
    assert (result['getters_completed'] == 120) == advances
    assert result['errors'] == [{'pool': addr(7000), 'reason': reason}]
    assert not set(result['expanded_tokens']) - set(old['expanded_tokens'])


def test_registry_change_invalidates_old_offset_and_completion_without_mutating_disk_state():
    cfg = config(); pools = known(cfg); rpc = Aggregates()
    first = run_expansion(rpc, cfg, previous(cfg, pools), pools, BLOCK)
    old = checkpoint(first, pools); before = copy.deepcopy(old)
    changed = copy.deepcopy(cfg)
    changed['factories'].append({'address': addr(1001), 'kind': 'v2', 'keys': []})
    reset = expansion_state(changed, old, pools)
    assert reset['factory_registry_id'] != old['factory_registry_id']
    assert reset['expanded_tokens'] == [] and reset['token_expansion']['factory_offset'] == 0
    result = run_expansion(rpc, changed, old, pools, BLOCK)
    assert result['getters_completed'] == 120 and old == before
    assert any(factory == addr(1001) and key is None for factory, _, _, key in rpc.queries)


def test_zero_new_token_limit_does_not_repeat_already_completed_seed_work():
    cfg = config(cap=0); pools = known(cfg)
    state = expansion_state(cfg, previous(cfg, pools), pools)
    assert state['token_expansion'] is None
    assert set(state['pending_expansion_tokens']) == {addr(n) for n in range(101, 105)}
    rpc = Aggregates()
    result = run_expansion(rpc, cfg, state, pools, BLOCK)
    assert result['getters_attempted'] == 0 and not rpc.requests


@pytest.mark.parametrize('damage', ['skip_middle', 'reorder'])
def test_invalid_remaining_pair_checkpoint_restarts_without_fabricated_completion(damage):
    cfg = config(); pools = known(cfg)
    state = expansion_state(cfg, previous(cfg, pools), pools)
    pairs = state['token_expansion']['remaining_pairs']
    unfinished = set(state['token_expansion']['tokens']) - set(state['expanded_tokens'])
    if damage == 'skip_middle': pairs.pop(1)
    else: pairs[0], pairs[1] = pairs[1], pairs[0]
    restored = expansion_state(cfg, state, pools)
    restarted = restored['token_expansion']
    assert restarted['remaining_pairs'] == [list(pair) for pair in combinations(sorted(restarted['tokens']), 2)]
    assert restarted['factory_offset'] == 0
    assert unfinished <= set(restarted['tokens']) | set(restored['pending_expansion_tokens'])
    assert set(restored['expanded_tokens']) == set(cfg['seed_tokens'])


class PrepareRPC:
    """Local identity replies; engine stages and canonical publication are real."""
    def __init__(self, pools=()):
        self.pools = {pool.address: pool for pool in pools}
        self.lookups = {(pool.token0, pool.token1, pool.fee): pool.address for pool in pools}
        self.stages = []
        self.fail_expansion = False
        self.reorganized = False
        self.header_reads = 0
    def verify_chain(self): pass
    def header(self, tag=None):
        self.header_reads += 1
        block_hash = '0x' + f'{BLOCK:064x}'
        if self.reorganized and self.header_reads > 1: block_hash = '0x' + 'ee' * 32
        return {'number': hex(BLOCK), 'hash': block_hash, 'parentHash': '0x' + f'{BLOCK-1:064x}'}
    def views(self, calls, block):
        assert block == BLOCK
        if calls: self.stages.append(list(calls))
        if self.fail_expansion and len(calls) == 120:
            raise RPCUnavailable('synthetic optional token expansion failure')
        replies = []
        for target, calldata in calls:
            if target == FACTORY:
                assert calldata[:10] == data('getPool(address,address,uint24)')
                pair = decode(['address', 'address', 'uint24'], unhex(calldata)[4:])
                replies.append(encode(['address'], [self.lookups.get(pair, ZERO)]))
            else:
                pool = self.pools[target]
                fields = {'factory()': ('address', pool.factory), 'token0()': ('address', pool.token0),
                          'token1()': ('address', pool.token1), 'tickSpacing()': ('int24', pool.spacing),
                          'fee()': ('uint24', pool.fee)}
                typ, value = next(value for signature, value in fields.items() if data(signature) == calldata)
                replies.append(encode([typ], [value]))
        return replies


def prepare_fixture(monkeypatch, cfg, events=None, *, fail_snapshot=False):
    from collections import Counter
    def activity(*args):
        return Counter({key: 1 for key in events or {}}), dict(events or {})
    class Feed:
        def __init__(self, rpc, config, saved): self.pools = saved['pools']
        def poll(self, height):
            if fail_snapshot: raise RPCUnavailable('synthetic final snapshot failure')
            return SimpleNamespace(snapshot_pools=self.pools)
    monkeypatch.setattr('searcher.universe.swap_activity', activity)
    monkeypatch.setattr('searcher.statefeed.StateFeed', Feed)


def test_actual_prepare_prioritizes_known_edges_and_publishes_bounded_progress(monkeypatch, tmp_path):
    from searcher import engine
    cfg = config(); pools = known(cfg); old = previous(cfg, pools)
    before = copy.deepcopy(old)
    prepare_fixture(monkeypatch, cfg)
    rpc = PrepareRPC()
    result = engine.prepare(rpc, cfg, previous=old)
    first_pairs = {decode(['address', 'address', 'uint24'], unhex(calldata)[4:])[:2]
                   for _, calldata in rpc.stages[0]}
    known_edges = {(pool.token0, pool.token1) for pool in pools.values()}
    assert first_pairs and first_pairs <= known_edges
    assert len(rpc.stages[0]) == 40
    assert len(rpc.stages[1]) == 120
    assert result['discovery_expansion_progress']['factory_getters_completed'] == 120
    assert result['token_expansion'] is not None and old == before
    path = tmp_path / 'engine-universe.json'
    write_universe(path, result)
    restored = read_universe(path)
    assert restored['token_expansion'] == result['token_expansion']
    assert restored['pending_expansion_tokens'] == result['pending_expansion_tokens']


def test_optional_expansion_failure_still_publishes_new_authenticated_pool(monkeypatch):
    from searcher import engine
    cfg = config(); pools = known(cfg); old = previous(cfg, pools)
    before = copy.deepcopy(old)
    fresh = next(iter(known(cfg, (109,)).values()))
    rpc = PrepareRPC([fresh]); rpc.fail_expansion = True
    prepare_fixture(monkeypatch, cfg, {fresh.address: BLOCK})
    result = engine.prepare(rpc, cfg, previous=old)
    assert fresh.address in {row['address'] for row in result['known_pools']}
    assert result['discovery_cursor'] == BLOCK
    assert result['discovery_expansion_progress']['factory_getters_completed'] == 0
    assert result['discovery_expansion_error']
    assert result['token_expansion']['factory_offset'] == 0 and old == before


@pytest.mark.parametrize('failure', ['snapshot', 'reorg'])
def test_failed_final_publication_cannot_commit_expansion_cursor(failure, monkeypatch):
    from searcher import engine
    cfg = config(); pools = known(cfg); old = previous(cfg, pools)
    # Begin with a real partially completed durable batch, not just an empty
    # dictionary whose shallow mutations would be harder to detect.
    partial = run_expansion(Aggregates(), cfg, old, pools, BLOCK)
    old.update(checkpoint(partial, pools))
    before = copy.deepcopy(old)
    prepare_fixture(monkeypatch, cfg, fail_snapshot=failure == 'snapshot')
    rpc = PrepareRPC(); rpc.reorganized = failure == 'reorg'
    with pytest.raises((RPCUnavailable, Reject)):
        engine.prepare(rpc, cfg, previous=old)
    assert old == before


@pytest.mark.parametrize('fresh_single_pool', [False, True])
def test_cold_bootstrap_reaches_late_seed_cycle_even_if_one_recent_pool_authenticated(
        fresh_single_pool, monkeypatch):
    from searcher import engine
    cfg = config(seeds=6)
    cfg['policy']['cycle_pages'] = True
    cfg['settlement_tokens'] = [dict(cfg['settlement_tokens'][0], address=cfg['seed_tokens'][-2])]
    a, b = cfg['seed_tokens'][-2:]
    closing = [Pool(addr(8000 + fee), FACTORY, 'v3', a, b, 10, fee,
                    sqrt_price=1 << 96, liquidity=10**12, block=BLOCK,
                    block_hash='0x' + f'{BLOCK:064x}') for fee in (500, 501)]
    single = next(iter(known(cfg, (109,)).values()))
    rpc = PrepareRPC([*closing, single])
    prepare_fixture(monkeypatch, cfg, {single.address: BLOCK} if fresh_single_pool else {})
    result = engine.prepare(rpc, cfg, previous={})
    assert {pool.address for pool in closing} <= {row['address'] for row in result['known_pools']}
    assert result['pools'] and result['discovery_expansion_progress']['seed_bootstrap']


def test_registry_removing_one_triangle_edge_does_not_mistake_survivors_for_a_usable_cycle(monkeypatch):
    from searcher import engine
    cfg = config(seeds=6)
    cfg['policy']['cycle_pages'] = True
    cfg['settlement_tokens'] = [dict(cfg['settlement_tokens'][0], address=cfg['seed_tokens'][0])]
    a, b, c = cfg['seed_tokens'][0], *cfg['seed_tokens'][-2:]
    removed_factory = addr(1001)
    def pool(identifier, x, y, fee=500, factory=FACTORY):
        return Pool(addr(identifier), factory, 'v3', *sorted((x, y)), 10, fee,
                    sqrt_price=1 << 96, liquidity=10**12, block=BLOCK,
                    block_hash='0x' + f'{BLOCK:064x}')
    survivors = [pool(8100, a, b), pool(8101, a, c)]
    removed = pool(8102, b, c, factory=removed_factory)
    replacement = pool(8103, b, c, fee=501)
    old_config = copy.deepcopy(cfg)
    old_config['factories'].append({'address': removed_factory, 'kind': 'v3', 'keys': [500]})
    old = previous(old_config, {p.address: p for p in [*survivors, removed]})
    old['pools'] = copy.deepcopy(old['known_pools'])
    before = copy.deepcopy(old)
    rpc = PrepareRPC([*survivors, replacement])
    prepare_fixture(monkeypatch, cfg)
    result = engine.prepare(rpc, cfg, previous=old)
    assert replacement.address in {row['address'] for row in result['known_pools']}
    assert removed.address not in {row['address'] for row in result['known_pools']}
    assert result['pools'] and old == before


def test_cold_first_cycle_can_require_a_late_pair_with_a_fresh_nonseed_token(monkeypatch):
    from searcher import engine
    cfg = config(seeds=6)
    cfg['policy']['cycle_pages'] = True
    cfg['settlement_tokens'] = [dict(cfg['settlement_tokens'][0], address=cfg['seed_tokens'][0])]
    a, y, x = cfg['seed_tokens'][0], cfg['seed_tokens'][-1], addr(109)
    fresh = next(iter(known(cfg, (109,)).values()))  # A-X visible in a current log.
    def pool(identifier, u, v, fee):
        return Pool(addr(identifier), FACTORY, 'v3', *sorted((u, v)), 10, fee,
                    sqrt_price=1 << 96, liquidity=10**12, block=BLOCK,
                    block_hash='0x' + f'{BLOCK:064x}')
    seed_link = pool(8201, a, y, 500)
    closing_link = pool(8202, y, x, 501)  # Last pair in the selected seven tokens.
    rpc = PrepareRPC([fresh, seed_link, closing_link])
    prepare_fixture(monkeypatch, cfg, {fresh.address: BLOCK})
    result = engine.prepare(rpc, cfg, previous={})
    assert {fresh.address, seed_link.address, closing_link.address} <= {
        row['address'] for row in result['known_pools']}
    assert result['pools']
