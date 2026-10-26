"""Persistent speculative work, using real RPC.views without external requests."""
import copy
from collections import Counter
from itertools import combinations
from types import SimpleNamespace

import pytest
from eth_abi import decode, encode

from searcher import engine
from searcher.core import MULTICALL, RPC, RPCUnavailable, ZERO, data, load_config
from searcher.discovery_expansion import (expansion_state, factory_registry_id,
                                         next_chunk, run_expansion)
from searcher.pools import Pool
from searcher.snapshots import read_universe, write_universe


def addr(n): return '0x' + f'{n:040x}'


FACTORY = addr(500)
HEIGHT = 123
HEADER = {'number': hex(HEIGHT), 'hash': '0x' + 'ab' * 32}
STATE_KEYS = ('factory_registry_id', 'expanded_tokens', 'pending_expansion_tokens', 'token_expansion')


def config_for(seeds, cap=12, keys=20):
    config = load_config()
    config['seed_tokens'] = list(seeds)
    config['factories'] = [{'address': FACTORY, 'kind': 'v3', 'keys': list(range(keys))}]
    config['policy'].update(max_new_tokens_per_refresh=cap, discovery_mode='logs', cycle_pages=False)
    return config


def known_for(tokens):
    return {addr(1000 + i): Pool(addr(1000 + i), FACTORY, 'v3', tokens[0], token, 60, 3000)
            for i, token in enumerate(tokens[1:])}


def checkpoint(result):
    return {key: copy.deepcopy(result[key]) for key in STATE_KEYS}


class AggregateRPC(RPC):
    def __init__(self):
        self.multicall_verified = True
        self.aggregates = []
        self.values = {}
        self.fail = None

    def verify_chain(self): pass

    def header(self, block=None):
        assert block in (None, HEIGHT)
        return dict(HEADER)

    def call(self, to, calldata, block):
        assert to == MULTICALL and block == HEIGHT
        assert calldata[:10] == data('aggregate3((address,bool,bytes)[])')
        entries = decode(['(address,bool,bytes)[]'], bytes.fromhex(calldata[10:]))[0]
        calls = [(target.lower(), '0x' + raw.hex()) for target, allow, raw in entries]
        assert calls and len(calls) <= 120 and all(allow for _, allow, _ in entries)
        self.aggregates.append(calls)
        if self.fail == 'transport':
            raise RPCUnavailable('local injected read failure')
        replies = [self.values.get(call, encode(['address'], [ZERO])) for call in calls]
        if self.fail == 'missing': replies[0] = None
        return encode(['(bool,bytes)[]'], [[(raw is not None, raw or b'') for raw in replies]])

    def add_pool(self, pool):
        self.values[(pool.factory, data('getPool(address,address,uint24)',
                     ['address', 'address', 'uint24'], [pool.token0, pool.token1, pool.fee]))] = encode(['address'], [pool.address])
        for signature, typ, value in [('factory()', 'address', pool.factory),
                                      ('token0()', 'address', pool.token0),
                                      ('token1()', 'address', pool.token1),
                                      ('tickSpacing()', 'int24', pool.spacing),
                                      ('fee()', 'uint24', pool.fee)]:
            self.values[(pool.address, data(signature))] = encode([typ], [value])


def test_full_selected_batch_is_bounded_and_resumes_every_pair_from_disk(tmp_path):
    tokens = [addr(i) for i in range(1, 19)]
    config, known = config_for(tokens[:6]), known_for(tokens)
    rpc, previous, observed = AggregateRPC(), {}, []
    path = tmp_path / 'universe.json'
    for turn in range(26):
        before = copy.deepcopy(previous)
        old_count = len(rpc.aggregates)
        result = run_expansion(rpc, config, previous, known, HEIGHT)
        assert previous == before
        assert len(rpc.aggregates) == old_count + 1
        assert result['getters_attempted'] <= 120
        assert result['getters_completed'] == result['getters_attempted']
        observed.extend(rpc.aggregates[-1])
        if turn < 25:
            assert not result['expanded_tokens']
            assert result['token_expansion'] is not None
        write_universe(path, checkpoint(result))
        previous = read_universe(path)
    expected = {(FACTORY, data('getPool(address,address,uint24)',
                 ['address', 'address', 'uint24'], [a, b, key]))
                for a, b in combinations(tokens, 2) for key in range(20)}
    assert len(observed) == len(set(observed)) == 3060
    assert set(observed) == expected
    assert previous['expanded_tokens'] == tokens
    assert previous['token_expansion'] is None and not previous['pending_expansion_tokens']
    assert [len(batch) for batch in rpc.aggregates] == [120] * 25 + [60]
    result = run_expansion(rpc, config, previous, known, HEIGHT)
    assert result['getters_attempted'] == 0 and len(rpc.aggregates) == 26


def test_old_pending_tokens_keep_fifo_position_during_continuous_arrivals(tmp_path):
    seed, waiting = addr(1), [addr(i) for i in range(2, 10)]
    config = config_for([seed], cap=2, keys=120)
    rpc, previous = AggregateRPC(), {}
    known = known_for([seed, *waiting])
    old_completed = set()
    for turn in range(15):
        result = run_expansion(rpc, config, previous, known, HEIGHT)
        old_completed |= set(result['expanded_tokens']) & set(waiting)
        pending = result['pending_expansion_tokens']
        assert len(pending) == len(set(pending))
        assert set(pending) <= {t for p in known.values() for t in (p.token0, p.token1)}
        # Two newly hot tokens per refresh cannot overtake the original FIFO.
        for j in range(2):
            token = addr(10000 + turn * 2 + j)
            known[addr(20000 + turn * 2 + j)] = Pool(addr(20000 + turn * 2 + j), FACTORY, 'v3', seed, token, 60, 3000)
        path = tmp_path / 'universe.json'
        write_universe(path, checkpoint(result))
        previous = read_universe(path)
        if old_completed == set(waiting): break
    assert old_completed == set(waiting)


@pytest.mark.parametrize('failure', ['transport', 'missing', 'wrong_count'])
def test_failed_chunk_is_not_committed_and_retries_the_same_getters(failure, tmp_path):
    config = config_for([addr(1)], cap=2, keys=120)
    known = known_for([addr(1), addr(2), addr(3)])
    rpc = AggregateRPC()
    if failure == 'wrong_count':
        real_views = rpc.views
        rpc.views = lambda calls, block: real_views(calls, block)[:-1]
    else:
        rpc.fail = failure
    failed = run_expansion(rpc, config, {}, known, HEIGHT)
    expected = copy.deepcopy(rpc.aggregates[0])
    assert failed['getters_completed'] == 0 and failed['error']
    assert not failed['expanded_tokens']
    file = tmp_path / 'checkpoint.json'
    write_universe(file, checkpoint(failed))
    rpc = AggregateRPC()
    recovered = run_expansion(rpc, config, read_universe(file), known, HEIGHT)
    assert rpc.aggregates == [expected]
    assert recovered['getters_completed'] == 120


@pytest.mark.parametrize('reason, committed', [
    ('metadata unavailable', False), ('factory reverse lookup unavailable', False),
    ('unexpected malformed ABI', False), ('factory outside supported registry', True),
    ('factory reverse lookup mismatch', True), ('invalid token order', True),
])
def test_identity_rejection_only_consumes_definitively_rejected_queries(reason, committed):
    config, known = config_for([addr(1)], cap=1, keys=1), known_for([addr(1), addr(2)])
    rpc = AggregateRPC()
    candidate = addr(999)
    query = data('getPool(address,address,uint24)', ['address', 'address', 'uint24'], [addr(1), addr(2), 0])
    rpc.values[(FACTORY, query)] = encode(['address'], [candidate])
    seen = []
    def validator(rpc, passed_config, pools, block):
        seen.append((passed_config, pools, block))
        return [], [{'pool': candidate, 'reason': reason}]
    result = run_expansion(rpc, config, {}, known, HEIGHT, validator=validator)
    assert seen == [(config, [candidate], HEIGHT)]
    assert bool(result['expanded_tokens']) == committed
    assert (result['token_expansion'] is None) == committed
    assert result['getters_completed'] == int(committed)
    assert result['errors'] == [{'pool': candidate, 'reason': reason}]


def test_real_identity_authentication_is_still_required_and_can_recover():
    config, known = config_for([addr(1)], cap=1, keys=1), known_for([addr(1), addr(2)])
    rpc = AggregateRPC()
    pool = Pool(addr(800), FACTORY, 'v3', addr(1), addr(2), 60, 0)
    rpc.add_pool(pool)
    rpc.values[(pool.address, data('token0()'))] = None
    failed = run_expansion(rpc, config, {}, known, HEIGHT)
    assert not failed['neighbors'] and failed['getters_completed'] == 0
    assert failed['errors'] == [{'pool': pool.address, 'reason': 'metadata unavailable'}]
    rpc.add_pool(pool)
    success = run_expansion(rpc, config, checkpoint(failed), known, HEIGHT)
    assert [p.address for p in success['neighbors']] == [pool.address]
    assert success['token_expansion'] is None and success['getters_completed'] == 1
    assert all(len(batch) <= 120 for batch in rpc.aggregates)


def test_registry_expansion_resets_old_completion_and_partial_cursor():
    config, known = config_for([addr(1)], cap=2, keys=120), known_for([addr(1), addr(2), addr(3)])
    rpc = AggregateRPC()
    first = run_expansion(rpc, config, {}, known, HEIGHT)
    changed = copy.deepcopy(config)
    changed['factories'][0]['keys'].append(120)
    restarted = run_expansion(rpc, changed, checkpoint(first), known, HEIGHT)
    assert restarted['factory_registry_id'] != first['factory_registry_id']
    assert not restarted['expanded_tokens']
    assert restarted['token_expansion']['remaining_pairs'][0] == [addr(1), addr(2)]
    assert restarted['token_expansion']['factory_offset'] == 120
    assert rpc.aggregates[-1] == rpc.aggregates[0]
    old_complete = dict(checkpoint(first), expanded_tokens=[addr(1), addr(2), addr(3)], token_expansion=None)
    state = expansion_state(changed, old_complete, known)
    assert not state['expanded_tokens'] and state['token_expansion']


def test_one_pair_with_more_than_one_multicall_of_keys_is_not_lost_on_resume(tmp_path):
    config, known = config_for([addr(1)], cap=1, keys=121), known_for([addr(1), addr(2)])
    rpc = AggregateRPC()
    first = run_expansion(rpc, config, {}, known, HEIGHT)
    assert first['token_expansion']['factory_offset'] == 120
    assert not first['expanded_tokens']
    file = tmp_path / 'checkpoint.json'
    write_universe(file, checkpoint(first))
    second = run_expansion(rpc, config, read_universe(file), known, HEIGHT)
    assert second['token_expansion'] is None and second['expanded_tokens'] == [addr(1), addr(2)]
    assert [len(batch) for batch in rpc.aggregates] == [120, 1]
    assert len({call for batch in rpc.aggregates for call in batch}) == 121


@pytest.mark.parametrize('seeds, tokens, keys, cap', [([], [], 20, 12), ([addr(1)], [], 20, 12),
    ([addr(1)], [addr(1), addr(2)], 0, 12), ([addr(1)], [addr(1), addr(2)], 20, 0)])
def test_empty_work_never_invokes_views(seeds, tokens, keys, cap):
    rpc = AggregateRPC()
    result = run_expansion(rpc, config_for(seeds, cap=cap, keys=keys), {}, known_for(tokens), HEIGHT)
    assert not rpc.aggregates and result['getters_attempted'] == 0


def install_feed_and_logs(monkeypatch, events, trace=None):
    monkeypatch.setattr('searcher.universe.swap_activity', lambda *args: (Counter(events), {p: HEIGHT for p in events}))
    class Feed:
        def __init__(self, rpc, config, saved): self.saved = saved
        def poll(self, height):
            assert height == HEIGHT
            if trace is not None: trace.append('snapshot')
            return SimpleNamespace(snapshot_pools=self.saved['pools'])
    monkeypatch.setattr('searcher.statefeed.StateFeed', Feed)


def test_prepare_prioritizes_recent_auth_then_known_edges_then_speculation(monkeypatch):
    tokens = [addr(i) for i in range(1, 5)]
    config, known = config_for(tokens[:2], cap=3, keys=20), known_for(tokens[:3])
    recent = Pool(addr(900), FACTORY, 'v3', tokens[0], tokens[3], 60, 1)
    trace = []
    install_feed_and_logs(monkeypatch, [recent.address], trace)
    def authenticate(rpc, config, pools, block):
        trace.append('recent_auth')
        assert pools == [recent.address] and block == HEIGHT
        return [recent], []
    def known_edges(rpc, config, block, pairs):
        trace.append('known_edges')
        assert block == HEIGHT and (recent.token0, recent.token1) in pairs
        return [], []
    original = engine.run_expansion
    def speculative(*args, **kwargs):
        trace.append('speculative')
        return original(*args, **kwargs)
    monkeypatch.setattr(engine, 'validate_pools', authenticate)
    monkeypatch.setattr(engine, 'discover_pairs', known_edges)
    monkeypatch.setattr(engine, 'run_expansion', speculative)
    result = engine.prepare(AggregateRPC(), config, previous={'known_pools': [p.json() for p in known.values()]})
    assert trace == ['recent_auth', 'known_edges', 'speculative', 'snapshot']
    assert recent.address in {p['address'] for p in result['known_pools']}


def test_speculative_transport_failure_still_publishes_current_universe(monkeypatch):
    tokens = [addr(i) for i in range(1, 19)]
    config, known = config_for(tokens[:6]), known_for(tokens)
    install_feed_and_logs(monkeypatch, [])
    monkeypatch.setattr(engine, 'discover_pairs', lambda *args: ([], []))
    previous = {'known_pools': [p.json() for p in known.values()], 'discovery_cursor': HEIGHT - 1}
    before = copy.deepcopy(previous)
    rpc = AggregateRPC()
    rpc.fail = 'transport'
    result = engine.prepare(rpc, config, previous=previous)
    assert previous == before
    assert result['discovery_cursor'] == HEIGHT and result['discovery_expansion_error']
    assert result['token_expansion'] and not result['expanded_tokens']
    assert result['discovery_expansion_progress']['factory_getters_completed'] == 0
    assert result['known_pools']


def test_empty_initial_seed_chunk_does_not_trap_cold_start_before_checkpoint(monkeypatch):
    tokens = [addr(i) for i in range(1, 7)]
    config, rpc = config_for(tokens), AggregateRPC()
    # The first 120 factory queries are empty; only the final seed pair has a pool.
    pool = Pool(addr(900), FACTORY, 'v3', tokens[-2], tokens[-1], 60, 19)
    rpc.add_pool(pool)
    install_feed_and_logs(monkeypatch, [])
    result = engine.prepare(rpc, config)
    assert pool.address in {p['address'] for p in result['known_pools']}
    assert result['discovery_expansion_progress']['seed_bootstrap'] is True
    assert result['expanded_tokens'] == tokens
    assert result['discovery_cursor'] == HEIGHT
    assert [len(batch) for batch in rpc.aggregates[:3]] == [120, 120, 60]
    assert result['token_expansion'] is None


def test_failed_final_snapshot_does_not_mutate_a_successful_expansion_checkpoint(monkeypatch):
    tokens = [addr(i) for i in range(1, 19)]
    config, known = config_for(tokens[:6]), known_for(tokens)
    install_feed_and_logs(monkeypatch, [])
    monkeypatch.setattr(engine, 'discover_pairs', lambda *args: ([], []))
    class FailingFeed:
        def __init__(self, *args): pass
        def poll(self, *args): raise RPCUnavailable('final snapshot failed')
    monkeypatch.setattr('searcher.statefeed.StateFeed', FailingFeed)
    first = run_expansion(AggregateRPC(), config, {}, known, HEIGHT)
    previous = dict(checkpoint(first), known_pools=[p.json() for p in known.values()])
    before = copy.deepcopy(previous)
    with pytest.raises(RPCUnavailable, match='final snapshot failed'):
        engine.prepare(AggregateRPC(), config, previous=previous)
    assert previous == before


def test_recent_isolated_pool_does_not_suppress_cold_seed_cycle_bootstrap(monkeypatch):
    tokens = [addr(i) for i in range(1, 7)]
    config, rpc = config_for(tokens), AggregateRPC()
    config['policy'].update(cycle_pages=True, max_hops=2)
    config['settlement_tokens'] = [dict(config['settlement_tokens'][0], address=tokens[-1])]
    isolated = Pool(addr(900), FACTORY, 'v3', addr(20), addr(21), 60, 0)
    pool_a = Pool(addr(901), FACTORY, 'v3', tokens[-2], tokens[-1], 60, 18)
    pool_b = Pool(addr(902), FACTORY, 'v3', tokens[-2], tokens[-1], 60, 19)
    for pool in (isolated, pool_a, pool_b): rpc.add_pool(pool)
    install_feed_and_logs(monkeypatch, [isolated.address])
    result = engine.prepare(rpc, config)
    assert result['discovery_expansion_progress']['seed_bootstrap'] is True
    assert {p['address'] for p in result['known_pools']} == {isolated.address, pool_a.address, pool_b.address}
    assert {p['address'] for p in result['pools']} == {pool_a.address, pool_b.address}
    assert result['discovery_cursor'] == HEIGHT


def test_zero_admission_keeps_pending_without_requerying_completed_seeds():
    tokens = [addr(i) for i in range(1, 5)]
    config, known = config_for(tokens[:2], cap=0), known_for(tokens)
    previous = dict(factory_registry_id=factory_registry_id(config), expanded_tokens=tokens[:2],
                    pending_expansion_tokens=tokens[2:], token_expansion=None)
    rpc = AggregateRPC()
    for _ in range(3):
        result = run_expansion(rpc, config, previous, known, HEIGHT)
        assert result['pending_expansion_tokens'] == tokens[2:]
        assert result['token_expansion'] is None
        previous = checkpoint(result)
    assert not rpc.aggregates


@pytest.mark.parametrize('corruption', ['middle_missing', 'reordered'])
def test_non_suffix_checkpoint_restarts_unfinished_work_instead_of_skipping(corruption):
    tokens = [addr(i) for i in range(1, 5)]
    config, known = config_for(tokens[:1], cap=3), known_for(tokens)
    state = expansion_state(config, {}, known)
    original = copy.deepcopy(state)
    if corruption == 'middle_missing':
        del state['token_expansion']['remaining_pairs'][2]
    else:
        state['token_expansion']['remaining_pairs'].reverse()
    recovered = expansion_state(config, state, known)
    assert recovered == original
