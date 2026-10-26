import json
import pytest
from eth_abi import decode, encode
from searcher.core import USDC, Reject
from searcher.math import Q96
from searcher.v4_market import Key
from searcher.v4_runtime import Inventory
from searcher.v4_peers import PeerFinder

ASSET = '0xff00000000000000000000000000000000000001'
OTHER = '0xff00000000000000000000000000000000000002'


class StorageRPC:
    def __init__(self, keys):
        self.words = {k.slot: Q96 | (k.fee << 208) for k in keys}
        self.words.update({k.slot+3: 1000 for k in keys})
        self.hash = 'canonical'
        self.calls = []

    def request(self, method, params):
        self.calls.append((method, params))
        if method == 'eth_call':
            slots = decode(['bytes32[]'], bytes.fromhex(params[0]['data'][10:]))[0]
            return '0x'+encode(['bytes32[]'], [[self.words.get(int.from_bytes(s, 'big'), 0).to_bytes(32, 'big') for s in slots]]).hex()
        if method == 'eth_getBlockByNumber':
            return {'number': params[0], 'hash': self.hash}
        raise AssertionError(method)


def seed(inv, keys):
    with inv.db:
        for i, k in enumerate(keys):
            inv.db.execute('INSERT INTO pools VALUES(?,?,?,?)', (k.id, json.dumps(k.values()), i+1, 'old'))
            inv.registry.keys[k.id] = k


def finder(monkeypatch):
    calls = []
    monkeypatch.setattr('searcher.v4_peers.verify_manager', lambda rpc: calls.append(rpc))
    return PeerFinder(), calls


def test_singleton_gains_real_peer_without_historical_logs_and_survives_restart(tmp_path, monkeypatch):
    path = tmp_path/'i.db'
    a, b = Key(USDC, ASSET, 375, 4), Key(USDC, ASSET, 100, 1)
    inv = Inventory(path); seed(inv, [a]); peer, verified = finder(monkeypatch)
    rpc = StorageRPC([a, b]); touched = peer.step(inv, rpc, {'number': '0x64', 'hash': 'canonical'})
    assert touched == {b.id} and len(verified) == 1
    assert rpc.calls[0][1][1] == '0x64' and not any(m == 'eth_getLogs' for m, _ in rpc.calls)
    assert list(inv.registry.pairs(USDC))
    inv.close(); inv = Inventory(path)
    assert set(inv.registry.keys) == {a.id, b.id}
    assert inv.db.execute('SELECT hash FROM checkpoints WHERE block=100').fetchone()[0] == 'canonical'
    inv.close()


@pytest.mark.parametrize('fault', ['reorg', 'fee', 'tick', 'protocol', 'upper_bits', 'short'])
def test_invalid_or_reorganized_state_does_not_mutate_inventory(tmp_path, monkeypatch, fault):
    a, b = Key(USDC, ASSET, 375, 4), Key(USDC, ASSET, 100, 1)
    inv = Inventory(tmp_path/'i.db'); seed(inv, [a]); peer, _ = finder(monkeypatch); rpc = StorageRPC([b])
    if fault == 'reorg': rpc.hash = 'fork'
    if fault == 'fee': rpc.words[b.slot] = Q96 | (101 << 208)
    if fault == 'tick': rpc.words[b.slot] |= 900000 << 160
    if fault == 'protocol': rpc.words[b.slot] |= 1001 << 184
    if fault == 'upper_bits': rpc.words[b.slot] |= 1 << 240
    if fault == 'short':
        original = rpc.request
        rpc.request = lambda m, p: '0x'+encode(['bytes32[]'], [[]]).hex() if m == 'eth_call' else original(m, p)
    with pytest.raises(Reject): peer.step(inv, rpc, {'number': '0x64', 'hash': 'canonical'})
    assert set(inv.registry.keys) == {a.id} and inv.get('peer:pair_cursor') is None
    assert inv.db.execute('SELECT count(*) FROM pools').fetchone()[0] == 1
    inv.close()


def test_counterpart_displaces_unrelated_singleton_at_capacity(tmp_path, monkeypatch):
    a, b, other = Key(USDC, ASSET, 375, 4), Key(USDC, ASSET, 100, 1), Key(USDC, OTHER, 375, 4)
    inv = Inventory(tmp_path/'i.db', maximum=2); seed(inv, [a, other]); peer, _ = finder(monkeypatch)
    assert peer.step(inv, StorageRPC([b]), {'number': '0x64', 'hash': 'canonical'}) == {b.id}
    assert set(inv.registry.keys) == {a.id, b.id} and peer.last['displaced_singletons'] == 1
    assert inv.db.execute('SELECT count(*) FROM pools').fetchone()[0] == 2
    inv.close()


def test_full_complete_pairs_remain_bounded_and_report_deferred(tmp_path, monkeypatch):
    a, b, extra = Key(USDC, ASSET, 375, 4), Key(USDC, ASSET, 100, 1), Key(USDC, ASSET, 500, 10)
    inv = Inventory(tmp_path/'i.db', maximum=2); seed(inv, [a, b]); peer, _ = finder(monkeypatch)
    assert not peer.step(inv, StorageRPC([extra]), {'number': '0x64', 'hash': 'canonical'})
    assert set(inv.registry.keys) == {a.id, b.id} and peer.last['capacity_deferred'] == 1
    inv.close()


def test_pair_and_template_pages_rotate_and_use_observed_nonstandard_fee(tmp_path, monkeypatch):
    a, other = Key(USDC, ASSET, 375, 4), Key(USDC, OTHER, 913, 7)
    b = Key(USDC, ASSET, 913, 7)
    inv = Inventory(tmp_path/'i.db'); seed(inv, [a, other]); peer, _ = finder(monkeypatch); rpc = StorageRPC([b])
    seen = set()
    for _ in range(40):
        peer.step(inv, rpc, {'number': '0x64', 'hash': 'canonical'}, limit=1)
        seen.add(tuple(peer.last['pair']))
        if b.id in inv.registry.keys: break
    assert b.id in inv.registry.keys and len(seen) == 2
    for method, params in rpc.calls:
        if method == 'eth_call': assert len(decode(['bytes32[]'], bytes.fromhex(params[0]['data'][10:]))[0]) <= 2
    inv.close()


def test_empty_current_range_is_retained_for_exact_gap_quotes(tmp_path, monkeypatch):
    a, b = Key(USDC, ASSET, 375, 4), Key(USDC, ASSET, 100, 1)
    inv = Inventory(tmp_path/'i.db'); seed(inv, [a]); peer, _ = finder(monkeypatch); rpc = StorageRPC([b])
    rpc.words[b.slot+3] = 0
    assert peer.step(inv, rpc, {'number': '0x64', 'hash': 'canonical'}) == {b.id}
    assert peer.last['empty_range'] == 1 and b.id in inv.registry.keys
    assert inv.registry.pairs(USDC)
    inv.close()


def test_reorg_restores_singleton_displaced_by_orphaned_counterpart(tmp_path, monkeypatch):
    a, b, other = Key(USDC, ASSET, 375, 4), Key(USDC, ASSET, 100, 1), Key(USDC, OTHER, 375, 4)
    inv = Inventory(tmp_path/'i.db', maximum=2); seed(inv, [a, other]); peer, _ = finder(monkeypatch)
    with inv.db:
        inv.db.execute('INSERT INTO checkpoints VALUES(90,?)', ('ancestor',))
        inv.put('cursor', 80)
    peer.step(inv, StorageRPC([b]), {'number': '0x64', 'hash': 'canonical'})
    assert set(inv.registry.keys) == {a.id, b.id}
    class Reorg:
        def request(self, method, params):
            height = int(params[0], 16)
            return {'number':params[0], 'hash':'ancestor' if height == 90 else 'replacement'}
    assert inv.check_reorg(Reorg())
    assert set(inv.registry.keys) == {a.id, other.id}
    assert inv.get('cursor') == 80
    assert inv.db.execute('SELECT count(*) FROM peer_displaced').fetchone()[0] == 0
    inv.close()
