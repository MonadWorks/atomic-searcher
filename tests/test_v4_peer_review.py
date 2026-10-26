"""Independent metadata admission boundaries; no remote node or signing."""
import json

import pytest
from eth_abi import decode, encode

from searcher.core import Reject, USDC
from searcher.math import Q96
from searcher.v4_market import Key
from searcher.v4_peers import PeerFinder
from searcher.v4_runtime import Inventory


ASSET = '0xff00000000000000000000000000000000000011'
OTHER = '0xff00000000000000000000000000000000000012'


class Storage:
    def __init__(self, keys):
        self.words = {k.slot: Q96 | k.fee << 208 for k in keys}
        self.words.update({k.slot + 3: 100 for k in keys})
        self.headers = {100: 'before', 101: 'observed'}
        self.calls = []
        self.fail = False

    def request(self, method, params):
        self.calls.append((method, params))
        if method == 'eth_getBlockByNumber':
            return {'number': params[0], 'hash': self.headers[int(params[0], 16)]}
        assert method == 'eth_call'
        if self.fail:
            raise TimeoutError('synthetic transport failure')
        slots = decode(['bytes32[]'], bytes.fromhex(params[0]['data'][10:]))[0]
        assert len(slots) <= 128
        return '0x' + encode(['bytes32[]'], [[self.words.get(int.from_bytes(s, 'big'), 0).to_bytes(32, 'big') for s in slots]]).hex()


def seed(inv, keys):
    with inv.db:
        for key in keys:
            inv.db.execute('INSERT INTO pools VALUES(?,?,?,?)',
                           (key.id, json.dumps(key.values()), 100, 'before'))
            inv.registry.keys[key.id] = key
        inv.db.execute('INSERT INTO checkpoints VALUES(100,?)', ('before',))


@pytest.fixture
def metadata_only(monkeypatch):
    monkeypatch.setattr('searcher.v4_peers.verify_manager', lambda rpc: None)


def test_manager_authentication_failure_leaves_cursor_and_inventory_unchanged(tmp_path, monkeypatch):
    a = Key(USDC, ASSET, 375, 4)
    inv = Inventory(tmp_path / 'i.sqlite'); seed(inv, [a])
    def deny(rpc):
        raise Reject('wrong manager code hash')
    monkeypatch.setattr('searcher.v4_peers.verify_manager', deny)
    finder = PeerFinder(); rpc = Storage([])
    with pytest.raises(Reject, match='manager code'):
        finder.step(inv, rpc, {'number': '0x65', 'hash': 'observed'})
    assert not finder.verified and not rpc.calls
    assert inv.get('peer:pair_cursor') is None
    assert set(inv.registry.keys) == {a.id}
    inv.close()


def test_transport_failure_retries_same_page_without_partial_persistence(tmp_path, metadata_only):
    a, b = Key(USDC, ASSET, 375, 4), Key(USDC, ASSET, 100, 1)
    inv = Inventory(tmp_path / 'i.sqlite'); seed(inv, [a])
    finder = PeerFinder(); rpc = Storage([b]); rpc.fail = True
    with pytest.raises(TimeoutError):
        finder.step(inv, rpc, {'number': '0x65', 'hash': 'observed'})
    assert set(inv.registry.keys) == {a.id} and inv.get('peer:pair_cursor') is None
    first_call = rpc.calls[-1]
    rpc.fail = False
    assert finder.step(inv, rpc, {'number': '0x65', 'hash': 'observed'}) == {b.id}
    assert rpc.calls[-2] == first_call
    assert inv.db.execute('SELECT key,block,hash FROM pools WHERE id=?', (b.id,)).fetchone() == (json.dumps(b.values()), 101, 'observed')
    inv.close()


def test_one_bad_slot_invalidates_even_earlier_valid_peer(tmp_path, metadata_only):
    a, b, c = Key(USDC, ASSET, 375, 4), Key(USDC, ASSET, 1, 1), Key(USDC, ASSET, 100, 1)
    inv = Inventory(tmp_path / 'i.sqlite'); seed(inv, [a])
    rpc = Storage([b, c]); rpc.words[c.slot] = Q96 | 99 << 208
    with pytest.raises(Reject):
        PeerFinder().step(inv, rpc, {'number': '0x65', 'hash': 'observed'})
    assert set(inv.registry.keys) == {a.id}
    assert inv.db.execute('SELECT COUNT(*) FROM pools').fetchone() == (1,)
    assert inv.get('peer:pair_cursor') is None
    inv.close()


def test_currently_funded_peer_wins_capacity_over_empty_range_key(tmp_path, metadata_only):
    a, empty, funded = Key(USDC, ASSET, 375, 4), Key(USDC, ASSET, 1, 1), Key(USDC, ASSET, 100, 1)
    other = Key(USDC, OTHER, 375, 4)
    inv = Inventory(tmp_path / 'i.sqlite', maximum=2); seed(inv, [a, other])
    rpc = Storage([empty, funded]); rpc.words[empty.slot + 3] = 0
    finder = PeerFinder()
    assert finder.step(inv, rpc, {'number': '0x65', 'hash': 'observed'}) == {funded.id}
    assert set(inv.registry.keys) == {a.id, funded.id}
    assert finder.last['capacity_deferred'] == 1
    assert finder.last['displaced_singletons'] == 1
    inv.close()


def test_reorg_removes_new_peer_from_disk_and_memory_without_advancing_scan(tmp_path, metadata_only):
    a, b = Key(USDC, ASSET, 375, 4), Key(USDC, ASSET, 100, 1)
    path = tmp_path / 'i.sqlite'
    inv = Inventory(path); seed(inv, [a])
    with inv.db:
        inv.put('cursor', 90)
        inv.put('activity_cursor', 99)
    rpc = Storage([b])
    PeerFinder().step(inv, rpc, {'number': '0x65', 'hash': 'observed'})
    assert b.id in inv.registry.keys
    rpc.headers[101] = 'replacement'
    assert inv.check_reorg(rpc)
    assert set(inv.registry.keys) == {a.id}
    assert inv.get('cursor') == 90 and inv.get('activity_cursor') == 99
    inv.close(); inv = Inventory(path)
    assert set(inv.registry.keys) == {a.id}
    inv.close()


def test_empty_range_peer_does_not_evict_singleton_when_inventory_full(tmp_path, metadata_only):
    a, b, other = Key(USDC, ASSET, 375, 4), Key(USDC, ASSET, 100, 1), Key(USDC, OTHER, 375, 4)
    inv = Inventory(tmp_path / 'i.sqlite', maximum=2); seed(inv, [a, other])
    rpc = Storage([b]); rpc.words[b.slot + 3] = 0
    finder = PeerFinder()
    assert not finder.step(inv, rpc, {'number': '0x65', 'hash': 'observed'})
    assert set(inv.registry.keys) == {a.id, other.id}
    assert finder.last['capacity_deferred'] == 1 and finder.last['displaced_singletons'] == 0
    inv.close()


def test_remined_transaction_at_another_height_rewinds_instead_of_retrying_forever(tmp_path):
    from test_v4_activity_queue import ActivityRPC
    rpc = ActivityRPC()
    key = Key(USDC, ASSET, 375, 4)
    txid = rpc.add(key, height=99)
    # getLogs returned the old fork, but the same transaction was re-mined at
    # a different height before the head/transaction calls completed.
    rpc.branches.update({98: 'b', 99: 'b', 100: 'b'})
    rpc.transactions[txid].update(blockNumber='0x62', blockHash=rpc.header(98)['hash'])
    inv = Inventory(tmp_path / 'i.sqlite')
    try:
        assert inv.active_keys(rpc, rpc.header()) == set()
        assert not inv.registry.keys
        assert inv.db.execute('SELECT COUNT(*) FROM activity_queue').fetchone() == (0,)
        assert inv.get('activity_cursor') == 97
        assert inv.get('activity_status')['orphaned'] == 1
        rpc.logs[0].update(blockNumber='0x62', blockHash=rpc.header(98)['hash'])
        assert inv.active_keys(rpc, rpc.header()) == {key.id}
        assert inv.get('activity_cursor') == 100
    finally:
        inv.close()


def test_transaction_disappearing_on_orphaned_fork_releases_pool_representative(tmp_path):
    from test_v4_activity_queue import ActivityRPC
    rpc = ActivityRPC()
    key = Key(USDC, ASSET, 375, 4)
    txid = rpc.add(key, height=99)
    rpc.branches.update({99: 'b', 100: 'b'})
    rpc.transactions.pop(txid)
    inv = Inventory(tmp_path / 'i.sqlite')
    try:
        assert inv.active_keys(rpc, rpc.header()) == set()
        assert not inv.registry.keys
        assert inv.db.execute('SELECT COUNT(*) FROM activity_queue').fetchone() == (0,)
        assert inv.get('activity_cursor') == 98
        assert inv.get('activity_status')['orphaned'] == 1
        rpc.logs.clear()
        rpc.add(key, height=99, txid='0x' + '12' * 32)
        assert inv.active_keys(rpc, rpc.header()) == {key.id}
        assert inv.get('activity_cursor') == 100
    finally:
        inv.close()
