"""Persistent V4 activity discovery regressions; synthetic RPC, no network."""
from collections import Counter

import pytest
from eth_abi import encode

from searcher import v4_runtime as runtime
from searcher.core import USDC
from searcher.v4_market import KEY_TYPES, MANAGER, SWAP, Key
from searcher.v4_runtime import Inventory


ASSET = '0xff00000000000000000000000000000000000001'


class ActivityRPC:
    def __init__(self, tip=100):
        self.tip = tip
        self.logs = []
        self.transactions = {}
        self.branches = {}
        self.log_ranges = []
        self.transaction_reads = []
        self.header_reads = []
        self.batches = []
        self.missing_headers = set()
        self.transport_failure = False
        self.on_transaction = lambda: None

    def header(self, height=None):
        height = self.tip if height is None else height
        return {'number': hex(height), 'hash': '0x'+format((ord(self.branches.get(height, 'a')) << 248)+height, '064x')}

    def add(self, key, height=99, *, decodable=True, txid=None):
        txid = txid or '0x'+format(len(self.logs)+1, '064x')
        log = {'address': MANAGER, 'topics': [SWAP, key.id],
               'blockNumber': hex(height), 'blockHash': self.header(height)['hash'],
               'logIndex': hex(sum(int(l['blockNumber'], 16) == height for l in self.logs)),
               'transactionHash': txid}
        self.logs.append(log)
        self.transactions[txid] = {'hash': txid, 'blockHash': log['blockHash'],
            'blockNumber': hex(height),
            'input': '0x12345678'+encode(KEY_TYPES, key.values()).hex() if decodable else '0x12345678'}
        return txid

    def request(self, method, params):
        if method == 'eth_getBlockByNumber':
            height = int(params[0], 16)
            self.header_reads.append(height)
            return None if height in self.missing_headers else self.header(height)
        if method == 'eth_getTransactionByHash':
            self.on_transaction()
            self.transaction_reads.append(params[0])
            if self.transport_failure:
                raise TimeoutError('synthetic transaction response lost')
            return self.transactions.get(params[0])
        assert method == 'eth_getLogs'
        lo, hi = int(params[0]['fromBlock'], 16), int(params[0]['toBlock'], 16)
        self.log_ranges.append((lo, hi))
        return [log for log in self.logs if lo <= int(log['blockNumber'], 16) <= hi]

    def batch(self, calls):
        assert 0 < len(calls) <= 2
        self.batches.append(len(calls))
        return [self.request(*call) for call in calls]


def keys(count):
    return [Key(USDC, ASSET, 500+i, 1) for i in range(count)]


def queued(inventory):
    return inventory.db.execute('SELECT COUNT(*) FROM activity_queue').fetchone()[0]


def test_unknown_transactions_after_first_two_survive_close_and_restart(tmp_path):
    rpc = ActivityRPC()
    market = keys(7)
    txids = [rpc.add(key, decodable=i >= 2) for i, key in enumerate(market)]
    path = tmp_path/'inventory.sqlite'
    inventory = Inventory(path)
    assert inventory.active_keys(rpc, rpc.header()) == set()
    assert queued(inventory) == 5 and inventory.get('activity_cursor') == 100
    assert rpc.transaction_reads == txids[:2]
    inventory.close()
    inventory = Inventory(path)
    try:
        # The tip need not advance to drain durable work already observed.
        for _ in range(3):
            inventory.active_keys(rpc, rpc.header())
        assert set(inventory.registry.keys) == {key.id for key in market[2:]}
        assert queued(inventory) == 0
        assert rpc.transaction_reads == txids
        assert rpc.log_ranges == [(99, 100)]
        assert max(rpc.batches) == 2
    finally:
        inventory.close()


def test_missing_transaction_and_header_retry_without_blocking_other_queued_work(tmp_path, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(runtime.time, 'time', lambda: clock[0])
    rpc = ActivityRPC()
    first, second = keys(2)
    first_tx, second_tx = rpc.add(first), rpc.add(second)
    delayed = rpc.transactions.pop(first_tx)
    path = tmp_path/'inventory.sqlite'
    inventory = Inventory(path)
    inventory.active_keys(rpc, rpc.header(), limit=1)
    assert queued(inventory) == 2
    inventory.close()
    inventory = Inventory(path)
    try:
        assert inventory.active_keys(rpc, rpc.header(), limit=1) == {second.id}
        assert first.id not in inventory.registry.keys and queued(inventory) == 1
        assert rpc.transaction_reads == [first_tx, second_tx]
        rpc.transactions[first_tx] = delayed
        rpc.missing_headers.add(99)
        clock[0] += 3
        assert inventory.active_keys(rpc, rpc.header(), limit=1) == set()
        assert queued(inventory) == 1
        rpc.missing_headers.clear()
        clock[0] += 5
        assert inventory.active_keys(rpc, rpc.header(), limit=1) == {first.id}
        assert queued(inventory) == 0
    finally:
        inventory.close()


def test_transport_failure_after_enqueue_preserves_cursor_and_work_across_restart(tmp_path):
    rpc = ActivityRPC()
    key = keys(1)[0]
    txid = rpc.add(key)
    path = tmp_path/'inventory.sqlite'
    inventory = Inventory(path)
    rpc.transport_failure = True
    with pytest.raises(TimeoutError, match='response lost'):
        inventory.active_keys(rpc, rpc.header())
    assert queued(inventory) == 1 and inventory.get('activity_cursor') == 100
    assert not inventory.registry.keys
    inventory.close()
    rpc.transport_failure = False
    inventory = Inventory(path)
    try:
        assert inventory.active_keys(rpc, rpc.header()) == {key.id}
        assert queued(inventory) == 0 and rpc.transaction_reads == [txid, txid]
    finally:
        inventory.close()


def test_overfull_single_block_has_durable_resume_without_redecoding_prefix(tmp_path):
    rpc = ActivityRPC()
    market = keys(7)
    txids = [rpc.add(key, decodable=i != 0) for i, key in enumerate(market)]
    path = tmp_path/'inventory.sqlite'
    inventory = Inventory(path, activity_capacity=2)
    peaks = []
    rpc.on_transaction = lambda: peaks.append(queued(inventory))
    inventory.active_keys(rpc, rpc.header(), limit=1)
    assert inventory.get('activity_cursor') == 98
    assert inventory.get('activity_resume') == {'block': 99, 'hash': rpc.header(99)['hash'], 'next': 2}
    assert inventory.get('activity_status')['backpressure'] is True
    inventory.close()
    inventory = Inventory(path, activity_capacity=2)
    try:
        # Advancing the head must not shift the default start past a partial block.
        rpc.tip = 110
        for _ in range(10):
            inventory.active_keys(rpc, rpc.header(), limit=1)
        assert set(inventory.registry.keys) == {key.id for key in market[1:]}
        assert Counter(rpc.transaction_reads) == Counter(txids)
        assert queued(inventory) == 0 and max(peaks) == 2
        assert inventory.get('activity_cursor') == 110
        assert inventory.get('activity_resume') is None
    finally:
        inventory.close()


def test_activity_catchup_scans_contiguous_pages_of_at_most_64_blocks(tmp_path):
    rpc = ActivityRPC(tip=230)
    market = keys(3)
    for key, height in zip(market, [82, 149, 227]):
        rpc.add(key, height)
    inventory = Inventory(tmp_path/'inventory.sqlite')
    try:
        with inventory.db:
            inventory.put('activity_cursor', 80)
        for _ in range(5):
            inventory.active_keys(rpc, rpc.header())
        # One bounded current window is prioritized; history still advances
        # through every original contiguous page without skipping coverage.
        assert rpc.log_ranges == [(167, 230), (81, 144), (145, 208), (209, 230)]
        assert all(hi-lo+1 <= 64 for lo, hi in rpc.log_ranges)
        assert set(inventory.registry.keys) == {key.id for key in market}
    finally:
        inventory.close()


def test_reorg_drops_orphan_queue_and_rediscovers_canonical_replacement(tmp_path):
    rpc = ActivityRPC()
    orphan, replacement = keys(2)
    txid = rpc.add(orphan)
    rpc.transactions.pop(txid)
    inventory = Inventory(tmp_path/'inventory.sqlite')
    try:
        inventory.active_keys(rpc, rpc.header())
        assert queued(inventory) == 1
        rpc.logs.clear()
        rpc.branches.update({99: 'b', 100: 'b'})
        rpc.add(replacement, txid='0x'+format(22, '064x'))
        assert inventory.active_keys(rpc, rpc.header()) == {replacement.id}
        assert orphan.id not in inventory.registry.keys
        assert queued(inventory) == 0
        assert inventory.get('activity_cursor') == 100
    finally:
        inventory.close()


def test_ahead_observation_checkpoint_reorg_cannot_advance_initialize_cursor(tmp_path):
    rpc = ActivityRPC(tip=130)
    inventory = Inventory(tmp_path/'inventory.sqlite')
    try:
        with inventory.db:
            inventory.put('cursor', 100)
            inventory.put('activity_cursor', 129)
            inventory.put('activity_resume', {'block': 130, 'hash': 'orphan-130', 'next': 2})
            inventory.db.executemany('INSERT INTO checkpoints VALUES(?,?)', [(120, rpc.header(120)['hash']), (130, 'orphan-130')])
            inventory.db.execute('INSERT INTO activity_queue(txid,pool_id,block,hash) VALUES(?,?,?,?)',
                                 ('tx', keys(1)[0].id, 125, 'orphan-125'))
        assert inventory.check_reorg(rpc)
        assert inventory.get('cursor') == 100
        assert inventory.get('activity_cursor') == 120
        assert queued(inventory) == 0
        assert inventory.get('activity_resume') is None
    finally:
        inventory.close()


def test_full_registry_keeps_valid_activity_deferred_until_room_is_available(tmp_path, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(runtime.time, 'time', lambda: clock[0])
    rpc = ActivityRPC()
    first, second = keys(2)
    rpc.add(second)
    inventory = Inventory(tmp_path/'inventory.sqlite', maximum=1)
    try:
        inventory.registry.keys[first.id] = first
        assert inventory.active_keys(rpc, rpc.header()) == set()
        assert queued(inventory) == 1 and inventory.get('activity_status')['deferred'] == 1
        inventory.registry.keys.clear()
        clock[0] += 3
        assert inventory.active_keys(rpc, rpc.header()) == {second.id}
        assert queued(inventory) == 0
    finally:
        inventory.close()


def test_repeated_hot_pool_cannot_fill_inbox_ahead_of_new_pool_tasks(tmp_path):
    rpc = ActivityRPC()
    hot, first, second = keys(3)
    for _ in range(100):
        rpc.add(hot, decodable=False)
    rpc.add(first)
    rpc.add(second)
    inventory = Inventory(tmp_path/'inventory.sqlite', activity_capacity=4)
    observed = []
    rpc.on_transaction = lambda: observed.append(dict(inventory.db.execute(
        'SELECT pool_id,COUNT(*) FROM activity_queue GROUP BY pool_id').fetchall()))
    try:
        inventory.active_keys(rpc, rpc.header())
        assert observed[0] == {hot.id: 2, first.id: 1, second.id: 1}
        assert inventory.get('activity_cursor') == 100
        assert inventory.get('activity_resume') is None
        assert inventory.get('activity_status')['coalesced'] == 98
        assert inventory.active_keys(rpc, rpc.header()) == {first.id, second.id}
        assert queued(inventory) == 0
        assert len(rpc.transaction_reads) == 4
    finally:
        inventory.close()


def test_first_representative_undecodable_second_resolves_and_cleans_pool_task(tmp_path):
    rpc = ActivityRPC()
    key = keys(1)[0]
    first = rpc.add(key, decodable=False)
    second = rpc.add(key)
    third = rpc.add(key)
    inventory = Inventory(tmp_path/'inventory.sqlite')
    try:
        assert inventory.active_keys(rpc, rpc.header(), limit=1) == set()
        assert queued(inventory) == 1 and rpc.transaction_reads == [first]
        assert inventory.active_keys(rpc, rpc.header(), limit=1) == {key.id}
        assert queued(inventory) == 0
        assert rpc.transaction_reads == [first, second] and third not in rpc.transaction_reads
    finally:
        inventory.close()


def test_undecodable_samples_do_not_permanently_exclude_future_pool_activity(tmp_path):
    rpc = ActivityRPC()
    key = keys(1)[0]
    rpc.add(key, decodable=False)
    rpc.add(key, decodable=False)
    inventory = Inventory(tmp_path/'inventory.sqlite')
    try:
        assert inventory.active_keys(rpc, rpc.header()) == set()
        assert queued(inventory) == 0 and not inventory.registry.keys
        rpc.tip = 101
        later = rpc.add(key, height=101)
        assert inventory.active_keys(rpc, rpc.header()) == {key.id}
        assert rpc.transaction_reads[-1] == later
    finally:
        inventory.close()


def test_multi_pool_priority_preserves_fifo_progress_for_single_pool_transactions(tmp_path):
    rpc = ActivityRPC()
    market = keys(9)
    singles = [rpc.add(key) for key in market[:3]]
    inventory = Inventory(tmp_path/'inventory.sqlite')
    try:
        for turn in range(3):
            pair = market[3+2*turn:5+2*turn]
            height = 99 if turn == 0 else 100+turn
            rpc.tip = max(100, height)
            txid = '0x'+format(100+turn, '064x')
            for key in pair:
                rpc.add(key, height, txid=txid)
            rpc.transactions[txid]['input'] = '0x12345678'+''.join(encode(KEY_TYPES, key.values()).hex() for key in pair)
            before = len(rpc.transaction_reads)
            result = inventory.active_keys(rpc, rpc.header())
            assert rpc.transaction_reads[before:] == [txid, singles[turn]]
            assert result == {pair[0].id, pair[1].id, market[turn].id}
        assert set(inventory.registry.keys) == {key.id for key in market}
        assert queued(inventory) == 0
    finally:
        inventory.close()


def test_external_canonical_discovery_removes_pending_pool_without_transaction_reread(tmp_path):
    rpc = ActivityRPC()
    key = keys(1)[0]
    for _ in range(2):
        txid = rpc.add(key)
        rpc.transactions.pop(txid)
    inventory = Inventory(tmp_path/'inventory.sqlite')
    try:
        inventory.active_keys(rpc, rpc.header())
        assert queued(inventory) == 2
        before = list(rpc.transaction_reads)
        inventory.registry.keys[key.id] = key  # Same published mapping used by Initialize/PeerFinder.
        assert inventory.active_keys(rpc, rpc.header()) == set()
        assert queued(inventory) == 0 and rpc.transaction_reads == before
        assert inventory.get('activity_status')['already_known'] == 2
    finally:
        inventory.close()


def test_unverified_log_hash_may_wait_in_inbox_but_cannot_authorize_pool(tmp_path):
    rpc = ActivityRPC()
    key = keys(1)[0]
    rpc.add(key)
    forged = '0x'+'ef'*32
    rpc.logs[0]['blockHash'] = forged
    inventory = Inventory(tmp_path/'inventory.sqlite')
    try:
        rpc.transport_failure = True
        with pytest.raises(TimeoutError):
            inventory.active_keys(rpc, rpc.header())
        assert inventory.db.execute('SELECT hash FROM activity_queue').fetchone()[0] == forged
        assert not inventory.registry.keys and rpc.header_reads == [100]
        rpc.transport_failure = False
        assert inventory.active_keys(rpc, rpc.header()) == set()
        assert queued(inventory) == 0 and not inventory.registry.keys
        assert rpc.header_reads == [100, 99]
        assert inventory.get('activity_status')['orphaned'] == 1
    finally:
        inventory.close()


def test_dense_64_block_page_only_reads_end_and_selected_transaction_headers(tmp_path):
    rpc = ActivityRPC(tip=64)
    for height, key in enumerate(keys(64), 1):
        rpc.add(key, height)
    inventory = Inventory(tmp_path/'inventory.sqlite')
    try:
        with inventory.db:
            inventory.put('activity_cursor', 0)
        inventory.active_keys(rpc, rpc.header())
        assert rpc.log_ranges == [(1, 64)]
        assert rpc.header_reads == [64, 1, 2]
        assert len(rpc.transaction_reads) == 2
        assert queued(inventory) == 62 and max(rpc.batches) == 2
    finally:
        inventory.close()


def test_legacy_duplicate_inbox_is_compacted_before_more_transaction_reads(tmp_path):
    rpc = ActivityRPC()
    hot, fresh = keys(2)
    inventory = Inventory(tmp_path/'inventory.sqlite')
    try:
        with inventory.db:
            inventory.put('activity_cursor', 100)
            for key in [hot]*6+[fresh]:
                txid = rpc.add(key, decodable=key == fresh)
                inventory.db.execute('INSERT INTO activity_queue(txid,pool_id,block,hash) VALUES(?,?,?,?)',
                                     (txid, key.id, 99, rpc.header(99)['hash']))
        inventory.active_keys(rpc, rpc.header())
        assert inventory.get('activity_status')['coalesced'] == 4
        assert queued(inventory) == 1
        assert inventory.active_keys(rpc, rpc.header()) == {fresh.id}
        assert len(rpc.transaction_reads) == 3 and queued(inventory) == 0
    finally:
        inventory.close()
