"""Independent causal coverage tests: synthetic public chain, no network or wallet."""
from eth_abi import encode
import pytest

from searcher import v4_runtime as runtime
from searcher.v4_market import KEY_TYPES
from test_v4_activity_queue import ActivityRPC, keys


def backlog(inventory, rpc, market, *, height=10, cursor=20, decodable=True):
    """Seed already observed durable work without providing any future key data."""
    txs = []
    with inventory.db:
        inventory.put('activity_cursor', cursor)
        for key in market:
            tx = rpc.add(key, height, decodable=decodable)
            txs.append(tx)
            inventory.db.execute('INSERT INTO activity_queue(txid,pool_id,block,hash) VALUES(?,?,?,?)',
                                 (tx, key.id, height, rpc.header(height)['hash']))
    return txs


def add_pair(rpc, pair, height, txid):
    for key in pair:
        rpc.add(key, height, txid=txid)
    rpc.transactions[txid]['input'] = '0x12345678'+''.join(encode(KEY_TYPES, key.values()).hex() for key in pair)


def test_full_old_inbox_cannot_hide_new_complete_pair_and_keeps_old_work(tmp_path):
    rpc = ActivityRPC(tip=200)
    market = keys(6)
    inventory = runtime.Inventory(tmp_path/'inventory.sqlite', activity_capacity=4)
    try:
        old = backlog(inventory, rpc, market[:4])
        latest = '0x'+format(900, '064x')
        add_pair(rpc, market[4:], 199, latest)
        inventory.active_keys(rpc, rpc.header())
        assert {key.id for key in market[4:]}.issubset(inventory.registry.keys)
        assert latest in rpc.transaction_reads and len(set(old).intersection(rpc.transaction_reads)) == 1
        assert inventory.db.execute('SELECT COUNT(*) FROM activity_queue').fetchone()[0] == 3
        assert inventory.get('activity_cursor') <= 20
        assert len(rpc.log_ranges) == 1 and rpc.log_ranges[0][1] == 200
        assert len(rpc.transaction_reads) <= 2 and max(rpc.batches) <= 2
    finally:
        inventory.close()


def test_two_old_undecodable_representatives_do_not_block_new_public_input(tmp_path):
    rpc = ActivityRPC(tip=200)
    hot, peer, old = keys(3)
    inventory = runtime.Inventory(tmp_path/'inventory.sqlite', activity_capacity=4)
    try:
        obsolete = backlog(inventory, rpc, [hot, hot, old], decodable=False)
        latest = '0x'+format(901, '064x')
        add_pair(rpc, [hot, peer], 199, latest)
        inventory.active_keys(rpc, rpc.header())
        assert {hot.id, peer.id}.issubset(inventory.registry.keys)
        assert latest in rpc.transaction_reads
        assert len(rpc.transaction_reads) <= 2
        assert inventory.db.execute('SELECT COUNT(*) FROM activity_queue WHERE pool_id=?', (hot.id,)).fetchone()[0] == 0
        assert any(tx not in rpc.transaction_reads for tx in obsolete)
    finally:
        inventory.close()


def test_continuous_recent_pairs_still_decode_old_fifo_with_original_tx_budget(tmp_path):
    rpc = ActivityRPC(tip=200)
    market = keys(20)
    inventory = runtime.Inventory(tmp_path/'inventory.sqlite', activity_capacity=4)
    try:
        old = backlog(inventory, rpc, market[:4])
        for turn in range(6):
            rpc.tip = 200+turn
            add_pair(rpc, market[4+2*turn:6+2*turn], rpc.tip, '0x'+format(910+turn, '064x'))
            before_tx, before_logs = len(rpc.transaction_reads), len(rpc.log_ranges)
            inventory.active_keys(rpc, rpc.header())
            assert len(rpc.transaction_reads)-before_tx <= 2
            assert len(rpc.log_ranges)-before_logs <= 1
        assert set(old).issubset(rpc.transaction_reads)
        assert {key.id for key in market[:4]}.issubset(inventory.registry.keys)
        assert inventory.get('activity_cursor') > 20
    finally:
        inventory.close()


def test_live_claimed_hash_cannot_admit_key_even_if_input_contains_key(tmp_path):
    rpc = ActivityRPC(tip=200)
    old, forged = keys(2)
    inventory = runtime.Inventory(tmp_path/'inventory.sqlite', activity_capacity=1)
    try:
        backlog(inventory, rpc, [old])
        tx = rpc.add(forged, 199)
        rpc.logs[-1]['blockHash'] = '0x'+'ff'*32
        inventory.active_keys(rpc, rpc.header())
        assert tx in rpc.transaction_reads
        assert forged.id not in inventory.registry.keys
        assert inventory.db.execute('SELECT 1 FROM pools WHERE id=?', (forged.id,)).fetchone() is None
    finally:
        inventory.close()


def test_missing_live_transaction_survives_restart_without_future_input_admission(tmp_path, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(runtime.time, 'time', lambda: clock[0])
    rpc = ActivityRPC(tip=200)
    old, new = keys(2)
    path = tmp_path/'inventory.sqlite'
    inventory = runtime.Inventory(path, activity_capacity=1)
    backlog(inventory, rpc, [old])
    tx = rpc.add(new, 199)
    captured = rpc.transactions.pop(tx)
    inventory.active_keys(rpc, rpc.header())
    assert tx in rpc.transaction_reads and new.id not in inventory.registry.keys
    inventory.close()
    rpc.transactions[tx] = captured
    clock[0] += 10
    inventory = runtime.Inventory(path, activity_capacity=1)
    try:
        for _ in range(4):
            inventory.active_keys(rpc, rpc.header())
            if new.id in inventory.registry.keys:
                break
        assert new.id in inventory.registry.keys
        assert inventory.db.execute('SELECT key FROM pools WHERE id=?', (new.id,)).fetchone() is not None
    finally:
        inventory.close()


def test_same_public_transaction_visible_in_both_lanes_is_read_once(tmp_path):
    rpc = ActivityRPC(tip=200)
    shared, peer, old = keys(3)
    inventory = runtime.Inventory(tmp_path/'inventory.sqlite', activity_capacity=4)
    try:
        backlog(inventory, rpc, [old])
        tx = '0x'+format(930, '064x')
        add_pair(rpc, [shared, peer], 199, tx)
        with inventory.db:
            inventory.db.execute('INSERT INTO activity_queue(txid,pool_id,block,hash) VALUES(?,?,?,?)',
                (tx, shared.id, 199, rpc.header(199)['hash']))
        inventory.active_keys(rpc, rpc.header())
        assert {shared.id, peer.id}.issubset(inventory.registry.keys)
        assert rpc.transaction_reads.count(tx) == 1
        assert len(rpc.transaction_reads) <= 2
        assert inventory.db.execute('SELECT 1 FROM activity_queue WHERE pool_id IN (?,?)',
            (shared.id, peer.id)).fetchone() is None
    finally:
        inventory.close()


def test_live_reorg_revokes_observed_key_and_accepts_only_new_canonical_input(tmp_path):
    rpc = ActivityRPC(tip=200)
    old, orphan, replacement = keys(3)
    inventory = runtime.Inventory(tmp_path/'inventory.sqlite', activity_capacity=4)
    try:
        backlog(inventory, rpc, [old])
        with inventory.db:
            inventory.put('cursor', 15)
            inventory.db.execute('INSERT INTO checkpoints VALUES(?,?)', (10, rpc.header(10)['hash']))
        rpc.add(orphan, 199)
        inventory.active_keys(rpc, rpc.header())
        assert orphan.id in inventory.registry.keys
        rpc.branches.update({199: 'b', 200: 'b'})
        rpc.logs = [log for log in rpc.logs if int(log['blockNumber'], 16) < 199]
        rpc.add(replacement, 199, txid='0x'+format(940, '064x'))
        inventory.active_keys(rpc, rpc.header())
        assert orphan.id not in inventory.registry.keys
        assert replacement.id in inventory.registry.keys
        assert inventory.get('cursor') <= 15
        assert inventory.db.execute('SELECT 1 FROM pools WHERE id=?', (orphan.id,)).fetchone() is None
    finally:
        inventory.close()


def test_fresh_valid_pair_not_queued_behind_old_recent_inbox(tmp_path):
    rpc = ActivityRPC(tip=200)
    market = keys(12)
    inventory = runtime.Inventory(tmp_path/'inventory.sqlite', activity_capacity=4, recent_activity_capacity=16)
    try:
        backlog(inventory, rpc, market[:4])
        with inventory.db:
            inventory.put('activity_split', True)
            inventory.put('activity_recent_cursor', 198)
            for key in market[4:10]:
                tx = rpc.add(key, 150, decodable=False)
                inventory.db.execute('INSERT INTO recent_activity_queue(txid,pool_id,block,hash) VALUES(?,?,?,?)',
                    (tx,key.id,150,rpc.header(150)['hash']))
        latest = '0x'+format(950,'064x')
        add_pair(rpc, market[10:], 199, latest)
        inventory.active_keys(rpc, rpc.header())
        assert {key.id for key in market[10:]}.issubset(inventory.registry.keys)
        assert latest in rpc.transaction_reads and len(rpc.transaction_reads) <= 2
    finally:
        inventory.close()


def test_activity_failure_does_not_suppress_independent_initialize_stage(tmp_path, monkeypatch):
    import threading
    import time
    from searcher.v4_peers import PeerFinder

    class OneTurnStop(threading.Event):
        def wait(self, timeout=None):
            self.set()
            return True

    rpc = ActivityRPC(tip=200)
    rpc.verify_chain = lambda: None
    rpc.close = lambda: None
    called = []

    def failing_activity(self, rpc, head, limit=2):
        called.append('activity')
        raise TimeoutError('transaction lookup unavailable')

    def successful_scan(self, rpc, head, span=64):
        called.append('initialize')
        return set()

    def no_peer_network(self, inventory, rpc, head):
        self.last = {}
        return set()

    monkeypatch.setattr(runtime.Inventory, 'active_keys', failing_activity)
    monkeypatch.setattr(runtime.Inventory, 'scan', successful_scan)
    monkeypatch.setattr(PeerFinder, 'step', no_peer_network)
    pump = runtime.DiscoveryPump(lambda:rpc, tmp_path/'inventory.sqlite', tmp_path,
        OneTurnStop(), time.monotonic()+5)
    pump.run()
    assert called == ['activity', 'initialize']


def test_recent_overflow_remains_recoverable_after_restart_without_budget_growth(tmp_path):
    rpc = ActivityRPC(tip=200)
    market = keys(10)
    path = tmp_path/'inventory.sqlite'
    inventory = runtime.Inventory(path, activity_capacity=4, recent_activity_capacity=2)
    backlog(inventory, rpc, market[:4])
    for key in market[4:]:
        rpc.add(key, 199)
    inventory.active_keys(rpc, rpc.header())
    assert inventory.get('activity_cursor') <= 20
    assert inventory.get('activity_recent_status')['delegated_to_history'] is True
    inventory.close()
    inventory = runtime.Inventory(path, activity_capacity=4, recent_activity_capacity=2)
    try:
        for _ in range(20):
            before_tx, before_logs = len(rpc.transaction_reads), len(rpc.log_ranges)
            inventory.active_keys(rpc, rpc.header())
            assert len(rpc.transaction_reads)-before_tx <= 2
            assert len(rpc.log_ranges)-before_logs <= 1
            assert inventory.db.execute('SELECT COUNT(*) FROM activity_queue').fetchone()[0] <= 4
            assert inventory.db.execute('SELECT COUNT(*) FROM recent_activity_queue').fetchone()[0] <= 2
            if {key.id for key in market}.issubset(inventory.registry.keys):
                break
        assert {key.id for key in market}.issubset(inventory.registry.keys)
    finally:
        inventory.close()


def test_recent_eviction_and_history_reingestion_do_not_bypass_missing_tx_retry(tmp_path, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(runtime.time, 'time', lambda:clock[0])
    rpc = ActivityRPC(tip=200)
    old, missing, newer = keys(3)
    inventory = runtime.Inventory(tmp_path/'inventory.sqlite', activity_capacity=4, recent_activity_capacity=1)
    try:
        backlog(inventory, rpc, [old], cursor=198)
        with inventory.db:
            inventory.put('activity_split', True)
            inventory.put('activity_recent_cursor', 198)
        tx = rpc.add(missing, 199)
        rpc.transactions.pop(tx)
        inventory.active_keys(rpc, rpc.header())
        assert rpc.transaction_reads.count(tx) == 1
        rpc.tip = 201
        rpc.add(newer, 201)
        inventory.active_keys(rpc, rpc.header())
        # Third turn fills the continuous history inbox, including the evicted tx.
        inventory.active_keys(rpc, rpc.header())
        assert rpc.transaction_reads.count(tx) == 1
        clock[0] += 3
        inventory.active_keys(rpc, rpc.header())
        assert rpc.transaction_reads.count(tx) == 2
    finally:
        inventory.close()


def test_retry_store_at_capacity_defers_without_forgetting_or_extra_reads(tmp_path, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(runtime.time, 'time', lambda:clock[0])
    rpc = ActivityRPC(tip=200)
    old, current = keys(2)
    inventory = runtime.Inventory(tmp_path/'inventory.sqlite', activity_capacity=4)
    try:
        backlog(inventory, rpc, [old])
        tx = rpc.add(current, 199)
        with inventory.db:
            inventory.db.executemany('INSERT INTO activity_retries VALUES(?,?,?)',
                [('0x'+format(10000+n,'064x'),1030.0,7) for n in range(4096)])
        inventory.active_keys(rpc, rpc.header())
        assert rpc.transaction_reads == []
        assert inventory.db.execute('SELECT COUNT(*) FROM activity_retries').fetchone()[0] == 4096
        assert inventory.db.execute('SELECT 1 FROM recent_activity_queue WHERE txid=?',(tx,)).fetchone()
        clock[0] = 1031.0
        inventory.active_keys(rpc, rpc.header())
        assert {old.id,current.id}.issubset(inventory.registry.keys)
        assert len(rpc.transaction_reads) <= 2
    finally:
        inventory.close()


def test_recent_height_full_history_inbox_also_triggers_live_admission(tmp_path):
    rpc = ActivityRPC(tip=200)
    market = keys(6)
    inventory = runtime.Inventory(tmp_path/'inventory.sqlite', activity_capacity=4)
    try:
        backlog(inventory,rpc,market[:4],cursor=198)
        latest = '0x'+format(960,'064x')
        add_pair(rpc,market[4:],199,latest)
        inventory.active_keys(rpc,rpc.header())
        assert {key.id for key in market[4:]}.issubset(inventory.registry.keys)
        assert latest in rpc.transaction_reads and len(rpc.transaction_reads) <= 2
    finally:
        inventory.close()


def test_partial_block_backpressure_keeps_live_priority_after_old_decode_frees_slots(tmp_path):
    rpc = ActivityRPC(tip=200)
    market = keys(6)
    inventory = runtime.Inventory(tmp_path/'inventory.sqlite', activity_capacity=4)
    try:
        with inventory.db:
            inventory.put('activity_cursor',198)
        for key in market[:4]:
            rpc.add(key,199)
        latest = '0x'+format(970,'064x')
        add_pair(rpc,market[4:],199,latest)
        inventory.active_keys(rpc,rpc.header())
        assert inventory.get('activity_resume') is not None
        assert inventory.db.execute('SELECT COUNT(*) FROM activity_queue').fetchone()[0] < 4
        before = len(rpc.transaction_reads)
        inventory.active_keys(rpc,rpc.header())
        assert {key.id for key in market[4:]}.issubset(inventory.registry.keys)
        assert latest in rpc.transaction_reads[before:]
        assert len(rpc.transaction_reads)-before <= 2
        for _ in range(4):
            inventory.active_keys(rpc,rpc.header())
        assert {key.id for key in market}.issubset(inventory.registry.keys)
    finally:
        inventory.close()


def test_recent_history_prefix_still_promotes_current_transaction_waiting_for_decode(tmp_path):
    rpc = ActivityRPC(tip=201)
    market = keys(6)
    inventory = runtime.Inventory(tmp_path/'inventory.sqlite',activity_capacity=8)
    try:
        backlog(inventory,rpc,market[:4],cursor=200)
        latest = '0x'+format(980,'064x')
        add_pair(rpc,market[4:],199,latest)
        with inventory.db:
            inventory.put('activity_split',True)
            inventory.put('activity_recent_cursor',198)
            for key in market[4:]:
                inventory.db.execute('INSERT INTO activity_queue(txid,pool_id,block,hash) VALUES(?,?,?,?)',
                    (latest,key.id,199,rpc.header(199)['hash']))
        inventory.active_keys(rpc,rpc.header())
        assert {key.id for key in market[4:]}.issubset(inventory.registry.keys)
        assert rpc.transaction_reads.count(latest)==1 and len(rpc.transaction_reads)<=2
        assert inventory.db.execute('SELECT COUNT(*) FROM activity_queue').fetchone()[0]==3
    finally:
        inventory.close()
