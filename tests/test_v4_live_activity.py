"""Author regressions for bounded V4 live intake; no network or signer."""
from collections import Counter
import threading
import time

import pytest

from searcher import v4_runtime as runtime
from searcher.v4_activity import TABLES
from test_v4_activity_queue import ActivityRPC, keys
from test_v4_live_independent import backlog


def run_bounded(inv, rpc, **kw):
    before_logs, before_tx = len(rpc.log_ranges), len(rpc.transaction_reads)
    result = inv.active_keys(rpc, rpc.header(), **kw)
    assert len(rpc.log_ranges)-before_logs <= 1
    assert len(rpc.transaction_reads)-before_tx <= kw.get('limit', 2)
    assert all(hi-lo+1 <= 64 for lo, hi in rpc.log_ranges)
    assert inv.db.execute('SELECT COUNT(*) FROM recent_activity_queue').fetchone()[0] <= inv.recent_activity_capacity
    assert inv.db.execute('SELECT COUNT(*) FROM activity_queue').fetchone()[0] <= inv.activity_capacity
    return result


def test_recent_overflow_is_delegated_and_history_recovers_every_key_across_restart(tmp_path):
    rpc = ActivityRPC(tip=200)
    market = keys(10)
    path = tmp_path/'inventory.sqlite'
    inv = runtime.Inventory(path, activity_capacity=3, recent_activity_capacity=2)
    old = backlog(inv, rpc, market[:3])
    live = [rpc.add(k, 199) for k in market[3:]]
    run_bounded(inv, rpc)
    status = inv.get('activity_recent_status')
    assert status['observed_end'] == 200 and not status['window_fully_enqueued']
    assert status['delegated_to_history'] and status['delegated_from'] == 199
    assert inv.get('activity_cursor') == 20
    inv.close()
    inv = runtime.Inventory(path, activity_capacity=3, recent_activity_capacity=2)
    try:
        for _ in range(20):
            run_bounded(inv, rpc)
        assert set(inv.registry.keys) == {key.id for key in market}
        assert set(old+live).issubset(rpc.transaction_reads)
        assert inv.get('activity_cursor') == 200
        assert inv.get('activity_resume') is None
        assert inv.get('activity_recent_debt_from') is None
    finally:
        inv.close()


def test_recent_eviction_after_history_passed_rewinds_and_preserves_retry_deadline(tmp_path, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(runtime.time, 'time', lambda: clock[0])
    rpc = ActivityRPC(tip=200)
    old, newer = keys(2)
    inv = runtime.Inventory(tmp_path/'i.sqlite', recent_activity_capacity=1)
    with inv.db:
        inv.put('activity_cursor', 20)
    lost = rpc.add(old, 199)
    tx = rpc.transactions.pop(lost)
    run_bounded(inv, rpc)
    assert rpc.transaction_reads == [lost]
    assert inv.db.execute('SELECT attempts,next_try FROM activity_retries WHERE txid=?', (lost,)).fetchone() == (1, 1002.0)
    with inv.db:
        inv.put('activity_cursor', 200)  # historical lane has already passed it
        inv.put('activity_resume', {'block': 201, 'hash': rpc.header(201)['hash'], 'next': 0})
    rpc.tip = 201
    rpc.add(newer, 201)
    run_bounded(inv, rpc)
    assert newer.id in inv.registry.keys
    assert inv.get('activity_cursor') == 198 and inv.get('activity_resume') is None
    # Historical reinsertion cannot reset next_try merely because the recent
    # representative was displaced by a newer block.
    run_bounded(inv, rpc)
    assert rpc.transaction_reads.count(lost) == 1
    assert inv.db.execute('SELECT COUNT(*) FROM activity_queue WHERE txid=?', (lost,)).fetchone() == (1,)
    rpc.transactions[lost] = tx
    clock[0] = 1003
    run_bounded(inv, rpc)
    assert old.id in inv.registry.keys and rpc.transaction_reads.count(lost) == 2
    inv.close()


def test_two_lane_duplicate_missing_tx_has_one_request_and_shared_cooldown(tmp_path, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(runtime.time, 'time', lambda: clock[0])
    rpc = ActivityRPC(tip=200)
    key = keys(1)[0]
    inv = runtime.Inventory(tmp_path/'i.sqlite')
    txid = rpc.add(key, 199)
    rpc.transactions.pop(txid)
    with inv.db:
        inv.put('activity_cursor', 20)
        inv.db.execute('INSERT INTO activity_queue(txid,pool_id,block,hash) VALUES(?,?,?,?)',
                       (txid, key.id, 199, rpc.header(199)['hash']))
    run_bounded(inv, rpc)
    assert rpc.transaction_reads == [txid]
    for table in TABLES:
        assert inv.db.execute(f'SELECT next_try FROM {table} WHERE txid=?', (txid,)).fetchone() == (1002.0,)
    rpc.tip += 1
    run_bounded(inv, rpc)
    assert rpc.transaction_reads == [txid]
    inv.close()


def test_live_and_history_queue_reorg_rewinds_both_without_advancing_old_initialize(tmp_path):
    rpc = ActivityRPC(tip=200)
    market = keys(2)
    inv = runtime.Inventory(tmp_path/'i.sqlite')
    with inv.db:
        inv.put('cursor', 10)
        inv.put('activity_cursor', 150)
        inv.put('activity_recent_cursor', 200)
        inv.put('activity_recent_status', {'observed_end': 200, 'window_fully_enqueued': True})
        inv.put('activity_resume', {'block': 151, 'hash': rpc.header(151)['hash'], 'next': 2})
        inv.db.executemany('INSERT INTO checkpoints VALUES(?,?)', [(100, rpc.header(100)['hash']), (200, rpc.header(200)['hash'])])
        for table, key in zip(TABLES, market):
            tx = rpc.add(key, 199)
            inv.db.execute(f'INSERT INTO {table}(txid,pool_id,block,hash) VALUES(?,?,?,?)',
                           (tx, key.id, 199, rpc.header(199)['hash']))
    rpc.branches[200] = 'b'
    assert inv.check_reorg(rpc, rpc.header())
    assert inv.get('cursor') == 10
    assert inv.get('activity_cursor') == inv.get('activity_recent_cursor') == 100
    assert inv.get('activity_resume') is None and inv.get('activity_recent_status') is None
    assert all(inv.db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0] == 0 for table in TABLES)
    inv.close()


def test_missing_live_window_header_commits_neither_cursor_nor_keys(tmp_path):
    rpc = ActivityRPC(tip=200)
    inv = runtime.Inventory(tmp_path/'i.sqlite')
    with inv.db:
        inv.put('activity_cursor', 20)
    rpc.add(keys(1)[0], 199)
    rpc.missing_headers.add(200)
    with pytest.raises(Exception, match='header missing'):
        inv.active_keys(rpc, rpc.header())
    assert inv.get('activity_recent_cursor') is None
    assert inv.get('activity_cursor') == 20 and not inv.registry.keys
    assert len(rpc.log_ranges) == 1 and not rpc.transaction_reads
    inv.close()


def test_one_transaction_budget_alternates_old_and_recent_after_restart(tmp_path):
    rpc = ActivityRPC(tip=200)
    market = keys(9)
    path = tmp_path/'i.sqlite'
    inv = runtime.Inventory(path)
    old = backlog(inv, rpc, market[:3])
    for turn in range(6):
        rpc.tip = 200+turn
        rpc.add(market[3+turn], rpc.tip)
        run_bounded(inv, rpc, limit=1)
        inv.close()
        inv = runtime.Inventory(path)
    assert all(tx in rpc.transaction_reads for tx in old)
    assert inv.get('activity_cursor') > 20
    inv.close()


def test_retry_records_bounded_and_saturation_recovers_without_extra_tx_reads(tmp_path, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(runtime.time, 'time', lambda: clock[0])
    rpc = ActivityRPC(tip=200)
    inv = runtime.Inventory(tmp_path/'i.sqlite')
    with inv.db:
        inv.put('activity_cursor', 20)
        inv.db.executemany('INSERT INTO activity_retries VALUES(?,?,?)',
                          [('0x'+format(i+10000, '064x'), 1002, 1) for i in range(4096)])
    key = keys(1)[0]
    rpc.add(key, 199)
    run_bounded(inv, rpc)
    assert not rpc.transaction_reads and not inv.registry.keys
    clock[0] = 1003
    run_bounded(inv, rpc)
    assert key.id in inv.registry.keys
    assert inv.db.execute('SELECT COUNT(*) FROM activity_retries').fetchone()[0] <= 4096
    inv.close()


def test_persistent_backpressure_enables_live_even_after_decode_frees_slots(tmp_path):
    rpc = ActivityRPC(tip=200)
    market = keys(8)
    inv = runtime.Inventory(tmp_path/'i.sqlite', activity_capacity=4)
    backlog(inv, rpc, market[:3], cursor=198)
    # First historical pass fills its last slot, records a resume offset, then
    # its two decodes leave space. The next turn must still prioritize recent.
    for key in market[3:]:rpc.add(key, 199)
    run_bounded(inv, rpc)
    assert inv.get('activity_status')['backpressure']
    assert inv.db.execute('SELECT COUNT(*) FROM activity_queue').fetchone()[0] < inv.activity_capacity
    run_bounded(inv, rpc)
    assert inv.get('activity_split') is True
    assert inv.get('activity_recent_cursor') == 200
    assert len(set(inv.registry.keys)&{key.id for key in market[4:]}) >= 1
    inv.close()


def test_history_ingested_tip_but_current_pending_pair_gets_recent_priority(tmp_path):
    from test_v4_live_independent import add_pair
    rpc = ActivityRPC(tip=200)
    market = keys(5)
    inv = runtime.Inventory(tmp_path/'i.sqlite', activity_capacity=5)
    backlog(inv, rpc, market[:3], cursor=200)
    current_tx = '0x'+format(2000, '064x')
    add_pair(rpc, market[3:], 199, current_tx)
    with inv.db:
        for key in market[3:]:
            inv.db.execute('INSERT INTO activity_queue(txid,pool_id,block,hash) VALUES(?,?,?,?)',
                           (current_tx, key.id, 199, rpc.header(199)['hash']))
    run_bounded(inv, rpc)
    assert {key.id for key in market[3:]}.issubset(inv.registry.keys)
    assert rpc.transaction_reads.count(current_tx) == 1
    inv.close()
