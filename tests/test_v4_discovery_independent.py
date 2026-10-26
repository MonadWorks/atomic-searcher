"""Independent discovery failure boundaries; local fakes, no network or signer.

Inventory.scan/active_keys and Worker.run remain real. Only state transport and
quote arithmetic are replaced in the threaded cases, so they test scheduling
and publication rather than repeat the separate real-PoolManager quote tests.
"""
import json
import queue
import threading
import time

import httpx
import pytest
from eth_abi import encode

from searcher.core import USDC
from searcher.v4 import Frame
from searcher.v4_market import INITIALIZE, KEY_TYPES, MANAGER, SWAP, Key, Snapshot
from searcher.v4_runtime import Inventory, Worker


ASSET = '0xff00000000000000000000000000000000000001'


def header(height):
    return {'number': hex(height), 'hash': '0x'+format(height, '064x'),
            'timestamp': hex(1000+height), 'baseFeePerGas': '0x0'}


def initialization(key, height):
    return {'address': MANAGER,
            'topics': [INITIALIZE, key.id,
                       '0x'+encode(['address'], [key.currency0]).hex(),
                       '0x'+encode(['address'], [key.currency1]).hex()],
            'data': '0x'+encode(['uint24', 'int24', 'address', 'uint160', 'int24'],
                               [key.fee, key.spacing, key.hooks, 1 << 96, 0]).hex(),
            'blockNumber': hex(height), 'blockHash': header(height)['hash']}


class BatchLimitedRPC:
    """Reject actual oversized Inventory calls, not an alternative scan model."""
    def __init__(self, tip=138):
        self.tip = tip
        self.initializations = []
        self.activity = []
        self.transactions = {}
        self.batches = []
        self.fail_batch = None

    def header(self, tag='latest', full=False):
        return header(self.tip if tag == 'latest' else int(tag, 16) if isinstance(tag, str) else tag)

    def request(self, method, params):
        if method == 'eth_getBlockByNumber':
            return self.header(params[0])
        if method == 'eth_getTransactionByHash':
            return self.transactions[params[0]]
        assert method == 'eth_getLogs', method
        filt = params[0]
        assert filt['address'] == MANAGER
        logs = self.initializations if filt['topics'][0] == INITIALIZE else self.activity
        lo, hi = int(filt['fromBlock'], 16), int(filt['toBlock'], 16)
        return [log for log in logs if lo <= int(log['blockNumber'], 16) <= hi]

    def batch(self, calls):
        self.batches.append(tuple(calls))
        assert 0 < len(calls) <= 2, 'provider accepts at most two methods per batch'
        if self.fail_batch == len(self.batches):
            raise TimeoutError('synthetic second-chunk failure')
        return [self.request(*call) for call in calls]


def seed_cursor(inventory, height=74):
    with inventory.db:
        inventory.put('cursor', height)
        inventory.put('backfill', 10)
        inventory.put('scan_turn', 1)
        inventory.db.execute('INSERT INTO checkpoints VALUES(?,?)', (height, header(height)['hash']))


def test_scan_multiblock_initializations_respects_provider_batch_cap_and_commits_complete_page(tmp_path):
    rpc = BatchLimitedRPC()
    keys = [Key(USDC, ASSET, 500+i, 1) for i in range(3)]
    rpc.initializations = [initialization(key, 80+i) for i, key in enumerate(keys)]
    inventory = Inventory(tmp_path/'inventory.sqlite')
    try:
        seed_cursor(inventory)
        changed = inventory.scan(rpc, rpc.header())
        assert set(inventory.registry.keys) == {key.id for key in keys}
        assert changed == {key.id for key in keys}
        assert inventory.get('cursor') == 138 and inventory.get('scan_turn') == 2
        requested = [int(params[0], 16) for batch in rpc.batches for method, params in batch]
        assert requested == [80, 81, 82, 138]
    finally:
        inventory.close()


def test_scan_later_header_chunk_failure_cannot_commit_partial_inventory_or_progress(tmp_path):
    rpc = BatchLimitedRPC()
    keys = [Key(USDC, ASSET, 500+i, 1) for i in range(3)]
    rpc.initializations = [initialization(key, 80+i) for i, key in enumerate(keys)]
    rpc.fail_batch = 2
    inventory = Inventory(tmp_path/'inventory.sqlite')
    try:
        seed_cursor(inventory)
        with pytest.raises(TimeoutError, match='second-chunk'):
            inventory.scan(rpc, rpc.header())
        assert inventory.get('cursor') == 74 and inventory.get('scan_turn') == 1
        assert not inventory.registry.keys
        assert inventory.db.execute('SELECT count(*) FROM pools').fetchone()[0] == 0
        rpc.fail_batch = None
        inventory.scan(rpc, rpc.header())
        assert set(inventory.registry.keys) == {key.id for key in keys}
        assert inventory.get('cursor') == 138
    finally:
        inventory.close()


def test_active_calldata_recovery_chunks_transactions_and_canonical_headers(tmp_path):
    rpc = BatchLimitedRPC(tip=138)
    keys = [Key(USDC, ASSET, 700+i, 1) for i in range(4)]
    for i, key in enumerate(keys):
        height = 135+i
        tx = '0x'+format(100+i, '064x')
        rpc.activity.append({'address': MANAGER, 'topics': [SWAP, key.id],
            'blockNumber': hex(height), 'blockHash': header(height)['hash'], 'transactionHash': tx})
        rpc.transactions[tx] = {'hash': tx, 'blockHash': header(height)['hash'],
            'input': '0x12345678'+encode(KEY_TYPES, key.values()).hex()}
    inventory = Inventory(tmp_path/'inventory.sqlite')
    try:
        with inventory.db:
            inventory.put('activity_cursor', 134)
        touched = inventory.active_keys(rpc, rpc.header(), limit=4)
        assert touched == {key.id for key in keys}
        assert set(inventory.registry.keys) == touched
        assert inventory.get('activity_cursor') == 138
        assert all(0 < len(batch) <= 2 for batch in rpc.batches)
        requested = [call for batch in rpc.batches for call in batch]
        assert {params[0] for method, params in requested if method == 'eth_getTransactionByHash'} == set(rpc.transactions)
        assert {int(params[0], 16) for method, params in requested if method == 'eth_getBlockByNumber'} == set(range(135, 139))
    finally:
        inventory.close()


class DiscoveryFailureRPC:
    def __init__(self, mode, stop):
        self.mode, self.stop = mode, stop
        self.entered, self.release, self.closed = (threading.Event() for _ in range(3))
        self.calls = 0
        self.thread_id = None

    def verify_chain(self):
        return None

    def header(self, tag='latest', full=False):
        return header(100 if tag == 'latest' else int(tag, 16) if isinstance(tag, str) else tag)

    def request(self, method, params):
        if method == 'eth_getBlockByNumber':
            return self.header(params[0])
        assert method == 'eth_getLogs', method
        self.thread_id = threading.get_ident()
        self.calls += 1
        self.entered.set()
        if self.mode == 'blocked':
            assert self.release.wait(5), 'test must release its synthetic blocked request'
            return []
        request = httpx.Request('POST', 'https://discovery.example.invalid')
        response = httpx.Response(429, request=request)
        raise httpx.HTTPStatusError('synthetic getLogs throttle', request=request, response=response)

    def batch(self, calls):
        assert len(calls) <= 2
        return [self.request(*call) for call in calls]

    def close(self):
        self.closed.set()


class HeldBackoffStop(threading.Event):
    """Pause at the discovery retry wait itself, without timing health callbacks."""
    def __init__(self):
        super().__init__()
        self.rpc = None
        self.backoff_entered = threading.Event()
        self.release_backoff = threading.Event()
        self.waits = []

    def wait(self, timeout=None):
        if (self.rpc is not None and self.rpc.calls and
                threading.get_ident() == self.rpc.thread_id and not self.backoff_entered.is_set()):
            self.waits.append(timeout)
            self.backoff_entered.set()
            assert self.release_backoff.wait(5), 'test must release discovery backoff'
            return self.is_set()
        return super().wait(timeout)

    def set(self):
        super().set()
        self.release_backoff.set()


def configured_worker(tmp_path, monkeypatch, stop, discovery):
    import searcher.v4_runtime as module
    keys = [Key(USDC, ASSET, 500+i, 1) for i in range(3)]
    path = tmp_path/'inventory.sqlite'
    inventory = Inventory(path)
    inventory.accept([initialization(key, 10) for key in keys[:2]], 10, 10, {10: header(10)['hash']})
    inventory.close()
    config = {'_v4_inventory_path': str(path), 'policy': {'snapshot_max_age_ms': 2000},
              'v4': {'poll_seconds': .5}}
    published = queue.Queue()

    class StateRPC:
        def __init__(self):
            self.closed = threading.Event()
        def request(self, *args):
            pytest.fail('discovery must not use the state endpoint')
        def close(self):
            self.closed.set()

    state_rpc = StateRPC()

    class StateReader:
        def __init__(self, rpc, _config):
            assert rpc is state_rpc
        def read(self, pair):
            assert len(pair) == 2 and pair[0].id != pair[1].id
            assert discovery.entered.wait(2), 'discovery must actually enter its failing/blocking path'
            now = time.monotonic_ns()
            return Frame(header(100), {}, Snapshot({}), tuple(pair), now, now, time.time())
        def request_missing(self, *args):
            pass

    monkeypatch.setattr(module, 'Reader', StateReader)
    monkeypatch.setattr(module, 'evaluate', lambda frame, config:
        {'block': 100, 'missing_slots': [], 'candidates': [{'net_after_modelled_cost': 100}]})
    worker = Worker(lambda: state_rpc, config, tmp_path/'session', stop,
                    time.monotonic()+8, discovery_rpc_factory=lambda: discovery)
    original_offer = worker.offer

    def offer(frame, report):
        original_offer(frame, report)
        published.put(frozenset(key.id for key in frame.keys))

    worker.offer = offer
    return worker, keys, published, state_rpc


def wait_for_group(published, predicate, timeout=3):
    until = time.monotonic()+timeout
    seen = []
    while time.monotonic() < until:
        try:
            group = published.get(timeout=max(.001, until-time.monotonic()))
        except queue.Empty:
            break
        seen.append(group)
        if predicate(group):
            return group
    pytest.fail(f'complete group was not published: {seen}')


@pytest.mark.parametrize('mode', ['blocked', '429'])
def test_worker_quotes_known_and_pending_pairs_while_real_discovery_cannot_finish(tmp_path, monkeypatch, mode):
    stop = threading.Event()
    discovery = DiscoveryFailureRPC(mode, stop)
    worker, keys, published, state_rpc = configured_worker(tmp_path, monkeypatch, stop, discovery)
    worker.start()
    try:
        assert discovery.entered.wait(2)
        wait_for_group(published, lambda group: group == frozenset(key.id for key in keys[:2]))
        worker.publish([initialization(keys[2], 101)])
        group = wait_for_group(published, lambda group: keys[2].id in group)
        assert len(group) == 2 and group & {key.id for key in keys[:2]}
        assert worker.consume(None)['status'] == 'awaiting_executor'
        # Pending metadata can schedule reads but must not become durable canonical metadata.
        check = Inventory(tmp_path/'inventory.sqlite')
        try:
            assert keys[2].id not in {row[0] for row in check.db.execute('SELECT id FROM pools')}
        finally:
            check.close()
    finally:
        stop.set()
        discovery.release.set()
        worker.thread.join(3)
    assert not worker.thread.is_alive() and discovery.closed.is_set() and state_rpc.closed.is_set()
    assert not worker.discovery.thread.is_alive()


def test_pending_events_do_not_release_discovery_failure_backoff(tmp_path, monkeypatch):
    stop = HeldBackoffStop()
    discovery = DiscoveryFailureRPC('429', stop)
    stop.rpc = discovery
    worker, keys, published, state_rpc = configured_worker(tmp_path, monkeypatch, stop, discovery)
    worker.start()
    try:
        assert stop.backoff_entered.wait(3), 'failed discovery must wait on stop, not event wake'
        assert stop.waits[0] is not None and stop.waits[0] > 0
        # Activity and Initialize each get one independent bounded attempt.
        # Once the turn enters backoff, quote events must not add more attempts.
        calls_before_events = discovery.calls
        assert calls_before_events == 2
        for _ in range(12):
            worker.publish([initialization(keys[2], 101)])
        wait_for_group(published, lambda group: keys[2].id in group)
        assert discovery.calls == calls_before_events, 'event-driven state progress must not restart discovery during its held backoff'
        assert not stop.release_backoff.is_set()
    finally:
        stop.set()
        discovery.release.set()
        worker.thread.join(3)
    assert not worker.thread.is_alive() and discovery.closed.is_set() and state_rpc.closed.is_set()
    assert not worker.discovery.thread.is_alive()


def test_worker_canonical_revision_removes_old_keys_preserves_then_confirms_pending_key(tmp_path, monkeypatch):
    """Inject a canonical snapshot through the real pump mailbox, not the worker registry."""
    stop = threading.Event()
    discovery = DiscoveryFailureRPC('blocked', stop)
    worker, keys, published, state_rpc = configured_worker(tmp_path, monkeypatch, stop, discovery)
    replacement = Key(USDC, ASSET, 900, 1)
    canonical = Inventory(':memory:')
    worker.start()
    try:
        assert discovery.entered.wait(2)
        wait_for_group(published, lambda group: group == frozenset(key.id for key in keys[:2]))
        worker.publish([initialization(keys[2], 101)])
        wait_for_group(published, lambda group: keys[2].id in group)

        # A canonical revision removes old B and adds D. Unconfirmed C must
        # survive that revision and remain eligible with canonical A or D.
        canonical.accept([initialization(key, 100) for key in (keys[0], replacement)],
                         100, 100, {100: header(100)['hash']})
        worker.discovery.publish(canonical, 'connected', changed={replacement.id})
        expected = {keys[0].id, keys[2].id, replacement.id}
        until = time.monotonic()+3
        while set(worker.pool_ids) != expected and time.monotonic() < until:
            stop.wait(.01)
        assert set(worker.pool_ids) == expected
        while not published.empty():
            published.get_nowait()
        group = wait_for_group(published, lambda group: keys[2].id in group)
        assert group <= expected and keys[1].id not in group

        # Confirm C in a later canonical revision. A stale removed pending
        # notification must no longer revoke that canonically admitted key.
        canonical.accept([initialization(keys[2], 101)], 101, 101, {101: header(101)['hash']})
        revision_before = worker.discovery.revision
        worker.discovery.publish(canonical, 'connected', changed={keys[2].id})
        assert worker.discovery.revision > revision_before
        removed_processed = threading.Event()
        original_pending = Inventory.pending

        def observed_pending(self, log, now=None):
            result = original_pending(self, log, now=now)
            if log.get('removed') and log['topics'][1] == keys[2].id:
                removed_processed.set()
            return result

        monkeypatch.setattr(Inventory, 'pending', observed_pending)
        worker.publish([dict(initialization(keys[2], 101), removed=True)])
        assert removed_processed.wait(3)
        while not published.empty():
            published.get_nowait()
        group = wait_for_group(published, lambda group: keys[2].id in group)
        assert group <= expected and set(worker.pool_ids) == expected
    finally:
        stop.set()
        discovery.release.set()
        worker.thread.join(3)
        canonical.close()
    assert not worker.thread.is_alive() and not worker.discovery.thread.is_alive()
    assert discovery.closed.is_set() and state_rpc.closed.is_set()
