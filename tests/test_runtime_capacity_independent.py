"""Independent depth-cache boundaries; fake transport, real Reader/Snapshot APIs."""
import threading
from types import SimpleNamespace

import pytest
from eth_abi import decode, encode

from searcher import v4, v4_runtime
from searcher.core import Reject, USDC
from searcher.v4_market import Key, MissingState


def pair(index):
    asset = '0x'+format((0xff << 152)+index+1, '040x')
    return (Key(USDC, asset, 500, 1), Key(USDC, asset, 3000, 60))


class LensTransport:
    """Returns only current-call words, allowing stale value reuse to be exposed."""
    def __init__(self):
        self.epoch = 1
        self.anchor = {'number': '0xa', 'timestamp': '0x64', 'hash': '0x'+'01'*32}
        self.pending = {'number': '0xb', 'timestamp': '0x66', 'parentHash': self.anchor['hash']}
        self.last_slots = ()
        self.bad_anchor = False
        self.bad_time = False
        self.omit = None

    def batch(self, calls):
        if calls[0][0] == 'eth_getBlockByNumber':
            return dict(self.anchor), dict(self.pending)
        assert len(calls) == 2 and calls[1][0] == 'eth_getBlockByNumber'
        args = decode(['address', 'bytes32[]', 'int24[]', 'uint16', 'uint16', 'bytes32[]'],
                      bytes.fromhex(calls[0][1][0]['data'][10:]))
        self.last_slots = tuple(int.from_bytes(slot, 'big') for slot in args[-1])
        words = [slot for slot in args[-1] if int.from_bytes(slot, 'big') != self.omit]
        values = [(self.epoch*100_000+int.from_bytes(slot, 'big') % 100_000).to_bytes(32, 'big') for slot in words]
        raw = '0x'+encode(['uint256', 'uint256', 'bytes32[]', 'bytes32[]'],
                         [11, 101 if self.bad_time else 102, words, values]).hex()
        after = dict(self.anchor)
        if self.bad_anchor:
            after['hash'] = '0x'+'02'*32
        return raw, after


@pytest.fixture
def reader(monkeypatch):
    monkeypatch.setattr(v4, 'verify_manager', lambda rpc: None)
    monkeypatch.setattr(v4, 'artifact', lambda name: {'deployedBytecode': {'object': '0x00'}})
    transport = LensTransport()
    return v4.Reader(transport, {}), transport


def test_reader_retains_sixteen_incremental_frontiers_over_162_rotating_pairs(reader):
    subject, transport = reader
    groups = [pair(i) for i in range(162)]
    first = None
    for depth in range(17):
        transport.epoch = depth+1
        for index, keys in enumerate(groups):
            # A reversed direction must share only the same address cache.
            frame = subject.read(keys if depth % 2 else tuple(reversed(keys)))
            if index == 0 and depth == 1:
                first = frame
            slots = [index*100+offset for offset in range(16)]
            for slot in slots[:depth]:
                assert frame.snapshot.word(slot) == transport.epoch*100_000+slot
            if depth < 16:
                with pytest.raises(MissingState) as missing:
                    frame.snapshot.word(slots[depth])
                subject.request_missing(keys, [missing.value.slot])
            else:
                subject.request_missing(keys, [])
        # Visiting completed/empty routes must not create cache metadata or
        # evict an unfinished pair merely through ordinary route rotation.
        for _ in range(10):
            subject.request_missing([], [])
        assert len(subject.extra) == 162
    assert first.snapshot.word(0) == 200_000
    assert sum(map(len, subject.extra.values())) == 162*16
    assert all(value is None for slots in subject.extra.values() for value in slots.values())


def test_learned_address_is_not_a_cached_value_when_next_atomic_read_omits_it(reader):
    subject, transport = reader
    keys = pair(0)
    subject.request_missing(keys, [77])
    before = subject.read(keys)
    assert before.snapshot.word(77) == 100_077
    transport.epoch = 2
    transport.omit = 77
    after = subject.read(keys)
    assert transport.last_slots == (77,)
    with pytest.raises(MissingState) as missing:
        after.snapshot.word(77)
    assert missing.value.slot == 77
    assert subject.extra[tuple(sorted(key.id for key in keys))][77] is None
    assert before.snapshot.word(77) == 100_077


@pytest.mark.parametrize('failure, message', [('bad_anchor', 'anchor changed'), ('bad_time', 'state/context mismatch')])
def test_populated_depth_cache_does_not_bypass_atomic_anchor_or_evm_clock(reader, failure, message):
    subject, transport = reader
    keys = pair(0)
    subject.request_missing(keys, range(512))
    assert len(subject.read(keys).snapshot.storage) == 512
    setattr(transport, failure, True)
    with pytest.raises(Reject, match=message):
        subject.read(keys)
    assert len(subject.extra[tuple(sorted(key.id for key in keys))]) == 512


def test_cache_budget_keeps_recently_extended_pair_and_bounds_mixed_sparse_dense_groups():
    subject = v4.Reader(None, {})
    groups = [pair(index) for index in range(129)]
    for keys in groups[:128]:
        subject.request_missing(keys, range(512))
    survivor = tuple(sorted(key.id for key in groups[0]))
    evicted = tuple(sorted(key.id for key in groups[1]))
    subject.request_missing(tuple(reversed(groups[0])), [999])
    subject.request_missing(groups[-1], [1])
    assert survivor in subject.extra and 999 in subject.extra[survivor]
    assert evicted not in subject.extra
    # Multiple sparse insertions share the same budget, instead of imposing
    # the old 128-group cliff on every unfinished route.
    for index in range(129, 4098):
        subject.request_missing(pair(index), [index])
        assert len(subject.extra) <= 4096
        assert sum(map(len, subject.extra.values())) <= 65536
    assert all(0 < len(words) <= 512 for words in subject.extra.values())


def test_completed_depth_still_uses_observation_deadline_and_existing_execution_entry(reader, tmp_path, monkeypatch):
    subject, _ = reader
    keys = pair(0)
    subject.request_missing(keys, [77])
    frame = subject.read(keys)
    candidate = {'v4_keys': [key.values() for key in keys], 'net_after_modelled_cost': 1}
    report = {'block': 10, 'candidates': [candidate]}
    config = {'policy': {'snapshot_max_age_ms': 2000}}
    worker = v4_runtime.Worker(None, config, tmp_path, threading.Event(), 100)
    calls = []
    def execute(*args):
        calls.append(args)
        raise Reject('synthetic current execution rejects this quote')
    executor = SimpleNamespace(execute=execute)
    monkeypatch.setattr(v4_runtime, 'atomic_json', lambda *args: None)
    now = [frame.requested_ns+2_000_000_001]
    monkeypatch.setattr(v4_runtime, 'time', SimpleNamespace(monotonic_ns=lambda: now[0], time=lambda: 100.))
    worker.offer(frame, report)
    assert worker.consume(executor) is None and not calls
    now[0] = frame.received_ns
    worker.offer(frame, report)
    result = worker.consume(executor)
    assert result['status'] == 'execution_rejected' and len(calls) == 1
    _, route, states, anchor, deadline = calls[0]
    assert route.pools == tuple(key.id for key in keys)
    assert states == {} and anchor == frame.anchor
    assert deadline == frame.requested_ns+2_000_000_000
