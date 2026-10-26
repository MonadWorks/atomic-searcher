"""Depth continuation keeps addresses under fixed memory and atomic-read bounds."""
import json
from collections import defaultdict
from types import SimpleNamespace

import pytest
from eth_abi import decode, encode

from searcher import v4, v4_runtime as runtime
from searcher.core import Reject, USDC
from searcher.v4_market import Key


def group(index):
    return (SimpleNamespace(id=f'pool-{index:08x}'),)


def test_complete_observations_do_not_create_or_promote_depth_cache_groups():
    reader = v4.Reader(None, {})
    reader.request_missing(group(1), [11])
    reader.request_missing(group(2), [22])
    before = list(reader.extra)
    for i in range(5000): reader.request_missing(group(i), [])
    assert list(reader.extra) == before
    assert [list(slots) for slots in reader.extra.values()] == [[11], [22]]


def test_sparse_groups_share_original_slot_budget_with_bounded_metadata():
    reader = v4.Reader(None, {})
    for i in range(4097): reader.request_missing(group(i), [i])
    assert len(reader.extra) == 4096
    assert ('pool-00000000',) not in reader.extra
    assert ('pool-00001000',) in reader.extra
    assert sum(len(x) for x in reader.extra.values()) == 4096


def test_dense_groups_keep_total_and_per_group_caps_and_refresh_recent_slots():
    reader = v4.Reader(None, {})
    for i in range(128): reader.request_missing(group(i), range(512))
    assert sum(len(x) for x in reader.extra.values()) == 128 * 512
    reader.request_missing(group(0), [0])  # Keep the actively extended group.
    reader.request_missing(group(128), [9999])
    assert ('pool-00000000',) in reader.extra
    assert ('pool-00000001',) not in reader.extra
    assert sum(len(x) for x in reader.extra.values()) <= 128 * 512
    reader.request_missing(group(128), range(600))
    assert len(reader.extra[('pool-00000080',)]) == 512
    assert set(reader.extra[('pool-00000080',)]) == set(range(88, 600))
    assert all(value is None for slots in reader.extra.values() for value in slots.values())


def _worker_rotation(tmp_path, monkeypatch, required_depth, read_limit=12000):
    # Shape from the observed 80-key inventory: 162 unordered pairs plus 19
    # singleton keys. No production file, RPC, price, or signer is used here.
    keys = []
    for asset_index, size in enumerate([12, 7, 7, 6] + [4]*5 + [3]*3 + [1]*19):
        asset = '0x'+format((0xff << 152) + asset_index + 1, '040x')
        keys.extend(Key(USDC, asset, 500+i, 1) for i in range(size))
    path = tmp_path/'inventory.sqlite';inventory = runtime.Inventory(path)
    with inventory.db:
        for key in keys:
            inventory.db.execute('INSERT INTO pools VALUES(?,?,?,?)',
                                 (key.id, json.dumps(key.values()), 1, 'canonical'))
    inventory.close()

    class Clock:
        now = 100.
        def monotonic(self): return self.now
        def monotonic_ns(self): return int(self.now * 1e9)
        def time(self): return 1700000000 + self.now
    clock = Clock()
    class Stop:
        flag = False
        def is_set(self): return self.flag
        def set(self): self.flag = True
        def wait(self, seconds=0):
            if not self.flag: clock.now += seconds
            return self.flag
    stop = Stop()
    class Wake:
        def wait(self, seconds): clock.now += seconds; return False
        def clear(self): pass
    class Discovery:
        def __init__(self, *args): self.thread = SimpleNamespace(join=lambda:None)
        def start(self): pass
        def take(self): return 1, {k.id:k for k in keys}, set(), {}
    reads = defaultdict(list);instances = [];count = [0]
    class Reader(v4.Reader):
        def __init__(self, rpc, config): super().__init__(rpc, config);instances.append(self)
        def read(self, selected):
            g = tuple(sorted(k.id for k in selected))
            slots = self.extra.get(g, {})
            progress = sum(i in slots for i in range(required_depth))
            missing = next((i for i in range(required_depth) if i not in slots), None)
            reads[g].append(progress);count[0] += 1;clock.now += .001
            if count[0] == read_limit: stop.set()
            return SimpleNamespace(keys=tuple(selected), requested_ns=clock.monotonic_ns(),
                received_ns=clock.monotonic_ns(), received_utc=clock.time(),
                anchor={'number':'0x1'}, missing=missing)
    def evaluate(frame, config):
        return {'block':1, 'candidates':[], 'missing_slots':[] if frame.missing is None else [frame.missing]}
    monkeypatch.setattr(runtime, 'time', clock)
    monkeypatch.setattr(runtime, 'DiscoveryPump', Discovery)
    monkeypatch.setattr(runtime, 'Reader', Reader)
    monkeypatch.setattr(runtime, 'evaluate', evaluate)
    monkeypatch.setattr(runtime, 'atomic_json', lambda *args:None)
    config = {'_v4_inventory_path':str(path), 'policy':{'snapshot_max_age_ms':2000}, 'v4':{'poll_seconds':.5}}
    worker = runtime.Worker(lambda:SimpleNamespace(close=lambda:None), config, tmp_path/'session', stop, read_limit + 20000)
    worker.wake = Wake();worker.run()
    return reads, instances[0], count[0]


@pytest.mark.parametrize('required_depth', [1, 16])
def test_real_worker_rotation_retains_sparse_frontiers_across_162_groups(tmp_path, monkeypatch, required_depth):
    reads, reader, count = _worker_rotation(tmp_path, monkeypatch, required_depth)
    assert len(reads) == 162 and count == 12000
    # Once a slot was discovered, a later complete snapshot must not need it
    # rediscovered solely because the ordinary scheduler visited other groups.
    assert all(progress == sorted(progress) for progress in reads.values())
    assert all(progress[-1] == required_depth for progress in reads.values()), {g: v[-1] for g, v in reads.items() if v[-1] != required_depth}
    assert len(reader.extra) == 162
    assert sum(len(x) for x in reader.extra.values()) == 162 * required_depth


def test_address_cache_never_reuses_values_or_bypasses_atomic_anchor_checks(monkeypatch):
    key = Key(USDC, '0xff00000000000000000000000000000000000001', 500, 1)
    slot = 987654321
    anchor = {'number':'0xa', 'timestamp':'0x64', 'hash':'0x'+'01'*32}
    pending = {'number':'0xb', 'timestamp':'0x66', 'parentHash':anchor['hash']}
    class RPC:
        value = 10
        wrong_anchor = False
        seen_extra = []
        def batch(self, calls):
            if calls[0][0] == 'eth_getBlockByNumber': return dict(anchor), dict(pending)
            assert len(calls) == 2 and calls[1][0] == 'eth_getBlockByNumber'
            args = decode(['address','bytes32[]','int24[]','uint16','uint16','bytes32[]'],
                          bytes.fromhex(calls[0][1][0]['data'][10:]))
            self.seen_extra.append([int.from_bytes(s,'big') for s in args[-1]])
            raw = '0x'+encode(['uint256','uint256','bytes32[]','bytes32[]'],
                             [11,102,[slot.to_bytes(32,'big')],[self.value.to_bytes(32,'big')]]).hex()
            after = dict(anchor)
            if self.wrong_anchor: after['hash'] = '0x'+'02'*32
            return raw, after
    monkeypatch.setattr(v4, 'verify_manager', lambda rpc:None)
    monkeypatch.setattr(v4, 'artifact', lambda name:{'deployedBytecode':{'object':'0x00'}})
    rpc = RPC();reader = v4.Reader(rpc, {})
    reader.request_missing([key], [slot])
    first = reader.read([key]);rpc.value = 20;second = reader.read([key])
    assert first.snapshot.word(slot) == 10 and second.snapshot.word(slot) == 20
    assert rpc.seen_extra == [[slot], [slot]]
    assert reader.extra[(key.id,)][slot] is None
    rpc.wrong_anchor = True
    with pytest.raises(Reject, match='anchor changed'): reader.read([key])
    assert reader.extra[(key.id,)][slot] is None
