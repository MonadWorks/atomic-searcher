"""Independent real Worker.run retry schedules, with transport and clock fakes."""
import json
from types import SimpleNamespace

import httpx
import pytest

from searcher import v4_runtime as runtime
from searcher.core import Reject, RPCError, RPCUnavailable, USDC
from searcher.v4_market import Key


ASSET = '0xff00000000000000000000000000000000000091'


def run_schedule(tmp_path, monkeypatch, outcomes, *, read_seconds=0, wake_on_failure=True):
    class Clock:
        now = 100.0
        def monotonic(self): return self.now
        def monotonic_ns(self): return int(self.now * 1e9)
        def time(self): return 1700000000 + self.now
    clock = Clock()

    class Stop:
        flag = False
        def is_set(self): return self.flag
        def set(self): self.flag = True
        def wait(self, timeout=0):
            assert timeout >= 0
            if not self.flag: clock.now += timeout
            return self.flag
    stop = Stop()

    class Wake:
        flag = False
        def set(self): self.flag = True
        def clear(self): self.flag = False
        def wait(self, timeout):
            if self.flag: return True
            clock.now += timeout
            return False

    path = tmp_path / 'metadata.sqlite'
    keys = tuple(Key(USDC, ASSET, fee, 1) for fee in (100, 500))
    inventory = runtime.Inventory(path)
    with inventory.db:
        for key in keys:
            inventory.db.execute('INSERT INTO pools VALUES(?,?,?,?)',
                                 (key.id, json.dumps(key.values()), 1, 'canonical'))
    inventory.close()

    class Discovery:
        def __init__(self, *a): self.thread = SimpleNamespace(join=lambda: None)
        def start(self): pass
        def take(self): return 1, {k.id: k for k in keys}, set(), {'state': 'connected'}

    state_rpc = SimpleNamespace(closed=False)
    def close(): state_rpc.closed = True
    state_rpc.close = close
    reads, evaluated, missing, publications = [], [], [], []

    class Reader:
        def __init__(self, rpc, config): assert rpc is state_rpc
        def read(self, selected):
            assert set(k.id for k in selected) == set(k.id for k in keys)
            index = len(reads)
            assert index < len(outcomes), 'worker must stop after final transport response'
            reads.append(clock.now)
            clock.now += read_seconds
            outcome = outcomes[index]
            if outcome is not None:
                if wake_on_failure: worker.wake.set()
                raise outcome
            frame = SimpleNamespace(keys=tuple(selected), requested_ns=clock.monotonic_ns(),
                received_ns=clock.monotonic_ns(), received_utc=clock.time(),
                anchor={'number': hex(1000 + index)}, sequence=index)
            if index == len(outcomes) - 1: stop.set()
            return frame
        def request_missing(self, selected, slots):
            missing.append((tuple(selected), tuple(slots)))

    def evaluate(frame, config):
        evaluated.append(frame)
        return {'block': int(frame.anchor['number'], 16), 'candidates': [], 'missing_slots': []}

    monkeypatch.setattr(runtime, 'time', clock)
    monkeypatch.setattr(runtime, 'DiscoveryPump', Discovery)
    monkeypatch.setattr(runtime, 'Reader', Reader)
    monkeypatch.setattr(runtime, 'evaluate', evaluate)
    config = {'_v4_inventory_path': str(path), 'policy': {'snapshot_max_age_ms': 2000},
              'v4': {'poll_seconds': .5}}
    worker = runtime.Worker(lambda: state_rpc, config, tmp_path / 'session', stop, 1000)
    worker.wake = Wake()
    offer = worker.offer
    def record(frame, report):
        publications.append(frame)
        offer(frame, report)
    worker.offer = record
    worker.run()
    assert state_rpc.closed and stop.is_set()
    assert len(missing) == len(evaluated) == len(publications)
    assert [f.sequence for f in evaluated] == [i for i, outcome in enumerate(outcomes) if outcome is None]
    assert publications == evaluated
    assert worker.health['state'] == 'scanning'
    return reads, publications


def test_anchor_races_reread_full_pair_with_bounded_delay_and_reset_after_success(tmp_path, monkeypatch):
    race = Reject('V4 state anchor changed')
    reads, frames = run_schedule(tmp_path, monkeypatch, [race]*4 + [None, race, None])
    assert [b-a for a, b in zip(reads, reads[1:])] == pytest.approx([.5, .75, 1, 1, .5, .5])
    assert [f.sequence for f in frames] == [4, 6]
    assert frames[0].anchor != frames[1].anchor


@pytest.mark.parametrize('kind', ['429', 'rpc_limit', 'unavailable', 'timeout', 'other_reject', 'wrong_exception_type'])
def test_transport_and_other_failures_keep_exponential_backoff_despite_event_wake(tmp_path, monkeypatch, kind):
    if kind == '429':
        request = httpx.Request('POST', 'https://example.invalid')
        error = httpx.HTTPStatusError('throttled', request=request,
                                      response=httpx.Response(429, request=request))
    elif kind == 'rpc_limit': error = RPCError('eth_call', {'code': -32007, 'message': 'request limit reached'})
    elif kind == 'unavailable': error = RPCUnavailable('synthetic transport unavailable')
    elif kind == 'timeout': error = TimeoutError('transport timed out')
    elif kind == 'other_reject': error = Reject('V4 state clock mismatch')
    else: error = ValueError('V4 state anchor changed')
    reads, frames = run_schedule(tmp_path, monkeypatch, [error]*4 + [None])
    assert [b-a for a, b in zip(reads, reads[1:])] == pytest.approx([2, 4, 8, 15])
    assert [f.sequence for f in frames] == [4]


def test_snapshot_retry_does_not_add_another_delay_after_long_read(tmp_path, monkeypatch):
    race = Reject('V4 state anchor changed')
    reads, frames = run_schedule(tmp_path, monkeypatch, [race, race, None], read_seconds=1.2)
    assert [b-a for a, b in zip(reads, reads[1:])] == pytest.approx([1.2, 1.2])
    assert [f.sequence for f in frames] == [2]
