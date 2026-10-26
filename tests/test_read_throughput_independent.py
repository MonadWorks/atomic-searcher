"""Independent regressions for bounded concurrent event reads; no network."""
import threading
import time

import pytest

from searcher.activity import ActivityInbox
from searcher.fastlane import FrameSequence, run_hot_reader
from searcher.statefeed import StateFrame
from test_fastlane import config
from test_pipeline import saved, header, H0, H1
from test_runtime import FakeRPC


def disjoint_pairs():
    universe = saved()
    template = universe['pools'][0]
    addresses = ['0x' + f'{i:040x}' for i in range(100, 104)]
    universe['pools'] = [dict(template, address=address) for address in addresses]
    for index in (2, 3):
        universe['pools'][index]['token1'] = '0x' + f'{23:040x}'
    return universe, addresses


def test_new_event_reads_while_old_rpc_is_blocked_and_late_old_result_cannot_replace_it(tmp_path):
    universe, addresses = disjoint_pairs()
    first_group, new_group = set(addresses[:2]), set(addresses[2:])
    cfg = config()
    cfg['policy']['hot_lane_event_ttl_seconds'] = 5
    stop, entered, release = threading.Event(), threading.Event(), threading.Event()
    newer_read, old_finished, newer_published = threading.Event(), threading.Event(), threading.Event()
    old_group_retried = threading.Event()
    inbox, outputs, starts, health, result = ActivityInbox(), [], [], [], []

    class Feed:
        def __init__(self, rpc, config, universe):
            from searcher.pools import Pool
            self.pools = [Pool.from_json(pool) for pool in universe['pools']]

        def poll(self):
            pools = {pool.address for pool in self.pools}
            start = time.monotonic_ns()
            starts.append((pools, time.monotonic()))
            if pools == first_group and not entered.is_set():
                entered.set()
                assert release.wait(3)
                old_finished.set()
            elif new_group <= pools:
                newer_read.set()
            end = time.monotonic_ns()
            return StateFrame(header(2, H1, H0), header(2, H1, H0),
                              [pool.json() for pool in self.pools], start, end,
                              time.time(), 3, 106)

    def publish(lane, recorded):
        pools = {pool['address'] for pool in recorded[1].snapshot_pools}
        outputs.append((recorded[1].transport['dispatch_id'], pools))
        if new_group <= pools:
            newer_published.set()
        if pools == first_group:
            old_group_retried.set()

    def run():
        run_hot_reader(FakeRPC, cfg, lambda: (0, universe), inbox, stop,
                       time.monotonic() + 4, tmp_path, FrameSequence(), publish,
                       lambda *args, **kwargs: health.append((args, kwargs)),
                       feed_factory=Feed)
        result.append('finished')

    inbox.publish([addresses[0]], time.monotonic())
    runner = threading.Thread(target=run)
    runner.start()
    try:
        assert entered.wait(1), health
        inbox.publish([addresses[2]], time.monotonic())
        assert newer_read.wait(1), 'New event waited behind a blocked independent read.'
        assert newer_published.wait(1), health
        assert not old_finished.is_set()
        assert starts[1][0] == new_group, 'An unchanged inflight group was read again.'
        release.set()
        assert old_finished.wait(1)
        assert old_group_retried.wait(1), 'Discarded work on a different group was consumed without evaluation.'
        assert outputs[0][1] == new_group and all(job != 1 for job, pools in outputs), outputs
        assert any(job > 2 and pools == first_group for job, pools in outputs)
        # New event work may use the second slot early; the rolling budget
        # still prevents a third dispatch within two configured intervals.
        assert starts[1][1] - starts[0][1] < cfg['policy']['hot_lane_min_interval_seconds'], starts
        assert starts[2][1] - starts[0][1] >= 2*cfg['policy']['hot_lane_min_interval_seconds']-.01, starts
    finally:
        release.set()
        stop.set()
        runner.join(3)
    assert not runner.is_alive() and result == ['finished']


def test_one_event_keeps_unread_parallel_groups_after_earlier_groups_complete(tmp_path):
    universe = saved()
    template = universe['pools'][0]
    addresses = ['0x' + f'{i:040x}' for i in range(100, 110)]
    universe['pools'] = [dict(template, address=address) for address in addresses]
    cfg = config()
    cfg['policy']['hot_lane_event_ttl_seconds'] = 5
    stop, inbox, groups, health = threading.Event(), ActivityInbox(), [], []

    class Feed:
        def __init__(self, rpc, config, universe):
            from searcher.pools import Pool
            self.pools = [Pool.from_json(pool) for pool in universe['pools']]

        def poll(self):
            now = time.monotonic_ns()
            return StateFrame(header(2, H1, H0), header(2, H1, H0),
                              [pool.json() for pool in self.pools], now, now,
                              time.time(), 3, 106)

    def publish(lane, recorded):
        groups.extend(tuple(group) for group in recorded[1].transport['route_groups'])
        if set(pool for group in groups for pool in group) == set(addresses):
            stop.set()

    inbox.publish([addresses[0]], time.monotonic())
    run_hot_reader(FakeRPC, cfg, lambda: (0, universe), inbox, stop,
                   time.monotonic() + 2, tmp_path, FrameSequence(), publish,
                   lambda *args, **kwargs: health.append((args, kwargs)),
                   feed_factory=Feed)
    assert set(pool for group in groups for pool in group) == set(addresses), health
    assert len(groups) == len(set(groups)), 'Same event/group generation was reread after success.'


@pytest.mark.parametrize('fatal_reason', ['wrong_chain', 'recording_error'])
def test_local_fatal_error_cancels_other_worker_without_stopping_trading(tmp_path, monkeypatch, fatal_reason):
    from searcher.core import Reject, RPCUnavailable
    from searcher.recorder import Recorder

    universe, addresses = disjoint_pairs()
    stop, limiter_entered, escape = threading.Event(), threading.Event(), threading.Event()
    failed = threading.Event()
    inbox, clients, outputs, health, closed = ActivityInbox(), [], [], [], []
    mutex = threading.Lock()

    class RPC(FakeRPC):
        def __init__(self):
            self.cancel_event = stop
            self.owner = threading.get_ident()
            with mutex:
                self.index = len(clients)
                clients.append(self)

        def verify_chain(self):
            if self.index == 0:
                limiter_entered.set()
                # Model the actual limiter's cancellation-controlled waiting,
                # which lies outside the network's total-request deadline.
                while not escape.is_set() and not self.cancel_event.is_set():
                    self.cancel_event.wait(.01)
                raise RPCUnavailable('cancelled blocked limiter')
            if fatal_reason == 'wrong_chain':
                raise Reject('wrong chain')

        def close(self):
            assert threading.get_ident() == self.owner
            closed.append(self.index)

    class Feed:
        def __init__(self, rpc, config, universe):
            from searcher.pools import Pool
            self.pools = [Pool.from_json(pool) for pool in universe['pools']]

        def poll(self):
            now = time.monotonic_ns()
            return StateFrame(header(2, H1, H0), header(2, H1, H0),
                              [pool.json() for pool in self.pools], now, now,
                              time.time(), 3, 106)

    if fatal_reason == 'recording_error':
        def broken_append(*args, **kwargs):
            raise OSError('independent test cannot record evidence')
        monkeypatch.setattr(Recorder, 'append', broken_append)

    def record_health(state, **detail):
        health.append((state, detail))
        if state == 'failed':
            failed.set()

    inbox.publish([addresses[0]], time.monotonic())
    runner = threading.Thread(target=run_hot_reader, kwargs=dict(
        rpc_factory=RPC, config=config(), universe=lambda: (0, universe),
        inbox=inbox, stop=stop, finish=time.monotonic()+4, directory=tmp_path,
        sequence=FrameSequence(), publish=lambda *args: outputs.append(args),
        health=record_health, feed_factory=Feed), daemon=True)
    runner.start()
    try:
        assert limiter_entered.wait(1), health
        inbox.publish([addresses[2]], time.monotonic())
        assert failed.wait(1.5), health
        runner.join(.5)
        assert not runner.is_alive(), 'Local shutdown left another worker in the limiter.'
        assert not stop.is_set(), 'Stopping the hot subsystem changed the trading stop flag.'
        assert not outputs
        assert sorted(closed) == [0, 1]
    finally:
        escape.set()
        stop.set()
        runner.join(2)


def test_transient_rpc_factory_error_records_stage_and_recovers(tmp_path):
    from searcher.pools import Pool

    universe, addresses = disjoint_pairs()
    cfg = config()
    cfg['policy']['hot_lane_event_ttl_seconds'] = 5
    inbox, stop, factories, outputs, health = ActivityInbox(), threading.Event(), [], [], []

    def factory():
        factories.append(time.monotonic())
        if len(factories) == 1:
            raise OSError('independent temporary initialization failure')
        return FakeRPC()

    class Feed:
        def __init__(self, rpc, config, universe):
            self.pools = [Pool.from_json(pool) for pool in universe['pools']]

        def poll(self):
            now = time.monotonic_ns()
            return StateFrame(header(2, H1, H0), header(2, H1, H0),
                              [pool.json() for pool in self.pools], now, now,
                              time.time(), 3, 106)

    def publish(*args):
        outputs.append(args)
        stop.set()

    inbox.publish([addresses[0]], time.monotonic())
    run_hot_reader(factory, cfg, lambda: (0, universe), inbox, stop,
                   time.monotonic()+2, tmp_path, FrameSequence(), publish,
                   lambda state, **detail: health.append((state, detail)), feed_factory=Feed)
    assert len(outputs) == 1 and len(factories) == 2, health
    assert any(state == 'retrying' and detail['stage'] == 'rpc_initialization'
               for state, detail in health)
    assert not any(state == 'failed' for state, detail in health)


def test_new_generation_of_same_group_survives_inflight_and_old_completion(tmp_path):
    from searcher.pools import Pool

    universe, addresses = disjoint_pairs()
    cfg = config()
    cfg['policy']['hot_lane_event_ttl_seconds'] = 5
    inbox, stop = ActivityInbox(), threading.Event()
    entered, release, published_new = threading.Event(), threading.Event(), threading.Event()
    starts, outputs, health = [], [], []

    class Feed:
        def __init__(self, rpc, config, universe):
            self.pools = [Pool.from_json(pool) for pool in universe['pools']]

        def poll(self):
            start = time.monotonic_ns()
            starts.append(start)
            if len(starts) == 1:
                entered.set()
                assert release.wait(3)
            return StateFrame(header(2, H1, H0), header(2, H1, H0),
                              [pool.json() for pool in self.pools], start, time.monotonic_ns(),
                              time.time(), 3, 106)

    def publish(lane, recorded):
        outputs.append(recorded[1].transport['dispatch_id'])
        published_new.set()

    inbox.publish([addresses[0]], time.monotonic())
    runner = threading.Thread(target=run_hot_reader, kwargs=dict(
        rpc_factory=FakeRPC, config=cfg, universe=lambda: (0, universe), inbox=inbox,
        stop=stop, finish=time.monotonic()+4, directory=tmp_path, sequence=FrameSequence(),
        publish=publish, health=lambda *a, **k: health.append((a,k)), feed_factory=Feed), daemon=True)
    runner.start()
    try:
        assert entered.wait(1), health
        inbox.publish([addresses[0]], time.monotonic())
        assert published_new.wait(1), health
        assert outputs == [2]
        release.set()
        time.sleep(.35)
        assert outputs == [2] and len(starts) == 2, (outputs, starts, health)
    finally:
        release.set()
        stop.set()
        runner.join(2)
    assert not runner.is_alive()


def test_inflight_old_metadata_is_recorded_but_never_published(tmp_path):
    import copy
    import sqlite3
    from searcher.pools import Pool
    from searcher.recorder import decode_payload

    universe, addresses = disjoint_pairs()
    changed = copy.deepcopy(universe)
    new_token = '0x' + f'{24:040x}'
    for pool in changed['pools'][:2]:
        pool['token1'] = new_token
    current = [(0, universe)]
    inbox, stop = ActivityInbox(), threading.Event()
    entered, release, newer_published, older_finished = [threading.Event() for _ in range(4)]
    outputs, health = [], []

    class Feed:
        def __init__(self, rpc, config, universe):
            self.pools = [Pool.from_json(pool) for pool in universe['pools']]

        def poll(self):
            start = time.monotonic_ns()
            old = self.pools[0].token1 != new_token
            if old:
                entered.set()
                assert release.wait(3)
                older_finished.set()
            return StateFrame(header(2, H1, H0), header(2, H1, H0),
                              [pool.json() for pool in self.pools], start, time.monotonic_ns(),
                              time.time(), 3, 106)

    def publish(lane, recorded):
        outputs.append(recorded[1])
        newer_published.set()

    inbox.publish([addresses[0]], time.monotonic())
    runner = threading.Thread(target=run_hot_reader, kwargs=dict(
        rpc_factory=FakeRPC, config=config(), universe=lambda: current[0], inbox=inbox,
        stop=stop, finish=time.monotonic()+4, directory=tmp_path, sequence=FrameSequence(),
        publish=publish, health=lambda *a, **k: health.append((a,k)), feed_factory=Feed), daemon=True)
    runner.start()
    try:
        assert entered.wait(1), health
        current[0] = (1, changed)
        inbox.publish([addresses[0]], time.monotonic())
        assert newer_published.wait(1), health
        release.set()
        assert older_finished.wait(1)
        time.sleep(.1)
    finally:
        release.set()
        stop.set()
        runner.join(2)
    assert not runner.is_alive()
    assert outputs and all(pool['token1'] == new_token for f in outputs for pool in f.snapshot_pools)
    with sqlite3.connect(f'file:{tmp_path}/hot-first-seen.sqlite?mode=ro', uri=True) as db:
        dispositions = [decode_payload(row[0])['transport']['hot_read_disposition']
                        for row in db.execute('SELECT payload FROM frames')]
    assert 'topology_changed' in dispositions and 'published' in dispositions
