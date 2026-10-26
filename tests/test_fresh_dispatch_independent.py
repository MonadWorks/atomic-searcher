"""Independent local scheduler boundaries; no sockets, signer, or production IO."""
import threading
import time

from searcher.activity import ActivityInbox
from searcher.fastlane import FrameSequence, HotDispatchBudget, HotRoutes, run_hot_reader
from searcher.pools import Pool
from searcher.statefeed import StateFrame
from test_fastlane import config
from test_fresh_dispatch import pairs, snapshot
from test_pipeline import header, H0, H1
from test_runtime import FakeRPC


def test_abort_fresh_probe_preserves_already_populated_rotation_and_reservations():
    universe, addresses = pairs(6)
    clock = [10.0]
    cfg = config()
    cfg['policy'].update(hot_lane_max_pools=5, hot_lane_max_groups=3)
    selector = HotRoutes(universe, cfg, clock=lambda:clock[0])
    selector.add([(address,10.0) for address in addresses])
    old = selector.take()
    selector.reserve(old)
    assert selector.pending and selector.remembered and selector.reserved_versions
    clock[0] = 10.1
    selector.add([(address,10.1) for address in addresses])
    selector.group_allowed = lambda *args: False
    before = snapshot(selector)
    assert selector.take_fresh(10.05) is None
    assert snapshot(selector) == before
    selector.group_allowed = None
    new = selector.take_fresh(10.05)
    assert new and all(max(dict(version).values()) > 10.05
                       for version in new['_group_versions'].values())
    # The failed trial did not acknowledge old work or consume its reservation.
    for group, version in old['_group_versions'].items():
        assert version in selector.reserved_versions[group]


def test_stop_during_successful_fresh_selection_cannot_start_another_worker(tmp_path, monkeypatch):
    universe, addresses = pairs(2)
    cfg = config()
    cfg['policy'].update(hot_lane_min_interval_seconds=1.0, hot_lane_event_ttl_seconds=5)
    inbox, stop = ActivityInbox(), threading.Event()
    starts, outputs, selected, health = [], [], [], []
    original = HotRoutes.take_fresh
    def stopping_selection(self, after):
        result = original(self, after)
        if result:
            selected.append(result)
            stop.set()
        return result
    monkeypatch.setattr(HotRoutes, 'take_fresh', stopping_selection)
    class Feed:
        def __init__(self, rpc, config, saved):
            self.pools = [Pool.from_json(p) for p in saved['pools']]
        def poll(self):
            started = time.monotonic_ns()
            starts.append(started)
            return StateFrame(header(2,H1,H0),header(2,H1,H0),[p.json() for p in self.pools],
                              started,time.monotonic_ns(),time.time(),3,106)
    def publish(*args):
        outputs.append(args)
        inbox.publish([addresses[2]],time.monotonic())
    inbox.publish([addresses[0]],time.monotonic())
    run_hot_reader(FakeRPC,cfg,lambda:(0,universe),inbox,stop,time.monotonic()+2,
                   tmp_path,FrameSequence(),publish,lambda *a,**k:health.append((a,k)),feed_factory=Feed)
    assert selected and stop.is_set(), health
    assert len(starts) == len(outputs) == 1


def test_other_worker_success_cannot_clear_real_coordinator_failure_backoff(tmp_path, monkeypatch):
    universe, addresses = pairs(3)
    cfg = config()
    cfg['policy'].update(hot_lane_min_interval_seconds=.2, hot_lane_event_ttl_seconds=5)
    inbox, stop = ActivityInbox(), threading.Event()
    first_entered, second_entered, failure_processed = (threading.Event() for _ in range(3))
    starts, failures, health, retry_deadlines = [], [], [], []
    original_backoff = HotDispatchBudget.backoff
    def capture_backoff(self, until):
        original_backoff(self, until)
        retry_deadlines.append(self.retry_after)
    monkeypatch.setattr(HotDispatchBudget, 'backoff', capture_backoff)
    class Feed:
        def __init__(self,rpc,config,saved):
            self.pools = [Pool.from_json(p) for p in saved['pools']]
        def poll(self):
            started = time.monotonic_ns()
            group = {p.address for p in self.pools}
            starts.append((group,time.monotonic()))
            if addresses[0] in group and not failure_processed.is_set():
                first_entered.set()
                assert second_entered.wait(1)
                # Both rolling-window slots have aged out by the failure;
                # only the retry gate can then enforce the half-second wait.
                time.sleep(.45)
                raise OSError('independent synthetic read failure')
            if addresses[2] in group:
                second_entered.set()
                assert failure_processed.wait(1)
            return StateFrame(header(2,H1,H0),header(2,H1,H0),[p.json() for p in self.pools],
                              started,time.monotonic_ns(),time.time(),3,106)
    def report(state, **details):
        health.append((state,details))
        if state == 'retrying':
            failures.append(time.monotonic())
            inbox.publish([addresses[4]],time.monotonic())
            failure_processed.set()
    def publish(lane, item):
        if addresses[4] in {p['address'] for p in item[1].snapshot_pools}: stop.set()
    inbox.publish([addresses[0]],time.monotonic())
    runner = threading.Thread(target=run_hot_reader, kwargs=dict(rpc_factory=FakeRPC,config=cfg,
        universe=lambda:(0,universe),inbox=inbox,stop=stop,finish=time.monotonic()+3,
        directory=tmp_path,sequence=FrameSequence(),publish=publish,health=report,feed_factory=Feed))
    runner.start()
    try:
        assert first_entered.wait(1), health
        inbox.publish([addresses[2]],time.monotonic())
        runner.join(3.5)
    finally:
        stop.set(); failure_processed.set(); second_entered.set(); runner.join(2)
    assert not runner.is_alive() and failures, health
    third = next(stamp for group, stamp in starts if addresses[4] in group)
    # Measure the coordinator's actual retry deadline. A thread preemption
    # before the later health callback must not shorten the measured wait.
    assert retry_deadlines and third >= retry_deadlines[0], (starts,failures,retry_deadlines,health)
