"""Bounded fresh-event dispatch, exercised without any network or signer."""
import copy
import random
import threading
import time

import pytest

from searcher.activity import ActivityInbox
from searcher.core import Reject
from searcher.fastlane import FrameSequence, HotDispatchBudget, HotRoutes, run_hot_reader
from searcher.pools import Pool
from searcher.statefeed import StateFrame
from test_fastlane import config
from test_pipeline import saved, header, H0, H1
from test_runtime import FakeRPC


def pairs(count=3):
    universe = saved()
    template = universe['pools'][0]
    addresses = ['0x'+f'{i:040x}' for i in range(100, 100+count*2)]
    universe['pools'] = [dict(template, address=address, token1='0x'+f'{30+i//2:040x}')
                         for i, address in enumerate(addresses)]
    return universe, addresses


def snapshot(selector):
    names = ('active','offsets','choices','pair_offsets','turns','coactive_key',
             'coactive_cursor','pending','read_versions','reserved_versions','remembered',
             'priority_streak','stats','expired_examples')
    return copy.deepcopy({name:getattr(selector,name) for name in names})


def test_fresh_second_slot_and_exact_window_boundary():
    budget = HotDispatchBudget(.25)
    assert budget.record(10)['mode'] == 'regular'
    assert budget.mode(10.0625) == 'fresh_event'
    second = budget.record(10.0625, 10.03125)
    assert second['mode'] == 'fresh_event' and second['fixed_gate_wait_avoided_ms'] == 187.5
    assert budget.mode(10.499999) is None
    assert budget.mode(10.5) == 'regular'
    budget.record(10.5)
    assert budget.mode(10.5624999) is None
    assert budget.mode(10.5625) == 'fresh_event'


@pytest.mark.parametrize('stamp', [None, 9.9, 10.0, 11.0])
def test_early_dispatch_needs_genuinely_new_received_event(stamp):
    budget = HotDispatchBudget(.25)
    budget.record(10)
    with pytest.raises(Reject, match='requires a new event'):
        budget.record(10.125, stamp)
    assert list(budget.recent) == [10] and budget.last_dispatch == 10


def test_ordinary_work_preserves_interval_and_failed_read_consumes_slot():
    budget = HotDispatchBudget(.25)
    first = budget.record(10)
    # No success/ack is reported: the dispatch nevertheless consumed quota.
    assert first['dispatches_in_window'] == 1
    assert budget.mode(10.125) == 'fresh_event'
    assert budget.mode(10.25) == 'regular'
    budget.record(10.25)
    assert budget.mode(10.375) is None
    assert budget.mode(10.5) == 'regular'


def test_backoff_survives_other_worker_success_and_new_events():
    budget = HotDispatchBudget(.25)
    budget.record(10)
    budget.record(10.125, 10.0625)
    budget.backoff(11)
    budget.backoff(10.75)
    assert budget.mode(10.9999) is None
    assert budget.next_wake(10.6) == 11
    assert budget.mode(11) == 'regular'


def test_many_adversarial_arrivals_never_exceed_rolling_budget():
    budget = HotDispatchBudget(.25)
    rng, now, starts = random.Random(420), 10.0, []
    for _ in range(10000):
        now += rng.uniform(.00001, .04)
        mode = budget.mode(now)
        if mode is None: continue
        fresh = budget.last_dispatch+(now-budget.last_dispatch)*.5 if mode == 'fresh_event' else None
        budget.record(now, fresh)
        starts.append(now)
        if len(starts) >= 3:
            assert starts[-1] >= starts[-3]+.5
        if rng.random() < .02: budget.backoff(now+.8)
    assert len(starts) > 100


def test_fresh_selection_preserves_quiet_ordinary_work_and_complete_group():
    universe, addresses = pairs(2)
    selector = HotRoutes(universe, config(), clock=lambda:10.2)
    selector.add([(addresses[0],10.0),(addresses[2],10.2)])
    first = selector.take_fresh(10.1)
    assert first['groups'] == [tuple(addresses[2:])]
    assert {p.address for p in first['pools']} == set(addresses[2:])
    assert addresses[0] not in selector.offsets and addresses[0] not in selector.pair_offsets
    assert list(selector.active)[0] == addresses[0]
    selector.reserve(first)
    selector.acknowledge(first)
    assert tuple(addresses[:2]) in selector.take()['groups']


@pytest.mark.parametrize('cause', ['old','filtered','reserved','consumed','future','expired'])
def test_unsent_fresh_probe_cannot_mutate_fairness_or_ack(cause):
    universe, addresses = pairs(1)
    now = [10.2]
    selector = HotRoutes(universe, config(), clock=lambda:now[0],
                         group_allowed=(lambda *args:False) if cause == 'filtered' else None)
    stamp = 10.0 if cause == 'old' else 10.2
    if cause == 'future': stamp = 10.3
    if cause == 'expired': stamp = 8
    selector.add([(addresses[0],stamp)])
    if cause in ('reserved','consumed'):
        first = selector.take()
        selector.reserve(first)
        if cause == 'consumed': selector.acknowledge(first)
    before = snapshot(selector)
    assert selector.take_fresh(10.1) is None
    assert snapshot(selector) == before


def test_new_same_group_generation_can_read_while_old_reserved_and_failure_retains_it():
    universe, addresses = pairs(1)
    now = [10.0]
    selector = HotRoutes(universe, config(), clock=lambda:now[0])
    selector.add([(addresses[0],10)])
    old = selector.take()
    selector.reserve(old)
    now[0] = 10.2
    selector.add([(addresses[1],10.2)])
    new = selector.take_fresh(10.1)
    selector.reserve(new)
    assert new['_group_versions'] != old['_group_versions']
    selector.failed(new)
    selector.acknowledge(old)
    retry = selector.take()
    assert retry['groups'] == old['groups']
    assert HotRoutes._stamp(retry['_group_versions'][old['groups'][0]]) == 10.2


def test_repeated_fresh_bursts_leave_ordinary_turn_and_no_same_event_reuse():
    universe, addresses = pairs(2)
    now = [10.0]
    selector = HotRoutes(universe, config(), clock=lambda:now[0])
    budget = HotDispatchBudget(.25)
    selector.add([(addresses[0],10)])
    old = selector.take(); budget.record(10); selector.reserve(old)
    for stamp in (10.0625,10.5,10.5625,11.0,11.0625):
        now[0] = stamp
        selector.add([(addresses[2],stamp)])
        mode = budget.mode(stamp)
        selected = selector.take_fresh(budget.last_dispatch) if mode == 'fresh_event' else selector.take()
        budget.record(stamp, selected['newest_event_at'])
        selector.reserve(selected); selector.acknowledge(selected)
        assert selector.take_fresh(stamp) is None
    selector.release(old)
    now[0] = 11.1
    assert tuple(addresses[:2]) in selector.take()['groups']


def test_threaded_early_event_does_not_allow_third_dispatch_or_exceed_two_workers(tmp_path):
    universe, addresses = pairs(3)
    cfg = config()
    cfg['policy'].update(hot_lane_min_interval_seconds=.5, hot_lane_event_ttl_seconds=5)
    stop, entered, release, second = (threading.Event() for _ in range(4))
    inbox, outputs, starts, health = ActivityInbox(), [], [], []
    active, max_active, lock = [0], [0], threading.Lock()

    class Feed:
        def __init__(self, rpc, config, universe):
            self.pools = [Pool.from_json(p) for p in universe['pools']]
        def poll(self):
            start = time.monotonic_ns()
            with lock:
                active[0] += 1; max_active[0] = max(max_active[0],active[0])
                starts.append((set(p.address for p in self.pools),time.monotonic()))
            if addresses[0] in starts[-1][0]:
                entered.set(); assert release.wait(2)
            else: second.set()
            with lock: active[0] -= 1
            return StateFrame(header(2,H1,H0),header(2,H1,H0),[p.json() for p in self.pools],
                              start,time.monotonic_ns(),time.time(),3,106)
    def publish(lane, item):
        outputs.append(item[1])
        if len(outputs) == 3: stop.set()
    inbox.publish([addresses[0]],time.monotonic())
    runner = threading.Thread(target=run_hot_reader, kwargs=dict(rpc_factory=FakeRPC,config=cfg,
        universe=lambda:(0,universe),inbox=inbox,stop=stop,finish=time.monotonic()+3,
        directory=tmp_path,sequence=FrameSequence(),publish=publish,
        health=lambda *a,**k:health.append((a,k)),feed_factory=Feed))
    runner.start()
    try:
        assert entered.wait(1), health
        inbox.publish([addresses[2]],time.monotonic())
        assert second.wait(.4), 'New event remained behind the half-second fixed gate.'
        inbox.publish([addresses[4]],time.monotonic())
        release.set()
        runner.join(3)
    finally:
        release.set(); stop.set(); runner.join(2)
    assert not runner.is_alive() and len(starts) >= 3, health
    assert starts[1][1]-starts[0][1] < .4
    assert starts[2][1]-starts[0][1] >= .99
    assert max_active[0] <= 2
    pacing = sorted((frame.transport['dispatch_pacing'] for frame in outputs),key=lambda p:p['dispatched_at'])
    assert any(p['mode'] == 'fresh_event' for p in pacing)
    assert all(p['dispatches_in_window'] <= 2 for p in pacing)
    for p in pacing:
        if p['mode'] == 'fresh_event':
            frame = next(f for f in outputs if f.transport['dispatch_pacing'] == p)
            assert all(max(g['triggers'].values()) > p['previous_dispatch_at']
                       for g in frame.transport['group_event_versions'])


def test_threaded_fresh_event_cannot_bypass_failed_read_backoff(tmp_path):
    universe, addresses = pairs(2)
    stop, retried = threading.Event(), threading.Event()
    inbox, starts, health = ActivityInbox(), [], []
    class Feed:
        def __init__(self,rpc,config,universe):
            self.pools = [Pool.from_json(p) for p in universe['pools']]
        def poll(self):
            start = time.monotonic_ns(); starts.append(time.monotonic())
            if len(starts) == 1: raise OSError('intentional read failure')
            return StateFrame(header(2,H1,H0),header(2,H1,H0),[p.json() for p in self.pools],
                              start,time.monotonic_ns(),time.time(),3,106)
    def report(state, **details):
        health.append((state,details))
        if state == 'retrying':
            inbox.publish([addresses[2]],time.monotonic()); retried.set()
    inbox.publish([addresses[0]],time.monotonic())
    run_hot_reader(FakeRPC,config(),lambda:(0,universe),inbox,stop,time.monotonic()+2,
                   tmp_path,FrameSequence(),lambda *args:stop.set(),report,feed_factory=Feed)
    assert retried.is_set() and len(starts) == 2, health
    assert starts[1]-starts[0] >= .5
