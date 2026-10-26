"""Independent adversarial regression tests for the optional event reader."""
import time
import threading

from searcher.activity import ActivityInbox
from searcher.core import RPCUnavailable
from searcher.fastlane import FrameSequence, run_hot_reader
from searcher.statefeed import StateFrame
from test_fastlane import config
from test_pipeline import saved, A, header, H0, H1
from test_runtime import FakeRPC


def test_hot_reader_recovers_from_transient_initial_chain_check(tmp_path):
    stop = threading.Event()
    observed, health, checks = [], [], []
    inbox = ActivityInbox()
    inbox.publish([A], time.monotonic())

    class RPC(FakeRPC):
        def verify_chain(self):
            checks.append(time.monotonic())
            if len(checks) == 1:
                raise RPCUnavailable('temporary test connection failure')

    class Feed:
        def __init__(self, rpc, cfg, universe):
            from searcher.pools import Pool
            self.pools = [Pool.from_json(p) for p in universe['pools']]

        def poll(self):
            now = time.monotonic_ns()
            return StateFrame(header(2, H1, H0), header(2, H1, H0),
                              [p.json() for p in self.pools], now, now, time.time(), 3, 106)

    def publish(lane, frame):
        observed.append((lane, frame))
        stop.set()

    run_hot_reader(rpc_factory=RPC, config=config(), universe=lambda: (0, saved()),
                   inbox=inbox, stop=stop, finish=time.monotonic()+1.4,
                   directory=tmp_path, sequence=FrameSequence(), publish=publish,
                   health=lambda state, **detail: health.append(state), feed_factory=Feed)
    assert len(checks) >= 2, health
    assert observed and observed[0][0] == 'hot', health


def test_separate_lane_observations_preserve_each_lanes_sizing_progress(tmp_path, monkeypatch):
    """Exercise actual pipeline wiring, then alternate unfinished exact-state quotes."""
    import copy
    import searcher.pipeline as pipeline
    import searcher.activity as activity
    from searcher.core import load_config, WETH
    from searcher.engine import evaluate
    from searcher.routes import RouteIndex
    from test_receipt_eligibility import pools, HEADER

    c = load_config()
    c['policy'].update(hot_lane_enabled=True, activity_priority=True,
                       sizing_mode='fixed', universe_refresh_seconds=100)
    c['settlement_tokens'] = [dict(c['settlement_tokens'][0], max_input_raw=str(10**18))]
    ps = pools()[2:]
    universe = {'header': HEADER, 'pools': [p.json() for p in ps]}
    instances = []
    original = pipeline.PendingStrategy

    class Strategy(original):
        def __init__(self, *args):
            super().__init__(*args)
            instances.append(self)

        def on_frame(self, frame):
            return {'block': 100, 'decision_age_ms': 0, 'candidates': []}

    class Feed:
        def __init__(self, *args): pass
        def poll(self):
            now = time.monotonic_ns()
            return StateFrame(HEADER, HEADER, universe['pools'], now, now, time.time(), 101, 200)

    monkeypatch.setattr(pipeline, 'RPC', FakeRPC)
    monkeypatch.setattr(pipeline, 'StateFeed', Feed)
    monkeypatch.setattr(pipeline, 'PendingStrategy', Strategy)
    monkeypatch.setattr(FakeRPC, 'request', lambda *args: [], raising=False)
    monkeypatch.setattr(activity, 'websocket_url', lambda: None)
    monkeypatch.setattr(activity, 'ROOT', tmp_path)
    pipeline.run_pending(FakeRPC(), c, universe, .05, tmp_path)
    assert len(instances) == 2
    ordinary, hot = instances
    ordinary.index = RouteIndex(copy.deepcopy(ps), [WETH], 2)
    hot.index = RouteIndex(copy.deepcopy(ps), [WETH], 2)
    # Two immutable snapshots can differ in price/depth: each must resume its
    # own interrupted search without the other lane erasing its work.
    hot.index.states[ps[0].address].reserve1 += 1
    ordinary.index.sizing_progress = ordinary.sizing_progress
    hot.index.sizing_progress = hot.sizing_progress
    observed = []
    def interrupted(route, states, maximum, fee, **kwargs):
        work = kwargs['continuation']
        work['steps'] = work.get('steps', 0)+1
        observed.append(work['steps'])
        return {'complete': False, 'sizing_time_limited': True}
    monkeypatch.setattr('searcher.engine.optimize', interrupted)
    for _ in range(2):
        evaluate(ordinary.index, c, HEADER)
        evaluate(hot.index, c, HEADER)
    assert observed == [1, 1, 2, 2], observed


def test_capacity_cutoff_does_not_permanently_skip_unread_pair_variants():
    """A second active family must not lose every other pair at the page boundary."""
    from searcher.fastlane import HotRoutes
    from test_pipeline import B, C
    def addr(n): return '0x'+f'{n:040x}'
    universe = saved()
    template = universe['pools'][0]
    first = [addr(i) for i in range(10,15)]
    second = [addr(i) for i in range(20,25)]
    extras = [(addr(30),A,addr(100)),(addr(31),B,addr(100)),
              (addr(32),A,addr(101)),(addr(33),C,addr(101))]
    universe['pools'] = ([dict(template,address=p,token0=A,token1=B) for p in first]
                        +[dict(template,address=p,token0=A,token1=C) for p in second]
                        +[dict(template,address=p,token0=x,token1=y) for p,x,y in extras])
    selector = HotRoutes(universe, config(), clock=lambda:10)
    selector.add([(first[0],10),(second[0],10)])
    selected = set()
    for _ in range(16):
        selected.update(selector.take()['groups'])
    expected = {tuple(sorted((second[0],p))) for p in second[1:]}
    expected.add(tuple(sorted((second[0],addr(32),addr(33)))))
    assert expected <= selected, {'missing':sorted(expected-selected), 'selected':sorted(selected)}


def test_hot_route_bounds_leave_room_for_one_complete_priority_turn():
    import pytest
    from searcher.core import Reject
    from searcher.fastlane import HotRoutes
    for key, value in [('hot_lane_max_groups',1),('hot_lane_max_groups',2),('hot_lane_max_pools',4)]:
        c = config()
        c['policy'][key] = value
        with pytest.raises(Reject, match='invalid hot route bounds'):
            HotRoutes(saved(), c)
