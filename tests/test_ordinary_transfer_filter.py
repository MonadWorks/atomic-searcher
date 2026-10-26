"""Read only complete groups with at least one eligible direction."""
import copy
import threading
import time

from eth_abi import decode, encode

from searcher.core import load_config
from searcher.math import sqrt_at_tick
from searcher.route_feedback import RouteFeedback
from searcher.routes import RouteIndex
from searcher.statefeed import STATE, StateFeed, StateFrame
from test_hot_transfer_filter import A, B, C, EXECUTOR, OWNER, incoming_shortfall, pool
from searcher.transfer_feedback import TransferFeedback
from test_pipeline import header, H0, H1, ZERO


class AtomicRPC:
    """Produce a coherent ABI lens result for the addresses actually requested."""
    def __init__(self): self.reads = []
    def batch(self, calls):
        head = header(2, H1, H0)
        pending = header(3, ZERO, H1)
        if calls[0][0] == 'eth_getBlockByNumber': return head, pending
        params = calls[0][1]
        addresses = decode(['address[]'], bytes.fromhex(params[0]['data'][10:]))[0]
        self.reads.append(tuple(addresses))
        clock = params[3]
        rows = [(a, True, sqrt_at_tick(0), 0, 10**12, 100, [-1,0,1], [0,0,0], []) for a in addresses]
        raw = '0x'+encode([STATE+'[]','uint256','uint256'],
            [rows, int(clock['number'],16), int(clock['time'],16)]).hex()
        return raw, head, pending


def setup():
    config = load_config()
    config['settlement_tokens'] = [{'address':A}]
    config['unsupported_transfer_tokens'] = {}
    config['policy'].update(cycle_pages=True, max_hops=2, max_pools=4,
                             route_groups_per_page=4, snapshot_encoding='abi')
    pools = [pool(11,A,B), pool(12,A,B), pool(13,A,C), pool(14,A,C)]
    saved = {'pools':[p.json() for p in pools], 'known_pools':[p.json() for p in pools]}
    now = [0.]
    transfers = TransferFeedback(EXECUTOR, OWNER, clock=lambda:now[0])
    feedback = RouteFeedback(clock=lambda:now[0])
    allowed = lambda group, states: not feedback.group_deferred(group, states, [A], transfers=transfers)
    return config, saved, pools, now, feedback, transfers, allowed


def test_normal_page_filters_combined_failures_before_atomic_read_and_recovers():
    config,saved,pools,now,feedback,transfers,allowed = setup()
    bad, good = tuple(p.address for p in pools[:2]), tuple(p.address for p in pools[2:])
    index = RouteIndex(pools[:2], [A], 2)
    forward, reverse = index.routes.values()
    feedback.record(forward.key, {'revert_name':'BadCallback'})
    rpc = AtomicRPC(); feed = StateFeed(rpc, config, saved, group_allowed=allowed)
    frame = feed.poll()
    assert set(bad).issubset(rpc.reads[-1]), 'one available reverse preserves both pools'
    incoming_shortfall(transfers, B, reverse.pools[0], reverse.pools[1])
    frame = feed.poll()
    assert set(rpc.reads[-1]) == set(good)
    assert feed.page_groups == [good]
    selection = frame.transport['cycle_pages']['selection']
    assert selection['quality_deferred_groups'] == 1
    assert selection['groups_by_hops'] == {'2':1, '3':0, '4':0}
    frozen = copy.deepcopy(selection)
    feedback.passed(forward.key)
    feed.poll()
    assert set(bad).issubset(rpc.reads[-1])
    assert frame.transport['cycle_pages']['selection'] == frozen


def test_all_blocked_group_stays_bounded_and_timeout_restores_original_groups():
    config,saved,pools,now,feedback,transfers,allowed = setup()
    for route in RouteIndex(pools, [A], 2).routes.values():
        feedback.record(route.key, {'revert_name':'BadCallback'})
    rpc = AtomicRPC(); feed = StateFeed(rpc, config, saved, group_allowed=allowed)
    feed.poll()
    assert rpc.reads == [()]
    assert feed.pages.selection_summary['quality_deferred_groups'] == 2
    now[0] = 31
    feed.poll()
    assert set(rpc.reads[-1]) == {p.address for p in pools}


def test_same_page_retry_keeps_coherent_topology_and_next_page_applies_new_feedback():
    config,saved,pools,now,feedback,transfers,allowed = setup()
    rpc = AtomicRPC(); feed = StateFeed(rpc, config, saved, group_allowed=allowed)
    feed.poll()
    initial = rpc.reads[-1]
    for route in RouteIndex(pools[:2], [A], 2).routes.values():
        feedback.record(route.key, {'revert_name':'BadCallback'})
    feed.poll(advance_page=False)
    assert rpc.reads[-1] == initial
    feed.poll()
    assert set(rpc.reads[-1]) == {p.address for p in pools[2:]}


def test_pipeline_installs_live_predicate_before_first_and_refreshed_feed(monkeypatch,tmp_path):
    import searcher.pipeline as pipeline
    import searcher.discovery_reads as discovery_reads
    from test_runtime import FakeRPC
    config,saved,pools,now,feedback,transfers,allowed = setup()
    config['v4'] = {'enabled':False}
    config['policy'].update(hot_lane_enabled=False, activity_priority=False,
        universe_refresh_seconds=.01, pending_poll_interval_seconds=.001)
    saved['header'] = header(1,H0,ZERO)
    updated = copy.deepcopy(saved); updated['marker'] = 'refreshed'; updated['discovery_cursor'] = 2
    first_read, published, stopped = threading.Event(), threading.Event(), threading.Event()
    instances, predicate_calls = [], []
    states = {p.address:p for p in pools}
    group = tuple(states)[:2]
    class SpyFeedback:
        def group_deferred(self, group_, pools_, settlements, route_keys=None, transfers=None):
            predicate_calls.append((group_,pools_,settlements,transfers))
            return bool(published.is_set())
    shared = SpyFeedback()
    class Strategy:
        def __init__(self,*args): self.execution_feedback = shared
        def on_frame(self,frame): return {'block':2,'decision_age_ms':0,'candidates':[]}
    class Feed:
        pages = None
        def __init__(self, rpc, cfg, state):
            self.state = state
            instances.append(self)
        def poll(self,*args,**kwargs):
            if self.state.get('marker') == 'refreshed':
                assert self.group_allowed(group, states) is False
                stopped.set()
            elif not first_read.is_set():
                assert self.group_allowed(group, states) is True
                first_read.set()
            ns = time.monotonic_ns()
            return StateFrame(header(2,H1,H0),header(2,H1,H0),saved['pools'],ns,ns,time.time(),3,106)
    class Discovery:
        def __init__(self,*args,**kwargs): pass
        def prepare(self,*args,**kwargs):
            assert first_read.wait(3)
            published.set()
            return updated
        def close(self): pass
    monkeypatch.setattr(pipeline,'RPC',FakeRPC)
    monkeypatch.setattr(pipeline,'StateFeed',Feed)
    monkeypatch.setattr(pipeline,'PendingStrategy',Strategy)
    monkeypatch.setattr(discovery_reads,'DiscoveryReads',Discovery)
    pipeline.run_pending(FakeRPC(),config,saved,5,tmp_path/'run',stop_event=stopped)
    assert stopped.is_set() and len(instances) >= 2
    assert instances[0].group_allowed is instances[-1].group_allowed
    assert len(predicate_calls) == 2
    assert all(call[2] == (A,) and call[3] is None for call in predicate_calls)
