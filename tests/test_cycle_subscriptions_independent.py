"""Independent subscription-scope and downstream capacity regressions."""
from types import SimpleNamespace
import json
import threading

from searcher.activity import CycleSubscriptions
from searcher.cycle_groups import CycleGroups
from searcher.stream_usage import StreamSelection, SubscriptionRoster
from searcher.universe import CyclePages
from test_coactivity import address, pool


def topology(pools, *, indexed=False):
    result = {'known_pools':[p.json() for p in pools], 'pools':[pools[0].json()]}
    if indexed: result['cycle_groups'] = CycleGroups(pools, [address(1)], 4)
    return result


def test_existing_prepare_index_is_reused_without_another_graph_build(monkeypatch):
    pools = [pool(101,1,2), pool(102,1,2), pool(103,1,3)]
    saved = topology(pools, indexed=True)
    def rebuild(*args, **kwargs): raise AssertionError('unexpected additional graph build')
    monkeypatch.setattr(CycleGroups, '__init__', rebuild)
    subscriptions = CycleSubscriptions(saved)
    assert subscriptions.snapshot() == {address(101), address(102)}
    # Refreshing a completed derived index cannot transiently expand to known.
    subscriptions.replace(dict(saved))
    assert subscriptions.details()['subscription_scope'] == 'executable_cycle_members'
    assert subscriptions.snapshot() == {address(101), address(102)}


def test_late_invalid_old_index_cannot_poison_new_completed_topology():
    pools = [pool(101,1,2), pool(102,1,2), pool(103,1,3)]
    old = topology(pools)
    subscriptions = CycleSubscriptions(old)
    latest = topology(pools+[pool(104,1,3)], indexed=True)
    subscriptions.replace(latest)
    assert not subscriptions.use_index(old, SimpleNamespace(covered={address(999)}))
    assert subscriptions.snapshot() == {address(i) for i in range(101,105)}
    assert subscriptions.details()['subscription_scope'] == 'executable_cycle_members'


def test_four_hop_only_members_survive_adaptive_selection_and_free_roster_capacity():
    four = [pool(201,1,2), pool(202,2,3), pool(203,3,4), pool(204,4,1)]
    four[1].kind = 'v2'
    dead = [pool(100+i,1,10+i) for i in range(9)]
    saved = topology(four+dead)
    subscriptions = CycleSubscriptions(saved)
    clock = [0.0]
    selector = StreamSelection(80000, clock=lambda:clock[0])
    roster = SubscriptionRoster(capacity=4, batch_size=2, clock=lambda:clock[0])
    first = roster.select(selector.select(subscriptions.snapshot()))
    assert first.isdisjoint(p.address for p in four)
    assert len(roster.waiting) == 9
    pages = CyclePages(four+dead, [address(1)], 4)
    assert pages.max_group_width == 4
    subscriptions.use_index(saved, pages)
    selected = roster.select(selector.select(subscriptions.snapshot()))
    assert selected == {p.address for p in four}
    assert not roster.waiting and selector.details()['http_only_pools'] == 0
    assert len(saved['known_pools']) == 13  # Discovery facts are preserved.


def test_removed_noise_cannot_keep_cooling_current_cycle_members():
    now = [0.0]
    p = [pool(101,1,2), pool(102,1,2), pool(103,1,3)]
    saved = topology(p)
    subscriptions = CycleSubscriptions(saved)
    selector = StreamSelection(80000, clock=lambda:now[0])
    selector.select(subscriptions.snapshot())
    selector.subscribed(subscriptions.snapshot())
    for _ in range(50): selector.observe(address(103))
    selector.observe(address(101)); now[0] = 11
    selector.select(subscriptions.snapshot())
    assert address(103) in selector.cool_until
    subscriptions.use_index(saved, CyclePages(p, [address(1)], 4))
    assert selector.select(subscriptions.snapshot()) == {address(101),address(102)}
    assert address(103) not in selector.cool_until


def test_legacy_json_list_remains_conservative_then_new_cycle_rejoins_immediately():
    p = [pool(101,1,2), pool(102,1,2), pool(103,1,3)]
    saved = topology(p); saved['cycle_groups'] = [[address(101),address(102)]]
    subscriptions = CycleSubscriptions(saved)
    assert subscriptions.snapshot() == {x.address for x in p}
    pages = CyclePages(p,[address(1)],4)
    subscriptions.use_index(saved,pages)
    assert address(103) not in subscriptions.snapshot()
    new = topology(p+[pool(104,1,3)], indexed=True)
    subscriptions.replace(new)
    assert subscriptions.snapshot() == {address(i) for i in range(101,105)}
    assert not subscriptions.use_index(saved,pages)


def test_pipeline_stream_consumes_derived_scope_and_emits_source_counts(tmp_path, monkeypatch):
    import searcher.activity as activity
    from test_runtime import setup_runtime, FakeRPC
    module, _, config = setup_runtime(monkeypatch)
    config['policy'].update(activity_priority=True,universe_refresh_seconds=100)
    config['v4']={'enabled':False}  # This assertion covers CL cycle membership.
    monkeypatch.setattr(activity,'ROOT',tmp_path)
    monkeypatch.setattr(activity,'websocket_url',lambda:'wss://example.invalid')
    class ReadRPC(FakeRPC):
        def request(self,*args): return []
    monkeypatch.setattr(module,'RPC',ReadRPC)
    seen = []
    def stream(url,chain,pools,stop,finish,publish,health,budget,**kwargs):
        seen.append(pools())
        health('connected',filtered_pools=len(seen[-1]),http_only_pools=0)
        stop.set()
    monkeypatch.setattr(activity,'run_stream',stream)
    market = topology([pool(101,1,2),pool(102,1,2),pool(103,1,3)],indexed=True)
    result = module.run_pending(ReadRPC(),config,market,.5,tmp_path/'pipeline',stop_event=threading.Event())
    assert seen == [{address(101),address(102)}]
    health = json.loads((tmp_path/'pipeline/activity-stream-health.json').read_text())
    assert health['subscription_scope'] == 'executable_cycle_members'
    assert health['known_universe_pools'] == 3 and health['cycle_subscription_pools'] == 2
    assert health['noncycle_subscription_pools_skipped'] == 1
    assert result['reader_stopped'] and result['activity_stream_stopped'] and result['discovery_stopped']
