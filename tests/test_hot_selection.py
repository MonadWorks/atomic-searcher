"""Lifecycle and complete live-pipeline regressions on isolated local inputs."""
import json
import threading
import time

from searcher.fastlane import HotRoutes
from test_fastlane import config
from test_pipeline import saved, A, B
from test_execution import market


def test_failed_read_retains_priority_and_ack_consumes_only_its_event_version():
    clock = [10.0]
    selector = HotRoutes(saved(), config(), clock=lambda:clock[0])
    selector.add([(A,10), (B,10)])
    first = selector.take()
    selector.failed(first)
    retry = selector.take()
    assert retry['coactive_priority_groups'] == [(A,B)]
    selector.acknowledge(retry)
    assert not selector.take()['coactive_priority_groups']
    clock[0] += .1
    selector.add([(B,clock[0])])
    later = selector.take()
    assert later['coactive_priority_groups'] == [(A,B)]
    selector.acknowledge(first)  # Late older ACK cannot erase newer work.
    assert selector.pending[(A,B)]['version'] == later['_coactive_versions'][(A,B)]


def test_expired_unread_is_visible_even_if_no_next_frame_is_read():
    clock = [10.0]
    selector = HotRoutes(saved(), config(), clock=lambda:clock[0])
    selector.add([(A,10), (B,10)])
    selected = selector.take(); selector.failed(selected)
    clock[0] = 12
    assert selector.take()['groups'] == []
    assert selector.summary()['tracked_expired_unread'] == 1
    assert selector.summary()['tracked_expired_unselected'] == 0
    selector.take()
    assert selector.summary()['tracked_expired_unread'] == 1


def test_metadata_replacement_drops_old_groups_and_retains_read_versions():
    selector = HotRoutes(saved(), config(), clock=lambda:10)
    selector.add([(A,10), (B,10)])
    selector.acknowledge(selector.take())
    fresh = HotRoutes(saved(), config(), clock=lambda:10)
    fresh.resume(selector)
    assert fresh.take()['coactive_priority_groups'] == []
    removed = saved(); removed['pools'] = removed['pools'][:1]
    fresh = HotRoutes(removed, config(), clock=lambda:10)
    fresh.resume(selector)
    assert fresh.take()['groups'] == []


def test_new_event_after_older_read_remains_priority_work():
    clock = [10.0]
    selector = HotRoutes(saved(), config(), clock=lambda:clock[0])
    selector.add([(A,10), (B,10)])
    first = selector.take()
    clock[0] = 10.2
    selector.add([(A,10.2)])
    selector.acknowledge(first)
    assert selector.take()['coactive_priority_groups'] == [(A,B)]


def test_hot_event_reaches_real_executor_and_canonical_local_ledger(market, monkeypatch):
    """Event → hot complete group → quote → EVM preflight → local send → receipt.

    Prices and tokens belong to the synthetic fixture, not mainnet evidence.
    Ordinary delivery is blocked to prove the hot path really supplied the trade.
    """
    import searcher.pipeline as pipeline
    import searcher.activity as activity
    from searcher.execution import Executor
    from searcher.statefeed import StateFrame
    from searcher.pools import Pool
    from searcher.core import number
    from test_activity import log

    rpc, cfg, contract, signer, directory, index, _, head, _ = market
    cfg['policy'].update(hot_lane_enabled=True, activity_priority=True,
                         hot_lane_min_interval_seconds=.2, universe_refresh_seconds=100)
    universe = {'header':head, 'pools':[p.json() for p in index.states.values()]}
    stop = threading.Event(); ordinary_started = threading.Event()

    class Feed:
        def __init__(self, connection, config, saved):
            self.rpc = connection
            self.hot = not config['policy'].get('cycle_pages')
            self.pools = [Pool.from_json(p) for p in saved['pools']]
        def poll(self):
            if not self.hot:
                ordinary_started.set(); stop.wait(8)
            started = time.monotonic_ns(); h = self.rpc.header()
            return StateFrame(h,h,[p.json() for p in self.pools],started,time.monotonic_ns(),
                              time.time(),number(h['number']),number(h['timestamp']))

    def stream(url, chain, pools, done, finish, publish, health, budget, **kwargs):
        assert ordinary_started.wait(2)
        events = [dict(log(pool),logIndex=hex(i)) for i,pool in enumerate(index.states)]
        publish(events, 'pendingLogs')
        done.wait(8)

    monkeypatch.setattr(pipeline, 'StateFeed', Feed)
    monkeypatch.setattr(activity, 'ROOT', directory)
    monkeypatch.setattr(activity, 'websocket_url', lambda:'wss://example.invalid/key')
    monkeypatch.setattr(activity, 'run_stream', stream)
    monkeypatch.setattr('searcher.fastlane.endpoint_url', lambda:None)
    executor = Executor(rpc,cfg,contract,signer,directory)
    submit, reconcile = executor.execute, executor.reconcile
    def mine_after(*args, **kwargs):
        result = submit(*args, **kwargs)
        rpc.request('evm_mine', [])
        return result
    def stop_after_receipt():
        reconcile()
        if executor.db.execute("SELECT 1 FROM attempts WHERE state='success'").fetchone(): stop.set()
    monkeypatch.setattr(executor, 'execute', mine_after)
    monkeypatch.setattr(executor, 'reconcile', stop_after_receipt)
    try:
        result = pipeline.run_pending(rpc,cfg,universe,8,directory/'pipeline',executor,stop)
        rows = executor.db.execute('SELECT state,outcome FROM attempts JOIN intents ON intents.attempt=attempts.id').fetchall()
        assert len(rows) == 1 and rows[0][0] == 'success'
        assert json.loads(rows[0][1])['net_profit_raw'] > 0
        decisions = [json.loads(p.read_text()) for p in (directory/'pipeline').glob('decision-*.json')]
        sent = [d for d in decisions if any(c.get('status') == 'submitted' for c in d['candidates'])]
        assert len(sent) == 1 and sent[0]['lane'] == 'hot'
        assert sent[0]['transport']['coactive_priority_groups']
        assert all(p['directions_considered'] == p['directions_in_page'] for p in sent[0]['coactive_route_progress'])
        assert result['hot_reader_stopped'] and result['reader_stopped']
    finally:
        stop.set(); executor.close()
