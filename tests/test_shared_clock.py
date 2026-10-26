"""Shared heads reduce redundant full reads; all acceptance guards stay active."""
import copy
import threading
import time

import pytest

from searcher.context import SharedClockHints
from searcher.core import Reject
from searcher.statefeed import StateFeed
from test_pipeline import ClockRPC, clock_config, saved, header, H0, H1, H2, TX, ZERO

URL = 'https://example.invalid/private-key-a'
OTHER = 'https://example.invalid/private-key-b'


def pair(n=2, h=H1, parent=H0):
    return header(n, h, parent), header(n+1, ZERO, h)


class CacheRPC(ClockRPC):
    def __init__(self, heads, cache, url=URL, chain_id=8453, **kwargs):
        super().__init__(heads, **kwargs)
        self.url, self.chain_id, self.clock_hints = url, chain_id, cache


def publish(cache, heads, requested=None):
    received = cache.clock()
    return cache.publish(URL, 8453, *heads, requested_ns=received-1 if requested is None else requested,
                         received_ns=received)


def test_peer_fresh_clock_avoids_second_complete_read_of_worker_local_old_clock():
    # Baseline worker retains block 2; block 3 has since arrived at the node.
    old = ClockRPC([header(2,H1,H0), header(3,H2,H1), header(3,H2,H1)])
    baseline = StateFeed(old, clock_config(), saved())
    baseline.poll()
    before = len(old.calls)
    result = baseline.poll()
    assert len(old.calls)-before == 2 and result.context_block == 4

    cache = SharedClockHints()
    rpc = CacheRPC([header(2,H1,H0), header(3,H2,H1)], cache)
    feed = StateFeed(rpc, clock_config(), saved())
    feed.poll()
    # A second reader has already received block 3 and a valid matching pending
    # head. Its own first snapshot may still have used its previous clock.
    peer_rpc = CacheRPC([header(3,H2,H1), header(3,H2,H1)], cache)
    peer = StateFeed(peer_rpc, clock_config(), saved())
    peer.poll()
    before = len(rpc.calls)
    result = feed.poll()
    assert len(rpc.calls)-before == 1
    assert result.transport['shared_clock_used']
    assert result.transport['shared_clock_hint']['anchor_block'] == 3
    assert 0 <= result.transport['shared_clock_hint']['age_ms'] <= 1500
    assert result.transport['clock_attempts'] == 1
    assert result.transport['clock_bootstraps'] == 0
    assert result.context_block == 4 and result.context_time == 108
    assert len(result.snapshot_pools) == len(saved()['pools'])
    assert rpc.calls[-1][0][1][3]['blockHash'] == {'3':H2}


@pytest.mark.parametrize('url,chain', [(OTHER,8453),(URL,1)])
def test_endpoint_and_chain_are_isolated(url, chain):
    cache = SharedClockHints()
    assert publish(cache,pair(3,H2,H1))
    assert cache.get(url,chain) is None
    rpc = CacheRPC([header(2,H1,H0)], cache, url, chain)
    result = StateFeed(rpc,clock_config(),saved()).poll()
    assert not result.transport['shared_clock_used']
    assert result.transport['clock_bootstraps'] == 1
    assert result.context_block == 3


def test_bounded_ttl_immutable_headers_and_old_response_cannot_renew_expiry():
    now = [100_000_000_000]
    cache = SharedClockHints(clock=lambda:now[0],capacity=2)
    assert publish(cache,pair(),now[0]-100)
    hint = cache.get(URL,8453)
    hint['anchor']['hash'] = TX
    assert cache.get(URL,8453)['anchor']['hash'] == H1
    now[0] += 100_000_000
    assert not publish(cache,pair(),hint['requested_ns']-1)
    assert cache.get(URL,8453)['received_ns'] == 100_000_000_000
    now[0] += 1_400_000_001
    assert cache.get(URL,8453) is None
    # Memory remains bounded without storing URLs or unrelated header payloads.
    for i in range(4):
        assert cache.publish(str(i),8453,*pair(),requested_ns=now[0]-1,received_ns=now[0])
    assert len(cache.entries)==2
    assert all('private-key' not in str(key) for key in cache.entries)


def test_late_response_lower_height_and_pending_clock_regression_are_ignored():
    now=[100_000_000_000];cache=SharedClockHints(clock=lambda:now[0])
    assert publish(cache,pair(3,H2,H1),now[0]-100)
    now[0]+=100
    assert not publish(cache,pair(),now[0]-1)  # Later request, lagging node.
    assert not publish(cache,pair(3,TX,H1),now[0]-300)  # Old reorg branch late response.
    assert cache.get(URL,8453)['anchor']['hash']==H2
    assert publish(cache,pair(3,TX,H1),now[0]-1)  # Genuine later same-height reorg.
    anchor,pending=pair(3,TX,H1);pending['number']='0x5';pending['timestamp']='0x6e'
    now[0]+=100
    assert publish(cache,(anchor,pending),now[0]-1)
    now[0]+=100
    assert not publish(cache,pair(3,TX,H1),now[0]-1)


def test_discard_is_generation_specific_and_closed_or_stopped_cache_is_empty():
    now=[100_000_000_000];stop=threading.Event();cache=SharedClockHints(clock=lambda:now[0],stop=stop)
    assert publish(cache,pair())
    old=cache.get(URL,8453);now[0]+=100
    assert publish(cache,pair(3,H2,H1))
    cache.discard(URL,8453,old)
    assert cache.get(URL,8453)['anchor']['hash']==H2
    stop.set()
    assert cache.get(URL,8453) is None and not publish(cache,pair())
    cache.close()
    assert not cache.entries
    stop.clear()
    assert cache.get(URL,8453) is None and not publish(cache,pair())


@pytest.mark.parametrize('mutation',['parent','hash','window','missing'])
def test_invalid_heads_never_enter_cache(mutation):
    cache=SharedClockHints();anchor,pending=pair()
    if mutation=='parent':pending['parentHash']=H2
    elif mutation=='hash':anchor['hash']=ZERO
    elif mutation=='window':pending['number']='0x7'
    else:pending.pop('timestamp')
    assert not publish(cache,(anchor,pending))
    assert cache.get(URL,8453) is None


def test_borrowed_reorg_hint_still_requires_matching_atomic_read_then_recovers():
    cache=SharedClockHints();assert publish(cache,pair())
    changed=header(2,H2,H0)
    rpc=CacheRPC([changed,changed],cache)
    frame=StateFeed(rpc,clock_config(),saved()).poll()
    assert frame.transport['shared_clock_used'] and len(rpc.calls)==2
    assert frame.header['hash']==H2
    assert cache.get(URL,8453)['anchor']['hash']==H2


def test_shared_clock_cannot_override_lens_clock_mismatch_or_retry_limit():
    cache=SharedClockHints();assert publish(cache,pair())
    ignored=StateFeed(CacheRPC([header(2,H1,H0)],cache,ignore_override=True),clock_config(),saved())
    with pytest.raises(Reject,match='clock mismatch'):ignored.poll()
    jumping=StateFeed(CacheRPC([header(3,H2,H1),header(4,TX,H2)],cache),clock_config(),saved())
    with pytest.raises(Reject,match='anchor changed'):jumping.poll()


def test_invalid_returned_heads_clear_borrowed_hint_and_next_poll_bootstraps():
    cache=SharedClockHints();assert publish(cache,pair())
    class BadParent(CacheRPC):
        broken=True
        def batch(self,calls):
            values=super().batch(calls)
            if self.broken and calls[0][0]=='eth_call':
                values=list(values);values[2]=dict(values[2],parentHash=TX)
            return values
    rpc=BadParent([header(2,H1,H0)]*3,cache)
    feed=StateFeed(rpc,clock_config(),saved())
    with pytest.raises(Reject,match='pending parent differs'):feed.poll()
    assert cache.get(URL,8453) is None
    rpc.broken=False
    recovered=feed.poll()
    assert recovered.transport['clock_bootstraps']==1


def test_multithread_old_inflight_reply_cannot_replace_newer_clock():
    cache=SharedClockHints();old_started=time.monotonic_ns();ready=threading.Event();release=threading.Event();out=[]
    def old():
        ready.set();assert release.wait(1)
        out.append(cache.publish(URL,8453,*pair(),requested_ns=old_started,received_ns=time.monotonic_ns()))
    thread=threading.Thread(target=old);thread.start()
    try:
        assert ready.wait(1)
        assert publish(cache,pair(3,H2,H1))
    finally:release.set();thread.join(1)
    assert out==[False] and cache.get(URL,8453)['anchor']['hash']==H2


def test_run_lifecycle_wires_only_state_readers_and_does_not_reuse_cache(tmp_path, monkeypatch):
    monkeypatch.delenv('BASE_DISCOVERY_RPC_URL',raising=False)
    import searcher.pipeline as pipeline
    import searcher.fastlane as fastlane
    import searcher.activity as activity
    from searcher.statefeed import StateFrame
    from test_fastlane import config
    from test_runtime import FakeRPC

    connections, runs = [], []
    class TrackingRPC(FakeRPC):
        def __init__(self,*args,**kwargs):
            super().__init__(*args,**kwargs)
            self.created_on=threading.current_thread().name;connections.append(self)
            if self.created_on=='v4-canonical-discovery':discovery_connected.set()
        def request(self,*args):return []
    class Feed:
        pages=None
        def __init__(self,rpc,*args):self.rpc=rpc
        def poll(self,*args,**kwargs):
            assert discovery_connected.wait(2) and hot_connected.wait(2)
            run_stop.set()
            now=time.monotonic_ns()
            return StateFrame(header(2,H1,H0),header(2,H1,H0),saved()['pools'],now,now,time.time(),3,106)
    class Strategy:
        def __init__(self,*args):self.execution_feedback=object()
        def on_frame(self,frame):return {'block':2,'decision_age_ms':0,'candidates':[]}
    def hot_reader(rpc_factory,fallback_factory,stop,**kwargs):
        clients=[rpc_factory(),rpc_factory(),fallback_factory()]
        runs.append(clients)
        hot_connected.set();stop.wait(3)
        for client in clients:client.close()
    monkeypatch.setattr(pipeline,'RPC',TrackingRPC)
    monkeypatch.setattr(pipeline,'StateFeed',Feed)
    monkeypatch.setattr(pipeline,'PendingStrategy',Strategy)
    monkeypatch.setattr(fastlane,'run_hot_reader',hot_reader)
    monkeypatch.setattr(fastlane,'endpoint_url',lambda:OTHER)
    monkeypatch.setattr(activity,'websocket_url',lambda:None)
    for n in range(2):
        discovery_connected=threading.Event();hot_connected=threading.Event();run_stop=threading.Event()
        primary=TrackingRPC(URL);c=config()
        c['policy']['discovery_rpc_url']='https://discovery.example.invalid/'
        before=len(connections)
        result=pipeline.run_pending(primary,c,saved(),3,tmp_path/str(n),stop_event=run_stop)
        assert result['reader_stopped'] and result['hot_reader_stopped'] and result['v4_stopped']
        assert not hasattr(primary,'clock_hints')
        cache=runs[-1][0].clock_hints
        assert all(client.clock_hints is cache for client in runs[-1])
        ordinary=[client for client in connections if client not in runs[-1] and
                  getattr(client,'clock_hints',None) is cache]
        assert len(ordinary)==2 and {client.url for client in ordinary}=={URL,OTHER}
        discovery=[client for client in connections[before:]
                   if client.created_on=='v4-canonical-discovery']
        assert len(discovery)==1 and not hasattr(discovery[0],'clock_hints')
        assert discovery[0].url==c['policy']['discovery_rpc_url']
        assert discovery[0].minimum_request_interval>=1.0
        assert cache.closed and not cache.entries
        assert not publish(cache,pair())
    assert runs[0][0].clock_hints is not runs[1][0].clock_hints


def test_expired_higher_height_does_not_prevent_fresh_lower_reorg_recovery():
    now=[100_000_000_000];cache=SharedClockHints(clock=lambda:now[0])
    assert publish(cache,pair(3,H2,H1))
    now[0]+=2_000_000_000
    assert publish(cache,pair())
    assert cache.get(URL,8453)['anchor']['hash']==H1


def test_new_publication_while_caller_enters_lock_is_not_mistaken_for_future_hint():
    now=[100_000_000_000];cache=SharedClockHints(clock=lambda:now[0])
    assert publish(cache,pair())
    original=cache.lock
    class Interleave:
        def __enter__(self):
            # Represent a newer request publishing immediately before this
            # waiting caller acquires the lock. Expiry must use the lock-time
            # clock, not the caller's earlier scheduling-time observation.
            now[0]+=100
            cache.entries[cache._key(URL,8453)]=dict(anchor=pair(3,H2,H1)[0],pending=pair(3,H2,H1)[1],
                requested_ns=now[0]-1,received_ns=now[0])
            original.acquire()
        def __exit__(self,*args):original.release()
    cache.lock=Interleave()
    assert cache.get(URL,8453)['anchor']['hash']==H2
    before=now[0]
    assert not cache.publish(URL,8453,*pair(),requested_ns=before-100,received_ns=before)
    cache.lock=original
    assert cache.get(URL,8453)['anchor']['hash']==H2


def test_expiry_retains_bounded_watermark_against_late_old_request():
    now=[100_000_000_000];cache=SharedClockHints(clock=lambda:now[0])
    old_request=now[0]-1000
    assert publish(cache,pair(3,H2,H1),now[0]-1)
    now[0]+=2_000_000_000
    assert cache.get(URL,8453) is None
    assert not publish(cache,pair(),old_request)
    assert cache.get(URL,8453) is None
    # A genuinely later query can rediscover the canonical head after a reorg.
    assert publish(cache,pair(),now[0]-1)
    assert cache.get(URL,8453)['anchor']['hash']==H1


def test_invalidating_hint_preserves_watermark_but_allows_new_query_to_recover():
    now=[100_000_000_000];cache=SharedClockHints(clock=lambda:now[0])
    old_request=now[0]-100
    assert publish(cache,pair(3,H2,H1))
    cache.discard(URL,8453,cache.get(URL,8453))
    now[0]+=100
    assert cache.get(URL,8453) is None
    assert not publish(cache,pair(),old_request)
    assert publish(cache,pair(),now[0]-1)
    assert cache.get(URL,8453)['anchor']['hash']==H1
