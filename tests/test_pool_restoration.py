import threading
import json
from types import SimpleNamespace
from collections import deque
import pytest
from searcher.core import USDC,load_config
from searcher.morpho_universe import Universe,Pool,ConversionGraph
from searcher.morpho_worker import UniverseWorker

def worker(tmp_path):
    c=load_config();u=Universe(c);w=UniverseWorker(c,u,threading.Event(),tmp_path,'https://mainnet.base.org')
    f=c['factories'][0]['address'];coll='0x'+'ee'*20
    w.pool_hints=deque(Pool('0x'+format(i+100,'040x'),USDC,coll,f,1,i+100) for i in range(200))
    return u,w

def test_restore_uses_full_bounded_pages_outside_hot_lock(tmp_path,monkeypatch):
    u,w=worker(tmp_path);counts=[]
    def authenticate(self,rpc,hints):
        counts.append(len(hints));acquired=[]
        def other():
            with u.lock:acquired.append(True)
        t=threading.Thread(target=other);t.start();t.join(timeout=1);assert acquired
        for p in hints:self.add(p)
    monkeypatch.setattr(ConversionGraph,'authenticate',authenticate)
    for _ in range(3):w.prepare_pool_hints(None)
    assert counts==[96,96,8] and len(u.graph.pools)==200 and not w.pool_hints

def test_transient_failure_preserves_every_hint_and_order(tmp_path,monkeypatch):
    u,w=worker(tmp_path);before=list(w.pool_hints)
    def fail(self,rpc,hints):
        self.add(hints[0]);raise RuntimeError('network')
    monkeypatch.setattr(ConversionGraph,'authenticate',fail)
    with pytest.raises(RuntimeError,match='network'):w.prepare_pool_hints(None)
    assert list(w.pool_hints)==before and not u.graph.pools

def test_restart_keeps_unverified_hints_without_trusting_them(tmp_path,monkeypatch):
    u,w=worker(tmp_path);w.cache=tmp_path/'universe.json';before=list(w.pool_hints)
    def authenticate(self,rpc,hints):
        for p in hints:self.add(p)
    monkeypatch.setattr(ConversionGraph,'authenticate',authenticate)
    w.prepare_pool_hints(None);w.save()
    saved=json.loads(w.cache.read_text())
    assert len(saved['pools'])==96 and len(saved['pending_pools'])==104
    fresh=Universe(w.config)
    resumed=UniverseWorker(w.config,fresh,threading.Event(),tmp_path,'https://mainnet.base.org')
    resumed.cache=w.cache;resumed.restore()
    assert list(resumed.pool_hints)==before
    assert not fresh.graph.pools
    for _ in range(3):resumed.prepare_pool_hints(None)
    assert len(fresh.graph.pools)==200 and not resumed.pool_hints

def test_restored_pending_hints_deduplicate_against_validated_cache(tmp_path):
    u,w=worker(tmp_path);w.cache=tmp_path/'universe.json';p=w.pool_hints[0]
    u.graph.add(p);w.save();saved=json.loads(w.cache.read_text())
    assert len(saved['pools'])==1 and len(saved['pending_pools'])==199
    saved['pending_pools'].insert(0,saved['pools'][0])
    w.cache.write_text(json.dumps(saved))
    resumed=UniverseWorker(w.config,Universe(w.config),threading.Event(),tmp_path,'https://mainnet.base.org')
    resumed.cache=w.cache;resumed.restore()
    assert len(resumed.pool_hints)==200

def test_startup_restores_known_paths_before_slow_venue_expansion(tmp_path,monkeypatch):
    import searcher.morpho_worker as module
    u,w=worker(tmp_path);events=[]
    u.books['market']=object();u.markets['market']=SimpleNamespace(id='market',borrow_usd=1)
    def authenticate(self,rpc,hints):
        events.append(('restore',len(hints)))
        for p in hints:self.add(p)
    def prepare(rpc,client,m):
        events.append(('expand',len(u.graph.pools)));w.stop.set()
    monkeypatch.setattr(module,'ROOT',tmp_path)
    monkeypatch.setattr(module,'RPC',lambda *a,**kw:SimpleNamespace(verify_chain=lambda:None,close=lambda:None))
    monkeypatch.setattr(module,'market_pages',lambda *a,**kw:iter([]))
    monkeypatch.setattr(ConversionGraph,'authenticate',authenticate)
    monkeypatch.setattr(ConversionGraph,'discover_pairs',lambda *a,**kw:None)
    monkeypatch.setattr(w,'save',lambda:None)
    monkeypatch.setattr(w,'prepare_market',prepare)
    # A failed scheduling assertion must not leave the test in the worker loop.
    timer=threading.Timer(3,w.stop.set);timer.start()
    try:w.run()
    finally:timer.cancel()
    assert events==[('restore',96),('restore',96),('restore',8),('expand',200)]
