import threading
from types import SimpleNamespace
from dataclasses import replace
from eth_abi import encode
from searcher.core import load_config,data
from searcher.morpho_universe import Universe
from searcher.oracle_triggers import OracleTriggers
from test_morpho_universe import make_market


ANCHOR={'number':'0x64','timestamp':'0x3e8','hash':'0x'+'11'*32}

def setup(tmp_path,count=2):
    u=Universe(load_config());markets=[]
    for i in range(count):
        m=replace(make_market(coll='0x'+format(100+i,'040x')),oracle='0x'+format(1000+i,'040x'))
        u.add([m]);markets.append(m)
        u.books[m.id].update([{'borrower':'b','shares':1,'collateral':1,'debt':1,'boundary':100}])
    return u,markets,OracleTriggers(u,load_config(),threading.Event(),tmp_path)


def test_actual_oracle_price_crossing_promotes_existing_queue(tmp_path,monkeypatch):
    import searcher.oracle_triggers as mod
    u,markets,w=setup(tmp_path);answers={m.oracle:110 for m in markets}
    def read(rpc,calls,block,**kwargs):
        assert kwargs['context']['number']=='0x65'
        acquired=[]
        def other():
            with u.lock:acquired.append(True)
        t=threading.Thread(target=other);t.start();t.join(timeout=1);assert acquired
        assert all(c==data('price()') for _,c in calls)
        return [encode(['uint256'],[answers[o]]) for o,c in calls]
    monkeypatch.setattr(mod,'optional_views',read)
    assert w.poll(None,ANCHOR)['changed']==2
    assert w.poll(None,ANCHOR)['changed']==0
    # Even if background identities already have many pending entries, a new
    # liquidation boundary goes to the front rather than waiting behind them.
    for i in range(100):u.trigger(str(i))
    answers[markets[1].oracle]=99;out=w.poll(None,ANCHOR)
    assert out['crossed_markets']==[markets[1].id] and u.urgent[0]==markets[1].id


def test_bad_or_missing_oracle_isolated_and_recovery_detected(tmp_path,monkeypatch):
    import searcher.oracle_triggers as mod
    u,markets,w=setup(tmp_path)
    monkeypatch.setattr(mod,'optional_views',lambda *a,**k:[b'bad',encode(['uint256'],[110])])
    out=w.poll(None,ANCHOR);assert out['read']==1 and out['changed']==1
    monkeypatch.setattr(mod,'optional_views',lambda *a,**k:[None,b'\x00'*32])
    assert w.poll(None,ANCHOR)['read']==0 and len(w.last)==1
    monkeypatch.setattr(mod,'optional_views',lambda *a,**k:[encode(['uint256'],[99])]*2)
    assert w.poll(None,ANCHOR)['changed']==2


def test_page_bound_hot_priority_and_cold_fairness(tmp_path,monkeypatch):
    import searcher.oracle_triggers as mod
    u,markets,w=setup(tmp_path,240);seen=set()
    for m in markets[:10]:w.last[m.oracle]=101
    def read(rpc,calls,block,**kwargs):
        assert kwargs['context']['number']=='0x65'
        assert len(calls)<=96;seen.update(o for o,_ in calls)
        assert all(m.oracle in {o for o,_ in calls} for m in markets[:10])
        return [encode(['uint256'],[101 if o in {m.oracle for m in markets[:10]} else 150]) for o,_ in calls]
    monkeypatch.setattr(mod,'optional_views',read)
    for _ in range(4):w.poll(None,ANCHOR)
    assert seen=={m.oracle for m in markets}
    u.markets={};assert w.poll(None,ANCHOR)['oracles']==0 and not w.last


def test_price_change_shared_oracle_wakes_every_market_once(tmp_path,monkeypatch):
    import searcher.oracle_triggers as mod
    u,markets,w=setup(tmp_path);u.markets[markets[1].id]=replace(markets[1],oracle=markets[0].oracle)
    monkeypatch.setattr(mod,'optional_views',lambda rpc,calls,block,**k:[encode(['uint256'],[99])]*len(calls))
    out=w.poll(None,ANCHOR);assert out['read']==1 and set(out['triggered_markets'])=={m.id for m in markets}
    assert len(u.urgent)==len(u.pending)==2

def test_timeout_reduces_request_work_and_preserves_cold_market_rotation(tmp_path,monkeypatch):
    import searcher.oracle_triggers as mod
    from searcher.core import RPCUnavailable
    u,markets,w=setup(tmp_path,240);seen=set();sizes=[];health=[]
    hot={m.oracle for m in markets[:8]}
    w.last.update({o:101 for o in hot})
    class Stop:
        stopped=False
        def is_set(self):return self.stopped
        def wait(self,_):pass
    w.stop=Stop()
    rpc=SimpleNamespace(verify_chain=lambda:None,header=lambda:ANCHOR,close=lambda:None,
                        measurements=[{'method':'eth_call','network_total_ms':2000}])
    monkeypatch.setattr(mod,'RPC',lambda *a,**kw:rpc)
    def read(rpc,calls,block,**kwargs):
        sizes.append(len(calls))
        if len(sizes)>=50:w.stop.stopped=True
        if len(calls)>24:raise RPCUnavailable('RPC total request deadline exceeded')
        seen.update(o for o,_ in calls)
        return [encode(['uint256'],[101 if o in hot else 150]) for o,_ in calls]
    monkeypatch.setattr(mod,'optional_views',read)
    monkeypatch.setattr(mod,'atomic_json',lambda path,value:health.append(value))
    w.run()
    assert sizes[:3]==[96,48,24]
    assert all(12<=n<=96 for n in sizes)
    assert seen=={m.oracle for m in markets}
    assert any(v['state']=='ready' and v['last_success_at'] for v in health)
    errors=[v for v in health if v['state']=='retrying']
    assert errors[0]['failed_read']['method']=='eth_call'
    assert errors[0]['next_page_limit']==48

def test_adaptive_capacity_has_bounds_and_gradual_recovery(tmp_path):
    _,_,w=setup(tmp_path)
    for _ in range(20):w.record_failure()
    assert w.batch_limit==12
    for _ in range(7):w.record_success()
    assert w.batch_limit==12
    w.record_success();assert w.batch_limit==24
    for _ in range(100):w.record_success()
    assert w.batch_limit==96
