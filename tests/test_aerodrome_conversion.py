"""Native venue discovery, refresh fairness, and executable route encoding."""
import threading
import pytest
from eth_abi import encode,decode
from searcher.core import USDC,WETH,ZERO,data,unhex,load_config
from searcher.morpho_universe import Universe,ConversionGraph,Pool
from searcher.morpho_worker import UniverseWorker,AERO_CREATE
from searcher.structural import market_route,clock_call
from searcher.protocol_execution import route,clock_call as protocol_clock
from test_morpho_universe import make_market

AERO='0x420dd381b31aef6683db6b902084cb0ffece40da'
C='0x'+'ee'*20;MID='0x'+'bb'*20;P='0x'+'11'*20

def test_separate_from_ordinary_fixed_fee_engine():
    c=load_config();u=Universe(c)
    assert AERO in u.graph.factories
    assert AERO not in {f['address'] for f in c['factories']}

@pytest.mark.parametrize('stable',[False,True])
def test_factory_lookup_authentication_and_both_variants(monkeypatch,stable):
    import searcher.morpho_universe as mod
    g=Universe(load_config()).graph;g.factories={AERO:g.factories[AERO]}
    def views(rpc,calls,block):
        out=[]
        for addr,raw in calls:
            if addr==AERO:
                assert raw[:10]==data('getPool(address,address,bool)')[:10]
                a,b,s=decode(['address','address','bool'],unhex(raw)[4:]);assert (a,b)==(USDC,C)
                out.append(encode(['address'],[P if s==stable else ZERO]))
            else:
                values={'factory()':AERO,'token0()':USDC,'token1()':C}
                out.append(encode(['address'],[next(v for k,v in values.items() if raw==data(k))]))
        return out
    monkeypatch.setattr(mod,'optional_views',views)
    g.discover_pairs(None,[(C,USDC)])
    assert g.pools[P]==Pool(P,USDC,C,AERO,4,int(stable))
    assert g.paths(C,USDC)==[((P,C,USDC,4),)]

def test_false_membership_does_not_enter_graph(monkeypatch):
    import searcher.morpho_universe as mod
    g=Universe(load_config()).graph
    monkeypatch.setattr(mod,'optional_views',lambda *a:[encode(['address'],[x]) for x in [AERO,USDC,C,ZERO]])
    g.authenticate(None,[Pool(P,USDC,C,AERO,4,0)])
    assert not g.pools

def test_bridge_search_rotates_even_with_existing_path():
    g=Universe(load_config()).graph;f=next(iter(g.factories))
    g.add(Pool(P,USDC,C,f,1,500))
    for i in range(12):
        mid='0x'+format(100+i,'040x');p='0x'+format(1000+i,'040x')
        g.add(Pool(p,mid,C,f,1,500))
    assert g.has_path(C,USDC)
    seen=set()
    for _ in range(20):
        pairs=g.bridge_pairs(C,USDC);assert len(pairs)<=4;seen.update(pairs)
    for i in range(12):assert tuple(sorted(('0x'+format(100+i,'040x'),WETH))) in seen

def test_creation_event_authenticated(tmp_path,monkeypatch):
    c=load_config();u=Universe(c);u.add([make_market(coll=C)])
    w=UniverseWorker(c,u,threading.Event(),tmp_path,'https://mainnet.base.org')
    p=Pool(P,USDC,C,AERO,4,1);calls=[]
    def auth(self,rpc,pools,block='latest'):
        calls.extend(pools)
        for pool in pools:self.add(pool)
    monkeypatch.setattr(ConversionGraph,'authenticate',auth)
    log={'address':AERO,'topics':[AERO_CREATE,'0x'+encode(['address'],[USDC]).hex(),'0x'+encode(['address'],[C]).hex(),'0x'+encode(['bool'],[True]).hex()],
         'data':'0x'+encode(['address','uint256'],[P,42]).hex()}
    w.apply_log(None,log)
    assert calls==[p] and u.graph.pools[P]==p

def test_route_encoding_and_full_width_clock():
    h=[(P,C,USDC,4)]
    a=market_route(make_market(coll=C),[('0x'+'22'*20,100,100)],h)
    b=route(1,['0x'+'33'*20,C,[],h,[]],(C,USDC))
    for r,fn in [(a,clock_call),(b,protocol_clock)]:
        raw=unhex(fn(r,{})[1][0]['data'])
        pools,spacings=decode(['address[]','int24[]'],raw[4:]);assert pools==(P,) and spacings==(-1,)

def test_existing_protocol_path_refreshes_alternate_venues(monkeypatch):
    from searcher.protocol_scanners import Scanners
    c=load_config();u=Universe(c);f=next(iter(u.graph.factories))
    u.graph.add(Pool(P,USDC,C,f,1,500));u.graph.add(Pool('0x'+'12'*20,MID,C,f,1,500))
    calls=[]
    monkeypatch.setattr(ConversionGraph,'discover_pairs',lambda self,rpc,pairs:calls.append((set(self.factories),set(pairs))))
    s=Scanners(None,None,c,u,{},lambda **k:None,threading.Event())
    assert s.paths(C,USDC)
    assert AERO in calls[0][0] and (WETH,MID) in {tuple(sorted(p)) for p in calls[0][1]}
    s.paths(C,USDC);assert len(calls)==1

def test_restore_deduplicates_cache_hints(tmp_path):
    import json
    from dataclasses import asdict
    c=load_config();u=Universe(c);w=UniverseWorker(c,u,threading.Event(),tmp_path,'https://mainnet.base.org')
    p=Pool(P,USDC,C,AERO,4,0);w.cache=tmp_path/'cache.json'
    w.cache.write_text(json.dumps({'chain_id':8453,'markets':[],'pools':[asdict(p),asdict(p)]}))
    w.restore()
    assert list(w.pool_hints)==[p] and w.hints_seen=={P}

def test_production_runtime_fits_mainnet_contract_limits():
    import json
    from searcher.core import ROOT
    for name,file in [('StructuralExecutor','StructuralExecutor.sol'),('ProtocolExecutor','ProtocolExecutor.sol'),('BookExecutor','ProtocolExecutor.sol')]:
        obj=json.loads((ROOT/'out'/file/(name+'.json')).read_text())
        assert len(unhex(obj['deployedBytecode']['object']))<=24576
        assert len(unhex(obj['bytecode']['object']))+4*32<=49152
