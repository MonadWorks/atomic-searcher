import json
from dataclasses import replace
from types import SimpleNamespace
import pytest
from eth_abi import encode,decode
from eth_utils import keccak
from searcher.core import USDC,WETH,ZERO,Reject,load_config
from searcher.morpho_universe import Market,Universe,ConversionGraph,Pool,market_pages,borrower_pages,PARAMS
from searcher.structural import market_route,GENERIC,encode_plan
from searcher.structural_runner import liquidation_batches,candidate_for

def make_market(loan=USDC,coll='0x'+'ee'*20,lltv=860000000000000000,borrow=100000000):
    p=[loan,coll,'0x'+'aa'*20,'0x'+'bb'*20,lltv]
    return Market('0x'+keccak(encode(PARAMS,p)).hex(),*p,6,8,'L','C',borrow,100)

def api(m):
    return dict(marketId=m.id,lltv=str(m.lltv),irmAddress=m.irm,oracle={'address':m.oracle},
        loanAsset={'address':m.loan,'decimals':m.loan_decimals},collateralAsset={'address':m.collateral,'decimals':m.collateral_decimals},
        state={'borrowAssets':str(m.borrow_assets),'borrowAssetsUsd':m.borrow_usd})

class Client:
    def __init__(self,pages):self.pages=iter(pages);self.queries=[]
    def post(self,url,json):
        self.queries.append(json['query']);page=next(self.pages)
        return SimpleNamespace(raise_for_status=lambda:None,json=lambda:page)

def test_all_markets_paginated_including_new_unlisted_and_nonusdc():
    markets=sorted([make_market(),make_market(WETH),make_market(coll='0x'+'dd'*20)],key=lambda m:m.id)
    c=Client([{'data':{'markets':{'items':[api(m)],'pageInfo':{'countTotal':3}}}} for m in markets])
    assert [m.id for page,_ in market_pages(c,page_size=1) for m in page]==[m.id for m in markets]
    assert all('listed:' not in q and 'borrowShares_gte' not in q for q in c.queries)

def test_market_metadata_cannot_relabel_tokens():
    row=api(make_market());row['loanAsset']['address']=WETH
    with pytest.raises(Reject,match='hash mismatch'):Market.indexed(row)

def test_changed_catalog_never_claims_complete():
    m=make_market();c=Client([{'data':{'markets':{'items':[api(m)],'pageInfo':{'countTotal':2}}}},
                            {'data':{'markets':{'items':[],'pageInfo':{'countTotal':1}}}}])
    it=market_pages(c);next(it)
    with pytest.raises(Reject,match='membership'):next(it)

def test_no_health_factor_cutoff_and_empty_positions_complete():
    c=Client([{'data':{'marketPositions':{'items':[],'pageInfo':{'countTotal':0}}}}])
    assert list(borrower_pages(c,make_market().id))==[([],0)]
    assert 'healthFactor_lte' not in c.queries[0]

def test_feed_maps_every_market_and_upgrade_removes_old_aggregator():
    u=Universe(load_config());a=make_market();b=make_market(WETH);u.add([a,b])
    dep={'feeds':{'QUOTE_FEED_1()':'feed'},'aggregators':{'QUOTE_FEED_1()':'old'}}
    u.bind(a.id,dep);u.bind(b.id,dep)
    assert u.notice_markets('old')==sorted([a.id,b.id])
    u.bind(a.id,{'feeds':{'QUOTE_FEED_1()':'feed'},'aggregators':{'QUOTE_FEED_1()':'new'}})
    assert u.notice_markets('old')==[b.id] and u.notice_markets('new')==[a.id]

def test_hot_events_do_not_starve_clock_only_markets():
    u=Universe(load_config());markets=[make_market(coll='0x'+format(i,'040x')) for i in range(1,20)];u.add(markets)
    u.urgent.clear();u.pending.clear();seen=set()
    for _ in range(100):
        u.trigger(markets[0].id);m,_=u.choose();seen.add(m.id)
    assert seen=={m.id for m in markets}

def test_discovered_paths_handle_both_directions_and_new_intermediates():
    cfg=load_config();g=ConversionGraph(cfg['factories']);f=cfg['factories'][0]['address'];a='0x'+'ee'*20
    g.add(Pool('0x'+'11'*20,USDC,a,f,1,500));g.add(Pool('0x'+'22'*20,WETH,a,f,1,100));g.add(Pool('0x'+'33'*20,WETH,USDC,f,1,500))
    paths=g.paths(a,USDC);assert len(paths)==2
    assert paths[0][0][1:3]==(a,USDC)
    assert len(g.paths(USDC,a))==2
    assert g.paths(a,a)==[()]

def test_route_rotation_keeps_alternate_pool_choices():
    cfg=load_config();u=Universe(cfg);m=make_market();u.add([m]);f=cfg['factories'][0]['address']
    for i in range(12):u.graph.add(Pool('0x'+format(i+10,'040x'),USDC,m.collateral,f,1,100+i))
    seen={a[0][0] for _ in range(3) for a,b in u.routes(m,4)}
    assert len(seen)==12

def test_market_specific_lltv_and_precision_changes_partial_repayment():
    row=dict(borrower='0x'+'11'*20,shares=100*10**12,debt=100*10**6,collateral=10**18,boundary=110*10**24)
    a=liquidation_batches([row],100*10**24,10**15,lltv=625*10**15)
    b=liquidation_batches([row],100*10**24,10**15,lltv=86*10**16)
    assert a[0][0][1]<b[0][0][1]

def test_generic_route_has_no_historical_market_or_token_requirement():
    m=make_market();h=[('0x'+'11'*20,m.collateral,USDC,1)];route=market_route(m,[('0x'+'22'*20,10,12)],h)
    assert route.strategy==3 and decode(GENERIC,route.arguments)[0].hex()==m.id[2:]
    with pytest.raises(Reject):market_route(m,[('0x'+'22'*20,10,12)],[('0x'+'11'*20,USDC,m.collateral,1)])

def test_debt_free_market_events_cannot_block_live_event_intake(tmp_path,monkeypatch):
    import threading
    from searcher.morpho_worker import UniverseWorker,BORROW,topic
    from searcher.structural import MORPHO
    cfg=load_config();u=Universe(cfg);w=UniverseWorker(cfg,u,threading.Event(),tmp_path,'https://mainnet.base.org')
    reads=[];monkeypatch.setattr(w,'unknown_market',lambda rpc,id:reads.append(id))
    m=make_market();base={'address':MORPHO,'topics':[topic('Supply(bytes32,address,address,uint256,uint256)'),m.id]}
    for _ in range(100):w.apply_log(None,base)
    assert reads==[]
    base['topics'][0]=BORROW
    for _ in range(100):w.apply_log(None,base)
    assert reads==[m.id]

def test_empty_or_unroutable_markets_do_not_consume_the_hot_scan_budget():
    cfg=load_config();u=Universe(cfg);a=make_market();others=[make_market(coll='0x'+format(i,'040x')) for i in range(1,30)]
    u.add([a,*others]);u.books[a.id].seed('0x'+'11'*20,{'borrowShares':100,'borrowAssets':100,'collateral':100},a.lltv)
    f=cfg['factories'][0]['address'];u.graph.add(Pool('0x'+'22'*20,USDC,a.collateral,f,1,500))
    selected=[u.choose()[0].id for _ in range(120)]
    assert selected.count(a.id)>=90 and set(selected)==set(u.books)
