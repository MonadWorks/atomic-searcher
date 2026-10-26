import threading
from types import SimpleNamespace

from searcher.core import load_config
from searcher.structural_runner import BorrowerBook
from searcher.auction_selection import select_positions
from searcher.auction_selection import AuctionScreens
from searcher.auction_planner import packages
from searcher.morpho_universe import Universe
from searcher.morpho_worker import UniverseWorker

LLTV=860000000000000000


def test_twin_auctions_share_one_inflight_screen_and_receive_independent_rows():
    from concurrent.futures import ThreadPoolExecutor
    cache=AuctionScreens();entered=threading.Event();release=threading.Event();calls=[]
    def read():
        calls.append(1);entered.set();assert release.wait(2)
        return {'market':(100,20,[{'borrower':'one','debt':10}])}
    with ThreadPoolExecutor(2) as pool:
        first=pool.submit(cache.read,('same-report','same-head'),read,2)
        assert entered.wait(1)
        second=pool.submit(cache.read,('same-report','same-head'),read,2)
        release.set();a,one=first.result();b,two=second.result()
    assert len(calls)==1 and not one and two
    a['market'][2][0]['debt']=0
    assert b['market'][2][0]['debt']==10


def test_screen_cache_expires_and_never_reuses_a_different_anchor_or_failed_read():
    import pytest
    now=[0];cache=AuctionScreens(clock=lambda:now[0]);calls=[]
    def read():calls.append(1);return {}
    assert cache.read(('a',1),read,1)==({},False)
    assert cache.read(('a',1),read,1)==({},True)
    assert cache.read(('a',2),read,1)==({},False)
    now[0]=.2
    assert cache.read(('a',1),read,1)==({},False)
    def fail():raise ValueError('unavailable')
    with pytest.raises(ValueError):cache.read(('b',1),fail,1)
    assert cache.read(('b',1),read,1)==({},False)
    assert len(calls)==4


def row(i, debt, collateral, shares=None):
    threshold=(debt*10**18+LLTV-1)//LLTV
    return dict(borrower='0x'+format(i,'040x'),debt=debt,shares=debt if shares is None else shares,
                collateral=collateral,boundary=(threshold*10**36+collateral-1)//collateral)


def test_report_selects_large_newly_eligible_position_behind_dust():
    book=BorrowerBook()
    dust=[row(i,90,100) for i in range(1,80)]
    valuable=row(100,880000,1000000)
    healthy=row(101,80000000,100000000)
    book.update([*dust,valuable,healthy])
    assert valuable not in book.proposals(8)
    selected,details=select_positions(book,10**36,[],LLTV)
    assert selected[0]['borrower']==valuable['borrower']
    assert healthy['borrower'] not in {r['borrower'] for r in selected}
    assert details['eligible_hints']==80 and details['known_positions']==81
    assert packages(selected,10**36,2**255-1,LLTV,6)


def test_accrual_can_cross_threshold_and_fresh_share_ratio_handles_position_changes():
    book=BorrowerBook();a=row(1,85,100);b=row(2,8500,10000);book.update([a,b])
    current=row(1,87,100,shares=85)
    selected,_=select_positions(book,10**36,[current],LLTV)
    assert {r['borrower'] for r in selected}=={a['borrower'],b['borrower']}
    # A repayment changes the reference shares and assets together. Use their
    # current ratio, not old/new debt growth.
    changed=row(1,43,100,shares=43)
    selected,_=select_positions(book,10**36,[changed],LLTV)
    assert not selected
    closed={**current,'shares':0,'debt':0}
    selected,_=select_positions(book,10**36,[closed],LLTV)
    assert not selected


def test_first_group_size_variants_cannot_hide_all_later_groups():
    rows=[row(i,9000000-i,10000000) for i in range(1,33)]
    plans=packages(rows,10**36,2**255-1,LLTV,6)
    assert len(plans[0])==len(plans[1])==len(plans[2])==len(plans[3])==8
    assert {a for p in plans[:4] for a,_,_ in p}=={r['borrower'] for r in rows}


def test_valuable_head_retained_while_other_eligible_positions_get_turns():
    book=BorrowerBook();rows=[row(i,90*i,100*i) for i in range(1,90)];book.update(rows)
    seen=set()
    for _ in range(12):
        selected,_=select_positions(book,10**36,[],LLTV)
        assert selected[0]['borrower']==rows[-1]['borrower']
        seen.update(r['borrower'] for r in selected)
    assert seen=={r['borrower'] for r in rows}


def worker(tmp_path):
    cfg=load_config();u=Universe(cfg)
    w=UniverseWorker(cfg,u,threading.Event(),tmp_path,'https://mainnet.base.org')
    return w,u


def test_forwarder_preparation_prioritizes_noticed_feed_outside_pool_queue(tmp_path,monkeypatch):
    w,u=worker(tmp_path);feeds=['0x'+format(i,'040x') for i in range(1,13)]
    u.dependencies={'market':{'aggregators':dict(enumerate(feeds))}}
    u.forwarder_requests.add(feeds[-1]);calls=[]
    def lookup(rpc,selected):
        calls.append(selected);return {f:[{'address':'authorized','bundlers':['bundler']}] for f in selected}
    monkeypatch.setattr('searcher.atlas.forwarders',lookup)
    w.prepare_forwarders(None)
    assert calls[0][0]==feeds[-1] and len(calls[0])==8
    assert feeds[-1] in u.atlas_forwarders and not u.forwarder_requests
    w.prepare_forwarders(None)
    assert len(u.atlas_forwarders)==12


def test_missing_mapping_retries_without_deleting_last_verified_mapping(tmp_path,monkeypatch):
    w,u=worker(tmp_path);feed='0x'+'12'*20;u.forwarder_requests.add(feed)
    u.atlas_forwarders[feed]=[{'address':'old','bundlers':['b']}]
    monkeypatch.setattr('searcher.atlas.forwarders',lambda *args:{})
    w.prepare_forwarders(None)
    assert u.atlas_forwarders[feed][0]['address']=='old' and feed in u.forwarder_requests
    w.forwarder_due[feed]=0
    monkeypatch.setattr('searcher.atlas.forwarders',lambda *args:{feed:[]})
    w.prepare_forwarders(None)
    assert u.atlas_forwarders[feed]==[] and feed not in u.forwarder_requests


def test_oracle_batch_preserves_both_price_sides_and_deduplicates_proxies(monkeypatch):
    from eth_abi import encode
    from searcher.core import ZERO
    from searcher.morpho_universe import oracle_dependencies_batch
    a,b,feed,quote,agg=['0x'+f'{i:02x}'*20 for i in range(1,6)];calls=[]
    def views(rpc,rows,block):
        calls.append(rows)
        if len(calls)==1:
            return [encode(['address'],[x]) for x in [feed,ZERO,quote,ZERO,ZERO,ZERO]*2]
        return [encode(['address'],[agg if address==feed else ZERO]) for address,_ in rows]
    monkeypatch.setattr('searcher.morpho_universe.optional_views',views)
    result=oracle_dependencies_batch(None,[a,b,a])
    assert len(calls[0])==12 and len(calls[1])==2
    assert result[a]['aggregators']=={'BASE_FEED_1()':agg,'QUOTE_FEED_1()':quote}
    assert result[b]['mapping_known'] and result[b]['feeds']==result[a]['feeds']


def test_oracle_mapping_does_not_wait_for_pool_discovery(tmp_path,monkeypatch):
    from test_morpho_universe import make_market
    w,u=worker(tmp_path);markets=[make_market(coll='0x'+format(i,'040x')) for i in range(10,30)];u.add(markets)
    dep={'oracle':markets[0].oracle,'feeds':{'BASE_FEED_1()':'proxy'},'aggregators':{'BASE_FEED_1()':'feed'},'checked_at':1,'mapping_known':True}
    calls=[]
    def discover(rpc,oracles):calls.append(oracles);return {oracle:dep for oracle in oracles}
    monkeypatch.setattr('searcher.morpho_universe.oracle_dependencies_batch',discover)
    w.prepare_dependencies(None)
    assert len(calls)==1 and calls[0]==[markets[0].oracle]
    assert set(u.notice_markets('feed'))=={m.id for m in markets}
    assert not u.graph.pools
    later=make_market(coll='0x'+'51'*20);u.add([later]);w.prepare_dependencies(None)
    assert later.id in u.notice_markets('feed') and len(calls)==1
