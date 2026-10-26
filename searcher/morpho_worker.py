"""Incremental market/borrower preparation and live event intake."""
from collections import deque
from dataclasses import asdict,replace
import json
import threading
import time
import httpx
from eth_abi import decode
from eth_utils import keccak
from .core import ROOT,RPC,USDC,WETH,ZERO,data,number,unhex,atomic_json,safe_error,check
from .morpho_universe import Market,Pool,market_pages,borrower_pages,optional_views,oracle_dependencies,PARAMS,pool_hints,all_position_pages
from .structural import MORPHO,SUSDS

def topic(s):return '0x'+keccak(text=s).hex()
CREATE=topic('CreateMarket(bytes32,(address,address,address,address,uint256))')
BORROW=topic('Borrow(bytes32,address,address,address,uint256,uint256)')
V3_CREATE=topic('PoolCreated(address,address,uint24,int24,address)')
SLIP_CREATE=topic('PoolCreated(address,address,int24,address)')
V2_CREATE=topic('PairCreated(address,address,address,uint256)')
AERO_CREATE=topic('PoolCreated(address,address,bool,address,uint256)')
FEE_ENABLED=topic('FeeAmountEnabled(uint24,int24)')
POSITION_EVENTS={topic(s) for s in (
    'Borrow(bytes32,address,address,address,uint256,uint256)',
    'Repay(bytes32,address,address,uint256,uint256)',
    'SupplyCollateral(bytes32,address,address,uint256)',
    'WithdrawCollateral(bytes32,address,address,address,uint256)',
    'Liquidate(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)')}


class UniverseWorker:
    def __init__(self,config,universe,stop,output,rpc_url):
        self.config,self.universe,self.stop,self.output,self.rpc_url=config,universe,stop,output,rpc_url
        self.cache=ROOT/'evidence/morpho-universe.json'
        self.thread=threading.Thread(target=self.run,name='morpho-preparation',daemon=True)
        self.events_thread=threading.Thread(target=self.events,name='morpho-events',daemon=True)
        self.positions_thread=threading.Thread(target=self.positions,name='morpho-position-index',daemon=True)
        self.atlas_thread=threading.Thread(target=self.refresh_atlas,name='auction-protocol-context',daemon=True)
        from .oracle_triggers import OracleTriggers
        self.oracle_triggers=OracleTriggers(universe,config,stop,output)
        self.position_pages={};self.position_due={};self.prepared_due={};self.pair_due={};self.api_due=0;self.new_ids=set();self.forwarder_due={};self.oracle_due={};self.oracle_batch_size=8;self.pool_hints=deque();self.hints_seen=set();self.seed_due=0
    def start(self):
        self.restore();self.thread.start();self.events_thread.start();self.positions_thread.start();self.atlas_thread.start();self.oracle_triggers.thread.start()
    def refresh_atlas(self):
        if not self.config.get('structural',{}).get('atlas_enabled'):return
        from .atlas import metadata
        rpc=RPC(self.rpc_url,timeout=3,cancel_event=self.stop);verified=False;next_metadata=0
        try:
            while not self.stop.is_set():
                delay=.5
                try:
                    if not verified:rpc.verify_chain();verified=True
                    if time.monotonic()>=next_metadata:
                        current=metadata(rpc)
                        with self.universe.lock:self.universe.atlas_meta=current
                        next_metadata=time.monotonic()+30
                    self.prepare_dependencies(rpc)
                    self.prepare_forwarders(rpc)
                    with self.universe.lock:
                        known=len(self.universe.atlas_forwarders)
                        ready=sum(bool(v) for v in self.universe.atlas_forwarders.values())
                    atomic_json(self.output/'atlas-protocol-health.json',{'at':time.time(),'state':'ready',
                        'forwarders_checked':known,'auction_feeds_ready':ready,'markets_checked':len(self.universe.dependencies)})
                except Exception as exc:
                    delay=3
                    if not self.stop.is_set():atomic_json(self.output/'atlas-protocol-health.json',{'at':time.time(),'state':'retrying','error':safe_error(exc)})
                self.stop.wait(delay)
        finally:rpc.close()
    def prepare_dependencies(self,rpc):
        from .morpho_universe import oracle_dependencies_batch
        u=self.universe;now=time.monotonic()
        with u.lock:
            markets=[m for m in u.markets.values() if m.active]
            by={}
            for m in markets:
                by.setdefault(m.oracle,[]).append(m)
                if m.id not in u.dependencies and m.oracle in u.oracle_cache:
                    u.bind(m.id,u.oracle_cache[m.oracle])
            ordered=sorted((o for o in by if now>=self.oracle_due.get(o,0)),
                key=lambda o:(self.oracle_due.get(o,0),-sum(m.borrow_usd for m in by[o]),o))
            selected=ordered[:self.oracle_batch_size]
        if not selected:return
        try:found=oracle_dependencies_batch(rpc,selected)
        except Exception:
            for oracle in selected:self.oracle_due[oracle]=now+15
            self.oracle_batch_size=1
            raise
        self.oracle_batch_size=min(8,self.oracle_batch_size*2)
        with u.lock:
            for oracle,dependency in found.items():
                u.oracle_cache[oracle]=dependency
                for m in by[oracle]:u.bind(m.id,dependency)
                self.oracle_due[oracle]=now+60
    def prepare_forwarders(self,rpc):
        """Auction metadata cannot wait behind pool/market discovery RPCs."""
        from .atlas import forwarders
        u=self.universe;now=time.monotonic()
        with u.lock:
            feeds={f for dep in u.dependencies.values() for f in dep['aggregators'].values()}
            requested=set(u.forwarder_requests)
        due=[f for f in feeds|requested if now>=self.forwarder_due.get(f,0)]
        due.sort(key=lambda f:(f not in requested,self.forwarder_due.get(f,0),f))
        feeds=due[:8]
        if not feeds:return
        found=forwarders(rpc,feeds)
        with u.lock:
            for feed in feeds:
                if feed in found:
                    u.atlas_forwarders[feed]=found[feed]
                    self.forwarder_due[feed]=now+(60 if found[feed] else 30)
                    u.forwarder_requests.discard(feed)
                else:
                    # A missing read is unknown, not a permanent empty mapping.
                    self.forwarder_due[feed]=now+15
    def restore(self):
        path=self.cache if self.cache.exists() else ROOT/'config/market-bootstrap.json'
        if not path.exists():return
        try:
            cached=json.loads(path.read_text());check(cached['chain_id']==8453,'wrong universe chain')
            markets=[Market(**m) for m in cached['markets']]
            for m in markets:m.validate()
            self.universe.add(markets)
            for id,addresses in cached.get('borrowers',{}).items():
                if id in self.universe.books:self.universe.books[id].add(addresses)
            # Cached hints remain subject to actual contract/factory validation.
            for p in cached.get('pools',[])+cached.get('pending_pools',[]):
                hint=Pool(**p)
                if hint.address not in self.hints_seen:
                    self.hints_seen.add(hint.address);self.pool_hints.append(hint)
            self.universe.event_cursor=cached.get('event_cursor')
            self.universe.event_cursors={a:int(n) for a,n in cached.get('event_cursors',{}).items()}
            self.universe.event_pending=cached.get('event_pending',[])
        except Exception as exc:atomic_json(self.output/'universe-cache-error.json',{'at':time.time(),'error':safe_error(exc)})
    def save(self):
        u=self.universe
        with u.lock:
            payload={'chain_id':8453,'at':time.time(),'markets':[asdict(m) for m in u.markets.values()],
                'borrowers':{id:b.identities() for id,b in u.books.items()},'pools':u.graph.json(),
                'pending_pools':[asdict(p) for p in self.pool_hints if p.address not in u.graph.pools],
                'event_cursor':u.event_cursor,'event_cursors':getattr(u,'event_cursors',{}),
                'event_pending':getattr(u,'event_pending',[])}
        atomic_json(self.cache,payload);atomic_json(self.output/'market-coverage.json',u.coverage())
    def prepare_pool_hints(self,rpc):
        # Four full Multicall pages authenticate 96 pools. The old twelve-pool
        # slice used half-full RPC pages and repeated slow market preparation
        # between every slice, delaying restoration of otherwise known paths.
        hints=[self.pool_hints.popleft() for _ in range(min(96,len(self.pool_hints)))]
        fresh=type(self.universe.graph)(list(self.universe.graph.factories.values()))
        try:fresh.authenticate(rpc,hints)
        except Exception:
            self.pool_hints.extendleft(reversed(hints));raise
        with self.universe.lock:
            for pool in fresh.pools.values():self.universe.graph.add(pool)
        atomic_json(self.output/'pool-restoration-health.json',{'at':time.time(),'state':'preparing' if self.pool_hints else 'ready',
            'checked':len(hints),'accepted':len(fresh.pools),'remaining':len(self.pool_hints)})
    def prepare_market(self,rpc,client,m):
        u=self.universe;now=time.monotonic()
        if now>=self.prepared_due.get(m.id,0):
            # Validate parameters independently of index metadata.
            raw=rpc.call(MORPHO,data('idToMarketParams(bytes32)',['bytes32'],[unhex(m.id)]),'latest')
            params=decode(PARAMS,raw);check(tuple(params)==(m.loan,m.collateral,m.oracle,m.irm,m.lltv),'market chain/index disagreement')
            if not self.config.get('structural',{}).get('atlas_enabled'):
                cached=u.oracle_cache.get(m.oracle)
                if cached is None or time.time()-cached['checked_at']>=60:
                    cached=oracle_dependencies(rpc,m.oracle);u.oracle_cache[m.oracle]=cached
                u.bind(m.id,cached)
            self.prepared_due[m.id]=now+60
        pairs={(m.collateral,m.loan),(m.loan,USDC)}
        for token in (m.collateral,m.loan):
            pairs.update((token,hub) for hub in (WETH,USDC) if token!=hub)
        with u.lock:pairs.update(u.graph.bridge_pairs(m.collateral,m.loan))
        needed=[p for p in pairs if p[0]!=p[1] and now>=self.pair_due.get(tuple(sorted(p)),0)]
        if needed:
            # No network read holds the universe lock used by the hot loop.
            fresh=type(u.graph)(list(u.graph.factories.values()));fresh.discover_pairs(rpc,needed)
            with u.lock:
                for pool in fresh.pools.values():u.graph.add(pool)
            for p in needed:self.pair_due[tuple(sorted(p))]=now+300
        u.preparation.setdefault(m.id,{})['indexed_borrowers']=len(u.books[m.id].snapshot())
        u.preparation.setdefault(m.id,{})['last_error']=None
    def positions(self):
        u=self.universe;unmapped={}
        with httpx.Client(timeout=15) as client:
            while not self.stop.is_set():
                try:
                    for rows,progress in all_position_pages(client):
                        if self.stop.is_set():break
                        for id,borrower,state in rows:unmapped[(id,borrower)]=state
                        for (id,borrower),state in list(unmapped.items()):
                            with u.lock:m=u.markets.get(id);book=u.books.get(id)
                            if m and book:
                                try:book.seed(borrower,state,m.lltv)
                                except (ValueError,TypeError,OverflowError):pass
                                unmapped.pop((id,borrower),None)
                        atomic_json(self.output/'positions-index-health.json',{'at':time.time(),'state':'enumerating',**progress,'unmapped_positions':len(unmapped),'prices_are_execution_evidence':False})
                        self.stop.wait(.5)
                    self.stop.wait(300)
                except Exception as exc:
                    atomic_json(self.output/'positions-index-health.json',{'at':time.time(),'state':'retrying','error':safe_error(exc)})
                    self.stop.wait(15)

    def run(self):
        rpc=RPC(self.rpc_url,timeout=5,cancel_event=self.stop);rpc.minimum_request_interval=max(.25,self.config['policy'].get('discovery_request_interval_seconds',0));catalog=None;queue=deque();last_save=0;next_psm=0;verified=False
        try:
            with httpx.Client(timeout=12) as client:
                while not self.stop.is_set():
                    try:
                        if not verified:rpc.verify_chain();verified=True
                        if time.monotonic()>=next_psm:
                            fresh=type(self.universe.graph)(list(self.universe.graph.factories.values()));fresh.discover_pairs(rpc,[(SUSDS,USDC)])
                            with self.universe.lock:
                                for pool in fresh.pools.values():self.universe.graph.add(pool)
                            next_psm=time.monotonic()+300
                        if catalog is None and time.monotonic()>=self.api_due:
                            catalog=market_pages(client,on_reject=lambda id,reason:self.universe.quarantine.update({id:reason}));self.universe.catalog_complete=False
                        if catalog is not None:
                            try:
                                rows,total=next(catalog);self.universe.add(rows)
                                atomic_json(self.output/'catalog-health.json',{'at':time.time(),'state':'indexing','seen':len(self.universe.markets),'expected':total})
                            except StopIteration:
                                catalog=None;self.api_due=time.monotonic()+300;self.universe.catalog_complete=True
                                atomic_json(self.output/'catalog-health.json',{'at':time.time(),'state':'complete','markets':len(self.universe.markets)})
                        if time.monotonic()>=self.seed_due:
                            path=ROOT/'evidence/universe.json'
                            if path.exists():
                                try:
                                    source=json.loads(path.read_text());assets={t for m in self.universe.markets.values() if m.active for t in (m.loan,m.collateral)}
                                    added=[]
                                    for hint in pool_hints(source.get('known_pools',[]),assets):
                                        if hint.address not in self.hints_seen and hint.factory in self.universe.graph.factories:
                                            self.hints_seen.add(hint.address);added.append(hint)
                                    # Fresh intermediate-token edges must not sit behind a
                                    # full restart's cached-pool authentication backlog.
                                    self.pool_hints.extendleft(reversed(added))
                                except (OSError,ValueError,KeyError):pass
                            self.seed_due=time.monotonic()+300
                        if self.pool_hints:
                            self.prepare_pool_hints(rpc)
                            if self.pool_hints:
                                # Cached conversion paths must be ready before
                                # slow alternate-market venue expansion. Price,
                                # position, auction and execution workers run
                                # independently while these hints are restored.
                                if time.monotonic()-last_save>=10:self.save();last_save=time.monotonic()
                                self.stop.wait(.1)
                                continue
                        if not queue:
                            with self.universe.lock:
                                queue.extend(sorted(self.universe.books,key=lambda k:-self.universe.markets[k].borrow_usd))
                        if self.universe.prepare_pending or queue:
                            id=self.universe.prepare_pending.popleft() if self.universe.prepare_pending else queue.popleft()
                            m=self.universe.markets[id]
                            try:self.prepare_market(rpc,client,m)
                            except Exception as exc:
                                self.universe.preparation.setdefault(id,{})['last_error']=safe_error(exc)
                                self.position_pages.pop(id,None);self.position_due[id]=time.monotonic()+15
                                self.universe.preparation[id]['indexed_borrowers']=0
                        if time.monotonic()-last_save>=10:self.save();last_save=time.monotonic()
                    except Exception as exc:
                        atomic_json(self.output/'catalog-health.json',{'at':time.time(),'state':'retrying','error':safe_error(exc)})
                        catalog=None;self.api_due=time.monotonic()+15
                    self.stop.wait(.1)
        finally:rpc.close()
    def unknown_market(self,rpc,id):
        raw=rpc.call(MORPHO,data('idToMarketParams(bytes32)',['bytes32'],[unhex(id)]),'latest')
        loan,coll,oracle,irm,lltv=decode(PARAMS,raw)
        if loan==ZERO or coll==ZERO or oracle==ZERO or lltv==0:return
        values=optional_views(rpc,[(loan,data('decimals()')),(coll,data('decimals()'))])
        check(all(v and len(v)==32 for v in values),'new market decimals unavailable')
        state=decode(['uint128']*6,rpc.call(MORPHO,data('market(bytes32)',['bytes32'],[unhex(id)]),'latest'))
        m=Market(id,loan,coll,oracle,irm,lltv,decode(['uint8'],values[0])[0],decode(['uint8'],values[1])[0],borrow_assets=state[2])
        m.validate();self.universe.add([m])
    def apply_log(self,rpc,log):
        if log.get('removed'):return
        topics=log.get('topics',[])
        if len(topics)<2:return
        u=self.universe;id=topics[1].lower()
        if log['address'].lower()==MORPHO:
            # Deposits/withdrawals in debt-free markets cannot create a
            # liquidation. Only Borrow activates a missing borrower market.
            # Repeated logs for that market share one identity read per range.
            if id not in u.books:
                if topics[0]!=BORROW:return
                checked=getattr(self,'unknown_checked',set())
                if id in checked:return
                self.unknown_market(rpc,id);checked.add(id);self.unknown_checked=checked
            if id in u.books:
                actors=['0x'+topics[3][-40:].lower()] if topics[0] in POSITION_EVENTS and len(topics)>3 else []
                u.books[id].add(actors);u.trigger(id)
            return
        if log['address'].lower() in u.graph.factories:
            t=topics[0];f=log['address'].lower()
            if t==FEE_ENABLED:
                fee=number(topics[1]);keys=u.graph.factories[f]['keys']
                if fee not in keys:keys.append(fee);self.pair_due.clear()
                return
            if t not in (V3_CREATE,SLIP_CREATE,V2_CREATE,AERO_CREATE) or len(topics)<3:return
            if t!=V2_CREATE and len(topics)<4:return
            a='0x'+topics[1][-40:].lower();b='0x'+topics[2][-40:].lower()
            if t==V3_CREATE:key=number(topics[3]);_,pool=decode(['int24','address'],unhex(log['data']));kind=1
            elif t==SLIP_CREATE:key=decode(['int24'],unhex(topics[3]))[0];pool=decode(['address'],unhex(log['data']))[0];kind=2
            elif t==AERO_CREATE:key=int(decode(['bool'],unhex(topics[3]))[0]);pool,_=decode(['address','uint256'],unhex(log['data']));kind=4
            else:key=0;pool,_=decode(['address','uint256'],unhex(log['data']));kind=3
            if kind in (1,2) and key not in u.graph.factories[f]['keys']:u.graph.factories[f]['keys'].append(key);self.pair_due.clear()
            # A disconnected pool can become an intermediate leg when a later
            # pool joins it to a funding token. Do not discard it based on the
            # token graph at the instant its creation event arrives.
            with u.lock:
                if pool.lower() in u.graph.pools:return
            fresh=type(u.graph)(list(u.graph.factories.values()));fresh.authenticate(rpc,[Pool(pool.lower(),a,b,f,kind,key)])
            with u.lock:
                for p in fresh.pools.values():u.graph.add(p)
        else:
            for market in u.notice_markets(log['address']):u.trigger(market,priority=True)
    def events(self):
        from .event_intake import EventIntake,EventRPC
        rpc=EventRPC(self.config['policy'].get('identity_rpc_url',self.rpc_url),timeout=2,cancel_event=self.stop);u=self.universe
        intake=EventIntake(getattr(u,'event_cursors',{}),u.event_cursor,pending=getattr(u,'event_pending',[]))
        verified=False
        try:
            while not self.stop.is_set():
                try:
                    if not verified:rpc.verify_chain();verified=True
                    with u.lock:groups={'morpho':[MORPHO],'factories':list(u.graph.factories),'oracles':list(u.feed_markets)}
                    self.unknown_checked=set()
                    result=intake.poll(rpc,groups,self.apply_log,(V3_CREATE,SLIP_CREATE,V2_CREATE,AERO_CREATE,FEE_ENABLED))
                    with u.lock:
                        u.event_cursors=dict(intake.cursors)
                        u.event_pending=[dict(row) for row in intake.pending.values()]
                        u.event_cursor=min(intake.cursors.values(),default=result['head']+1)
                    atomic_json(self.output/'identity-health.json',result)
                except Exception as exc:
                    if self.stop.is_set():break
                    atomic_json(self.output/'identity-health.json',{'at':time.time(),'state':'retrying','error':safe_error(exc)})
                self.stop.wait(2)
        finally:rpc.close()
