"""Protocol-wide identities, authenticated conversion graph and oracle dependencies.

Index data seeds identities only. Contract parameters, positions and execution
are read on chain. Unknown routes/oracles stay explicit, with fair retry.
"""
from collections import defaultdict, deque
from dataclasses import dataclass, asdict
import json
import threading
import time
from eth_abi import decode, encode
from eth_utils import keccak
from .core import USDC,WETH,ZERO,MULTICALL,MULTICALL_HASH,check,data,number,unhex,address
from .structural import MORPHO

API='https://api.morpho.org/graphql'
FIELDS='marketId lltv irmAddress oracle{address} loanAsset{address symbol decimals} collateralAsset{address symbol decimals} state{borrowAssets borrowShares borrowAssetsUsd}'
PARAMS=['address','address','address','address','uint256']

def query(client,q):
    response=client.post(API,json={'query':q});response.raise_for_status();payload=response.json()
    check(not payload.get('errors'),'Morpho index rejected query: '+str(payload.get('errors'))[:180])
    return payload['data']

@dataclass(frozen=True)
class Market:
    id:str
    loan:str
    collateral:str
    oracle:str
    irm:str
    lltv:int
    loan_decimals:int=18
    collateral_decimals:int=18
    loan_symbol:str=''
    collateral_symbol:str=''
    borrow_assets:int=0
    borrow_usd:float=0
    @classmethod
    def indexed(cls,row):
        loan=row['loanAsset'];coll=row.get('collateralAsset');oracle=row.get('oracle');state=row.get('state') or {}
        # Idle markets have no collateral. They are catalogued but never borrowers.
        m=cls(row['marketId'].lower(),loan['address'].lower(),coll['address'].lower() if coll else ZERO,
            address(oracle['address']) if oracle and oracle['address'].lower()!=ZERO else ZERO,
            row['irmAddress'].lower(),int(row['lltv']),int(loan['decimals']),int(coll['decimals']) if coll else 18,
            loan.get('symbol',''),coll.get('symbol','') if coll else '',int(state.get('borrowAssets') or 0),float(state.get('borrowAssetsUsd') or 0))
        m.validate();return m
    def validate(self):
        check(0<=self.lltv<10**18 and 0<=self.loan_decimals<=255 and 0<=self.collateral_decimals<=255,'invalid market units')
        computed='0x'+keccak(encode(PARAMS,[self.loan,self.collateral,self.oracle,self.irm,self.lltv])).hex()
        check(self.id==computed,'market parameter hash mismatch')
    @property
    def active(self):return self.borrow_assets>0 and self.loan!=ZERO and self.collateral!=ZERO and self.oracle!=ZERO and self.lltv>0


def market_pages(client,*,active_only=False,page_size=100,on_reject=None):
    """Stable identity order, no market allowlist or top-N truncation."""
    skip=0;total=None;seen=set();last=''
    while True:
        where='chainId_in:[8453]'+(',borrowShares_gte:1' if active_only else '')
        page=query(client,'{markets(first:%d,skip:%d,orderBy:UniqueKey,orderDirection:Asc,where:{%s}){items{%s} pageInfo{countTotal}}}'%(page_size,skip,where,FIELDS))['markets']
        count=int(page['pageInfo']['countTotal'])
        if total is None:total=count
        check(count==total,'market membership changed during pagination')
        items=page['items'];result=[]
        for row in items:
            identity=row['marketId'].lower()
            check(identity not in seen and identity>last,'duplicate or unordered market page')
            seen.add(identity);last=identity
            try:result.append(Market.indexed(row))
            except Exception as exc:
                if on_reject is None:raise
                on_reject(identity,str(exc))
        yield result,total
        skip+=len(items)
        check(skip<=total,'invalid market total')
        if skip==total:return
        check(items,'incomplete market pagination')


def borrower_pages(client,market_id,page_size=100):
    """All borrowing positions, including currently healthy ones; incremental pages."""
    skip=0;total=None;seen=set()
    while True:
        page=query(client,'{marketPositions(first:%d,skip:%d,orderBy:HealthFactor,orderDirection:Asc,where:{chainId_in:[8453],marketUniqueKey_in:["%s"],borrowShares_gte:1}){items{user{address}} pageInfo{countTotal}}}'%(page_size,skip,market_id))['marketPositions']
        count=int(page['pageInfo']['countTotal'])
        if total is None:total=count
        check(count==total,'borrower membership changed during pagination')
        rows=[address(r['user']['address']) for r in page['items']]
        check(not seen.intersection(rows) and len(rows)==len(set(rows)),'duplicate borrower page')
        seen.update(rows);skip+=len(rows);yield rows,total
        check(skip<=total,'invalid borrower total')
        if skip==total:return
        check(rows,'incomplete borrower pagination')


def all_position_pages(client,page_size=1000):
    """Keyset enumeration beyond the API's 10,000-offset ceiling.

    A hot page seeds imminent positions first. Borrow shares then provide a
    bounded cursor for the whole index. Large ties partition by collateral,
    then market. This is a live index scan, never an atomic chain snapshot.
    Every executable position is subsequently inspected on chain.
    """
    check(0<page_size<=1000,'invalid position page size')
    fields='user{address} market{marketId} state{borrowShares borrowAssets collateral}'
    base={'chainId_in':'[8453]','borrowShares_gte':'"1"'}
    def fetch(filters,order,skip):
        check(skip<=10000,'position index offset bound')
        where=','.join(k+':'+v for k,v in filters.items())
        return query(client,'{marketPositions(first:%d,skip:%d,orderBy:%s,orderDirection:Asc,where:{%s}){items{%s} pageInfo{countTotal}}}'%(page_size,skip,order,where,fields))['marketPositions']
    hot=fetch(base,'HealthFactor',0);initial=int(hot['pageInfo']['countTotal']);seen=set()
    def unique(items):
        out=[]
        for row in items:
            key=(row['market']['marketId'].lower(),address(row['user']['address']))
            if key not in seen:
                seen.add(key);out.append((key[0],key[1],row.get('state') or {}))
        return out
    def progress(done=False):
        return {'unique_positions_seen':len(seen),'initial_count':initial,'reached_end':done,
                'snapshot_complete':False,'consistency':'live_index_plus_chain_events'}
    yield unique(hot['items']),progress()
    market_ids=None
    def walk(filters,field,order,minimum=0,partition_market=False):
        nonlocal market_ids
        cursor=max(minimum,int(filters.get(field+'_gte','"0"').strip('"')));skip=0
        upper=int(filters.get(field+'_lte','"'+str(2**128-1)+'"').strip('"'))
        while True:
            if cursor>upper:return
            page=fetch({**filters,field+'_gte':'"%d"'%cursor},order,skip)
            items=page['items']
            if not items:return
            values=[int(row['state'][field]) for row in items]
            check(values==sorted(values) and values[0]>=cursor,'unordered position keyset page')
            yield items
            if len(items)<page_size:return
            last=values[-1]
            skip=(skip if last==cursor else 0)+sum(value==last for value in values)
            cursor=last
            if skip>10000:
                exact={**filters,field+'_gte':'"%d"'%cursor,field+'_lte':'"%d"'%cursor}
                if field=='borrowShares' and not partition_market:
                    yield from walk(exact,'collateral','Collateral')
                elif not partition_market:
                    if market_ids is None:
                        market_ids=[m.id for rows,_ in market_pages(client) for m in rows]
                    for market in market_ids:
                        yield from walk({**exact,'marketUniqueKey_in':'["%s"]'%market},'borrowShares','BorrowShares',1,True)
                else:
                    raise ValueError('position index contains over 11000 indistinguishable positions in one market; coverage incomplete')
                cursor+=1;skip=0
    for items in walk({'chainId_in':'[8453]'},'borrowShares','BorrowShares',1):
        yield unique(items),progress()
    yield [],progress(True)


def optional_views(rpc,calls,block='latest',page=96,context=None):
    """A reverting exotic token/oracle cannot cancel unrelated metadata reads."""
    if not getattr(rpc,"multicall_verified",False):
        code=rpc.request("eth_getCode",[MULTICALL,"latest"])
        check("0x"+keccak(unhex(code)).hex()==MULTICALL_HASH,"unverified metadata Multicall")
        rpc.multicall_verified=True
    result=[]
    for start in range(0,len(calls),page):
        batch=calls[start:start+page]
        payload=data('aggregate3((address,bool,bytes)[])',['(address,bool,bytes)[]'],[[(a,True,unhex(c)) for a,c in batch]])
        args=[{'to':MULTICALL,'data':payload,'gas':hex(8_000_000)},hex(block) if isinstance(block,int) else block]
        if context is not None:args.extend([{},context])
        raw=rpc.request('eth_call',args)
        rows=decode(['(bool,bytes)[]'],unhex(raw))[0];check(len(rows)==len(batch),'incomplete metadata batch')
        result.extend(value if ok else None for ok,value in rows)
    return result


def oracle_dependencies_batch(rpc,oracles,block='latest'):
    names=('BASE_FEED_1()','BASE_FEED_2()','QUOTE_FEED_1()','QUOTE_FEED_2()','BASE_VAULT()','QUOTE_VAULT()')
    oracles=list(dict.fromkeys(oracles));check(len(oracles)<=8,'oracle preparation batch bound')
    values=optional_views(rpc,[(o,data(n)) for o in oracles for n in names],block)
    result={};feed_addresses=set()
    for index,oracle in enumerate(oracles):
        vals=values[index*len(names):(index+1)*len(names)];refs={}
        for name,value in zip(names,vals):
            if value and len(value)==32:
                try:a=decode(['address'],value)[0].lower()
                except Exception:continue
                if a!=ZERO:refs[name]=a
        feeds={k:v for k,v in refs.items() if 'FEED' in k};feed_addresses.update(feeds.values())
        result[oracle]={'oracle':oracle,'references':refs,'feeds':feeds,
                        'mapping_known':len(vals)==6 and all(v is not None and len(v)==32 for v in vals[:4])}
    ordered=sorted(feed_addresses)
    raw=optional_views(rpc,[(f,data('aggregator()')) for f in ordered],block)
    pairs={}
    for feed,value in zip(ordered,raw):
        try:agg=decode(['address'],value)[0].lower() if value and len(value)==32 else feed
        except Exception:agg=feed
        pairs[feed]=feed if agg==ZERO else agg
    for row in result.values():
        row.update(aggregators={name:pairs[feed] for name,feed in row['feeds'].items()},
                   proxy_to_aggregator={feed:pairs[feed] for feed in row['feeds'].values()},checked_at=time.time())
    return result


def oracle_dependencies(rpc,oracle,block='latest'):
    return oracle_dependencies_batch(rpc,[oracle],block)[oracle]


@dataclass(frozen=True)
class Pool:
    address:str
    token0:str
    token1:str
    factory:str
    kind:int
    key:int
    def hop(self,token):
        check(token in (self.token0,self.token1),'pool token mismatch')
        return (self.address,token,self.token1 if token==self.token0 else self.token0,self.kind)

CONVERSION_KINDS={'v3':1,'slipstream':2,'v2':3,'aerodrome_v2':4}


def pool_lookup(factory,a,b,kind,key):
    if kind==3:return factory,data('getPair(address,address)',['address','address'],[a,b])
    arg='bool' if kind==4 else 'uint24' if kind==1 else 'int24'
    check(kind in (1,2,4) and (kind!=4 or key in (0,1)),'unknown conversion pool')
    return factory,data(f'getPool(address,address,{arg})',['address','address',arg],[a,b,bool(key) if kind==4 else key])


class ConversionGraph:
    def __init__(self,factories):
        self.factories={f['address'].lower():f for f in factories};self.pools={};self.edges=defaultdict(dict);self.path_cache={};self.pages={};self.graph_version=0;self.reach_cache={};self.bridge_cursor={}
    def add(self,pool):
        expected=CONVERSION_KINDS.get(self.factories.get(pool.factory,{}).get('kind'))
        check(expected==pool.kind and pool.token0<pool.token1 and pool.token0!=ZERO,'untrusted pool family')
        check(pool.kind!=4 or pool.key in (0,1),'invalid Aerodrome variant')
        if self.pools.get(pool.address)==pool:return
        self.path_cache.clear();self.reach_cache.clear();self.graph_version+=1;self.pools[pool.address]=pool
        for token in (pool.token0,pool.token1):self.edges[token][pool.address]=pool
    def discover_pairs(self,rpc,pairs,block='latest'):
        calls=[];specs=[]
        for a,b in sorted({tuple(sorted(p)) for p in pairs if p[0]!=p[1]}):
            for f,info in self.factories.items():
                kind=info['kind'];keys=[0] if kind=='v2' else [0,1] if kind=='aerodrome_v2' else info['keys']
                for key in keys:
                    code=CONVERSION_KINDS[kind]
                    calls.append(pool_lookup(f,a,b,code,key));specs.append((a,b,f,code,key))
        vals=optional_views(rpc,calls,block);found=[]
        for spec,v in zip(specs,vals):
            if not v or len(v)!=32:continue
            addr=decode(['address'],v)[0].lower()
            if addr==ZERO or addr in self.pools:continue
            a,b,f,kind,key=spec;found.append(Pool(addr,a,b,f,kind,key))
        self.authenticate(rpc,found,block)
        return found
    def authenticate(self,rpc,pools,block='latest'):
        if not pools:return
        calls=[]
        for p in pools:
            calls.extend((p.address,data(sig)) for sig in ('factory()','token0()','token1()'))
            calls.append(pool_lookup(p.factory,p.token0,p.token1,p.kind,p.key))
        vals=optional_views(rpc,calls,block)
        for i,p in enumerate(pools):
            v=vals[4*i:4*i+4]
            if all(x and len(x)==32 for x in v) and [decode(['address'],x)[0].lower() for x in v]==[p.factory,p.token0,p.token1,p.address]:self.add(p)
    def bridge_pairs(self,start,end,limit=4):
        """Rotate missing venue checks through known intermediate tokens.

        A known route does not imply the best route is known. Return bounded
        pair pages even for already-connected markets; callers rate-limit reads.
        """
        anchors={start,end,USDC,WETH};neighbors=set()
        for token in {start,end}-{USDC,WETH}:
            for pool in self.edges.get(token,{}).values():
                neighbors.add(pool.token1 if token==pool.token0 else pool.token0)
        pairs=sorted({tuple(sorted((a,b))) for a in neighbors-anchors for b in anchors})
        if not pairs:return []
        key=(start,end);offset=self.bridge_cursor.get(key,0)%len(pairs)
        page=(pairs[offset:]+pairs[:offset])[:limit];self.bridge_cursor[key]=offset+len(page)
        return page
    def paths(self,start,end,max_hops=3):
        if start==end:return [()]
        cache_key=(start,end,max_hops)
        if cache_key in self.path_cache:return self.path_cache[cache_key]
        result=[];q=deque([(start,(),frozenset([start]))])
        while q:
            token,path,seen=q.popleft()
            for pool in self.edges.get(token,{}).values():
                hop=pool.hop(token);nxt=hop[2]
                if nxt in seen:continue
                candidate=path+(hop,)
                if nxt==end:result.append(candidate)
                elif len(candidate)<max_hops:q.append((nxt,candidate,seen|{nxt}))
        # All paths remain available; the scheduler rotates quotes across them.
        result=sorted(result,key=lambda p:(len(p),tuple(h[0] for h in p)))
        self.path_cache[cache_key]=result
        return result
    def direct(self,start,end):
        if start==end:return [()]
        return [(p.hop(start),) for p in tuple(self.edges.get(start,{}).values()) if end in (p.token0,p.token1)]
    def has_path(self,start,end,max_hops=3):
        key=(start,end,max_hops)
        if key not in self.reach_cache:self.reach_cache[key]=self._has_path(start,end,max_hops)
        return self.reach_cache[key]
    def _has_path(self,start,end,max_hops=3):
        if start==end:return True
        seen={start};front={start}
        for _ in range(max_hops):
            nxt=set()
            for token in front:
                for p in tuple(self.edges.get(token,{}).values()):
                    other=p.token1 if token==p.token0 else p.token0
                    if other==end:return True
                    if other not in seen:nxt.add(other)
            seen.update(nxt);front=nxt
            if not front:break
        return False
    def path_page(self,start,end,limit=12,work=2048):
        """Incremental exploration, independent of the auction's shared lock.

        Yield after bounded edge visits even when no route exists. A growing
        graph never restarts an unfinished traversal and starves later paths.
        """
        if start==end:return [()]
        key=(start,end)
        if key not in self.pages:
            edges={t:tuple(rows.values()) for t,rows in tuple(self.edges.items())}
            def walk(token,path,seen):
                for pool in edges.get(token,()):
                    hop=pool.hop(token);other=hop[2]
                    yield None
                    if other in seen:continue
                    value=path+(hop,)
                    if other==end:yield value
                    elif len(value)<3:yield from walk(other,value,seen|{other})
            self.pages[key]=walk(start,(),{start})
        out=[];it=self.pages[key]
        for _ in range(work):
            try:value=next(it)
            except StopIteration:self.pages.pop(key,None);break
            if value is not None:
                out.append(value)
                if len(out)>=limit:break
        return out
    def json(self):return [asdict(p) for p in self.pools.values()]


def pool_hints(rows,assets):
    """Reuse known pools connecting market assets, including discovered intermediate tokens.

    Hints carry no price or executable status; factory membership is checked on chain.
    """
    neighbors=defaultdict(set);parsed=[]
    for r in rows:
        try:
            kind=CONVERSION_KINDS[r['kind']]
            p=Pool(address(r['address']),address(r['token0']),address(r['token1']),address(r['factory']),kind,
                int(r['fee'] if kind==1 else r['spacing'] if kind==2 else r['stable'] if kind==4 else 0))
        except (KeyError,ValueError,TypeError):continue
        parsed.append(p);neighbors[p.token0].add(p.token1);neighbors[p.token1].add(p.token0)
    hubs={USDC,WETH};meaningful=set(assets)-hubs
    bridges={a for a,ns in neighbors.items() if ns&meaningful and ns&hubs}
    allowed=set(assets)|hubs|bridges
    return [p for p in parsed if p.token0 in allowed and p.token1 in allowed]


class Universe:
    def __init__(self,config):
        from .auction_selection import AuctionScreens
        self.auction_screens=AuctionScreens()
        self.lock=threading.RLock();self.markets={};self.books={};self.dependencies={};self.feed_markets=defaultdict(set)
        self.graph=ConversionGraph(config['factories']+config.get('structural',{}).get('conversion_factories',[]));self.urgent=deque();self.pending=set();self.cursor=0;self.ready_cursor=0;self.turn=0
        self.auctions=deque(maxlen=128);self.auction_wakeup=threading.Event();self.atlas_meta=None;self.atlas_forwarders={};self.forwarder_requests=set();self.quarantine={};self.prepare_pending=deque();self.preparation={};self.catalog_complete=False;self.event_cursor=None;self.route_cursor={};self.oracle_cache={};self.route_book=None;self.market_failures={}
    def add(self,markets):
        from .structural_runner import BorrowerBook
        with self.lock:
            for m in markets:
                old=self.markets.get(m.id);self.markets[m.id]=m
                if m.active or m.id in self.books:
                    if m.id not in self.books:
                        self.books[m.id]=BorrowerBook();self.prepare_pending.append(m.id)
                    if old is None or not old.active:self.trigger(m.id)
    def trigger(self,market,*,priority=False):
        with self.lock:
            if market in self.pending:
                if not priority:return
                self.urgent.remove(market)
            else:self.pending.add(market)
            if priority:self.urgent.appendleft(market)
            else:self.urgent.append(market)
    def choose(self):
        with self.lock:
            all_markets=list(self.books);self.turn+=1
            ready=[id for id in all_markets if self.books[id].has_candidates() and
                   self.market_failures.get(id,(0,0))[0]<=time.monotonic() and
                   self.graph.has_path(self.markets[id].collateral,self.markets[id].loan) and
                   self.graph.has_path(self.markets[id].loan,USDC)]
            ready_set=set(ready)
            # A sustained stream of hot events cannot starve time/accrual or unknown oracles.
            if self.urgent and self.turn%4:
                while self.urgent:
                    m=self.urgent.popleft();self.pending.discard(m)
                    if m in ready_set:return self.markets[m],self.books[m]
            if ready and self.turn%4:
                key=ready[self.ready_cursor%len(ready)];self.ready_cursor+=1
            elif all_markets:
                available=[id for id in all_markets if self.market_failures.get(id,(0,0))[0]<=time.monotonic()]
                if not available:return None
                key=available[self.cursor%len(available)];self.cursor+=1
            else:return None
            return self.markets[key],self.books[key]
    def bind(self,market,dependency):
        with self.lock:
            for markets in self.feed_markets.values():markets.discard(market)
            self.dependencies[market]=dependency
            for feed in {*dependency['feeds'].values(),*dependency['aggregators'].values()}:self.feed_markets[feed].add(market)
    def notice_markets(self,feed):
        with self.lock:return sorted(self.feed_markets.get(feed.lower(),()))
    def read_failed(self,market):
        with self.lock:
            _,n=self.market_failures.get(market,(0,0));n=min(n+1,6)
            self.market_failures[market]=(time.monotonic()+min(60,2**n),n)
    def read_succeeded(self,market):
        with self.lock:self.market_failures.pop(market,None)
    def routes(self,market,limit=4,amount=None,settlement_amount=None):
        if self.route_book:
            a=self.route_book.ranked(market.collateral,market.loan,amount or 10**market.collateral_decimals,limit)
            b=self.route_book.ranked(market.loan,USDC,settlement_amount or 10**market.loan_decimals,limit)
            return [(a[i],b[j]) for i,j in sorted(((i,j) for i in range(len(a)) for j in range(len(b))),key=lambda x:(sum(x),x))][:limit]
        with self.lock:
            unwind=self.graph.direct(market.collateral,market.loan)
            settle=self.graph.direct(market.loan,USDC)
        rows=[(a,b) for a in unwind for b in settle]
        if not rows:return []
        offset=self.route_cursor.get(market.id,0)%len(rows);self.route_cursor[market.id]=offset+limit
        return (rows[offset:]+rows[:offset])[:limit]
    def coverage(self):
        with self.lock:
            rows=[]
            for m in self.markets.values():
                if m.id not in self.books:continue
                a=self.graph.has_path(m.collateral,m.loan);b=self.graph.has_path(m.loan,USDC)
                dep=self.dependencies.get(m.id,{})
                rows.append({'market':m.id,'collateral':m.collateral_symbol,'loan':m.loan_symbol,'borrowers':len(self.books[m.id].snapshot()),
                    'unwind_paths':len(self.route_book.paths_for(m.collateral,m.loan)) if self.route_book else int(a),'settlement_paths':len(self.route_book.paths_for(m.loan,USDC)) if self.route_book else int(b),'oracle_mapping_known':dep.get('mapping_known',False),
                    'preparation':dict(self.preparation.get(m.id,{})),'public_execution_route':bool(a and b)})
            return {'at':time.time(),'catalog_complete':self.catalog_complete,'catalog_markets':len(self.markets),'active_markets':len(self.books),'quarantined_markets':dict(self.quarantine),'markets':rows}
