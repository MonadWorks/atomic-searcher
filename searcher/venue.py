"""Authenticated Forte discovery and bounded mixed-venue cycle enumeration.

No signer. The factory uses CREATE; every nonce is visited, including old pools.
Only verified runtime templates are accepted. Per-pool actual code hashes are
bound into signed plans, never inferred from a token symbol or a winner's route.
"""
from dataclasses import dataclass
from pathlib import Path
import json
import sqlite3
import time
import math
import rlp
from eth_abi import encode,decode
from eth_utils import keccak
from .core import ROOT,USDC,check,data,unhex,number,atomic_json
from .forte_templates import FACTORY_HASH,TEMPLATES
from .v4_market import Key,MANAGER

FACTORY='0xd5a8965c648f0d0f0dd5b15f042de5f32b3ff238'
LEG='(address,address,address,uint8,bytes)'
KEY='(address,address,uint24,int24,address)'

def created_address(nonce):
    check(isinstance(nonce,int) and 0<nonce<1<<64,'invalid factory nonce')
    return '0x'+keccak(rlp.encode([unhex(FACTORY),nonce]))[-20:].hex()

def authenticate_runtime(raw):
    code=unhex(raw)
    for template in TEMPLATES:
        if len(code)!=template['length']:continue
        masked=bytearray(code)
        for start,length in template['ranges']:masked[start:start+length]=bytes(length)
        if '0x'+keccak(masked).hex()==template['normalized_hash']:return '0x'+keccak(code).hex()
    raise ValueError('unrecognized Forte runtime; requires source verification')

@dataclass(frozen=True)
class Edge:
    pool:str
    a:str
    b:str
    kind:int
    auxiliary:bytes=b''
    @property
    def identity(self):return self.pool+(':'+keccak(self.auxiliary).hex() if self.kind==6 else '')
    def hop(self,token):
        check(token in (self.a,self.b),'venue edge token mismatch')
        return (self.pool,token,self.b if token==self.a else self.a,self.kind,self.auxiliary)

class Catalog:
    def __init__(self,path=None):
        self.path=Path(path or ROOT/'evidence/forte-catalog.json');self.cursor=1;self.rows={};self.rejected={}
        if self.path.exists():
            d=json.loads(self.path.read_text());check(d.get('factory')==FACTORY,'Forte catalog factory mismatch')
            self.cursor=int(d['cursor']);self.rows=d['pools'];self.rejected=d.get('rejected',{})
        self.total=0;self.checked=False
    def refresh(self,rpc,page=8):
        check(1<=page<=32,'Forte discovery page bound')
        if not self.checked:
            code=rpc.request('eth_getCode',[FACTORY,'latest'])
            check('0x'+keccak(unhex(code)).hex()==FACTORY_HASH,'Forte factory changed');self.checked=True
        # Canonical block pinning prevents mixing a factory nonce with a reorged pool.
        head=rpc.header();tag=head['number']
        self.total=number(rpc.request('eth_getTransactionCount',[FACTORY,tag]))
        if self.cursor>self.total:self.cursor=1;self.rows={};self.rejected={}
        end=min(self.total,self.cursor+page)
        for n in range(self.cursor,end):
            a=created_address(n);raw=rpc.request('eth_getCode',[a,tag])
            try:runtime=authenticate_runtime(raw)
            except ValueError as exc:self.rejected[a]=str(exc);continue
            vals=rpc.batch([('eth_call',[{'to':a,'data':data(s)},tag]) for s in ('xToken()','yToken()')])
            x,y=(decode(['address'],unhex(v))[0].lower() for v in vals)
            check(x!=y and int(x,16)>0 and int(y,16)>0,'invalid Forte token pair')
            self.rows[a]={'nonce':n,'x':x,'y':y,'code_hash':runtime}
        # A transport failure before this point leaves the page cursor unchanged.
        canonical=rpc.header(number(tag));check(canonical['hash']==head['hash'],'Forte discovery anchor changed')
        self.cursor=end
        atomic_json(self.path,{'at':time.time(),'factory':FACTORY,'cursor':self.cursor,'factory_nonce':self.total,
                              'pools':self.rows,'rejected':self.rejected,'complete':self.cursor==self.total})
        return self.cursor==self.total
    def edges(self):
        return [Edge(a,r['x'],r['y'],5,unhex(r['code_hash'])) for a,r in self.rows.items()]

def live_forte(rpc,edges):
    from .morpho_universe import optional_views
    paused=optional_views(rpc,[(e.pool,data('paused()')) for e in edges])
    return [e for e,p in zip(edges,paused) if p is not None and int.from_bytes(p,'big')==0]

def venue_rates(rpc,edges,units=None):
    """Advisory finite-size Forte / marginal V4 prices; never authorize sends."""
    from .morpho_universe import optional_views
    forte=[e for e in edges if e.kind==5];tokens=sorted({t for e in forte for t in (e.a,e.b)})
    units={} if units is None else units
    missing=[t for t in tokens if t not in units]
    raw=optional_views(rpc,[(t,data('decimals()')) for t in missing]) if missing else []
    units.update({t:max(1,10**int.from_bytes(v,'big')//1000) for t,v in zip(missing,raw) if v and int.from_bytes(v,'big')<=36})
    calls=[];bindings=[]
    for e in forte:
        for t in (e.a,e.b):
            if t in units:
                calls.append((e.pool,data('simSwap(address,uint256)',['address','uint256'],[t,units[t]])));bindings.append((e,t))
    values=optional_views(rpc,calls) if calls else []
    rates={}
    for (e,t),v in zip(bindings,values):
        if v and len(v)==96:
            out=decode(['uint256']*3,v)[0]
            if out:rates[e.identity,t]=math.log(out)-math.log(units[t])
    # Refresh core connections first; other keys remain in the exploration pass.
    v4=[e for e in edges if e.kind==6 and e.a in units and e.b in units]
    vals=optional_views(rpc,[(MANAGER,data('extsload(bytes32)',['bytes32'],[(Key(*decode([KEY],e.auxiliary)[0]).slot+offset).to_bytes(32,'big')])) for e in v4 for offset in (0,3)]) if v4 else []
    from .v4_market import total_fee
    for i,e in enumerate(v4):
        v,liquidity=vals[2*i:2*i+2]
        if not v or len(v)!=32 or not liquidity or int.from_bytes(liquidity,'big')==0:continue
        packed=int.from_bytes(v,'big');sqrt=packed&((1<<160)-1);protocol=(packed>>184)&((1<<24)-1);lp=(packed>>208)&((1<<24)-1)
        if not sqrt:continue
        try:
            forward=total_fee(protocol,lp,True);reverse=total_fee(protocol,lp,False)
            rate=2*math.log(sqrt)-192*math.log(2)
            rates[e.identity,e.a]=rate+math.log1p(-forward/1_000_000)
            rates[e.identity,e.b]=-rate+math.log1p(-reverse/1_000_000)
        except (ValueError,OverflowError):continue
    return rates

def v4_edges(path=None):
    p=Path(path or ROOT/'evidence/v4-inventory.sqlite')
    if not p.exists():return []
    result=[]
    with sqlite3.connect(f'file:{p}?mode=ro',uri=True,timeout=.2) as db:
        for pid,raw in db.execute('SELECT id,key FROM pools ORDER BY id'):
            k=Key(*json.loads(raw));check(k.id==pid,'V4 inventory key hash mismatch')
            result.append(Edge(MANAGER,k.currency0,k.currency1,6,encode([KEY],[k.values()])))
    return result

def cycles(edges,max_hops=4):
    """Interleave every Forte anchor instead of walking irrelevant V4-only paths."""
    from collections import deque
    check(2<=max_hops<=4,'venue cycle hop bound')
    graph={}
    for edge in sorted(edges,key=lambda e:e.identity):
        for t in (edge.a,edge.b):graph.setdefault(t,[]).append(edge)
    distance_cache={}
    def distances(end):
        if end not in distance_cache:
            result={end:0};front={end}
            for depth in range(1,max_hops):
                new=set()
                for token in front:
                    if token==USDC and token!=end:continue
                    for e in graph.get(token,()):
                        other=e.b if token==e.a else e.a
                        if other not in result:result[other]=depth;new.add(other)
                front=new
            distance_cache[end]=result
        return distance_cache[end]
    def paths(start,end,limit,used,seen):
        if start==end:
            yield ();return
        if limit==0:return
        for i,e in enumerate(graph.get(start,())):
            if i%64==0:yield None
            if e.identity in used:continue
            h=e.hop(start);out=h[2]
            if out==end:yield (h,)
            elif out not in seen and limit>1 and distances(end).get(out,max_hops)<=limit-1:
                for tail in paths(out,end,limit-1,used|{e.identity},seen|{out}):
                    yield None if tail is None else (h,)+tail
    def anchored(e,start):
        h=e.hop(start);other=h[2]
        # The pre/post paths may use other Forte pools. Cross-check all pool
        # ids and intermediate tokens so multiple anchors never repeat a pool.
        for prefix in paths(USDC,start,max_hops-2 if other!=USDC else max_hops-1,{e.identity},{USDC,other}):
            if prefix is None:yield None;continue
            prefix_ids={p[0]+(':'+keccak(p[4]).hex() if p[3]==6 else '') for p in prefix}
            seen={USDC,*(p[2] for p in prefix),other}
            for suffix in paths(other,USDC,max_hops-len(prefix)-1,prefix_ids|{e.identity},seen):
                if suffix is None:yield None;continue
                result=prefix+(h,)+suffix
                if len(result)>=2:
                    # Canonical anchor removes duplicates without an unbounded set.
                    anchors=[p[0] for p in result if p[3]==5]
                    if e.pool==min(anchors):yield result
    work=deque(anchored(e,t) for e in edges if e.kind==5 for t in (e.a,e.b))
    while work:
        iterator=work.popleft()
        try:result=next(iterator)
        except StopIteration:continue
        work.append(iterator);yield result


def cycle_route(legs):
    from .protocol_execution import Route
    legs=tuple(legs);check(2<=len(legs)<=4 and legs[0][1]==USDC and legs[-1][2]==USDC,'invalid venue cycle')
    for i,h in enumerate(legs):
        check(len(h)==5 and h[3] in (1,2,3,4,5,6) and (i==0 or legs[i-1][2]==h[1]),'invalid venue leg')
    args=encode([LEG+'[]'],[legs])
    # Hop metadata preserves distinct V4 ids in route binding, while execution
    # retains the real manager address in its authenticated arguments.
    metadata=tuple((h[0] if h[3]!=6 else '0x'+keccak(h[4]).hex(),h[1],h[2],h[3]) for h in legs)
    return Route(6,args,metadata,tuple(h[1] for h in legs))

def plan_batch(routes,amounts):
    from .structural import PLAN
    check(0<len(routes)==len(amounts)<=8,'venue quote batch bound')
    return data(f'quotePlans({PLAN}[],uint256)',[PLAN+'[]','uint256'],
                [[(6,r.arguments,a,1,0,0,0,b'\0'*32,0,0) for r,a in zip(routes,amounts)],1200000])

class Scanner:
    def __init__(self,rpc,config,universe,contract,stop,report,catalog=None,regular_prices=None):
        self.rpc,self.config,self.u,self.contract,self.stop,self.report=rpc,config,universe,contract,stop,report
        self.catalog=catalog or Catalog();self.next_discovery=0;self.next_graph=0;self.iterator=iter(());self.edges=[];self.round=0
        self.regular_prices=regular_prices or (lambda:{});self.rates={};self.units={};self.next_prices=0;self.audit_iter=iter(());self.checked=0;self.turn=0
        self.sizes=[10**6,10**7,10**8,10**9,10**10,10**11,10**12]
        self.cap=int(next(t for t in config['settlement_tokens'] if t['address']==USDC)['max_input_raw'])
        self.sizes=sorted({min(n,self.cap) for n in self.sizes}|{self.cap})
    def proposals(self):
        while not self.stop.is_set():
            now=time.monotonic()
            if now>=self.next_discovery:
                complete=self.catalog.refresh(self.rpc);self.next_discovery=now+(60 if complete else 5)
                self.report(state='scanning',detail='venue_catalog',known_forte_pools=len(self.catalog.rows),
                            catalog_complete=complete,unknown_runtimes=len(self.catalog.rejected))
                self.next_graph=0
            if now>=self.next_graph:
                with self.u.lock:regular=[Edge(p.address,p.token0,p.token1,p.kind) for p in self.u.graph.pools.values()]
                active=live_forte(self.rpc,self.catalog.edges())
                blocked=self.config.get('unsupported_transfer_tokens',{})
                edges=[e for e in active+regular+v4_edges() if e.a not in blocked and e.b not in blocked]
                # Finish the current topology traversal; restarting it whenever
                # an unrelated pool is added would starve old route tails.
                self.edges=edges
                self.next_prices=0
                self.report(state='scanning',detail='venue_topology_ready',active_forte_pools=len(active))
                self.next_graph=now+30
            if time.monotonic()>=self.next_prices:
                self.rates=venue_rates(self.rpc,self.edges,self.units)
                self.next_prices=time.monotonic()+2
                self.report(state='scanning',detail='venue_prices_ready',priced_directions=len(self.rates),price_received_at=time.time())
            routes=[];amounts=[];deadline=time.monotonic()+.03;regular_rates=self.regular_prices();self.turn+=1
            live={e.identity for e in self.edges}
            while len(routes)<4 and time.monotonic()<deadline:
                try:legs=next(self.iterator)
                except StopIteration:
                    self.iterator=cycles(self.edges);self.round+=1;break
                if legs:
                    ids=[h[0]+(':'+keccak(h[4]).hex() if h[3]==6 else '') for h in legs]
                    if any(i not in live for i in ids):continue
                    weights=[self.rates.get((i,h[1]),regular_rates.get((h[0],h[1]))) for i,h in zip(ids,legs)]
                    self.checked+=1
                    promising=all(v is not None for v in weights) and sum(weights)>0
                    if not promising:
                        continue
                    route=cycle_route(legs)
                    for offset in (0,2,4,6):
                        if len(routes)>=4:break
                        routes.append(route);amounts.append(self.sizes[(self.round+offset)%len(self.sizes)])
            if len(routes)<4 and (self.turn%8==0 or not routes):
                # Independent lazy cursor: unknown/negative estimates retain a
                # fair exploration path without a lossy bounded route queue.
                for _ in range(256):
                    try:legs=next(self.audit_iter)
                    except StopIteration:self.audit_iter=cycles(self.edges);break
                    if not legs:continue
                    ids=[h[0]+(':'+keccak(h[4]).hex() if h[3]==6 else '') for h in legs]
                    if any(i not in live for i in ids):continue
                    route=cycle_route(legs)
                    for offset in (0,2,4,6):
                        if len(routes)>=4:break
                        routes.append(route);amounts.append(self.sizes[(self.round+offset)%len(self.sizes)])
                    break
            if not routes:
                self.stop.wait(.1);yield None;continue
            raw=self.rpc.request('eth_call',[{'from':self.config['owner'],'to':self.contract,'data':plan_batch(routes,amounts),
                                            'gas':hex(self.config['policy']['max_gas'])},'pending'])
            quotes,errors=decode(['uint256[2][]','bytes4[]'],unhex(raw))
            check(len(quotes)==len(routes)==len(errors),'venue quote result binding')
            positive=sorted([(p,i,g) for i,(p,g) in enumerate(quotes) if p>0],reverse=True)
            self.report(state='scanning',detail='venue_complete_cycles',routes_checked=len(routes),positive=len(positive),
                        max_gross_usdc_raw=max((q[0] for q in quotes),default=0),reverts=[e.hex() for e in errors if e!=b'\0'*4])
            # Final complete_quote applies fee budgets, the 0.1U floor, and a fresh
            # canonical context before the shared executor can reserve or sign.
            for profit,i,gas in positive:
                if profit>=int(self.config['policy'].get('min_net_profit_usdc_raw',100000)):
                    yield routes[i],amounts[i]
            yield None
