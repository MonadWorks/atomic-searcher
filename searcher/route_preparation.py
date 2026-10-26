"""Amount-aware path preparation. Quotes rank work; only full calls authorize it.

No network call or combinatorial traversal runs under the universe lock. The
auction reads immutable path/curve snapshots and queues preparation demands.
"""
from collections import OrderedDict
from dataclasses import dataclass
import threading
import time
from eth_abi import decode
from .core import RPC, USDC, WETH, data, unhex, atomic_json, safe_error
from .structural import HOPS, artifact

QUOTER = '0x000000000000000000000000000000000000c1d3'

def bucket(amount):
    return 1 << max(0, int(amount).bit_length()-1)

@dataclass(frozen=True)
class Point:
    amount: int
    output: int
    at: float

class RouteBook:
    def __init__(self, universe):
        self.u=universe;self.lock=threading.RLock()
        self.paths={};self.curves={};self.demands=OrderedDict();self.cursor={};self.failed={};self.steps=0

    def demand(self,start,end,amount):
        if start==end:return
        with self.lock:
            key=(start,end,bucket(max(1,amount)))
            self.demands[key]=None;self.demands.move_to_end(key,last=False)
            while len(self.demands)>512:self.demands.popitem(last=False)

    def paths_for(self,start,end):
        if start==end:return [()]
        with self.lock:prepared=list(self.paths.get((start,end),()))
        if prepared:return prepared
        # Known direct venues need no warm-up cycle. Cold curves may rank
        # poorly, but must not turn a newly discovered market into an empty route.
        with self.u.lock:direct=self.u.graph.direct(start,end)
        with self.lock:self.paths.setdefault((start,end),direct)
        return list(direct)

    def estimate(self,path,amount,now=None):
        if not path:return amount
        now=time.monotonic() if now is None else now
        with self.lock:points=list(self.curves.get(tuple(path),{}).values())
        points=[p for p in points if now-p.at<=45 and p.amount//4<=amount<=p.amount*4
                and (p.output>0 or p.amount==amount)]
        if not points:return None
        # This scaling is an ordering hint, never a finite-size execution quote.
        p=min(points,key=lambda p:abs(p.amount-amount))
        return p.output*amount//p.amount

    def ranked(self,start,end,amount,limit=4):
        if start==end:return [()]
        self.demand(start,end,amount)
        paths=self.paths_for(start,end);now=time.monotonic()
        with self.lock:
            available=[p for p in paths if self.failed.get((p,bucket(amount)),0)<=now]
        rows=[(self.estimate(p,amount,now),p) for p in available]
        priced=sorted(((v,p) for v,p in rows if v is not None and v>0),key=lambda x:(-x[0],len(x[1])))
        unknown=[p for v,p in rows if v is None]
        best=[p for _,p in priced[:limit]]
        # One exploration place, while keeping the strongest measured path.
        if unknown and (not best or limit>1):
            with self.lock:
                key=(start,end);i=self.cursor.get(('rank',key),0);self.cursor[('rank',key)]=i+1
            if len(best)>=limit:best=best[:limit-1]
            best.append(unknown[i%len(unknown)])
        # A fresh failed quote must not re-enter through a fallback. Unknown
        # routes still rotate, and expired failures become unknown again.
        return best[:limit]

    def record(self,path,amount,output,now=None):
        path=tuple(path);now=time.monotonic() if now is None else now
        with self.lock:
            points=self.curves.setdefault(path,{})
            points[bucket(amount)]=Point(amount,output,now)
            if len(points)>12:points.pop(min(points,key=lambda k:points[k].at))
            if len(self.curves)>4096:
                oldest=min(self.curves,key=lambda k:max(p.at for p in self.curves[k].values()))
                self.curves.pop(oldest,None)

    def reject(self,path,amount,seconds=5):
        with self.lock:self.failed[(tuple(path),bucket(amount))]=time.monotonic()+seconds

    def explore(self,start,end):
        # Pool topology is copied briefly; the bounded traversal happens after
        # releasing the lock shared with notification intake.
        with self.u.lock:direct=self.u.graph.direct(start,end)
        extra=self.u.graph.path_page(start,end,limit=8,work=1024)
        with self.lock:
            key=(start,end);old=self.paths.get(key,[])
            combined=list(dict.fromkeys([*direct,*old,*extra]))
            # Keep known good paths plus rotating new ones; cap memory, not the
            # domain explored by path_page across subsequent iterations.
            if len(combined)>256:
                known=sorted((p for p in old if p not in direct),key=lambda p:max((q.output/max(1,q.amount) for q in self.curves.get(p,{}).values()),default=0),reverse=True)
                combined=list(dict.fromkeys([*direct,*known[:128],*extra]))[:256]
            self.paths[key]=combined
            return list(combined)

    def next_work(self,preferred=None):
        with self.lock:
            self.steps+=1
            if preferred is not None:
                key=preferred;self.demands.pop(key,None)
            elif self.demands:key,_=self.demands.popitem(last=self.steps%4==0)
            else:return None
        start,end,amount=key;paths=self.explore(start,end)
        if not paths:return None
        with self.lock:
            offset=self.cursor.get(('quote',key),0)%len(paths)
            selected=(paths[offset:]+paths[:offset])[:12]
            self.cursor[('quote',key)]=offset+len(selected)
        # Both sides of an amount bucket; at most 24 paths in one discarded call.
        return [(p,a) for p in selected for a in (amount,amount*2)]


def quote_paths(rpc,rows,block='pending'):
    call={'to':QUOTER,'gas':hex(15_000_000),'data':data(f'quotePaths({HOPS}[],uint256[],uint256)',
        [HOPS+'[]','uint256[]','uint256'],[[list(p) for p,a in rows],[a for p,a in rows],500000])}
    args=[call,block,{QUOTER:{'code':artifact('CLQuoter')['deployedBytecode']['object']}}]
    result=decode(['uint256[]'],unhex(rpc.request('eth_call',args)))[0]
    if len(result)!=len(rows):raise ValueError('incomplete path quote batch')
    return result

class RouteWorker:
    def __init__(self,universe,config,stop,output):
        self.u,self.config,self.stop,self.output=universe,config,stop,output
        self.book=RouteBook(universe);universe.route_book=self.book
        self.thread=threading.Thread(target=self.run,name='route-preparation',daemon=True)
        self.turn=0

    def seed(self):
        self.turn+=1
        # Shared bid conversion is valuable to every auction. Direct pools all
        # enter preparation, including those far down address-sorted lists.
        if self.turn%3==1:
            self.book.demand(USDC,WETH,(10**6,10**7,10**8,10**9,10**10,10**11)[(self.turn//3)%6])
        with self.u.lock:
            ids=list(self.u.books)
            if not ids:return
            id=ids[self.turn%len(ids)];market=self.u.markets[id];book=self.u.books[id]
        rows=book.snapshot()
        rows=sorted((r for r in rows if r['shares'] and r['collateral']),key=lambda r:-r['boundary'])[:2]
        for row in rows:
            self.book.demand(market.collateral,market.loan,max(1,row['collateral']))
            self.book.demand(market.loan,USDC,max(1,row['debt']//20))

    def run(self):
        rpc=RPC(self.config['policy'].get('discovery_rpc_url'),timeout=2,cancel_event=self.stop)
        rpc.minimum_request_interval=.7
        verified=False
        try:
            while not self.stop.is_set():
                try:
                    if not verified:
                        rpc.verify_chain()
                        if rpc.request('eth_getCode',[QUOTER,'latest'])!='0x':raise ValueError('quote lens collision')
                        verified=True
                    self.seed()
                    preferred=(USDC,WETH,bucket((10**6,10**7,10**8,10**9,10**10,10**11)[(self.turn//3)%6])) if self.turn%3==1 else None
                    rows=self.book.next_work(preferred)
                    if rows:
                        outputs=quote_paths(rpc,rows)
                        for (path,amount),output in zip(rows,outputs):self.book.record(path,amount,output)
                        atomic_json(self.output/'route-preparation-health.json',{'at':time.time(),'state':'ready','last_batch_paths':len(rows),'positive_quotes':sum(v>0 for v in outputs),'quotes_are_execution_evidence':False})
                except Exception as exc:
                    atomic_json(self.output/'route-preparation-health.json',{'at':time.time(),'state':'retrying','error':safe_error(exc)})
                self.stop.wait(.3)
        finally:rpc.close()
