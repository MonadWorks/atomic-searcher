"""Bounded price-first DEX scheduling. Every estimate is advisory only.

Pool metadata comes exclusively from the authenticated conversion graph. Prices
can span different reads; native cycle and whole-transaction calls remain the
only authorities for executable amounts and profit.
"""
from collections import defaultdict, deque, OrderedDict
from dataclasses import dataclass
import math
import time

from eth_abi import decode
from .core import USDC, WETH, check, data, unhex
from .structural import artifact

SCREEN='(bool,uint160,uint128,uint256,uint256,uint256,uint256,uint256)'
LENS='0x000000000000000000000000000000000000c1d6'


def read_markets(rpc, pools, clock):
    call={'to':LENS,'gas':hex(15_000_000),'data':data('screenPools(address[],uint8[])',
          ['address[]','uint8[]'],[[p.address for p in pools],[p.kind for p in pools]])}
    rows,block,stamp=decode([SCREEN+'[]','uint256','uint256'],unhex(rpc.request('eth_call',
        [call,'pending',{LENS:{'code':artifact('CycleQuoteLens')['deployedBytecode']['object']}},clock])))
    check(len(rows)==len(pools),'incomplete DEX screen')
    check(block==int(clock['number'],16) and stamp==int(clock['time'],16),'DEX screen clock mismatch')
    return rows,block


@dataclass(frozen=True)
class Edge:
    hop: tuple
    rate: float
    capacity: float
    fingerprint: tuple


def edges_for(pool,row):
    ok,sqrt,liquidity,r0,r1,out0,out1,fee=row
    if not ok:return ()
    if pool.kind in (1,2):
        if sqrt<=0 or liquidity<=0 or not 0<=fee<1_000_000:return ()
        rate=2*math.log(sqrt)-192*math.log(2)
        loss=math.log1p(-fee/1_000_000)
        cap0=liquidity*2**96/sqrt;cap1=liquidity*sqrt/2**96
        forward,reverse=rate+loss,-rate+loss
    else:
        if r0<=0 or r1<=0:return ()
        cap0,cap1=r0,r1
        if pool.kind==3:
            forward=math.log(r1)-math.log(r0)+math.log(.997)
            reverse=math.log(r0)-math.log(r1)+math.log(.997)
        elif pool.kind==4:
            # Native tiny finite quotes include the stable/volatile invariant
            # and current custom fee. They rank work, never authorize a trade.
            if out0<=0 or out1<=0:return ()
            forward=math.log(out0)-math.log(max(1,r0//1_000_000))
            reverse=math.log(out1)-math.log(max(1,r1//1_000_000))
        else:return ()
    return (Edge(pool.hop(pool.token0),forward,cap0*.01,tuple(row)),
            Edge(pool.hop(pool.token1),reverse,cap1*.01,tuple(row)))


class Scheduler:
    def __init__(self,settlements,max_hops,clock=time.monotonic,feedback=None):
        self.settlements={s['address']:s for s in settlements};self.max_hops=max_hops;self.clock=clock
        self.feedback=feedback
        self.pools={};self.samples={};self.new=deque();self.hot=deque();self.cursor=0
        self.explorers={};self.explore_cursor=0;self.tried={};self.pages=0;self.native_checked=0
        self.page_size=96;self.last_search={};self.last_quotes=[];self.graph_updates=0
        self.native_failures={};self.edge_failures={};self.edge_failure_history={}
        self.activity=OrderedDict();self.pairs={};self.last_activity_reads=[]
        self.priority_signals={}

    def sync(self,pools):
        added=[a for a in pools if a not in self.pools]
        if added:
            # A newly discovered venue never waits for the old DFS to finish.
            self.new.extendleft(reversed(added));self.graph_updates+=1
        self.pools=dict(pools)
        if added or any(a not in self.pools for rows in self.pairs.values() for a in rows):
            pairs=defaultdict(list)
            for p in self.pools.values():pairs[tuple(sorted((p.token0,p.token1)))].append(p.address)
            self.pairs=dict(pairs)
        for a in set(self.samples)-self.pools.keys():self.samples.pop(a,None)

    def prioritize(self,events):
        now=self.clock()
        for address,at in events:
            if address not in self.pools or not 0<=now-at<=5:continue
            p=self.pools[address]
            keys=[tuple(sorted((p.token0,p.token1)))]
            keys += [tuple(sorted((token,base))) for token in (p.token0,p.token1)
                     for base in self.settlements if token!=base]
            related=[address]
            for key in dict.fromkeys(keys):
                venues=self.pairs.get(key,())
                # Keep alternative venues rotating when a pair is unusually
                # large; one busy token cannot enqueue the whole universe.
                offset=self.pages%max(1,len(venues))
                related.extend((list(venues)[offset:]+list(venues)[:offset])[:8])
            for a in reversed(tuple(dict.fromkeys(related))):
                self.activity[a]=at;self.activity.move_to_end(a)
        self.activity=OrderedDict((a,at) for a,at in self.activity.items()
                                  if a in self.pools and 0<=now-at<=5)
        while len(self.activity)>512:self.activity.popitem(last=False)

    def page(self):
        addresses=list(self.pools);selected=[];seen=set()
        if not addresses:return []
        def take(queue,count,unseen=False):
            while queue and count:
                a=queue.popleft()
                if unseen and a in self.samples:continue
                if a in self.pools and a not in seen:selected.append(a);seen.add(a);count-=1
        self.prioritize(())
        self.last_activity_reads=list(reversed(self.activity))[:self.page_size//2]
        selected.extend(self.last_activity_reads);seen.update(selected)
        other=max(0,(self.page_size-self.page_size//3-len(selected))//2)
        take(self.hot,other);take(self.new,other,unseen=True)
        # At least a third of every read is a fair scan, even during a hot storm.
        for _ in range(len(addresses)):
            a=addresses[self.cursor%len(addresses)];self.cursor+=1
            if a not in seen:selected.append(a);seen.add(a)
            if len(selected)>=self.page_size:break
        return [self.pools[a] for a in selected]

    def record(self,pools,rows,block):
        now=self.clock();changed=0;repriced=set()
        for p,row in zip(pools,rows):
            old=self.samples.get(p.address)
            if old is None or old[0]!=tuple(row):changed+=1
            if row[0]:
                if old is None or old[0]!=tuple(row):repriced.add(p.address)
                # An actual fresh price/depth/fee change reopens the venue.
                # A failed state read is not evidence that a swap now works.
                for token in (p.token0,p.token1):
                    edge=(p.address,token)
                    prior=self.edge_failure_history.get(edge)
                    if prior and (prior[2] is None or prior[2]!=tuple(row)):
                        self.edge_failures.pop(edge,None);self.edge_failure_history.pop(edge,None)
            self.samples[p.address]=(tuple(row),now,block)
            if row[0]:self.activity.pop(p.address,None)
        if repriced:
            # A zero native quote belongs to its old prices. A newly read
            # price may reopen it immediately; measured transfer-loss feedback
            # remains independent and still excludes its directed edges.
            self.native_failures={path:until for path,until in self.native_failures.items()
                                  if not any(h[0] in repriced for h in path)}
        self.pages+=1
        return changed

    @staticmethod
    def _trim(states):
        # Preserve both price advantage and capacity. A dust pool with a very
        # distorted marginal price must not monopolize every path to a token.
        price=sorted(states,key=lambda x:x[0],reverse=True)[:2]
        capacity=sorted(states,key=lambda x:(math.log(max(1,x[1]))+x[0]),reverse=True)[:2]
        return list(dict.fromkeys(price+capacity))

    def ranked(self):
        now=self.clock();edges=defaultdict(list);valid=0
        for address,(row,at,_) in self.samples.items():
            if now-at>45 or address not in self.pools:continue
            pair=edges_for(self.pools[address],row)
            if pair:valid+=1
            for e in pair:
                if self.edge_failures.get((address,e.hop[1]),0)<=now:edges[e.hop[1]].append(e)
        found={};visits=0
        blocked=self.feedback.blocked_hops() if self.feedback else {}
        for scope in (('AtomicCycle','StructuralExecutor') if any(blocked.values()) else (None,)):
            excluded=blocked.get(scope,())
            scoped_edges={token:[e for e in candidates
                if (scope!='AtomicCycle' or e.hop[3]!=4)
                and (e.hop[0],e.hop[1],'in') not in excluded
                and (e.hop[0],e.hop[2],'out') not in excluded]
                for token,candidates in edges.items()}
            for start,settlement in self.settlements.items():
                minimum_input=max(1,int(settlement.get('min_net_raw',10**settlement['decimals']//100)))
                frontier={start:[(0.,float('inf'),(),(start,),())]}
                for depth in range(self.max_hops):
                    following=defaultdict(list)
                    for token,states in frontier.items():
                        for score,capacity,path,tokens,prints in states:
                            for e in scoped_edges.get(token,()):
                                visits+=1;out=e.hop[2]
                                if any(h[0]==e.hop[0] for h in path):continue
                                total=score+e.rate;cap=min(capacity,e.capacity/math.exp(max(-700,min(700,score))))
                                # Sub-cent input capacities plus extreme marginal
                                # rates are dominated by integer rounding. Keep
                                # them out of the beam; bounded exploration below
                                # still checks native execution independently.
                                if cap<minimum_input:continue
                                new=path+(e.hop,);fp=prints+(e.fingerprint,)
                                if out==start:
                                    if (len(new)>=2 and total>1e-7 and cap>=1
                                        and (scope!='StructuralExecutor' or any(h[3]==4 for h in new))
                                        and (self.feedback is None or self.feedback.allowed(new))):
                                        gross=cap*math.expm1(min(100,total))/(10**settlement['decimals'])
                                        # Only relative ranking within each settlement is used below.
                                        found[new]=(gross,cap,fp,settlement)
                                elif out not in tokens and depth+1<self.max_hops and cap>=1:
                                    row=(total,cap,new,tokens+(out,),fp);bucket=following[out];bucket.append(row)
                                    if len(bucket)>8:following[out]=self._trim(bucket)
                    frontier={t:self._trim(rows) for t,rows in following.items()}
        by=defaultdict(list)
        for path,(gross,cap,fp,settlement) in found.items():
            if self.native_failures.get(path,0)>now:continue
            prior=self.tried.get(path)
            if prior and prior[0]==fp and now-prior[1]<5:continue
            by[settlement['address']].append((gross,path,cap,fp,settlement))
        for rows in by.values():rows.sort(key=lambda x:-x[0])
        # Alternate settlement assets rather than compare ETH and USDC units.
        out=[]
        for i in range(8):
            for token in self.settlements:
                if len(by[token])>i:out.append(by[token][i])
        self.last_search={'screened_pools':valid,'known_pools':len(self.pools),
                          'edge_visits':visits,'positive_priority_paths':len(found),
                          'temporarily_deprioritized_edges':sum(t>now for t in self.edge_failures.values()),
                          'repeat_failure_edges':sum(v[0]>1 for v in self.edge_failure_history.values()),
                          'execution_transfer_deferred_edges':sum(len(v) for v in blocked.values())}
        return out

    def _explore(self,start):
        # Rotate the FIRST edge and path length on every visit. An empty first
        # pool cannot monopolize millions of combinations of the later legs.
        edges=defaultdict(list);now=self.clock()
        for p in self.pools.values():
            sample=self.samples.get(p.address)
            if sample and now-sample[1]<15 and not sample[0][0]:continue
            for t in (p.token0,p.token1):edges[t].append(p)
        seeds=edges[start]
        if not seeds:return None
        cursor=self.explorers.get(start,0);self.explorers[start]=cursor+1
        first=seeds[cursor%len(seeds)].hop(start)
        depth=2+(cursor//len(seeds))%max(1,self.max_hops-1)
        def walk(token,path,seen):
            for p in edges[token]:
                yield None
                if any(h[0]==p.address for h in path):continue
                hop=p.hop(token);out=hop[2];new=path+(hop,)
                if out==start and len(new)==depth:
                    if self.feedback is None or self.feedback.allowed(new):yield new
                elif out not in seen and len(new)<depth:yield from walk(out,new,seen|{out})
        it=walk(first[2],(first,),{start,first[2]})
        for _ in range(2048):
            try:path=next(it)
            except StopIteration:return None
            if path is not None:return path
        return None

    def quotes(self):
        now=self.clock();quotes=[];selected=[]
        ranked=self.ranked()
        picked=[];assets=set()
        for candidate in ranked:
            path=candidate[1];other={h[1] for h in path}-{USDC,WETH}
            if picked and other&assets:continue
            picked.append(candidate);assets.update(other)
            if len(picked)==2:break
        for _,path,cap,fp,s in picked:
            for scale in (.01,.25,1.):
                amount=min(int(s['max_input_raw']),max(1,int(cap*scale)))
                if not any(path==p and amount==a for p,a,_ in quotes):quotes.append((path,amount,s))
            self.tried[path]=(fp,now);selected.extend(h[0] for h in path)
        for _ in range(2):
            start=list(self.settlements)[self.explore_cursor%len(self.settlements)];self.explore_cursor+=1
            path=self._explore(start)
            if path:
                s=self.settlements[start];sizes=(100*10**6,1000*10**6,10000*10**6,100000*10**6) if start==USDC else (10**16,10**17,10**18,10**19)
                amount=min(int(s['max_input_raw']),sizes[(self.pages//2)%len(sizes)])
                quotes.append((path,amount,s))
        self.hot.extend(dict.fromkeys(selected))
        while len(self.hot)>512:self.hot.popleft()
        if len(self.tried)>1024:self.tried={p:v for p,v in self.tried.items() if now-v[1]<30}
        return quotes[:8]

    def event_quotes(self, events):
        """Quote prepared two-venue routes directly on delivery, before a scan.

        Cached depth only sizes/prioritizes the native calls. Neither old prices
        nor a log can approve execution. Both directions are tried because the
        trigger may already have reversed the previously observed price gap.
        """
        now=self.clock();result=[];seen=set()
        for address,at in reversed(events):
            if address not in self.pools or not 0<=now-at<=1:continue
            p=self.pools[address]
            if p.kind not in (1,2,3):continue
            for start in self.settlements:
                if start not in (p.token0,p.token1):continue
                other=p.token1 if start==p.token0 else p.token0
                venues=self.pairs.get(tuple(sorted((start,other))),())
                # Prefer actually prepared liquid counterparts; bound work even
                # when hundreds of factories share a pair.
                ready=[]
                for a in venues:
                    sample=self.samples.get(a);q=self.pools[a]
                    if q.kind not in (1,2,3) or not sample or now-sample[1]>45:continue
                    edges=edges_for(q,sample[0])
                    if edges:
                        edge=next(e for e in edges if e.hop[1]==start)
                        ready.append((edge.capacity,a))
                if address not in {a for _,a in ready}:continue
                ready.sort(reverse=True)
                own=next(cap for cap,a in ready if a==address)
                for cap,a in ready:
                    if a==address:continue
                    q=self.pools[a];s=self.settlements[start]
                    size=min(int(s['max_input_raw']),max(1,int(min(own,cap))))
                    if size<int(s.get('min_net_raw',1)):continue
                    for first,second in ((p,q),(q,p)):
                        path=(first.hop(start),second.hop(other))
                        if path in seen or self.feedback and not self.feedback.allowed(path):continue
                        seen.add(path)
                        for scale in (.05,.25,1.):
                            amount=max(1,int(size*scale))
                            if not any(path==old and amount==v for old,v,_ in result):result.append((path,amount,s))
                    if len(result)>=6:return result[:8]
        return result[:8]

    def stream_priority(self):
        """Prepared, liquid cross-venue pairs, excluding measured bad routes.

        An opportunity's old profit is not required: a new trade creates the
        next gap. The scores allocate observation, never authorize execution.
        """
        now=self.clock();groups=[]
        for pair,addresses in self.pairs.items():
            starts=[s for s in self.settlements if s in pair]
            if not starts:continue
            start=starts[0];other=pair[1] if pair[0]==start else pair[0];ready=[]
            for a in addresses:
                sample=self.samples.get(a);p=self.pools[a]
                if p.kind not in (1,2,3) or not sample or now-sample[1]>45:continue
                edges=edges_for(p,sample[0])
                if edges:
                    cap=next(e.capacity for e in edges if e.hop[1]==start)
                    ready.append((cap,p))
            ready.sort(key=lambda item:item[0],reverse=True)
            for i,(cap,p) in enumerate(ready[:4]):
                for qcap,q in ready[i+1:4]:
                    paths=((p.hop(start),q.hop(other)),(q.hop(start),p.hop(other)))
                    if self.feedback and not any(self.feedback.allowed(path) for path in paths):continue
                    size=min(cap,qcap)/10**self.settlements[start]['decimals']
                    if size<=0:continue
                    members=tuple(sorted((p.address,q.address)))
                    recent=self.priority_signals.get(members,0)
                    # Native profit evidence ranks above raw liquidity; it
                    # expires and remains subject to full-call feedback.
                    groups.append(((int(now-recent<120 and recent>0),recent,
                                    math.log1p(size)),members))
        return sorted(groups,reverse=True)[:32]

    def native_results(self,quotes,outputs,failed_hops=None,priority_successes=None):
        # A zero native quote is unknown/unexecutable at that tested size,
        # not a negative-profit measurement. Briefly rotate to other routes.
        groups=defaultdict(list);now=self.clock()
        if priority_successes is not None:
            for (path,_,_),worth_checking in zip(quotes,priority_successes):
                if worth_checking:self.priority_signals[tuple(sorted(h[0] for h in path))]=now
            self.priority_signals={p:t for p,t in self.priority_signals.items() if now-t<120}
        for (path,_,_),output in zip(quotes,outputs):groups[tuple(path)].append(output)
        for path,values in groups.items():
            if not any(values):self.native_failures[path]=now+2
            else:self.native_failures.pop(path,None)
        self.native_failures={p:t for p,t in self.native_failures.items() if t>now}
        if failed_hops is not None:
            productive=set();failed=set()
            if priority_successes is None:
                priority_successes=[output>amount for (_,amount,_),output in zip(quotes,outputs)]
            check(len(priority_successes)==len(quotes),'incomplete priority feedback')
            for (path,_,_),output,index,worth_checking in zip(quotes,outputs,failed_hops,priority_successes):
                # Completing a losing small swap does not justify repeatedly
                # retrying larger failures on the same directed venue. Only a
                # cycle clearing the caller's native profit floor restores
                # priority. Independent exploration and state changes remain
                # available to discover other working sizes and routes.
                if output>0 and worth_checking:
                    productive.update((hop[0],hop[1]) for hop in path)
                if 1<=index<=len(path):
                    hop=path[index-1];failed.add((hop[0],hop[1]))
            # A smaller financially promising cycle still reopens its edges.
            for edge in productive:
                self.edge_failures.pop(edge,None);self.edge_failure_history.pop(edge,None)
            for edge in failed-productive:
                prior=self.edge_failure_history.get(edge)
                strikes=(prior[0] if prior and now-prior[1]<900 else 0)+1
                sample=self.samples.get(edge[0])
                fingerprint=sample[0] if sample and sample[0][0] else prior[2] if prior else None
                self.edge_failure_history[edge]=(strikes,now,fingerprint)
                # Count once per batch, not once per amount. Repeated failure
                # progressively yields priority to other venues; independent
                # exploration still checks the edge, and success reopens it.
                delay=(5,15,45,120,300)[min(strikes-1,4)]
                self.edge_failures[edge]=now+delay
        self.edge_failures={p:t for p,t in self.edge_failures.items() if t>now}
        self.edge_failure_history={p:v for p,v in self.edge_failure_history.items()
                                   if p[0] in self.pools and now-v[1]<900}
