"""Price-change wakeups independent of historical log catch-up.

Read each market's actual oracle price, preserving nested adapters and caller
context. These values only prioritize scanning; they never authorize a trade.
"""
import threading
import time
from .core import RPC,data,atomic_json,safe_error
from .morpho_universe import optional_views
from .context import simulation_context


class OracleTriggers:
    def __init__(self,universe,config,stop,output):
        self.u,self.config,self.stop,self.output=universe,config,stop,output
        self.last={};self.hot_cursor=0;self.cold_cursor=0;self.clock_provider=None
        self.batch_limit=96;self.success_streak=0;self.last_success_at=None
        self.thread=threading.Thread(target=self.run,name='oracle-price-wakeups',daemon=True)

    def record_success(self):
        self.last_success_at=time.time();self.success_streak+=1
        if self.success_streak>=8:
            self.batch_limit=min(96,self.batch_limit+12);self.success_streak=0

    def record_failure(self):
        # A large oracle Multicall can exceed a public node's short latency
        # budget. Rotate smaller pages instead of repeating the same load.
        self.batch_limit=max(12,self.batch_limit//2);self.success_streak=0

    def poll(self,rpc,anchor=None):
        with self.u.lock:
            refs={};boundaries={}
            for market,m in self.u.markets.items():
                if market not in self.u.books or not m.active:continue
                rows=self.u.books[market].proposals(1)
                boundary=rows[0]['boundary'] if rows else 0
                boundaries[market]=boundary;refs.setdefault(m.oracle,set()).add(market)
        self.last={o:v for o,v in self.last.items() if o in refs}
        # Near-liquidation positions are checked every page; other markets
        # retain rotating slots so a large move or a stale seed is revisited.
        hot=sorted(o for o,markets in refs.items() if o in self.last and
                   any(boundaries[m]>0 and self.last[o]<=boundaries[m]*105//100 for m in markets))
        hot_set=set(hot);cold=sorted(set(refs)-hot_set)
        def page(rows,cursor,limit):
            if not rows:return [],cursor
            offset=cursor%len(rows);chosen=(rows[offset:]+rows[:offset])[:limit]
            return chosen,offset+len(chosen)
        hot_limit=self.batch_limit-max(1,self.batch_limit//3) if cold else self.batch_limit
        selected,next_hot=page(hot,self.hot_cursor,hot_limit)
        extra,next_cold=page(cold,self.cold_cursor,self.batch_limit-len(selected));selected+=extra
        if not selected:return {'oracles':0,'read':0,'changed':0,'triggered_markets':[]}
        hint=self.clock_provider() if anchor is None and self.clock_provider else None
        if hint and 0<=time.monotonic_ns()-hint.get('requested_ns',0)<=1_500_000_000:
            clock=simulation_context(hint['head'],hint.get('pending_head'))
        else:clock=simulation_context(anchor or rpc.header())
        # Delayed oracle adapters can change at the next execution timestamp
        # without another feed update. Match the full execution's block clock.
        try:values=optional_views(rpc,[(o,data('price()')) for o in selected],'pending',context=clock)
        except Exception:
            # Retry a failed large page at the smaller capacity. If even the
            # smallest page fails, rotate so one slow oracle cannot freeze all
            # others; the failed oracles return on a later rotation.
            if self.batch_limit<=12:self.hot_cursor,self.cold_cursor=next_hot,next_cold
            raise
        self.hot_cursor,self.cold_cursor=next_hot,next_cold
        markets=set();crossed=set();changed=0;valid=0
        for oracle,raw in zip(selected,values):
            if raw is None or len(raw)!=32:continue
            value=int.from_bytes(raw,'big')
            if value<=0:continue
            valid+=1;previous=self.last.get(oracle)
            if previous!=value:
                changed+=1;markets.update(refs[oracle])
                for market in refs[oracle]:
                    if previous is not None and value<boundaries[market]<=previous:crossed.add(market)
                self.last[oracle]=value
        # Promote newly crossed boundaries ahead of already queued background
        # work. Existing underwater positions still retain their fair scan slots.
        ordered=sorted(markets,key=lambda m:(m not in crossed,m))
        for market in reversed(ordered):self.u.trigger(market,priority=True)
        return {'oracles':len(refs),'requested':len(selected),'read':valid,'changed':changed,'near_boundary_oracles':len(hot),
                'crossed_markets':sorted(crossed),'triggered_markets':ordered,'execution_clock':clock}

    def run(self):
        url=self.config['policy'].get('discovery_rpc_url','https://mainnet.base.org')
        rpc=RPC(url,timeout=2,cancel_event=self.stop);verified=False
        try:
            while not self.stop.is_set():
                started=time.monotonic();delay=1.0
                try:
                    if not verified:rpc.verify_chain();verified=True
                    result=self.poll(rpc)
                    if not result['oracles']:delay=2.0
                    if result['read']:self.record_success()
                    atomic_json(self.output/'oracle-trigger-health.json',{'at':time.time(),'state':'ready' if result['read'] else 'idle',
                        'read_ms':round((time.monotonic()-started)*1000,3),**result,
                        'next_page_limit':self.batch_limit,'last_success_at':self.last_success_at,
                        'role':'scan_priority_only','execution_authority':False})
                except Exception as exc:
                    self.record_failure()
                    timing=rpc.measurements[-1] if rpc.measurements else {}
                    atomic_json(self.output/'oracle-trigger-health.json',{'at':time.time(),'state':'retrying','error':safe_error(exc),
                        'next_page_limit':self.batch_limit,'last_success_at':self.last_success_at,
                        'failed_read':{k:timing.get(k) for k in ('method','elapsed_ms','network_total_ms','public_limiter_wait_ms','http_status')}})
                self.stop.wait(max(.1,delay-(time.monotonic()-started)))
        finally:rpc.close()
