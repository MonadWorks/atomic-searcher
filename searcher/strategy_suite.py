"""Isolated read-only strategy lanes feeding one existing account executor.

Each lane owns its RPC/client and latest candidate slot. No worker receives a
signer or opens the account ledger. Only consume(), on the runner thread, sends.
"""
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass,asdict
import threading
import time
import httpx
from eth_abi import decode
from .core import RPC,USDC,WETH,check,data,unhex,number,atomic_json,safe_error
from .execution import simulation_context,execution_failure_details
from .structural import HOPS,artifact
from .protocol_scanners import Scanners

class StopScope:
    def __init__(self,parent):self.parent=parent;self.local=threading.Event()
    def is_set(self):return self.parent.is_set() or self.local.is_set()
    def set(self):self.local.set()
    def wait(self,seconds):
        end=time.monotonic()+max(0,seconds)
        while not self.is_set():
            left=end-time.monotonic()
            if left<=0:break
            self.local.wait(min(.05,left))
        return self.is_set()

class ReadBudget:
    def __init__(self,rate=4):
        self.rate=rate;self.lock=threading.Lock();self.next=0.;self.queue=deque();self.urgent_units=0
    def acquire(self,rpc,cost):
        ticket=(object(),bool(getattr(rpc,'suite_urgent',False)))
        with self.lock:self.queue.append(ticket)
        try:
            while True:
                rpc.check_cancelled()
                with self.lock:
                    now=time.monotonic()
                    urgent=next((t for t in self.queue if t[1]),None)
                    ordinary=next((t for t in self.queue if not t[1]),None)
                    chosen=(ordinary if ordinary is not None and self.urgent_units>=2
                            else urgent if urgent is not None else ordinary)
                    if chosen is ticket and now>=self.next:
                        self.queue.remove(ticket);self.next=now+cost/self.rate
                        self.urgent_units=self.urgent_units+cost if ticket[1] else 0
                        return
                time.sleep(.02)
        finally:
            with self.lock:
                if ticket in self.queue:self.queue.remove(ticket)

class SuiteRPC(RPC):
    def __init__(self,budget,*args,**kwargs):super().__init__(*args,**kwargs);self.suite_budget=budget
    def verify_chain(self):
        # These are read-only discovery clients. Recheck each endpoint at
        # startup, on a URL change, and once per minute, rather than spending
        # one rate-limited method on chainId before every scan iteration.
        now=time.monotonic();key=(self.url,self.chain_id)
        if getattr(self,'verified_suite_chain',None)==key and now-getattr(self,'verified_suite_at',0)<60:return
        super().verify_chain()
        self.verified_suite_chain=key;self.verified_suite_at=now
    @contextmanager
    def fresh_reads(self,count):
        # Reserve BOTH budgets before selecting the clock. The public limiter
        # is shared with the old strategies and can otherwise split a frame.
        self.suite_budget.acquire(self,count)
        limiter=self.public_rate_limiter
        if limiter is not None:
            limiter.acquire(count,self.cancel_event,critical=True)
        self.prepaid_reads=count;self.public_prepaid_reads=count if limiter is not None else 0
        try:yield
        finally:self.prepaid_reads=0;self.public_prepaid_reads=0
    def read_slot(self):return self.fresh_reads(1)
    def frame_reads(self):return self.fresh_reads(4)
    def post(self,payload,*,prepare=None):
        cost=len(payload) if isinstance(payload,list) else 1
        if getattr(self,'prepaid_reads',0)>=cost:self.prepaid_reads-=cost
        else:self.suite_budget.acquire(self,cost)
        limiter=self.public_rate_limiter
        if limiter is not None and getattr(self,'public_prepaid_reads',0)>=cost:
            self.public_prepaid_reads-=cost;self.public_rate_limiter=None
        try:return super().post(payload,prepare=prepare)
        finally:self.public_rate_limiter=limiter

@dataclass
class Job:
    lane:str
    candidate:dict
    route:object
    states:dict
    anchor:dict
    deadline:int

class Mailbox:
    def __init__(self):self.lock=threading.Lock();self.ready=threading.Condition(self.lock);self.slots={};self.cursor=0
    def offer(self,job):
        with self.ready:
            self.slots[job.lane]=job
            self.ready.notify_all()
    def wait(self,timeout,stop):
        deadline=time.monotonic()+max(0,timeout)
        with self.ready:
            while not stop.is_set():
                now=time.monotonic_ns()
                if any(job.deadline>now for job in self.slots.values()):return
                remaining=deadline-time.monotonic()
                if remaining<=0:return
                self.ready.wait(min(.05,remaining))
    def take(self,now=None):
        now=time.monotonic_ns() if now is None else now
        with self.lock:
            self.slots={k:v for k,v in self.slots.items() if v.deadline>now}
            if not self.slots:return None
            # Rotate lanes; USDC and WETH raw-unit profits are not comparable.
            names=sorted(self.slots);name=names[self.cursor%len(names)];self.cursor+=1
            return self.slots.pop(name)

class Suite:
    LANES=('compound','euler','preliquidation','midnight_liquidation','midnight_books','dex')
    def __init__(self,config,universe,contracts,context_provider,stop,output,url,finish=float('inf')):
        self.config,self.u,self.contracts,self.context_provider=config,universe,dict(contracts),context_provider
        self.stop,self.output,self.url,self.finish=stop,output,url,finish
        total=config.get('strategy_suite',{}).get('read_methods_per_second',6)
        v4_rate=min(2,total/2) if config.get('v4',{}).get('enabled') else 0
        self.budget=ReadBudget(total-v4_rate);self.v4_budget=ReadBudget(v4_rate or 1)
        from .dex_feedback import DexFeedback
        self.dex_feedback=DexFeedback(config['owner'],contracts,output)
        self.mailbox=Mailbox();self.threads=[];self.health={};self.lock=threading.RLock();self.v4=None;self.v4_scope=None
        from .dex_activity import DexActivity
        self.activity=DexActivity(universe,config,stop,output,finish)
    def report(self,lane,**values):
        with self.lock:
            old=self.health.setdefault(lane,{'execution_ready':True,'complete_quotes':0,'submitted':0})
            if 'error' not in values:old.pop('error',None)
            old.update(at=time.time(),**values)
            atomic_json(self.output/'strategy-suite.json',{'at':time.time(),'lanes':self.health})
    def rpc(self,scope):return SuiteRPC(self.budget,self.url,timeout=3,cancel_event=scope)
    def start(self):
        self.activity.start()
        for lane in self.LANES+(('venues',) if self.config.get('venues',{}).get('enabled',False) else ()):
            self.report(lane,state='starting')
            t=threading.Thread(target=self.run_lane,args=(lane,),name='suite-'+lane,daemon=True);self.threads.append(t);t.start()
        t=threading.Thread(target=self.run_dex_diagnostics,name='suite-dex-diagnostics',daemon=True)
        self.threads.append(t);t.start()
        if self.config.get('v4',{}).get('enabled'):
            t=threading.Thread(target=self.run_v4,name='suite-v4-supervisor',daemon=True);self.threads.append(t);t.start()
    def run_dex_diagnostics(self):
        scope=StopScope(self.stop);rpc=self.rpc(scope)
        verified=False
        try:
            while not scope.is_set() and time.monotonic()<self.finish:
                try:
                    if not verified:rpc.verify_chain();verified=True
                    self.dex_feedback.diagnose_one(rpc)
                    self.dex_feedback.publish()
                except Exception as exc:
                    if scope.is_set():break
                    atomic_json(self.output/'dex-diagnostic-health.json',{'at':time.time(),'state':'retrying','error':safe_error(exc)})
                scope.wait(10)
        finally:rpc.close()
    def current(self):
        c=self.context_provider()
        check(c is not None and time.monotonic_ns()-c['requested_ns']<1_500_000_000,'account context refreshing')
        return c,c['head'],simulation_context(c['head'],c['pending_head'])
    def venue_regular_prices(self):
        from .dex_screen import edges_for
        scheduler=getattr(self,'dex_scheduler',None)
        if scheduler is None:return {}
        samples=dict(scheduler.samples);pools=dict(scheduler.pools);now=time.monotonic();rates={}
        for address,(row,at,block) in samples.items():
            if now-at>30 or address not in pools:continue
            for edge in edges_for(pools[address],row):rates[address,edge.hop[1]]=edge.rate
        return rates
    def run_lane(self,lane):
        scope=StopScope(self.stop)
        with httpx.Client(timeout=5) as client:
            rpc=self.rpc(scope)
            try:
                scanner=Scanners(rpc,client,self.config,self.u,self.contracts,lambda **v:self.report(lane,**v),scope)
                while not scope.is_set() and time.monotonic()<self.finish:
                    try:
                        rpc.verify_chain()
                        if lane=='dex':
                            # Fresh event work gets up to two read units before
                            # a waiting background reader. Keep the total rate
                            # unchanged and preserve other lanes' progress.
                            rpc.suite_urgent=self.activity.fresh()
                            try:self.dex(rpc,scope)
                            finally:rpc.suite_urgent=False
                            continue
                        if lane=='venues':
                            from .venue import Scanner
                            proposals=Scanner(rpc,self.config,self.u,self.contracts['VenueExecutor'],scope,lambda **v:self.report(lane,**v),regular_prices=self.venue_regular_prices).proposals()
                        else:proposals=getattr(scanner,lane)()
                        for proposal in proposals:
                            if scope.is_set() or time.monotonic()>=self.finish:break
                            if proposal is not None:
                                route,amount=proposal
                                try:
                                    with rpc.read_slot():
                                        c,anchor,clock=self.current()
                                        from .structural_runner import complete_quote
                                        candidate,started=complete_quote(rpc,self.config,self.contracts[route.contract_name],route,amount,anchor,clock,c['nonce'])
                                    self.offer(lane,candidate,route,{},anchor,started)
                                except Exception as exc:
                                    self.report(lane,state='scanning',detail='complete_quote_rejected',error=safe_error(exc),route_key=route.key)
                                    atomic_json(self.output/f'suite-{lane}-latest-quote.json',{'at':time.time(),'stage':'complete_quote_rejected','strategy':route.strategy,
                                        'amount_in':amount,'arguments':route.arguments.hex(),'pools':route.pools,'assets':route.assets,'error':safe_error(exc)})
                            scope.wait(.15)
                    except Exception as exc:
                        if scope.is_set():break
                        self.report(lane,state='retrying',error=safe_error(exc))
                        scope.wait(.1 if any(s in str(exc) for s in
                            ('account context refreshing','event clock refreshing','event quote admission expired')) else 5)
            except Exception as exc:self.report(lane,state='retrying',error=safe_error(exc))
            finally:rpc.close()
    def offer(self,lane,candidate,route,states,anchor,started):
        deadline=started+int(self.config['policy']['snapshot_max_age_ms']*1e6)
        if time.monotonic_ns()>=deadline:
            self.report(lane,state='scanning',detail='complete_quote_expired');return
        self.mailbox.offer(Job(lane,candidate,route,states,anchor,deadline))
        with self.lock:count=self.health[lane]['complete_quotes']+1
        self.report(lane,state='candidate_ready',detail='complete_execution_profit_passed',complete_quotes=count,
                    gross_surplus=candidate['gross_surplus'],minimum_gross_surplus=candidate['minimum_gross_surplus'],block=number(anchor['number']))
    def consume(self,executor):
        job=self.mailbox.take()
        if job is not None:
            execution_started=time.monotonic()
            try:result=executor.execute(job.candidate,job.route,job.states,job.anchor,job.deadline)
            except Exception as exc:
                pending=executor.db.execute("SELECT i.tx_hash FROM intents i JOIN attempts a ON a.id=i.attempt WHERE a.state='submitted' AND json_extract(i.payload,'$.route_key')=?",(job.route.key,)).fetchone()
                result=({'status':'submitted','tx_hash':pending[0],'receipt_pending':True,'reason':safe_error(exc)} if pending else {'status':'execution_rejected','reason':safe_error(exc)})
                failure=execution_failure_details(exc)
                if result['status']=='execution_rejected':
                    result['failure']={k:failure[k] for k in ('revert_name','revert_selector','gas_attempts','anchor_mismatch','context_timing') if k in failure}
                    if failure.get('execution_probe'):
                        result['failure']['probe']={k:failure['execution_probe'][k] for k in ('stage','started_utc','elapsed_ms') if k in failure['execution_probe']}
            result['execution_elapsed_ms']=1000*(time.monotonic()-execution_started)
            if job.candidate.get('execution_anchor_rollover'):
                result['execution_anchor_rollover']=job.candidate['execution_anchor_rollover']
            with self.lock:sent=self.health[job.lane]['submitted']+(result['status']=='submitted')
            self.report(job.lane,state='scanning',detail=result['status'],submitted=sent,last_execution=result)
            return result
        worker=self.v4
        if worker:
            result=worker.consume(executor)
            if result:self.report('v4',state='scanning',detail=result['status'],last_execution=result)
            return result
    def run_v4(self):
        from .v4_runtime import Worker
        while not self.stop.is_set() and time.monotonic()<self.finish:
            scope=StopScope(self.stop);self.v4_scope=scope
            worker=Worker(lambda:SuiteRPC(self.v4_budget,self.url,timeout=3,cancel_event=scope),self.config,self.output,scope,self.finish,
                discovery_rpc_factory=lambda:SuiteRPC(self.budget,self.config['policy'].get('discovery_rpc_url',self.url),timeout=3,cancel_event=scope))
            self.v4=worker;self.report('v4',state='scanning');worker.start()
            next_health=0
            while worker.thread.is_alive() and not self.stop.is_set():
                worker.thread.join(.2)
                if time.monotonic()>=next_health:
                    self.report('v4',state='scanning',detail=worker.health.get('state','starting'),worker_at=worker.health.get('at'))
                    next_health=time.monotonic()+5
            if self.stop.is_set():scope.set();return
            self.report('v4',state='retrying',detail='lane_worker_restarting');self.stop.wait(5)
    def dex(self,rpc,scope):
        from .pools import Pool
        from .routes import Route
        from .costs import cost_budget,quote_profit_floor
        from .morpho_universe import optional_views
        from .engine import probe_cycle
        from .dex_screen import Scheduler,read_markets
        with self.u.lock:pools=dict(self.u.graph.pools)
        if not pools:scope.wait(1);return
        if not hasattr(self,'dex_scheduler'):
            self.dex_scheduler=Scheduler(self.config['settlement_tokens'],self.config['policy']['max_hops'],feedback=self.dex_feedback)
        scheduler=self.dex_scheduler;scheduler.sync(pools)
        generation,events=self.activity.inbox.drain_batch(time.monotonic())
        fast_events=self.activity.fast_inbox.drain(time.monotonic())
        direct=scheduler.event_quotes(fast_events)
        scheduler.prioritize(events)
        now=time.monotonic()
        # Cached topology/depth supplies sizing only. The first network call on
        # a fresh event is now the actual native cycle, not a 96-pool sweep.
        # Reserve a periodic discovery turn so a busy pair cannot freeze setup.
        if direct and now-getattr(self,'dex_last_background',0)<5:
            self.dex_quote(rpc,direct,pools,Pool,Route,cost_budget,quote_profit_floor,optional_views,probe_cycle,event_batch=fast_events)
            return
        if not events and now<getattr(self,'dex_next_background',0):
            self.activity.inbox.wait(min(.5,self.dex_next_background-now),after=generation,stop=scope)
            return
        with rpc.read_slot():
            # Waiting for the shared account clock is not a failed pool read.
            # Select a page only once that clock is fresh, so retries cannot
            # consume the new-pool queue or shrink the native batch size.
            _,anchor,clock=self.current()
            scheduler.prioritize(self.activity.inbox.drain(time.monotonic()))
            page=scheduler.page();read_started=time.monotonic();read_requested=time.time()
            try:
                rows,block=read_markets(rpc,page,clock)
            except Exception:
                scheduler.page_size=max(16,scheduler.page_size//2)
                raise
        read_received=time.time()
        changed=scheduler.record(page,rows,block)
        self.dex_last_background=time.monotonic();self.dex_next_background=self.dex_last_background+1
        self.activity.prepare_stream(scheduler.stream_priority())
        scheduler.page_size=min(96,scheduler.page_size+8)
        quotes=scheduler.quotes()
        atomic_json(self.output/'dex-screen-health.json',{'at':time.time(),'state':'ready',
            'snapshot_read_ms':1000*(time.monotonic()-read_started),'block':block,
            'read_pools':len(page),'changed_pools':changed,'quote_paths':len(quotes),
            'activity_pool_reads':scheduler.last_activity_reads,
            'graph_updates':scheduler.graph_updates,**scheduler.last_search,
            'execution_evidence':False})
        try:
            if quotes:
                self.dex_quote(rpc,quotes,pools,Pool,Route,cost_budget,quote_profit_floor,optional_views,probe_cycle)
        finally:
            # Save actual returned prices after the trading hot path. Event
            # arrival order alone cannot prove a price preceded a competitor.
            self.activity.record_read(page,rows,scheduler.last_activity_reads,
                read_requested,read_received,anchor,clock)
        self.dex_feedback.publish()
        scope.wait(.1)
    def dex_quote(self,rpc,quotes,pools,Pool,Route,cost_budget,quote_profit_floor,optional_views,probe_cycle,event_batch=None):
        with rpc.read_slot():
            _,anchor,clock=self.current();lens='0x000000000000000000000000000000000000c1d6'
            if event_batch:
                check(time.monotonic()-max(at for _,at in event_batch)<=1,'event quote admission expired')
                check(number(clock['number'])>=self.activity.required_block(event_batch),'event clock refreshing')
            quote_started=time.time()
            raw=rpc.request('eth_call',[{'to':lens,'gas':hex(15_000_000),'data':data('quoteExecutableCyclesDetailed('+HOPS+'[],uint256[],uint256)',[HOPS+'[]','uint256[]','uint256'],[[p for p,a,s in quotes],[a for p,a,s in quotes],1_500_000])},'pending',{lens:{'code':artifact('CycleQuoteLens')['deployedBytecode']['object']}},clock])
        outputs,inputs,failed_hops,failed_amounts=decode(['uint256[]','uint256[]','uint8[]','uint256[]'],unhex(raw))
        check(len(outputs)==len(inputs)==len(failed_hops)==len(failed_amounts)==len(quotes),'incomplete cycle quotes')
        budgets=[cost_budget(self.config['policy'],s['address'],number(anchor['baseFeePerGas']),len(path))
                 for path,_,s in quotes]
        floors=[b.settlement_cost_raw+quote_profit_floor(self.config,s,b.settlement_cost_raw)
                for (_,_,s),b in zip(quotes,budgets)]
        if hasattr(self,'dex_scheduler'):
            self.dex_scheduler.native_results(quotes,outputs,failed_hops,
                [amount>0 and out>=amount+floor for out,amount,floor in zip(outputs,inputs,floors)])
            self.activity.prepare_stream(self.dex_scheduler.stream_priority())
        page_id=getattr(self,'dex_quote_page',0)+1;self.dex_quote_page=page_id
        observed={'at':time.time(),'requested_at':quote_started,'block':number(anchor['number']),
            'trigger':'prepared_event' if event_batch else 'background_screen',
            'event_to_request_ms':1000*(time.monotonic()-max(at for _,at in event_batch))-(time.time()-quote_started)*1000 if event_batch else None,
            'stage':'native_quote_only','page':page_id,'quotes':[
                {'pools':[h[0] for h in p],'tokens':[p[0][1],*(h[2] for h in p)],
                 'requested_input':a,'effective_input':amount,'output':out,'gross_difference':out-amount,
                 'failed_hop':failed,'failed_hop_input':failed_amount,
                 'minimum_gross_surplus':floor,'clears_native_cost_floor':amount>0 and out>=amount+floor}
                for (p,a,s),out,amount,failed,failed_amount,floor in zip(quotes,outputs,inputs,failed_hops,failed_amounts,floors)]}
        atomic_json(self.output/f'dex-quote-{page_id%64}.json',observed)
        self.report('dex',state='scanning',detail='native_cycles_checked',block=number(anchor['number']),
                    native_quote_pages=page_id,native_positive=sum(o>a>0 for o,a in zip(outputs,inputs)))
        for (path,requested,settlement),out,amount,budget,floor in zip(quotes,outputs,inputs,budgets,floors):
            check(0<=amount<=requested,'invalid funded quote amount')
            if amount==0:continue
            if out<amount+floor:continue
            if any(h[3]==4 for h in path):
                self.conversion_cycle_quote(rpc,path,amount,max(1,out-amount))
                continue
            if not self.dex_feedback.allowed(path):continue
            route=Route((path[0][1],*(h[2] for h in path)),tuple(h[0] for h in path));states={}
            samples=getattr(getattr(self,'dex_scheduler',None),'samples',{})
            missing=[h[0] for h in path if h[3]==2 and not
                     (samples.get(h[0]) and samples[h[0]][0][0])]
            fallback=dict(zip(missing,optional_views(rpc,[(a,data('fee()')) for a in missing]))) if missing else {}
            for h in path:
                p=pools[h[0]]
                sample=samples.get(p.address)
                # screenPools already returned this fee. The execution contract
                # enforces it as maxFee and executes the real pool swap; a later
                # increase rejects instead of silently widening the fee bound.
                fee=3000 if h[3]==3 else int(sample[0][7]) if sample and sample[0][0] else int(p.key) if h[3]==1 else int.from_bytes(fallback.get(p.address) or b'','big')
                check((h[3]!=2 or sample and sample[0][0] or fallback.get(p.address) is not None)
                      and 0<=fee<1_000_000,'invalid cycle fee')
                states[p.address]=Pool(p.address,p.factory,{1:'v3',2:'slipstream',3:'v2'}[p.kind],p.token0,p.token1,p.key if p.kind==2 else 1,fee)
            attempted=False;passed=False
            try:
                with rpc.read_slot():
                    _,anchor,clock=self.current();started=time.monotonic_ns()
                    if not self.dex_feedback.admit(path):continue
                    attempted=True
                    proof=probe_cycle(rpc,route,states,amount,floor,anchor=anchor,block_context=clock)
                    passed=True
                self.dex_feedback.passed(path,amount,block=number(anchor['number']),gross=proof['gross_surplus'],floor=floor)
                gas=max(budget.gas_limit,proof['first_transaction_gas_budget'])
                fee=gas*budget.max_fee_per_gas+budget.l1_fee_allowance
                check(gas<=self.config['policy']['max_gas'] and fee<=int(self.config['policy']['max_total_fee_wei']),'cycle measured fee cap')
                fee_raw=fee if settlement['address']==WETH else (fee*self.config['policy']['eth_usdc_ceiling']+10**12-1)//10**12
                floor=fee_raw+quote_profit_floor(self.config,settlement,fee_raw)
                if proof['gross_surplus']<floor:continue
                candidate={'execution_kind':'mixed-v1','allow_execution_anchor_rollover':True,
                    'execution_gas_estimate':proof['first_transaction_gas_budget'],
                    'execution_gas_source':'whole_cycle_probe','route_key':route.key,'pools':list(route.pools),'tokens':list(route.tokens),'amount_in':amount,
                    'gross_surplus':proof['gross_surplus'],'minimum_gross_surplus':floor,'cost_budget':{'gas_limit':gas,'max_fee_per_gas':budget.max_fee_per_gas,'priority_fee':budget.priority_fee,'total_fee_wei':fee}}
                self.offer('dex',candidate,route,states,anchor,started)
            except Exception as exc:
                failure=execution_failure_details(exc)
                if attempted and not passed:
                    self.dex_feedback.rejected(path,amount,failure,block=number(anchor['number']),gross=out-amount,floor=floor)
                self.report('dex',state='scanning',detail='complete_quote_rejected',error=safe_error(exc))

    def conversion_cycle_quote(self,rpc,path,amount,residual_hint):
        from .structural import cycle_route
        from .structural_runner import complete_quote
        from .structural_screening import UnprofitableQuote
        if not self.dex_feedback.allowed(path):return
        funding=path[0][1]
        conversions=[()] if funding==USDC else (self.u.route_book.ranked(funding,USDC,residual_hint,2)
            if self.u.route_book else self.u.graph.direct(funding,USDC))
        for conversion in conversions:
            route=None;attempted=False
            try:
                route=cycle_route(path,conversion)
                with rpc.read_slot():
                    c,anchor,clock=self.current()
                    if not self.dex_feedback.admit(path,route.key):continue
                    attempted=True
                    candidate,started=complete_quote(rpc,self.config,self.contracts['StructuralExecutor'],route,amount,anchor,clock,c['nonce'])
                self.dex_feedback.passed(path,amount,block=number(anchor['number']),gross=candidate['gross_surplus'],floor=candidate['minimum_gross_surplus'],execution_key=route.key)
                self.offer('dex',candidate,route,{},anchor,started)
            except Exception as exc:
                if isinstance(exc,UnprofitableQuote):
                    self.dex_feedback.passed(path,amount,block=number(anchor['number']),gross=exc.gross,floor=exc.required,execution_key=route.key)
                elif attempted:
                    self.dex_feedback.rejected(path,amount,execution_failure_details(exc),block=number(anchor['number']),gross=residual_hint,floor=None,execution_key=route.key)
                self.report('dex',state='scanning',detail='funded_cycle_quote_rejected',error=safe_error(exc))
                atomic_json(self.output/'suite-dex-latest-quote.json',{'at':time.time(),
                    'stage':'funded_cycle_quote_rejected','strategy':4,'amount_in':amount,
                    'arguments':route.arguments.hex() if route else None,
                    'pools':[h[0] for h in path],'funding':funding,'error':safe_error(exc)})
    def close(self):
        self.activity.close()
        if self.v4_scope:self.v4_scope.set()
        for t in self.threads:
            if t.is_alive():t.join(timeout=4)
