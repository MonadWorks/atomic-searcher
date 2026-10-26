"""Prepared borrower lane, isolated from wide DEX discovery and its queues.

Public APIs enumerate identities only. Every candidate uses an accrued on-chain
read, a complete callback execution, and the common executor's final check.
"""
import fcntl
import json
import os
import signal
import threading
import time
import gzip
from fractions import Fraction
from collections import deque
from pathlib import Path
import httpx
from eth_abi import decode
from .core import ROOT,RPC,USDC,check,data,number,unhex,atomic_json,safe_error,Reject
from .context import simulation_context
from .structural_screening import UnprofitableQuote,ProbeSchedule
from .structural import (MORPHO,MARKET,ORACLE,PSM,SUSDS,PSM_POOLS,inspect,morpho_route,
                         psm_route,encode_plan,cap,market_route)

def fetch_seed(client,health_factor=1.15):
    rows=[];total=None
    for skip in range(0,10000,100):
        query='''{marketPositions(first:100,skip:%d,orderBy:HealthFactor,orderDirection:Asc,
          where:{chainId_in:[8453],marketUniqueKey_in:["%s"],borrowShares_gte:1,healthFactor_lte:%s}){
          items{user{address}} pageInfo{countTotal}}}'''%(skip,MARKET,health_factor)
        response=client.post('https://api.morpho.org/graphql',json={'query':query});response.raise_for_status()
        payload=response.json();check(not payload.get('errors'),'Morpho seed API rejected query')
        page=payload['data']['marketPositions'];count=int(page['pageInfo']['countTotal'])
        if total is None:total=count
        check(total==count,'Morpho seed changed during pagination')
        rows.extend(r['user']['address'].lower() for r in page['items'])
        if len(rows)>=total:break
        check(page['items'],'incomplete borrower pagination')
    check(total==len(rows)==len(set(rows)),'incomplete or duplicated borrower seed')
    return rows

class BorrowerBook:
    def __init__(self):
        self.lock=threading.RLock();self.rows={};self.unknown=deque();self.pending=set();self.cursor=0;self._ranked=None
    def add(self,addresses):
        with self.lock:
            for a in addresses:
                if a not in self.pending:
                    self.pending.add(a);self.unknown.append(a)
    def seed(self,borrower,state,lltv):
        """Index amounts are ranking hints, never execution evidence."""
        shares=int(state.get('borrowShares') or 0);debt=int(state.get('borrowAssets') or 0);collateral=int(state.get('collateral') or 0)
        if min(shares,debt,collateral)<=0 or max(shares,debt,collateral)>=1<<128 or not 0<lltv<10**18:return
        threshold=(debt*10**18+lltv-1)//lltv
        row={'borrower':borrower,'shares':shares,'debt':debt,'collateral':collateral,
             'boundary':(threshold*10**36+collateral-1)//collateral,'state_source':'index_hint'}
        with self.lock:
            old=self.rows.get(borrower)
            if old is None or old.get('state_source')=='index_hint':
                self.rows[borrower]=row;self._ranked=None
            elif (old['shares']!=shares or old['collateral']!=collateral) and borrower not in self.pending:
                # A missed Borrow/SupplyCollateral event must not permanently
                # hide a reopened position. Refresh it without overwriting
                # authoritative state with index values.
                self.pending.add(borrower);self.unknown.append(borrower)
    def batch(self,size=64):
        with self.lock:
            if size<4:
                check(size>0,'invalid borrower read size')
                batch=[]
                while self.unknown and len(batch)<size:
                    a=self.unknown.popleft();self.pending.discard(a);batch.append(a)
                ranked=self.proposals(len(self.rows))
                for offset in range(min(size,len(ranked))):
                    a=ranked[(self.cursor+offset)%len(ranked)]['borrower']
                    if a not in batch and len(batch)<size:batch.append(a)
                if ranked:self.cursor=(self.cursor+size)%len(ranked)
                return batch
            # Reserve changed/new, hot, and cold capacity independently.
            batch=[]
            while self.unknown and len(batch)<size//4:
                a=self.unknown.popleft();self.pending.discard(a);batch.append(a)
            ranked=self.proposals(len(self.rows))
            hot=ranked[:size//2];cold=ranked[size//2:]
            selected=hot
            if cold:
                selected=hot+[cold[(self.cursor+i)%len(cold)] for i in range(min(size//4,len(cold)))]
                self.cursor=(self.cursor+size//4)%len(cold)
            for r in selected:
                if r['borrower'] not in batch and len(batch)<size:batch.append(r['borrower'])
            return batch
    def update(self,rows):
        with self.lock:
            for row in rows:self.rows[row['borrower']]=row
            self._ranked=None
    def proposals(self,size=8):
        with self.lock:
            if self._ranked is None:
                self._ranked=sorted((r for r in self.rows.values() if r['shares']>0 and r['collateral']>0),key=lambda r:(-r['boundary'],-r['debt']))
            return self._ranked[:size]
    def identities(self):
        with self.lock:return sorted(set(self.rows)|self.pending)
    def has_candidates(self):
        with self.lock:return bool(self.pending) or any(r['shares']>0 and r['collateral']>0 for r in self.rows.values())
    def snapshot(self):
        with self.lock:return list(self.rows.values())

def liquidation_batches(rows,price,maximum,max_batch=8,lltv=77*10**16,loan_decimals=6):
    eligible=sorted((r for r in rows if r['shares']>0 and r['collateral']>0 and price<r['boundary']),key=lambda r:-r['debt'])
    # Separate potentially bad-debt positions: a reverted large position must
    # not monopolize every batch; successful borrowers retain their own floor.
    result=[];group=[];total=0
    check(1<=max_batch<=8,'invalid liquidation batch bound')
    # Incentive is derived from the actual market LLTV.
    # Insolvent positions need a collateral-affordable PARTIAL repayment.
    incentive=min(115*10**16,10**36//(10**18-3*10**17*(10**18-lltv)//10**18))
    for r in eligible:
        affordable=(r['collateral']*price//10**36)*10**18//incentive
        shares=r['shares']
        if affordable<r['debt']:
            shares=min(shares,shares*affordable*99//(max(1,r['debt'])*100))
        if shares==0:continue
        repayment=(r['debt']*shares+r['shares']-1)//r['shares']
        quantum=max(1,10**max(0,loan_decimals-2))
        debt=((repayment*101+99)//100+quantum)//quantum*quantum
        if debt>maximum:continue
        if group and (len(group)==max_batch or total+debt>maximum):result.append(group);group=[];total=0
        group.append((r['borrower'],shares,debt));total+=debt
    if group:result.append(group)
    return result

def sized_requests(requests):
    """Bounded smaller unwinds when full liquidation exceeds DEX depth."""
    for divisor in (1,2,4,8):
        subset=[(a,s//divisor,(d+divisor-1)//divisor) for a,s,d in requests if s//divisor>0]
        if subset:yield subset

def candidate_for(route,amount,config,anchor,gas=700000):
    p=config['policy'];priority=p['min_priority_fee_wei'];maximum=2*number(anchor['baseFeePerGas'])+priority
    fee=gas*maximum+int(p['l1_fee_allowance_wei'])
    floor=(fee*p['eth_usdc_ceiling']+10**12-1)//10**12
    floor+=max(int(p.get('min_net_profit_usdc_raw',100000)),floor*p.get('min_net_profit_fee_bps',10000)//10000)
    return {'execution_kind':route.execution_kind,'route_key':route.key,'pools':list(route.pools),'tokens':list(route.tokens),
            'amount_in':amount,'minimum_gross_surplus':floor,'gross_surplus':0,
            'model_fee_raw':(fee*p['eth_usdc_ceiling']+10**12-1)//10**12,
            'cost_budget':{'gas_limit':gas,'max_fee_per_gas':maximum,'priority_fee':priority,'total_fee_wei':fee}}

def complete_quote(rpc,config,contract,route,amount,anchor,context,nonce,gas=700000):
    candidate=candidate_for(route,amount,config,anchor,gas)
    p=config['policy'];budget=candidate['cost_budget']
    # The first whole call also measures execution work. Use a small absolute
    # floor here, then price measured gas and enforce the full floor before signing.
    # Only this discarded measurement uses one raw settlement unit. The
    # candidate below MUST satisfy the unchanged net-profit/fee policy before
    # it can reach the signing executor. Small positive results remain visible
    # as economics, instead of being hidden behind a contract revert.
    common=[None,amount,1,budget['max_fee_per_gas'],
        number(anchor['timestamp'])+6,number(anchor['number']),unhex(anchor['hash']),number(anchor['number'])+2,nonce]
    if route.execution_kind=='protocol-v1':
        from .protocol_execution import encode_plan as protocol_encode
        encoded=protocol_encode(candidate,route,common)
    else:encoded=encode_plan(candidate,route,common)
    from .structural import PLAN
    measured=data(f'measure({PLAN})')[:10]+encoded[10:]
    call={'from':config['owner'],'to':contract,'data':measured,
          'gas':hex(p['max_gas']),'maxFeePerGas':hex(budget['max_fee_per_gas']),'maxPriorityFeePerGas':hex(budget['priority_fee'])}
    started=time.monotonic_ns()
    raw=rpc.request('eth_call',[call,'pending',{},context])
    profit,used=decode(['uint256','uint256'],unhex(raw))
    # Include transaction intrinsic/calldata and dispatch costs outside measure().
    estimated=used+70000+16*len(unhex(encoded))
    gas=(estimated*115+99)//100
    check(0<used and gas<=p['max_gas'],'structural measured gas cap')
    candidate=candidate_for(route,amount,config,anchor,gas)
    candidate['execution_gas_estimate']=estimated
    candidate['gross_surplus']=profit
    check(candidate['cost_budget']['total_fee_wei']<=int(p['max_total_fee_wei']),'structural measured fee cap')
    if candidate['gross_surplus']<candidate['minimum_gross_surplus']:
        raise UnprofitableQuote(candidate)
    return candidate,started

def psm_edge(rpc,pool,reverse,block='pending',context=None):
    """Optimistic marginal bound; whole execution still proves finite depth."""
    from .structural import LENS,artifact
    args=[{'to':LENS,'gas':hex(500000),'data':data('inspectPSM(address)',['address'],[pool])},block,
          {LENS:{'code':artifact('MorphoStateLens')['deployedBytecode']['object']}}]
    if context is not None:args.append(context)
    values=decode(['uint160','uint24','uint128','uint256','uint256','uint256'],unhex(rpc.request('eth_call',args)))
    sqrt,fee,liquidity,rate,usdc,susds=values
    check(sqrt>0 and rate>0 and 0<=fee<1_000_000,'invalid PSM market state')
    if not liquidity or (susds==0 if reverse else usdc==0):return Fraction(0),values
    price=Fraction(sqrt*sqrt,1<<192);conversion=Fraction(rate,10**39)
    edge=(price/conversion if reverse else conversion/price)*Fraction(1_000_000-fee,1_000_000)
    return edge,values

def publish_decision(output,decision,sequence):
    """Existing local and remote readers consume this bounded evidence format."""
    decision['observed_at']=decision['at']
    for candidate in decision['candidates']:
        outcome=next((o for o in decision['outcomes'] if o.get('route_key')==candidate['route_key']),{})
        status=outcome.get('status')
        candidate['status']='submitted' if status=='submitted' else 'execution_rejected' if status in ('rejected','not_profitable') else 'quoted_only'
        candidate['execution']=outcome
        candidate['execution_failure']=outcome.get('detail')
        candidate['net_after_modelled_cost']=outcome.get('simulated_gross_profit',candidate['gross_surplus'])-outcome.get('fee_in_settlement',candidate['model_fee_raw'])
    atomic_json(output/f'decision-{sequence}.json',decision)
    old=output/f'decision-{sequence-256}.json'
    if old.exists():old.unlink()
    if decision['candidates']:
        target=output/f'opportunity-{sequence}.json.gz';temp=target.with_suffix('.tmp')
        with gzip.open(temp,'wt') as handle:json.dump({'received_utc':decision['at'],'decision':decision},handle)
        os.replace(temp,target)
        prior=sorted(output.glob('opportunity-*.json.gz'),key=lambda p:int(p.name.split('-')[1].split('.')[0]))
        for path in prior[:-128]:path.unlink()

class SeedWorker:
    def __init__(self,config,book,stop,output,rpc_url):
        self.config,self.book,self.stop,self.output,self.rpc_url=config,book,stop,output,rpc_url
        self.thread=threading.Thread(target=self.run,name='structural-identities',daemon=True)
    def run(self):
        rpc=RPC(self.rpc_url,timeout=4,cancel_event=self.stop);next_seed=0;cursor=None
        with httpx.Client(timeout=10) as client:
            try:
                verified=False
                while not self.stop.is_set():
                    try:
                        if not verified:rpc.verify_chain();verified=True
                        if time.monotonic()>=next_seed:
                            addresses=fetch_seed(client);self.book.add(addresses);next_seed=time.monotonic()+300
                            atomic_json(self.output/'borrower-seed.json',{'at':time.time(),'market':MARKET,'addresses':addresses,'scope':'indexed HF<=1.15; live event additions retained'})
                        head=rpc.header();height=number(head['number'])
                        start=max(0,height-32) if cursor is None else max(0,cursor-2)
                        end=min(height,start+63)
                        if start<=end:
                            logs=rpc.request('eth_getLogs',[{'address':MORPHO,'topics':[None,MARKET],
                                                           'fromBlock':hex(start),'toBlock':hex(end)}])
                            addresses=set()
                            for log in logs:
                                # All indexed actors are cheap seed candidates. Authoritative
                                # position reads decide whether they actually own debt.
                                for t in log.get('topics',[])[2:]:
                                    if int(t,16)>0 and int(t,16)<1<<160:addresses.add('0x'+t[-40:].lower())
                            self.book.add(sorted(addresses));cursor=end+1
                            atomic_json(self.output/'identity-health.json',{'at':time.time(),'state':'connected','block':end,'next_seed_in_seconds':max(0,next_seed-time.monotonic())})
                    except Exception as exc:
                        if self.stop.is_set():break
                        atomic_json(self.output/'identity-health.json',{'at':time.time(),'state':'retrying','error':safe_error(exc)})
                        next_seed=max(next_seed,time.monotonic()+15)
                    self.stop.wait(2)
            finally:rpc.close()

def run(config,seconds=1800,executor=None):
    check(0<=seconds<=86400,'invalid duration')
    evidence=ROOT/'evidence';evidence.mkdir(exist_ok=True)
    lock=(evidence/'runtime.lock').open('a+')
    try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except OSError:lock.close();raise Reject('another searcher session is already running')
    stop=threading.Event();handlers={}
    if threading.current_thread() is threading.main_thread():
        for sig in (signal.SIGINT,signal.SIGTERM):handlers[sig]=signal.signal(sig,lambda *_:stop.set())
    session=evidence/(time.strftime('session-%Y%m%d-%H%M%S')+f'-{os.getpid()}-structural')
    output=session/'pipeline';output.mkdir(parents=True)
    from .artifacts import source_manifest
    atomic_json(session/'source-manifest.json',source_manifest());atomic_json(session/'config.json',config)
    from .morpho_universe import Universe
    from .morpho_worker import UniverseWorker
    universe=Universe(config) if config.get('structural',{}).get('market_universe') else None
    runtime=evidence/'runtime.json';book=BorrowerBook();counts={'decisions':0,'candidates':0,'submitted':0,'recheck_failures':0,'economic_rejections':0,'deferred_quotes':0}
    url=executor.rpc.url if executor else None
    rpc=RPC(url,timeout=3,cancel_event=stop);rpc.critical_reads=True
    worker=(UniverseWorker(config,universe,stop,output,config['policy'].get('discovery_rpc_url',rpc.url)) if universe else
            SeedWorker(config,book,stop,output,config['policy'].get('discovery_rpc_url',rpc.url)))
    quote_cursor={}
    auction_manager=None;route_worker=None;suite=None
    extension=None;result={'state':'stopped'};last_report={};probes=ProbeSchedule();psm_states={};psm_index=0;psm_screen_start=0;next_psm=time.monotonic()+10
    finish=time.monotonic()+seconds if seconds else float('inf')
    try:
        from .recovery import retry_connection
        atomic_json(runtime,{'pid':os.getpid(),'session':str(session),'state':'running','executor_connected':executor is not None,'strategy_mode':'structural'})
        def connect():
            rpc.verify_chain()
            check(rpc.request('eth_getCode',['0x000000000000000000000000000000000000c1d1','latest'])=='0x','state lens collision')
        def waiting(error,delay):
            atomic_json(output/'health.json',{'at':time.time(),'state':'waiting_for_data','phase':'startup_connection',
                'feed':{'state':'retrying'},'error':error,'retry_in_seconds':delay,'counts':counts,
                'executor_connected':executor is not None,'strategy_mode':'structural'})
        retry_connection(connect,stop=stop,on_retry=waiting)
        if executor:
            check(executor.structural_contract is not None,'structural deployment missing')
            executor.bind_stop_event(stop);executor.start_context_worker()
        if universe:
            from .route_preparation import RouteWorker
            route_worker=RouteWorker(universe,config,stop,output)
            if executor:worker.oracle_triggers.clock_provider=lambda:executor.context
            worker.start();route_worker.thread.start()
        else:worker.thread.start()
        from .strategy_access import AccessWorker
        if universe and executor and config.get('structural',{}).get('atlas_enabled'):
            from .atlas_runtime import AuctionWorker
            auction_manager=AuctionWorker(executor,universe,stop,output);auction_manager.start()
        extension=AccessWorker(config,stop,output,universe=universe);extension.thread.start()
        if universe and executor and config.get('strategy_suite',{}).get('enabled'):
            from .strategy_suite import Suite
            suite=Suite(config,universe,{**executor.protocol_contracts,'StructuralExecutor':executor.structural_contract,'AtomicCycle':executor.contract},lambda:executor.context,stop,output,rpc.url,finish)
            suite.start()
        def consume_suite():
            if suite:
                outcome=suite.consume(executor)
                if outcome and outcome.get('status')=='submitted':
                    counts['submitted']+=1;return True
            return False
        print('清算市场、借款仓位与卖出路径正在自动发现；'+('自动执行已连接。' if executor else '数据检查已启动。'),flush=True)
        atomic_json(runtime,{'pid':os.getpid(),'session':str(session),'state':'running','executor_connected':executor is not None,'strategy_mode':'structural'})
        while not stop.is_set() and time.monotonic()<finish:
            iteration=time.monotonic();phase='account_recovery';borrowers=[];market=None
            try:
                if executor:executor.reconcile()
                if consume_suite():continue
                phase='borrower_state'
                context=executor.context if executor else None
                if context is not None:
                    check(time.monotonic_ns()-context['requested_ns']<=1_500_000_000,'account context refreshing')
                    anchor=context['head'];clock=simulation_context(anchor,context['pending_head']);nonce=context['nonce']
                else:
                    anchor,pending=rpc.batch([('eth_getBlockByNumber',['latest',False]),('eth_getBlockByNumber',['pending',False])])
                    clock=simulation_context(anchor,pending);nonce=0
                market=None
                if universe:
                    selected=universe.choose()
                    if selected:market,book=selected
                borrowers=book.batch() if market or not universe else [];routes=[];price=None;rows=[];psm_screened=False;screening=[]
                if borrowers:
                    started=time.monotonic_ns();price,stamp,rows=inspect(rpc,borrowers,'pending',clock,**({'market_id':market.id} if market else {}))
                    check(stamp==number(clock['time']),'borrower state clock mismatch')
                    book.update(rows)
                    if universe:universe.read_succeeded(market.id)
                    fee_price=2*number(anchor['baseFeePerGas'])+config['policy']['min_priority_fee_wei']
                    available_gas=max(0,int(config['policy']['max_total_fee_wei'])-int(config['policy']['l1_fee_allowance_wei']))//max(1,fee_price)
                    batch_size=max(1,min(8,available_gas//460000))
                    groups=liquidation_batches(rows,price,cap(config),batch_size,**({'lltv':market.lltv,'loan_decimals':market.loan_decimals} if market else {}))
                    near=sorted((r for r in rows if r['shares'] and r['collateral']),key=lambda r:-r['boundary'])[:2]
                    paths=universe.routes(market,amount=max((r['collateral'] for r in near),default=1),settlement_amount=max((r['debt']//20 for r in near),default=1)) if universe else None
                    # Visit every group's full size before reduced-size retries.
                    variants=[list(sized_requests(g)) for g in groups]
                    for size_index in range(4):
                        for sizes in variants:
                            if size_index>=len(sizes):continue
                            requests=sizes[size_index]
                            prepared=[market_route(market,requests,a,b) for a,b in paths] if universe else [morpho_route(requests)]
                            for route in prepared:routes.append((route,sum(r[2] for r in requests),max(700000,len(requests)*400000)))
                    last_report={'market':market.id if market else MARKET,'price':price,'checked_borrowers':len(rows),'known_borrowers':len(book.snapshot()),
                        'state_read_ms':(time.monotonic_ns()-started)/1e6,'eligible_in_read':sum(r['shares']>0 and r['collateral']>0 and price<r['boundary'] for r in rows)}
                if consume_suite():continue
                # A bounded PSM size/direction per turn keeps it off the liquidation critical path.
                if not routes and time.monotonic()>=next_psm:
                    if universe:
                        with universe.lock:psm_pools=[p.address for p in universe.graph.pools.values() if p.kind==1 and {p.token0,p.token1}=={USDC,SUSDS}]
                    else:psm_pools=list(PSM_POOLS)
                    if psm_pools:
                        from .psm_planner import discover as psm_proposals
                        # Rotate an unusually large pool family without excluding
                        # later pools. Both directions and sizes share one read.
                        offset=psm_index%len(psm_pools);psm_index+=32
                        ranked_pools=psm_pools[offset:]+psm_pools[:offset]
                        routes=psm_proposals(rpc,ranked_pools,cap(config),int(config['policy']['min_net_profit_usdc_raw']),clock,
                            on_snapshot=psm_states.update,native=True,on_screening=screening.extend,screen_start=psm_screen_start)
                        psm_screen_start+=8
                        psm_screened=True
                    next_psm=time.monotonic()+2
                if consume_suite():continue
                decision={'at':time.time(),'block':number(anchor['number']),'lane':'structural','borrowers':last_report,'candidates':[],'outcomes':[]}
                if screening:
                    decision['screening']=screening
                    counts['economic_rejections']+=sum(r['status']=='not_profitable' for r in screening)
                native_profit={(r['route_key'],r['amount_in']):r['gross_surplus'] for r in screening if r['status']=='native_quote_admitted'}
                borrowers_by_address={r['borrower']:r for r in rows}
                def epoch_for(route,amount):
                    if route.strategy==2:
                        # New blocks can change gas fees or off-range ticks even
                        # when the current price is identical. Never carry an
                        # economic cooldown across that observation boundary.
                        return (number(anchor['number']),tuple(psm_states.get(route.pools[0],())),native_profit.get((route.key,amount)))
                    from .structural import GENERIC,REQUESTS
                    requests=decode([REQUESTS],route.arguments)[0] if route.strategy==1 else decode(GENERIC,route.arguments)[1]
                    return (number(anchor['number']),price,tuple((a,borrowers_by_address[a]['shares'],borrowers_by_address[a]['collateral'])
                        for a,_,_ in requests if a in borrowers_by_address))
                ready_routes=[r for r in routes if probes.ready(r[0],r[1],epoch_for(r[0],r[1]))]
                deferred=len(routes)-len(ready_routes);counts['deferred_quotes']+=deferred
                if deferred:decision['deferred_quotes']=deferred
                if universe and market and ready_routes and not psm_screened:
                    offset=quote_cursor.get(market.id,0)%len(ready_routes)
                    ready_routes=ready_routes[offset:]+ready_routes[:offset];quote_cursor[market.id]=offset+2
                elif psm_screened and ready_routes:
                    offset=quote_cursor.get('psm',0)%len(ready_routes)
                    ready_routes=ready_routes[offset:]+ready_routes[:offset];quote_cursor['psm']=offset+2
                for route,amount,gas in ready_routes[:2]:
                    if consume_suite():break
                    if executor is None:
                        decision['outcomes'].append({'strategy':route.strategy,'status':'executor_required'});continue
                    epoch=epoch_for(route,amount)
                    if not probes.ready(route,amount,epoch):continue
                    phase='whole_execution_quote'
                    try:
                        if route.strategy==2 and not psm_screened:
                            pool,reverse=decode(['address','bool'],route.arguments)
                            edge,_=psm_edge(rpc,pool,reverse,'pending',clock)
                            floor=int(config['policy']['min_net_profit_usdc_raw'])
                            if edge<=1 or amount*(edge-1)<floor:
                                decision['outcomes'].append({'strategy':2,'status':'marginal_edge_below_cost','route_key':route.key})
                                continue
                        candidate,started=complete_quote(rpc,config,executor.structural_contract,route,amount,anchor,clock,nonce,gas)
                        probes.clear(route,amount)
                        counts['candidates']+=1;decision['candidates'].append(candidate)
                        phase='final_preflight_and_submission'
                        outcome=executor.execute(candidate,route,{},anchor,started+int(config['policy']['snapshot_max_age_ms']*1e6))
                        decision['outcomes'].append({'strategy':route.strategy,'route_key':route.key,**outcome})
                        if outcome['status']=='submitted':counts['submitted']+=1;break
                    except UnprofitableQuote as exc:
                        counts['economic_rejections']+=1
                        delay=probes.reject(route,amount,epoch,economic=True)
                        decision['outcomes'].append({'strategy':route.strategy,'route_key':route.key,'amount_in':amount,
                            'phase':phase,**exc.evidence(),'retry_after_seconds':delay})
                    except Exception as exc:
                        from .execution import execution_failure_details,ExecutionAnchorChanged
                        pending=executor.db.execute("SELECT i.tx_hash FROM intents i JOIN attempts a ON a.id=i.attempt WHERE a.state='submitted' AND json_extract(i.payload,'$.route_key')=?",(route.key,)).fetchone()
                        if pending:
                            decision['outcomes'].append({'strategy':route.strategy,'route_key':route.key,'phase':phase,
                                'status':'submitted','tx_hash':pending[0],'receipt_pending':True,'reason':safe_error(exc)})
                            break
                        counts['recheck_failures']+=1
                        decision['outcomes'].append({'strategy':route.strategy,'route_key':route.key,'phase':phase,'status':'rejected','reason':safe_error(exc),'detail':execution_failure_details(exc)})
                        # Re-probe after a new short opportunity window; never an unbounded failure queue.
                        refresh_needed=isinstance(exc,ExecutionAnchorChanged) or any(t in str(exc) for t in ('context stale','observation expired','clock changed','final validation expired'))
                        probes.reject(route,amount,epoch,refresh=refresh_needed)
                counts['decisions']+=1
                publish_decision(output,decision,counts['decisions'])
                atomic_json(output/'latest-decision.json',decision)
                if not suite:
                    atomic_json(output/'activity-stream-health.json',{'at':time.time(),'state':'not_configured','mode':'prepared-position-http'})
                atomic_json(output/'health.json',{'at':time.time(),'state':'running','feed':{'state':'connected','last_block':number(anchor['number'])},
                    'counts':counts,'executor_connected':executor is not None,'strategy_mode':'structural','borrowers':last_report})
            except Exception as exc:
                if stop.is_set():break
                # batch() dequeues new identities before the network read. A
                # transient failure must retain them for the next iteration.
                if phase=='borrower_state' and borrowers:
                    book.add(borrowers)
                    if universe and market and ('0x11' in str(exc) or 'execution reverted' in str(exc)):
                        universe.read_failed(market.id)
                atomic_json(output/'health.json',{'at':time.time(),'state':'degraded','phase':phase,'market':market.id if phase=='borrower_state' and market else None,'error':safe_error(exc),'counts':counts,'executor_connected':executor is not None})
            delay=max(.05,.5-(time.monotonic()-iteration))
            if suite:suite.mailbox.wait(delay,stop)
            else:stop.wait(delay)
        result={'state':'stopped','counts':counts};return {'session':str(session),**result}
    except Exception as exc:
        if stop.is_set():
            result={'state':'stopped','counts':counts}
            return {'session':str(session),**result}
        result={'state':'failed','error':safe_error(exc),'counts':counts}
        raise
    finally:
        stop.set();rpc.close()
        if suite:suite.close()
        if auction_manager:
            for thread in (auction_manager.thread,auction_manager.parallel_thread,auction_manager.bond_thread,auction_manager.reconcile_thread,auction_manager.gateway_thread):
                if thread.is_alive():thread.join(timeout=8)
        if route_worker and route_worker.thread.is_alive():route_worker.thread.join(timeout=4)
        if worker.thread.is_alive():worker.thread.join(timeout=12)
        if universe and worker.events_thread.is_alive():worker.events_thread.join(timeout=12)
        if universe and worker.positions_thread.is_alive():worker.positions_thread.join(timeout=12)
        if universe and worker.atlas_thread.is_alive():worker.atlas_thread.join(timeout=5)
        if universe and worker.oracle_triggers.thread.is_alive():worker.oracle_triggers.thread.join(timeout=3)
        if extension and extension.thread.is_alive():extension.thread.join(timeout=12)
        atomic_json(session/'status.json',result)
        atomic_json(runtime,{'pid':None,'session':str(session),**result,'executor_connected':False})
        for sig,handler in handlers.items():signal.signal(sig,handler)
        fcntl.flock(lock,fcntl.LOCK_UN);lock.close()
