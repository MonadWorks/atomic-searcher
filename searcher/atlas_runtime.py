"""Auction preparation, submission and bonded-fee reconciliation on one account.

Uses the same daily risk ledger as ordinary transactions. Auction acceptance,
canonical execution and net income have distinct records.
"""
import json
import time
from collections import deque
from eth_abi import decode
from eth_utils import keccak
import httpx
from .core import USDC,WETH,ZERO,data,number,unhex,check,safe_error,atomic_json
from .structural import inspect,inspect_markets,market_route,cap
from .structural_runner import liquidation_batches,sized_requests
from .context import simulation_context
from . import atlas

EXECUTED='0x'+keccak(text='AuctionExecuted(bytes32,uint256,uint256,uint256,uint256)').hex()
BOND='0x'+keccak(text='Bond(address,uint256)').hex()
UNBOND='0x'+keccak(text='Unbond(address,uint256,uint256)').hex()
SOLVER_RESULT='0x'+keccak(text='SolverTxResult(address,address,address,address,uint256,bool,bool,uint256)').hex()

class AuctionManager:
    def __init__(self,executor,universe,output,*,recover=True):
        self.e=executor;self.u=universe;self.output=output;self.client=httpx.Client(timeout=1.5,limits=httpx.Limits(keepalive_expiry=120))
        import threading
        self.reservation_lock=getattr(executor,'reservation_lock',threading.RLock())
        self.meta=None;self.last_reconcile=0;self.market_cursor={}
        db=self.e.db
        db.execute('CREATE TABLE IF NOT EXISTS auctions (attempt TEXT PRIMARY KEY, payload TEXT NOT NULL, outcome TEXT, finalized INTEGER NOT NULL DEFAULT 0)')
        # Network submission is preceded by a durable submitted marker. A crash
        # before that marker leaves a provably unsent reservation, even if the
        # separate auction payload had not been written yet.
        if recover:
            db.execute("UPDATE auctions SET finalized=1 WHERE attempt IN (SELECT id FROM attempts WHERE kind='auction' AND state='reserved')")
            db.execute("UPDATE attempts SET state='cancelled',actual='0' WHERE kind='auction' AND state='reserved'")
            db.commit()
    def close(self):self.client.close()
    def bond(self,block='pending',rpc=None):
        return number((rpc or self.e.rpc).request('eth_call',[{'to':atlas.ATLAS,'data':data('balanceOfBonded(address)',['address'],[self.e.owner])},block]))
    def process(self,notice,*,submit=True):
        from .auction_evidence import unsigned_notice,trial_record
        self.audit={'notice':unsigned_notice(notice),'stage':'validation',
            'state_scope':'pending reads with observed canonical anchor; pending state root not exposed',
            'timings_ms':{}}
        started=time.monotonic()
        e=self.e;p=e.config['policy'];context=e.context
        check(context is not None and time.monotonic_ns()-context['requested_ns']<=1_500_000_000,'auction account context stale')
        head=context['head'];clock=simulation_context(head,context['pending_head']);height=number(head['number'])
        self.audit.update(anchor={k:head[k] for k in ('number','hash','timestamp')},clock=clock)
        op,feed,median,stamp=atlas.validate_notice(notice,height)
        # Price the exact package before deciding affordability. The notice's
        # gas unit price alone cannot establish whether this auction pays.
        # The fee and conversion readings are refreshed outside the auction's short window.
        meta=self.u.atlas_meta
        if meta:meta={**meta,'forwarders':dict(self.u.atlas_forwarders)}
        check(meta and time.time()-meta['at']<90,'auction protocol metadata refreshing')
        check(context.get('weth_profit_floor',0)>0 and context.get('weth_profit_usdc_raw')==int(p['min_net_profit_usdc_raw']),'auction profit conversion refreshing')
        minimum=max(context['weth_profit_floor'],int(next(t for t in e.config['settlement_tokens'] if t['address']==WETH)['min_net_raw']))
        liability_cap=int(p['max_total_fee_wei'])
        overrides=atlas.report_override(feed,median,stamp)
        markets=[self.u.markets[id] for id in notice['markets'] if id in self.u.books]
        # Prepared near-boundary positions first; membership is never restricted to a token allowlist.
        markets.sort(key=lambda m:-m.borrow_usd)
        mapped_count=len(markets)
        if len(markets)>128:
            offset=self.market_cursor.get(feed,0)%len(markets)
            self.market_cursor[feed]=offset+128;markets=(markets[offset:]+markets[:offset])[:128]
        size=max(1,min(64,128//max(1,len(markets))))
        self.audit['stage']='report_price_screen'
        # Prepared positions are proposals, not eligibility evidence. The
        # following whole-execution batch validates current Morpho state and
        # exact pool execution together, saving a redundant serial RPC read.
        inspected={};cold=[];selection={}
        for m in markets:
            book=self.u.books[m.id]
            proposed=book.proposals(8) if hasattr(book,'proposals') else []
            if proposed:inspected[m.id]=(None,0,proposed)
            else:
                addresses=book.batch(size)
                if addresses:cold.append((m.id,addresses))
        # A cold start may use a single batch. Prepared markets do not wait for
        # unrelated missing identities; discovery fills them for future events.
        def screen(groups):
            operation=lambda:inspect_markets(e.rpc,groups,'pending',clock,overrides)
            shared=getattr(self.u,'auction_screens',None)
            if shared is None:return operation()
            key=(feed,median,stamp,head['hash'],clock['number'],clock['time'],
                 tuple((id,tuple(addresses)) for id,addresses in groups))
            values,reused=shared.read(key,operation,max(0,1.5-(time.time()-notice['received_at'])))
            self.audit['screen_shared']=reused
            return values
        if not inspected and cold:
            inspected=screen(cold)
        elif inspected:
            # Read the arriving report's market price before selecting from the
            # entire prepared book. The first eight by risk can be dust, while
            # another newly eligible position carries the realizable incentive.
            from .auction_selection import select_positions,reference_borrower
            screening=screen([(id,[reference_borrower(self.u.books[id],rows)]) for id,(_,_,rows) in inspected.items()]+cold)
            inspected={id:value for id,value in inspected.items() if id in screening}
            for id,(price,stamp,current) in screening.items():
                proposed,details=select_positions(self.u.books[id],price,current,self.u.markets[id].lltv)
                inspected[id]=(price,stamp,proposed);selection[id]=details
        self.audit['timings_ms']['screen_complete']=(time.monotonic()-started)*1000
        self.audit['screened']={id:{'price':price,'timestamp':stamp,'positions':rows} for id,(price,stamp,rows) in inspected.items() if rows}
        self.audit['coverage']={'mapped_markets':mapped_count,'requested_markets':len(markets),
            'read_markets':len(inspected),'unread_markets':[m.id for m in markets if m.id not in inspected],
            'deferred_markets':max(0,mapped_count-len(markets))}
        from .auction_planner import trials_for
        best=None;reasons=[];quoted_gross=0;least_fee=None;quotes_failed=0;quotes_measured=0;best_net_before_bid=None
        planning=[]
        self.audit['stage']='plan_generation'
        trials=trials_for(self.u,e.config,op,inspected,markets,minimum,liability_cap,diagnostics=planning)
        self.audit['planning']=planning
        if not trials:
            reasons=[reason for d in planning for reason in d['reasons'] if d['positions']]
            if not reasons:reasons=['no_eligible_positions' if inspected else 'market_state_unavailable']
        if trials:
            atlas.validate_notice(notice,height)
            largest=max(t['borrowers'] for t in trials)
            per_plan=min(int(p['max_gas']),max(1_800_000,largest*400000+400000))
            trials=trials[:max(1,min(8,14_000_000//(per_plan+300000)))]
            self.audit['trials']=[trial_record(t) for t in trials]
            self.audit['stage']='whole_execution_quote'
            self.audit['timings_ms']['quote_started']=(time.monotonic()-started)*1000
            e.rpc.auction_quote_diagnostics=[]
            measured=atlas.measure_charges(e.rpc,meta,op,e.owner,e.structural_contract,
                [x['plan'] for x in trials],per_plan,min(p['max_gas'],meta['solver_gas']),overrides,clock)
            self.audit['timings_ms']['quote_complete']=(time.monotonic()-started)*1000
            self.audit['quote_reverts']=getattr(e.rpc,'auction_quote_diagnostics',[])
            for index,(trial,(gross,gas,charge)) in enumerate(zip(trials,measured)):
                self.audit['trials'][index]['measurement']={'gross_wei':gross,'gas':gas,'maximum_charge_wei':charge}
                if gas<=0:
                    quotes_failed+=1;reasons.append('whole execution quote failed');continue
                quotes_measured+=1
                quoted_gross=max(quoted_gross,gross)
                if gross<=minimum:
                    reasons.append('measured gross below retained profit floor');continue
                if gas>min(p['max_gas'],meta['solver_gas']):
                    reasons.append('measured execution gas exceeds protocol limit');continue
                if charge is None:
                    reasons.append('auction protocol fee estimate unavailable');continue
                least_fee=charge if least_fee is None else min(least_fee,charge)
                net=gross-charge
                best_net_before_bid=net if best_net_before_bid is None else max(best_net_before_bid,net)
                if charge>liability_cap:
                    reasons.append('auction maximum gas liability exceeds account fee cap');continue
                retained=max(minimum,(charge*p.get('min_net_profit_fee_bps',0)+9999)//10000)
                bid=gross-charge-retained
                if bid<=0:
                    reasons.append('measured profit cannot cover gas and retained profit');continue
                plan=trial['plan'];plan=(*plan[:4],retained,charge,bid,plan[7],plan[8])
                solver=atlas.solution(e.owner,e.structural_contract,op,gas,bid,plan)
                if best is None or bid>best['bid']:
                    best={'solver':solver,'gross':gross,'bid':bid,'fee':charge,'minimum':retained,'market':trial['market'],'route_key':trial['route'].key}
        economics={'gas_price_wei':number(op['maxFeePerGas']),'fee_policy':'profit_and_total_liability',
            'whole_quotes_failed':quotes_failed,'whole_quotes_measured':quotes_measured,
            'best_measured_net_before_bid_wei':best_net_before_bid,'market_coverage':self.audit['coverage'],
            'planning_rejections':[d for d in planning if d['positions'] and not d['plans_generated']],
            'stage_timings_ms':self.audit['timings_ms']}
        # A stale borrower must be reread outside the auction hot path instead
        # of being proposed forever. This refresh says nothing about route health.
        failed_markets={t['market'] for t,q in zip(trials,measured if trials else []) if q[1]<=0}
        for id in failed_markets:
            book=self.u.books[id]
            if hasattr(book,'add'):book.add([r['borrower'] for r in inspected[id][2]])
            if hasattr(self.u,'trigger'):self.u.trigger(id,priority=True)
        if best is None:return {'status':'no_executable_auction','reasons':list(dict.fromkeys(reasons))[-3:],'markets_read':len(inspected),'packages_compared':len(trials),'positive_quoted_gross_wei':quoted_gross,'minimum_quoted_fee_reservation_wei':least_fee,'positions_proposed':sum(len(rows) for _,_,rows in inspected.values()),'eligibility_evidence':'whole_execution_quote' if quotes_measured else 'whole_execution_quote_failed' if trials else 'report_price_screen','position_selection':selection,**economics}
        atlas.validate_notice(notice,height)
        bond_snapshot=getattr(e,'bond_snapshot',None)
        check(bond_snapshot and 0<=time.monotonic_ns()-bond_snapshot['requested_ns']<=3_000_000_000,'auction bond context refreshing')
        bonded=int(bond_snapshot['balance']);check(bonded>=best['fee'],'bonded account balance below auction liability')
        if not submit:
            return {'status':'quoted_only','market':best['market'],'route_key':best['route_key'],
                    'gross_wei':best['gross'],'bid_wei':best['bid'],'maximum_fee_liability_wei':best['fee'],
                    'retained_minimum_wei':best['minimum'],'unsigned_solver':best['solver'],'position_selection':selection,**economics}
        with self.reservation_lock:
            self.audit['stage']='reserve_and_sign'
            group,record,signed=self._reserve_and_sign(notice,op,feed,best,bond_snapshot,bonded,meta)
        try:
            self.audit['stage']='authentic_report_full_simulation'
            atlas.full_simulation(e.rpc,meta,op,signed,context=clock)
            self.audit['timings_ms']['simulation_complete']=(time.monotonic()-started)*1000
            atlas.validate_notice(notice,number(e.context['head']['number']))
        except Exception as exc:
            record['gateway']='simulation_not_submitted';record['simulation_error']=safe_error(exc)
            self._save_record(group,record)
            raise
        record['full_simulation_passed']=True
        try:
            atlas.validate_notice(notice,number(e.context['head']['number']))
            remaining=max(.01,1.5-(time.time()-notice['received_at']))
            self.client.timeout=httpx.Timeout(remaining)
            self.audit['stage']='gateway_submission'
            result=atlas.submit_solution(self.client,notice['auction_id'],signed);record['gateway']='accepted'
        except Exception as exc:
            record['gateway']='unresolved';record['error']=safe_error(exc);result={'gateway_accepted':False,'submission_unresolved':True}
        self._save_record(group,record)
        return {'status':'auction_submitted',**result,'auction_id':notice['auction_id'],'bid_wei':best['bid'],'retained_minimum_wei':best['minimum'],**economics}
    def _reserve_and_sign(self,notice,op,feed,best,bond_snapshot,bonded,meta):
        e=self.e
        atlas.validate_notice(notice,number(e.context['head']['number']))
        group='auction:'+keccak(unhex(feed)+unhex(op['hints']['rawReport'])).hex()
        pending=e.db.execute("SELECT x.payload FROM auctions x JOIN attempts a ON a.id=x.attempt WHERE x.attempt=? AND a.state='submitted'",(group,)).fetchone()
        if not pending and e.db.execute('SELECT 1 FROM attempts WHERE id=?',(group,)).fetchone():group+=':'+notice['auction_id']
        if not pending:
            settled=e.db.execute('SELECT outcome FROM auctions WHERE outcome IS NOT NULL').fetchall()
            check(all(bond_snapshot['block']>=int(json.loads(row[0])['block']) for row in settled),'auction accounting context refreshing')
        payload=json.loads(pending[0]) if pending else {'start_block':bond_snapshot['block'],'deadline':number(op['deadline']),'bond_before':str(bonded),
            'start_hash':bond_snapshot['block_hash'],'operations':[],'contract':e.structural_contract,'owner':e.owner}
        check(all(o['auction_id']!=notice['auction_id'] for o in payload['operations']),'duplicate auction submission')
        check(len(payload['operations'])<2,'parallel auction reservation bound')
        total=sum(o['fee'] for o in payload['operations'])+best['fee'];check(bonded>=total,'parallel bids exceed bonded balance')
        if pending:e.ledger.extend(group,total)
        else:e.ledger.reserve(group,best['fee'],kind='auction')
        payload['deadline']=max(payload['deadline'],number(op['deadline']))
        record={'auction_id':notice['auction_id'],'user_hash':op['userOpHash'],'fee':best['fee'],'bid':best['bid'],'minimum':best['minimum'],
                'market':best['market'],'route_key':best['route_key'],'at':time.time(),'gateway':'pending'}
        payload['operations'].append(record)
        # Persist liability before signing or any network write. Ambiguous submissions stay reserved.
        with e.db:
            e.db.execute('INSERT OR REPLACE INTO auctions(attempt,payload) VALUES(?,?)',(group,json.dumps(payload)))
        try:
            signed=atlas.sign_solution(e.signer,meta,best['solver'])
        except Exception:
            # Local signing failed; no authorization was disclosed to any service.
            with e.db:
                payload['operations'].pop()
                if pending:
                    e.db.execute('UPDATE attempts SET reserved=? WHERE id=?',(str(total-best['fee']),group))
                    e.db.execute('UPDATE auctions SET payload=? WHERE attempt=?',(json.dumps(payload),group))
                else:
                    e.db.execute("UPDATE attempts SET state='cancelled',actual='0' WHERE id=? AND state='reserved'",(group,))
                    e.db.execute('UPDATE auctions SET finalized=1 WHERE attempt=?',(group,))
            raise
        # A signed solver operation sent to a simulation RPC is a live
        # authorization, even though eth_call itself discards state. Retain its
        # liability until expiry if that RPC fails or returns a failed result.
        record['gateway']='signed_simulation_pending'
        with e.db:
            e.db.execute("UPDATE attempts SET state='submitted' WHERE id=? AND kind='auction'",(group,))
            e.db.execute('UPDATE auctions SET payload=? WHERE attempt=?',(json.dumps(payload),group))
        return group,record,signed

    def _save_record(self,group,record):
        # Another parallel auction may have appended its own operation while
        # this RPC/gateway call ran. Merge by id; never overwrite that sibling.
        with self.reservation_lock, self.e.db:
            row=self.e.db.execute('SELECT payload FROM auctions WHERE attempt=?',(group,)).fetchone()
            check(row is not None,'auction reservation missing')
            payload=json.loads(row[0]);found=False
            for i,current in enumerate(payload['operations']):
                if current['auction_id']==record['auction_id']:
                    payload['operations'][i]=record;found=True;break
            check(found,'auction operation missing')
            self.e.db.execute('UPDATE auctions SET payload=? WHERE attempt=?',(json.dumps(payload),group))

    def reconcile(self):
        if time.monotonic()-self.last_reconcile<2:return
        self.last_reconcile=time.monotonic();e=self.e;rpc=getattr(e,'reconciliation_rpc',e.rpc)
        rows=e.db.execute("SELECT a.id,x.payload,x.outcome FROM attempts a JOIN auctions x ON a.id=x.attempt WHERE a.kind='auction' AND x.finalized=0").fetchall()
        if not rows:return
        head=rpc.header();height=number(head['number'])
        for attempt,raw,oldraw in rows:
            payload=json.loads(raw);original_payload=raw
            if oldraw:
                old=json.loads(oldraw);canonical=rpc.header(old['block'])
                if canonical['hash']!=old['block_hash']:
                    with e.db:e.ledger.reopen(attempt,commit=False);e.db.execute('UPDATE auctions SET outcome=NULL WHERE attempt=?',(attempt,))
                    continue
                final=rpc.request('eth_getBlockByNumber',['finalized',False])
                if final and number(final['number'])>=old['block']:
                    with e.db:e.db.execute('UPDATE auctions SET finalized=1 WHERE attempt=?',(attempt,))
                continue
            # Deadline is an inclusion bound, not the off-chain auction closing time.
            if height<=payload['deadline']+1:continue
            start=rpc.header(payload['start_block'])
            if start['hash']!=payload['start_hash']:
                # The replacement block can include a bid that the orphaned
                # block did not. Include that block in logs and balance changes.
                payload['start_block']=max(0,payload['start_block']-1)
                start=rpc.header(payload['start_block'])
                payload['bond_before']=str(self.bond(hex(payload['start_block']),rpc))
                check(rpc.header(payload['start_block'])['hash']==start['hash'],'auction baseline reorganized during recovery')
                payload['start_hash']=start['hash']
                with self.reservation_lock,e.db:
                    if e.db.execute('SELECT payload FROM auctions WHERE attempt=?',(attempt,)).fetchone()[0]!=original_payload:continue
                    original_payload=json.dumps(payload)
                    e.db.execute('UPDATE auctions SET payload=? WHERE attempt=?',(original_payload,attempt))
            owner_topic='0x'+payload['owner'][2:].rjust(64,'0')
            changes=rpc.request('eth_getLogs',[{'address':atlas.ATLAS,'topics':[[BOND,UNBOND],owner_topic],
                'fromBlock':hex(payload['start_block']+1),'toBlock':hex(height)}])
            adjustment=0
            for log in changes:
                value=int.from_bytes(unhex(log['data'])[:32],'big');adjustment+=value if log['topics'][0]==BOND else -value
            bonded=self.bond(hex(height),rpc);actual=int(payload['bond_before'])+adjustment-bonded
            check(actual>=0,'bond accounting changed outside known bond events')
            logs=rpc.request('eth_getLogs',[{'address':payload['contract'],'topics':[EXECUTED],
                'fromBlock':hex(payload['start_block']+1),'toBlock':hex(height)}])
            expected={o['user_hash'].lower() for o in payload['operations']};payout=0;executed=[]
            for log in logs:
                if log['topics'][1].lower() not in expected:continue
                receipt=rpc.request('eth_getTransactionReceipt',[log['transactionHash']]);check(receipt and number(receipt['status'])==1,'auction receipt unavailable')
                check(rpc.header(number(log['blockNumber']))['hash']==log['blockHash']==receipt['blockHash'],'auction receipt reorganized')
                gross,bid,paid,budget=decode(['uint256']*4,unhex(log['data']));payout+=paid;executed.append({'transaction_hash':log['transactionHash'],'gross_wei':gross,'bid_wei':bid,'payout_wei':paid})
            check(rpc.header(height)['hash']==head['hash'],'auction accounting head changed')
            outcome={'block':height,'block_hash':head['hash'],'actual_fee_wei':actual,'payout_wei':payout,'net_profit_wei':payout-actual,'executions':executed,
                'status':'executed' if executed else 'fee_charged' if actual else 'expired_without_fee','accounting':'bond balance change adjusted for Bond/Unbond events'}
            with self.reservation_lock,e.db:
                # A sibling may have extended liability or its inclusion bound
                # while receipts were read. Reconcile that newer snapshot next.
                if e.db.execute('SELECT payload FROM auctions WHERE attempt=?',(attempt,)).fetchone()[0]!=original_payload:continue
                e.ledger.settle(attempt,bool(executed),actual,True,commit=False)
                if not executed and actual==0:e.db.execute("UPDATE attempts SET state='cancelled' WHERE id=?",(attempt,))
                e.db.execute('UPDATE auctions SET outcome=? WHERE attempt=?',(json.dumps(outcome),attempt))
            atomic_json(self.output/'atlas-latest-result.json',outcome)


class AuctionWorker:
    """Immediate auction intake, independent of ordinary scans and their RPC waits.

    A separate SQLite connection uses the same durable account reservation. It
    cannot spend concurrently with a pending ordinary transaction.
    """
    def __init__(self,executor,universe,stop,output):
        import threading
        self.source,self.u,self.stop,self.output=executor,universe,stop,output
        self.path=executor.db.execute('PRAGMA database_list').fetchone()[2]
        self.thread=threading.Thread(target=self.run,name='auction-execution',daemon=True)
        self.parallel_thread=threading.Thread(target=self.run,name='auction-execution-parallel',daemon=True)
        self.reservation_lock=threading.RLock();self.log_lock=threading.Lock();self.recovered=False
        self.gateway_clients=set()
        self.gateway_thread=threading.Thread(target=self.warm_gateway,name='auction-gateway-connection',daemon=True)
        self.bond_snapshot=None
        self.bond_thread=threading.Thread(target=self.refresh_bond,name='auction-bond-context',daemon=True)
        self.reconcile_thread=threading.Thread(target=self.run_reconciliation,name='auction-accounting',daemon=True)
    def start(self):self.bond_thread.start();self.thread.start();self.parallel_thread.start();self.reconcile_thread.start();self.gateway_thread.start()
    def warm_gateway(self):
        # HTTP HEAD is connection maintenance only, with no solver payload or
        # submission. The synchronous client is thread-safe; this never holds
        # the queue or fee-reservation lock while waiting on the gateway.
        while not self.stop.is_set():
            with self.log_lock:clients=list(self.gateway_clients)
            for client in clients:
                if self.stop.is_set():break
                try:client.head(atlas.GATEWAY,timeout=2)
                except Exception:pass
            self.stop.wait(20 if clients else .2)
    def refresh_bond(self):
        from .core import RPC
        rpc=RPC(self.source.rpc.url,timeout=1.2,cancel_event=self.stop)
        verified=False
        try:
            while not self.stop.is_set():
                try:
                    if not verified:rpc.verify_chain();verified=True
                    context=self.source.context
                    check(context and time.monotonic_ns()-context['requested_ns']<=1_500_000_000,'account context refreshing')
                    head=context['head'];height=number(head['number']);started=time.monotonic_ns()
                    amount=number(rpc.request('eth_call',[{'to':atlas.ATLAS,'data':data('balanceOfBonded(address)',['address'],[self.source.owner])},hex(height)]))
                    check(rpc.header(height)['hash']==head['hash'],'bond context reorganized')
                    self.bond_snapshot={'requested_ns':started,'block':height,'block_hash':head['hash'],'balance':amount}
                except Exception as exc:
                    if not self.stop.is_set():atomic_json(self.output/'atlas-bond-health.json',{'at':time.time(),'state':'retrying','error':safe_error(exc)})
                self.stop.wait(.5)
        finally:rpc.close()

    def run_reconciliation(self):
        from types import SimpleNamespace
        from .core import RPC
        from .ledger import RiskLedger
        source=self.source;p=source.config['policy']
        while not self.stop.is_set():
            rpc=RPC(p.get('identity_rpc_url',source.rpc.url),timeout=8,cancel_event=self.stop)
            ledger=None;manager=None
            try:
                rpc.verify_chain();ledger=RiskLedger(self.path,int(p['max_daily_fee_wei']),p['max_consecutive_failures'])
                account=SimpleNamespace(rpc=rpc,reconciliation_rpc=rpc,db=ledger.db,ledger=ledger,owner=source.owner,reservation_lock=self.reservation_lock)
                manager=AuctionManager(account,None,self.output,recover=False)
                while not self.stop.is_set():
                    try:manager.reconcile()
                    except Exception as exc:
                        if not self.stop.is_set():atomic_json(self.output/'atlas-reconciliation-health.json',{'at':time.time(),'state':'retrying','error':safe_error(exc)})
                    self.stop.wait(.5)
            except Exception as exc:
                if not self.stop.is_set():atomic_json(self.output/'atlas-reconciliation-health.json',{'at':time.time(),'state':'retrying','error':safe_error(exc)})
            finally:
                if manager:manager.close()
                if ledger:ledger.close()
                rpc.close()
            self.stop.wait(2)

    def run(self):
        while not self.stop.is_set():
            try:self.run_connected()
            except Exception as exc:
                with self.log_lock:atomic_json(self.output/'atlas-worker-health.json',{'at':time.time(),'state':'retrying','error':safe_error(exc)})
                self.stop.wait(2)
    def run_connected(self):
        from .core import RPC
        from .ledger import RiskLedger
        source=self.source;worker=self
        class AccountView:
            @property
            def context(self):return source.context
            @property
            def bond_snapshot(self):return worker.bond_snapshot
        account=AccountView()
        for key in ('owner','config','signer','structural_contract'):setattr(account,key,getattr(source,key))
        account.rpc=RPC(source.rpc.url,timeout=1.2,cancel_event=self.stop);account.rpc.critical_reads=True
        account.reservation_lock=self.reservation_lock
        manager=None;account.ledger=None;account.reconciliation_rpc=None
        try:
            account.rpc.verify_chain()
            from .morpho_universe import optional_views
            optional_views(account.rpc,[])
            account.reconciliation_rpc=RPC(source.config['policy'].get('identity_rpc_url',source.rpc.url),timeout=8,cancel_event=self.stop)
            account.reconciliation_rpc.verify_chain()
            p=source.config['policy'];account.ledger=RiskLedger(self.path,int(p['max_daily_fee_wei']),p['max_consecutive_failures']);account.db=account.ledger.db
            with self.reservation_lock:
                manager=AuctionManager(account,self.u,self.output,recover=not self.recovered)
                self.recovered=True
            with self.log_lock:self.gateway_clients.add(manager.client)
            with self.log_lock:atomic_json(self.output/'atlas-worker-health.json',{'at':time.time(),'state':'connected'})
            last_io=time.monotonic()
            while not self.stop.is_set():
                with self.u.lock:
                    notice=self.u.auctions.popleft() if self.u.auctions else None
                    if not self.u.auctions:self.u.auction_wakeup.clear()
                if notice:
                    started=time.monotonic()
                    account.rpc.deadline_ns=time.monotonic_ns()+int(max(0,1.5-(time.time()-notice['received_at']))*1e9)
                    try:result=manager.process(notice)
                    except Exception as exc:result={'status':'auction_rejected','reason':safe_error(exc)}
                    finally:account.rpc.deadline_ns=None
                    last_io=time.monotonic()
                    result.update(at=time.time(),auction_id=notice['auction_id'],aggregator=notice.get('aggregator'),markets=notice.get('markets',[]),queue_age_ms=max(0,(time.time()-notice['received_at'])*1000-(time.monotonic()-started)*1000),processing_ms=(time.monotonic()-started)*1000)
                    with self.log_lock:
                        atomic_json(self.output/'atlas-latest-decision.json',result)
                        history=self.output/'auction-decisions.jsonl'
                        if history.exists() and history.stat().st_size>2*1024*1024:history.replace(history.with_suffix('.previous.jsonl'))
                        with history.open('a') as out:out.write(json.dumps(result)+'\n')
                        from .auction_evidence import save
                        try:save(self.output,manager.audit,result)
                        except Exception as exc:
                            atomic_json(self.output/'auction-evidence-health.json',{'at':time.time(),'state':'write_failed','error':safe_error(exc)})
                elif time.monotonic()-last_io>=20:
                    # Keep an infrequently used auction connection warm after
                    # discovery/quoting have already prepared the opportunity.
                    account.rpc.deadline_ns=time.monotonic_ns()+800_000_000
                    try:account.rpc.verify_chain()
                    except Exception:pass
                    finally:account.rpc.deadline_ns=None;last_io=time.monotonic()
                else:self.u.auction_wakeup.wait(.1)
        finally:
            if manager:
                with self.log_lock:self.gateway_clients.discard(manager.client)
                manager.close()
            if account.ledger:account.ledger.close()
            if account.reconciliation_rpc:account.reconciliation_rpc.close()
            account.rpc.close()
