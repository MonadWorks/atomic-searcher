"""Event hints for the structural DEX lane; prices and execution are reread."""
import threading
import time
from collections import OrderedDict
from eth_abi import decode
from urllib.parse import urlsplit
from .core import ROOT,atomic_json,number,safe_error,unhex
from .activity import ActivityInbox,ActivityJournal,StreamBudget,websocket_url,run_stream
from .universe import ActivityHints
from .events import POOL_ACTIVITY_TOPICS,V3_SWAP,SLIP_SWAP
from .costs import quote_profit_floor


class DexActivity:
    def __init__(self,universe,config,stop,output,finish=float('inf')):
        self.u,self.config,self.stop,self.output,self.finish=universe,config,stop,output,finish
        self.inbox=ActivityInbox();self.fast_inbox=ActivityInbox();self.lock=threading.RLock();self.hints=ActivityHints()
        self.journal=ActivityJournal(output/'dex-activity.jsonl',max_bytes=4*1024**2)
        self.prepared_groups=[];self.prepared_at=0.
        self.latest_blocks=OrderedDict()
        self.flow_transactions=OrderedDict();self.closed_flows=OrderedDict()
        self.signal_floors={s['address']:quote_profit_floor(config,s) for s in config['settlement_tokens']}
        self.ws={'state':'starting','at':0};self.events=0;self.reads=0
        self.http_states={}
        self.threads=[threading.Thread(target=self.stream,name='dex-event-stream',daemon=True),
                      threading.Thread(target=self.http,name='dex-event-fallback',daemon=True),
                      threading.Thread(target=self.sealed_http,name='dex-event-catchup',daemon=True)]

    def pools(self):
        with self.u.lock:return set(self.u.graph.pools)

    def prepare_stream(self,groups):
        with self.lock:self.prepared_groups=list(groups);self.prepared_at=time.monotonic()

    def stream_priorities(self):
        with self.lock:
            now=time.monotonic()
            if now-self.prepared_at>=45:return []
            groups=[]
            for score,members in self.prepared_groups:
                signal=self.closed_flows.get(tuple(sorted(members)))
                if signal and now-signal[0]<120:score=(2,signal[0],signal[1])
                groups.append((score,members))
            return sorted(groups,reverse=True)

    def stream_pools(self):
        # Keep the existing broad roster stable. Rebuilding filters whenever
        # price rankings change creates control traffic and delivery gaps.
        return self.pools()

    def observe_flows(self,logs,pools,now):
        settlements={s['address']:s for s in self.config['settlement_tokens']}
        for log in logs:
            p=pools.get(log['address'].lower())
            if p is None or p.kind not in (1,2) or log.get('topics',[None])[0] not in (V3_SWAP,SLIP_SWAP):continue
            try:amounts=decode(['int256','int256'],unhex(log['data'])[:64])
            except (ValueError,TypeError,OverflowError):continue
            except Exception:continue  # Malformed ABI is not a market signal.
            if not (amounts[0]*amounts[1]<0):continue
            key=(log.get('blockHash'),log['transactionHash'])
            item=self.flow_transactions.setdefault(key,{'at':now,'pools':set(),'net':{}})
            if p.address in item['pools'] or len(item['pools'])>=4:continue
            item['pools'].add(p.address)
            for token,amount in zip((p.token0,p.token1),amounts):item['net'][token]=item['net'].get(token,0)+amount
            self.flow_transactions.move_to_end(key)
            balances={t:v for t,v in item['net'].items() if v}
            if len(item['pools'])<2 or len(balances)!=1:continue
            token,delta=next(iter(balances.items()));s=settlements.get(token)
            if s and -delta>=max(1,self.signal_floors[token]):
                members=tuple(sorted(item['pools']))
                # Pool-flow surplus is an observation priority, not a receipt
                # profit: gas, transfers and the actual executor remain checks.
                self.closed_flows[members]=(now,-delta/10**s['decimals'])
                self.closed_flows.move_to_end(members)
        while len(self.flow_transactions)>512:self.flow_transactions.popitem(last=False)
        while self.flow_transactions and now-next(iter(self.flow_transactions.values()))['at']>15:self.flow_transactions.popitem(last=False)
        while len(self.closed_flows)>128:self.closed_flows.popitem(last=False)

    def required_block(self,events):
        with self.lock:return max((self.latest_blocks.get(p,0) for p,_ in events),default=0)

    def fresh(self):
        now=time.monotonic()
        with self.fast_inbox.lock:
            return any(0<=now-at<=1 for at in self.fast_inbox.pending.values())

    def publish(self,logs,source):
        with self.u.lock:
            pools={r.get('address','').lower():self.u.graph.pools.get(r.get('address','').lower()) for r in logs}
        with self.lock:
            accepted=self.hints.accept_logs(logs)
            if not accepted:return
            now=time.monotonic();received=time.time()
            self.observe_flows(accepted,pools,now)
            fast=set()
            for r in accepted:
                p=r['address'].lower()
                try:height=number(r['blockNumber'])
                except (KeyError,TypeError,ValueError):continue
                if not 0<height<2**64:continue
                fast.add(p)
                self.latest_blocks[p]=max(self.latest_blocks.get(p,0),height)
                self.latest_blocks.move_to_end(p)
            while len(self.latest_blocks)>8192:self.latest_blocks.popitem(last=False)
            self.inbox.publish({r['address'].lower() for r in accepted},now)
            if source=='pendingLogs':self.fast_inbox.publish(fast,now)
            self.journal.append(accepted,source,received)
            self.events+=len(accepted)
            atomic_json(self.output/'dex-activity-health.json',{'at':received,'state':'receiving',
                'source':source,'events':self.events,'execution_evidence':False})

    def health(self,state,**details):
        known=len(self.pools());focused=len(self.stream_pools())
        details['prepared_scope_pools']=focused
        details['http_only_pools']=details.get('http_only_pools',0)+max(0,known-focused)
        with self.lock:
            self.ws={'at':time.time(),'state':state,**details}
            atomic_json(self.output/'activity-stream-health.json',self.ws)

    def record_read(self,pools,rows,selected,requested,received,anchor,clock):
        selected=set(selected)
        if not selected:return
        self.reads+=1
        atomic_json(self.output/f'dex-activity-read-{self.reads%64}.json',{
            'requested_at':requested,'received_at':received,'sequence':self.reads,
            'anchor':{k:anchor[k] for k in ('number','hash') if k in anchor},
            'call_clock':clock,'rows':[
                {'pool':p.address,'tokens':[p.token0,p.token1],'kind':p.kind,'state':list(row)}
                for p,row in zip(pools,rows) if p.address in selected],
            'scope':'Actual pending screen result; anchor is not its state root. Not an execution proof.'})

    def stream(self):
        try:
            url=websocket_url()
            if not url:self.health('not_configured',fallback='http-sealed-logs');return
            host=urlsplit(url).hostname or '';selector=None
            if host.endswith(('.chainstack.com','.p2pify.com')):
                from .stream_usage import StreamSelection
                from .discovery_reads import DiscoveryStreamBudget
                limit=self.config['policy'].get('activity_ws_daily_request_limit',80000)
                budget=DiscoveryStreamBudget(ROOT/'evidence/activity-stream-requests.json',limit,
                    # This structural lane performs HTTP discovery publicly.
                    # Do not reserve half the allocation for an absent private
                    # reader. Today's persisted HTTP debits still constrain
                    # the combined budget, including after a mode switch.
                    ROOT/'evidence/discovery-http-requests.json',split_active=False,
                    pacing_mode=self.config['policy'].get('activity_ws_pacing_mode','daily'))
                selector=StreamSelection(budget.limit,budget=budget,priority_groups=self.stream_priorities)
            else:
                budget=StreamBudget(ROOT/'evidence/activity-stream-usage.json',
                    self.config['policy'].get('activity_ws_hourly_byte_limit',500000))
            run_stream(url,self.config['chain_id'],self.stream_pools,self.stop,self.finish,
                       self.publish,self.health,budget,selector=selector)
        except Exception as exc:
            self.health('unavailable',error=type(exc).__name__,fallback='http-sealed-logs')

    def needs_http(self,now=None):
        now=time.time() if now is None else now
        with self.lock:
            return not (self.ws['state']=='connected' and now-self.ws['at']<3
                        and not self.ws.get('http_only_pools',0))

    def http_health(self,kind,values):
        with self.lock:
            now=time.time();self.http_states[kind]={'at':now,**values}
            pending=self.http_states.get('pending',{});sealed=self.http_states.get('sealed',{})
            fresh_pending=pending.get('state')=='connected' and now-pending['at']<5
            fresh_sealed=sealed.get('state')=='connected' and now-sealed['at']<12
            source='http-pending-logs' if fresh_pending else 'http-sealed-logs' if fresh_sealed else None
            atomic_json(self.output/'activity-health.json',{
                **pending,'at':now,'state':'connected' if source else 'retrying','source':source,
                'block':sealed.get('block'),'sealed':sealed,
                'scope':'Activity hints; prices and execution are reread.'})

    def http(self):
        from .event_intake import EventRPC
        policy=self.config['policy']
        rpc=EventRPC(policy.get('activity_pending_rpc_url',policy.get('discovery_rpc_url',
            'https://base.gateway.tenderly.co')),timeout=3,cancel_event=self.stop)
        # This priority is only for fresh event intake. Historical readers keep
        # their FIFO order and at least every other provider admission.
        rpc.live_events=True
        interval=max(1.,float(policy.get('activity_poll_seconds',1)))
        try:
            while not self.stop.is_set() and time.monotonic()<self.finish:
                started=time.monotonic();delay=interval
                if not self.needs_http():self.stop.wait(.25);continue
                try:
                    requested=time.time()
                    logs=rpc.request('eth_getLogs',[{'fromBlock':'pending','toBlock':'pending',
                        'topics':[list(POOL_ACTIVITY_TOPICS)]}])
                    received=time.time();self.publish(logs,'http-pending-logs')
                    measurements=getattr(rpc,'measurements',())
                    timing=measurements[-1] if measurements else {}
                    self.http_health('pending',dict(state='connected',
                        pending_requested_at=requested,pending_received_at=received,
                        pending_host=rpc.active_host,pending_events=len(logs),
                        pending_max_block=max((number(x['blockNumber']) for x in logs),default=None),
                        pending_timing={k:timing.get(k) for k in ('elapsed_ms','network_total_ms',
                            'public_limiter_wait_ms','http_status')}))
                except Exception as exc:
                    if self.stop.is_set():break
                    delay=10
                    self.http_health('pending',dict(state='retrying',pending_error=safe_error(exc),
                        pending_retry_at=time.time()+delay))
                # Never wait here for a sealed header or an old log page.
                self.stop.wait(max(.1,delay-(time.monotonic()-started)))
        finally:rpc.close()

    def sealed_http(self):
        from .event_intake import EventRPC
        rpc=EventRPC(self.config['policy'].get('activity_rpc_url','https://base.drpc.org'),
                     timeout=3,cancel_event=self.stop)
        last=None
        try:
            while not self.stop.is_set() and time.monotonic()<self.finish:
                if not self.needs_http():self.stop.wait(.25);continue
                delay=6
                try:
                    head=rpc.header();height=number(head['number'])
                    if height!=last:
                        logs=rpc.request('eth_getLogs',[{'fromBlock':hex(max(0,height-1)),
                            'toBlock':head['number'],'topics':[list(POOL_ACTIVITY_TOPICS)]}])
                        self.publish(logs,'http-sealed-logs');last=height
                    self.http_health('sealed',dict(state='connected',block=last))
                    with self.lock:
                        if self.http_states.get('pending',{}).get('state')!='connected':delay=2
                except Exception as exc:
                    if self.stop.is_set():break
                    delay=4;self.http_health('sealed',dict(state='retrying',block=last,error=safe_error(exc)))
                self.stop.wait(delay)
        finally:rpc.close()

    def start(self):
        for thread in self.threads:thread.start()

    def close(self):
        for thread in self.threads:
            if thread.is_alive():thread.join(timeout=4)
