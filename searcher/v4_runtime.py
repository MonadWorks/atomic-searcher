"""Bounded V4 event discovery and coherent read/quote worker; never signs.

Canonical Initialize logs persist PoolKeys. Pending logs only prioritize fresh
reads. Every quote uses a single atomic lens result, including previously missing
slots; stored prices are never combined across observations.
"""
import json
import queue
import re
import sqlite3
import threading
import time
import httpx
from collections import deque
from pathlib import Path
from .core import ROOT, RPC, USDC, Reject, RPCError, RPCUnavailable, atomic_json, check, number, safe_error
from .v4 import Reader, Route, evaluate, MANAGER, TOPICS
from .v4_market import Registry, Key, initialization, INITIALIZE, key_from_input

def discovery_batch(rpc,calls):
    """Keep historical metadata reads within small public-provider batches."""
    return [value for start in range(0,len(calls),2)
            for value in rpc.batch(calls[start:start+2])]

def transport_stalled(exc):
    # Rebuild a failed connection, never change providers or sidestep a quota.
    return (isinstance(exc,httpx.TransportError) and
            not isinstance(exc,(httpx.UnsupportedProtocol,httpx.LocalProtocolError)) or
            isinstance(exc,RPCUnavailable) and str(exc)=='RPC total request deadline exceeded')

def read_transport(rpc):
    rows=getattr(rpc,'measurements',())
    if not rows:return {}
    row=rows[-1]
    # No URLs, call parameters or response bodies enter public health output.
    return {key:row[key] for key in ('method','methods','outcome','elapsed_ms','network_total_ms',
        'public_limiter_wait_ms','response_bytes','http_status') if key in row}

class Inventory:
    def __init__(self,path,maximum=4096,activity_capacity=4096,recent_activity_capacity=128):
        check(1<=activity_capacity<=4096,'invalid V4 activity queue capacity')
        check(1<=recent_activity_capacity<=128,'invalid V4 recent activity queue capacity')
        self.db=sqlite3.connect(path)
        self.db.execute('CREATE TABLE IF NOT EXISTS pools(id TEXT PRIMARY KEY, key TEXT, block INTEGER, hash TEXT)')
        self.db.execute('CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT)')
        self.db.execute('CREATE TABLE IF NOT EXISTS checkpoints(block INTEGER PRIMARY KEY,hash TEXT)')
        self.db.execute('CREATE TABLE IF NOT EXISTS activity_queue(sequence INTEGER PRIMARY KEY AUTOINCREMENT, '
                        'txid TEXT, pool_id TEXT, block INTEGER, hash TEXT, next_try REAL DEFAULT 0, '
                        'attempts INTEGER DEFAULT 0, UNIQUE(txid,pool_id,hash))')
        self.db.execute('CREATE TABLE IF NOT EXISTS recent_activity_queue(sequence INTEGER PRIMARY KEY AUTOINCREMENT, '
                        'txid TEXT, pool_id TEXT, block INTEGER, hash TEXT, next_try REAL DEFAULT 0, '
                        'attempts INTEGER DEFAULT 0, UNIQUE(txid,pool_id,hash))')
        self.db.execute('CREATE TABLE IF NOT EXISTS activity_retries(txid TEXT PRIMARY KEY, next_try REAL, attempts INTEGER)')
        self.recent_activity_capacity=recent_activity_capacity
        self.activity_capacity=activity_capacity
        self.registry=Registry(maximum);self.tentative={}
        for pid,key in self.db.execute('SELECT id,key FROM pools ORDER BY block,id LIMIT ?', (maximum,)):
            parsed=Key(*json.loads(key));check(parsed.id==pid,'V4 persisted key hash mismatch');self.registry.keys[pid]=parsed
    def get(self,key,default=None):
        row=self.db.execute('SELECT value FROM metadata WHERE key=?',(key,)).fetchone()
        return json.loads(row[0]) if row else default
    def put(self,key,value):self.db.execute('INSERT OR REPLACE INTO metadata VALUES(?,?)',(key,json.dumps(value)))
    def pending(self,log,now=None):
        now=time.monotonic() if now is None else now
        pid=log.get('topics',['',''])[1] if len(log.get('topics',[]))>1 else ''
        if log.get('removed'):
            if pid in self.tentative:self.tentative.pop(pid,None);self.registry.keys.pop(pid,None)
            return
        key=initialization(log)
        if key and USDC in (key.currency0,key.currency1) and key.id not in self.registry.keys:
            self.registry.accept(log)
            if key.id in self.registry.keys:self.tentative[key.id]=now+20
    def expire_pending(self,now=None):
        now=time.monotonic() if now is None else now
        for pid,expiry in list(self.tentative.items()):
            if expiry<=now:self.tentative.pop(pid,None);self.registry.keys.pop(pid,None)
    def accept(self,logs,lo,hi,hashes):
        admitted=[]
        known=set(self.registry.keys)
        for log in logs:
            height=number(log['blockNumber']);check(lo<=height<=hi,'V4 log outside requested range')
            check(log.get('blockHash')==hashes.get(height),'V4 discovery log not canonical')
            key=initialization(log)
            if not key or USDC not in (key.currency0,key.currency1):continue
            if key.id not in known and len(known)>=self.registry.maximum:continue
            known.add(key.id);admitted.append((key,height,log['blockHash']))
        with self.db:
            for key,height,block_hash in admitted:
                self.db.execute('INSERT INTO pools VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET block=MIN(pools.block,excluded.block)',
                    (key.id,json.dumps(key.values()),height,block_hash))
        changed=set()
        for key,_,_ in admitted:
            self.registry.keys[key.id]=key;self.tentative.pop(key.id,None)
            changed.update(k.id for k in self.registry.keys.values() if (k.currency0,k.currency1)==(key.currency0,key.currency1))
        return changed
    def check_reorg(self,rpc,head=None):
        rows=self.db.execute('SELECT block,hash FROM checkpoints ORDER BY block DESC').fetchall()
        if not rows:return False
        common=None
        for height,stored in rows:
            header=head if head is not None and number(head['number'])==height else rpc.request('eth_getBlockByNumber',[hex(height),False])
            check(header is not None,'V4 canonical checkpoint unavailable')
            if header['hash']==stored:common=height;break
        if common==rows[0][0]:return False
        with self.db:
            if common is None:
                # No trusted ancestor in retained checkpoints: rebuild all metadata.
                self.db.execute('DELETE FROM pools');self.db.execute('DELETE FROM metadata');self.db.execute('DELETE FROM checkpoints')
                self.db.execute('DELETE FROM activity_queue');self.db.execute('DELETE FROM recent_activity_queue');self.db.execute('DELETE FROM activity_retries')
                self.registry.keys.clear();self.tentative.clear()
            else:
                self.db.execute('DELETE FROM pools WHERE block>?',(common,));self.db.execute('DELETE FROM checkpoints WHERE block>?',(common,))
                # Activity/peer observations can be ahead of Initialize scanning.
                # A common ancestor must never advance an unscanned cursor.
                self.put('cursor',min(self.get('cursor',common),common))
                self.put('activity_cursor',min(self.get('activity_cursor',common),common))
                self.db.execute('DELETE FROM activity_queue WHERE block>?',(common,))
                self.db.execute('DELETE FROM recent_activity_queue WHERE block>?',(common,))
                self.put('activity_recent_cursor',min(self.get('activity_recent_cursor',common),common))
                for name in ('activity_recent_status','activity_recent_debt_from'):
                    self.db.execute('DELETE FROM metadata WHERE key=?',(name,))
                self.db.execute("DELETE FROM metadata WHERE key='activity_resume'")
                surviving={x[0] for x in self.db.execute('SELECT id FROM pools')}
                self.registry.keys={p:k for p,k in self.registry.keys.items() if p in surviving}
            from .v4_peers import restore_displaced
            restore_displaced(self,common)
        return True
    def scan(self,rpc,head,span=64):
        """Catch up current logs first; alternate a bounded older page when caught up."""
        self.check_reorg(rpc)
        span=min(span,max(1,self.get('initialize_log_span',span)))
        tip=number(head['number']);cursor=self.get('cursor')
        if cursor is None:
            cursor=max(0,tip-256);self.put('backfill',cursor)
        turn=self.get('scan_turn',0)
        ahead=cursor<tip and (self.get('backfill',cursor)<=0 or tip-cursor>span or turn%3!=2)
        end=min(tip,cursor+span) if ahead else self.get('backfill',cursor)
        if not ahead and end<=0:return set()
        lo=cursor+1 if ahead else max(0,end-span+1)
        def read_logs():
            return rpc.request('eth_getLogs',[{'address':MANAGER,'topics':[INITIALIZE],
                                              'fromBlock':hex(lo),'toBlock':hex(end)}])
        try:
            logs=read_logs()
        except RPCError as exc:
            # A provider's explicit per-query block cap is not a quota. Learn
            # only that cap and retry one smaller contiguous page on this RPC.
            # Do not move a cursor until normal canonical validation succeeds.
            match=re.search(r'up to (?:a )?([0-9,]+) block range',exc.rpc_message.lower())
            if exc.code not in (-32600,-32602,-32005) or match is None:raise
            limit=int(match[1].replace(',',''))
            if not 0<limit<end-lo+1:raise
            span=min(span,limit)
            with self.db:self.put('initialize_log_span',span)
            if ahead:end=min(tip,cursor+span)
            else:lo=max(0,end-span+1)
            logs=read_logs()
        supported=[]
        for log in logs:
            height=number(log['blockNumber']);check(lo<=height<=end,'V4 log outside requested range')
            key=initialization(log)
            if key and USDC in (key.currency0,key.currency1):supported.append(log)
        # Unusable Initialize events cannot register metadata. Authenticate only
        # potentially supported blocks plus the page endpoint; accept retains
        # the full canonical-hash and PoolKey checks for every admitted event.
        heights=sorted({number(log['blockNumber']) for log in supported}|{end})
        headers=discovery_batch(rpc,[('eth_getBlockByNumber',[hex(n),False]) for n in heights])
        check(len(headers)==len(heights) and all(h is not None and number(h['number'])==n
              for h,n in zip(headers,heights)),'V4 discovery header missing')
        hashes={n:h['hash'] for n,h in zip(heights,headers)}
        if end==tip:check(hashes[end]==head['hash'],'V4 discovery head changed')
        changed=self.accept(supported,lo,end,hashes)
        with self.db:
            self.put('scan_turn',turn+1)
            if ahead:
                self.put('cursor',end)
                self.db.execute('INSERT OR REPLACE INTO checkpoints VALUES(?,?)',(end,hashes[end]))
                self.db.execute('DELETE FROM checkpoints WHERE block NOT IN (SELECT block FROM checkpoints ORDER BY block DESC LIMIT 32)')
            else:self.put('backfill',lo-1)
        return changed
    def active_keys(self,rpc,head,limit=2):
        from .v4_activity import active_keys
        return active_keys(self,rpc,head,limit)
    def close(self):self.db.close()

class DiscoveryPump:
    """Canonical metadata on its own connection and thread, never a quote gate."""
    def __init__(self,rpc_factory,path,directory,stop,finish):
        self.rpc_factory=rpc_factory;self.path=Path(path);self.directory=Path(directory)
        self.stop=stop;self.finish=finish;self.lock=threading.Lock()
        self.thread=None;self.health={};self.keys=None;self.touched=set();self.revision=0
    def publish(self,inventory,state,changed=(),**detail):
        metadata=dict(state=state,at=time.time(),**detail)
        keys=None
        if inventory is not None:
            keys=dict(inventory.registry.keys)
            metadata.update(known_pools=len(keys),cursor=inventory.get('cursor'),backfill=inventory.get('backfill'),
                activity_history_cursor=inventory.get('activity_cursor'),
                activity_recent_cursor=inventory.get('activity_recent_cursor'),
                activity=inventory.get('activity_status',{}),
                recent_activity=inventory.get('activity_recent_status',{}))
        with self.lock:
            if keys is not None:self.keys=keys
            self.touched.update(changed)
            self.health=metadata;self.revision+=1
        atomic_json(self.directory/'v4-discovery-health.json',metadata)
    def take(self):
        with self.lock:
            result=(self.revision,None if self.keys is None else dict(self.keys),set(self.touched),dict(self.health))
            self.touched.clear()
            return result
    def run(self):
        rpc=inventory=None;failures=0;verified=False
        from .v4_peers import PeerFinder
        peers=PeerFinder()
        try:
            while not self.stop.is_set() and time.monotonic()<self.finish:
                stage='initialize';started=time.monotonic()
                try:
                    if rpc is None:rpc=self.rpc_factory()
                    if inventory is None:inventory=Inventory(self.path)
                    if not verified:
                        stage='verify_chain';rpc.verify_chain();verified=True
                    stage='head';head=rpc.header()
                    # Each bounded phase is independent: a provider's historical
                    # failure cannot hide live keys, and activity-specific failure
                    # cannot block successful Initialize or counterpart discovery.
                    phase_errors=[]
                    for stage,operation in (
                        ('recent_active_pool_keys',lambda:inventory.active_keys(rpc,head)),
                        ('canonical_initializations',lambda:inventory.scan(rpc,head)),
                        ('funded_counterpart_keys',lambda:peers.step(inventory,rpc,head))):
                        if self.stop.is_set() or time.monotonic()>=self.finish:break
                        try:
                            touched=operation()
                            self.publish(inventory,'discovering',changed=touched,stage=stage)
                        except Exception as exc:
                            phase_errors.append(dict(stage=stage,error=safe_error(exc)))
                            measurements=getattr(rpc,'measurements',())
                            last=measurements[-1] if measurements else {}
                            self.publish(inventory,'retrying',stage=stage,error=safe_error(exc),
                                rpc_method=last.get('method'),rpc_methods=last.get('methods'),
                                http_status=last.get('http_status'))
                    if phase_errors:
                        failures+=1;wait=min(30,2**min(failures,5))
                        self.publish(inventory,'retrying',stage='partial',phase_errors=phase_errors,
                            retry_at=time.time()+wait,peer_discovery=peers.last)
                    else:
                        failures=0;wait=max(0,4-(time.monotonic()-started))
                        self.publish(inventory,'connected',stage='complete',peer_discovery=peers.last)
                except Exception as exc:
                    failures+=1;wait=min(30,2**min(failures,5))
                    measurements=getattr(rpc,'measurements',())
                    last=measurements[-1] if measurements else {}
                    self.publish(inventory,'retrying',stage=stage,error=safe_error(exc),
                        retry_at=time.time()+wait,rpc_method=last.get('method'),rpc_methods=last.get('methods'),
                        http_status=last.get('http_status'))
                # Quote events wake only the reader. They cannot shorten a
                # provider's retry gate or restart an in-flight metadata read.
                self.stop.wait(min(wait,max(0,self.finish-time.monotonic())))
        finally:
            if inventory is not None:inventory.close()
            if rpc is not None:rpc.close()
    def start(self):
        self.thread=threading.Thread(target=self.run,name='v4-canonical-discovery',daemon=True);self.thread.start()

class Worker:
    def __init__(self,rpc_factory,config,directory,stop,finish,discovery_rpc_factory=None):
        self.rpc_factory=rpc_factory;self.config=config;self.directory=Path(directory)
        self.stop=stop;self.finish=finish;self.output={};self.output_lock=threading.Lock()
        self.hints=queue.Queue(maxsize=8);self.health={};self.thread=None;self.pool_ids=();self.wake=threading.Event()
        self.discovery_rpc_factory=discovery_rpc_factory or rpc_factory;self.discovery=None
    def publish(self,logs):
        matching=[l for l in logs if l.get('address','').lower()==MANAGER and l.get('topics',[None])[0] in TOPICS]
        if not matching:return
        try:self.hints.put_nowait(matching[:256]);self.wake.set()
        except queue.Full:pass
    def status(self,state,**detail):
        self.health.update(state=state,at=time.time(),**detail)
        atomic_json(self.directory/'v4-health.json',self.health)
    def run(self):
        rpc=self.rpc_factory();inventory=None
        try:
            self.directory.mkdir(parents=True,exist_ok=True)
            (self.directory/'v4-evidence').mkdir(exist_ok=True)
            inventory_path=Path(self.config.get('_v4_inventory_path',ROOT/'evidence/v4-inventory.sqlite'))
            inventory_path.parent.mkdir(parents=True,exist_ok=True)
            inventory=Inventory(inventory_path)
            self.discovery=DiscoveryPump(self.discovery_rpc_factory,inventory_path,self.directory,self.stop,self.finish)
            self.discovery.start();discovery_revision=0;discovery_health={}
            reader=Reader(rpc,self.config);pending=deque();queued=set();urgent=deque();urgent_ids=set();failures=0;state_races=0;turn=0;event_backlog={}
            transport_failures=0;reconnections=0
            while not self.stop.is_set() and time.monotonic()<self.finish:
                self.stop.wait(max(0,.2-(time.monotonic()-locals().get("last_started",0))))
                started=time.monotonic();last_started=started
                try:
                    touched=set()
                    revision,canonical,discovered,discovery_health=self.discovery.take()
                    if revision!=discovery_revision:
                        discovery_revision=revision
                        if canonical is not None:
                            # Canonical removals must remove old keys after a
                            # reorg; keep only still-live unconfirmed init hints.
                            temporary={pid:inventory.registry.keys[pid] for pid in inventory.tentative
                                       if pid in inventory.registry.keys and pid not in canonical}
                            inventory.registry.keys=canonical
                            for pid,key in temporary.items():
                                if len(inventory.registry.keys)<inventory.registry.maximum:inventory.registry.keys[pid]=key
                            inventory.tentative={pid:expiry for pid,expiry in inventory.tentative.items()
                                                 if pid in temporary and pid in inventory.registry.keys}
                    touched.update(discovered)
                    while not self.hints.empty():
                        for log in self.hints.get_nowait():
                            # Pending initialization may be reverted; canonical inventory
                            # accepts it only during scan. Known events just prioritize.
                            inventory.pending(log)
                            pid=log['topics'][1].lower() if len(log['topics'])>1 else ''
                            if pid in inventory.registry.keys:touched.add(pid)
                    inventory.expire_pending()
                    self.pool_ids=tuple(inventory.registry.keys)
                    # Advance the ordinary page cursor only by available queue
                    # capacity. Dropped pages can otherwise form a repeating cycle
                    # that permanently starves many pairs even in a static universe.
                    free=128-len(pending)
                    pairs=inventory.registry.pairs(USDC,limit=min(32,free)) if free else []
                    for a,b in pairs:
                        ids=tuple(sorted((a.id,b.id)))
                        if ids not in queued:pending.append((a,b));queued.add(ids)
                    groups={}
                    for pid,key in inventory.registry.keys.items():
                        groups.setdefault((key.currency0,key.currency1),[]).append((pid,key))
                    for pid in touched:
                        if pid in inventory.registry.keys:event_backlog.setdefault(pid,0)
                    for pid in list(event_backlog):
                        if pid not in inventory.registry.keys:event_backlog.pop(pid,None)
                    scheduled=0
                    for pid,offset in list(event_backlog.items()):
                        if self.stop.is_set() or len(urgent)>=64 or scheduled>=32:break
                        active=inventory.registry.keys[pid]
                        peers=groups[(active.currency0,active.currency1)]
                        scanned=0
                        while scanned<min(32,len(peers)) and len(urgent)<64:
                            peer_id,peer=peers[(offset+scanned)%len(peers)];scanned+=1
                            if peer_id==pid:continue
                            ids=tuple(sorted((pid,peer_id)))
                            if ids not in urgent_ids:urgent.append((active,peer));urgent_ids.add(ids)
                        scheduled+=1
                        if offset+scanned>=len(peers):event_backlog.pop(pid,None)
                        else:
                            event_backlog.pop(pid,None);event_backlog[pid]=offset+scanned
                    if pending or urgent:
                        turn+=1
                        if urgent and (turn%4 or not pending):
                            keys=urgent.popleft();urgent_ids.discard(tuple(sorted(k.id for k in keys)))
                        else:
                            keys=pending.popleft();queued.discard(tuple(sorted(k.id for k in keys)))
                        if any(k.id not in inventory.registry.keys for k in keys):continue
                        frame=reader.read(keys);report=evaluate(frame,self.config)
                        reader.request_missing(keys,report['missing_slots'])
                        # Missing depth gets a follow-up read, but never monopolizes
                        # the queue. No old state value enters that next snapshot.
                        if report['missing_slots'] and len(pending)<128:
                            ids=tuple(sorted(k.id for k in keys))
                            if ids not in queued:pending.append(keys);queued.add(ids)
                        self.offer(frame,report)
                        if report['candidates']:
                            from .retention import save_opportunity
                            save_opportunity(self.directory/'v4-evidence',time.time_ns(),
                                {'requested_ns':frame.requested_ns,'received_ns':frame.received_ns,'received_utc':frame.received_utc,
                                 'anchor':frame.anchor,'context':frame.context,'keys':[k.values() for k in frame.keys],
                                 'storage':{hex(k):hex(v) for k,v in frame.snapshot.storage.items()},'decision':report},
                                16*1024**2,64)
                        atomic_json(self.directory/'v4-latest-quote.json',{**report,'observed_at':frame.received_utc})
                        self.status('scanning',known_pools=len(inventory.registry.keys),cursor=inventory.get('cursor'),
                            backfill=inventory.get('backfill'),block=report['block'],candidates=len(report['candidates']),
                            last_state_at=frame.received_utc,missing_slots=len(report['missing_slots']),discovery=discovery_health,
                            error=None,read_phase=getattr(reader,'last_phase',None),transport=read_transport(rpc),
                            connection_rebuilds=reconnections,transport_failures=0,reconnect_error=None)
                    else:self.status('discovering',known_pools=len(inventory.registry.keys),cursor=inventory.get('cursor'),backfill=inventory.get('backfill'),discovery=discovery_health)
                    failures=0;state_races=0;transport_failures=0
                except Exception as exc:
                    if isinstance(exc,Reject) and str(exc)=='V4 state anchor changed':
                        # A new block invalidates this snapshot, not the transport.
                        # Refresh from a new anchor with bounded, phase-shifting
                        # pacing instead of exponential multi-block blind spots.
                        state_races+=1;failures=0
                    else:failures+=1;state_races=0
                    transport_failures=transport_failures+1 if transport_stalled(exc) else 0
                    failed_transport=read_transport(rpc)
                    reconnect_error=None
                    if transport_failures>=3:
                        # Preserve depth addresses, inventory and the worker's
                        # queues. A new transport must verify the protocol again.
                        try:replacement=self.rpc_factory()
                        except Exception as reconnect_exc:reconnect_error=safe_error(reconnect_exc)
                        else:
                            previous=rpc;rpc=replacement;reader.rpc=rpc;reader.verified=False
                            reconnections+=1;transport_failures=0
                            try:previous.close()
                            except Exception as close_exc:reconnect_error=safe_error(close_exc)
                    self.status('retrying',error=safe_error(exc),read_phase=getattr(reader,'last_phase',None),
                                transport=failed_transport,connection_rebuilds=reconnections,
                                transport_failures=transport_failures,reconnect_error=reconnect_error)
                delay=(min(15,2**min(failures,4)) if failures else min(1,.25+.25*state_races)
                       if state_races else max(.5,float(self.config.get('v4',{}).get('poll_seconds',1))))
                until=time.monotonic()+max(0,delay-(time.monotonic()-started))
                while not self.stop.is_set() and time.monotonic()<until:
                    if failures or state_races:
                        self.stop.wait(min(.1,max(0,until-time.monotonic())));continue
                    if self.wake.wait(min(.1,max(0,until-time.monotonic()))):break
                self.wake.clear()
        except BaseException:
            # A fatal reader failure must also release its child discovery
            # thread; the shared stop lets the enclosing pipeline close cleanly.
            self.stop.set()
            raise
        finally:
            if self.discovery is not None:self.discovery.thread.join()
            if inventory:inventory.close()
            rpc.close()
    def start(self):
        self.thread=threading.Thread(target=self.run,name='v4-discovery-state',daemon=True);self.thread.start()
    def offer(self,frame,report):
        group=tuple(sorted(k.id for k in frame.keys))
        with self.output_lock:
            self.output[group]=(frame,report)
            if len(self.output)>32:
                victim=min(self.output,key=lambda k:(bool(self.output[k][1]['candidates']),self.output[k][0].requested_ns))
                self.output.pop(victim)
    def consume(self,executor):
        with self.output_lock:
            now=time.monotonic_ns();ttl=int(self.config['policy']['snapshot_max_age_ms']*1e6)
            for key,(frame,_) in list(self.output.items()):
                if now-frame.requested_ns>ttl:self.output.pop(key)
            if not self.output:return None
            group=max(self.output,key=lambda k:max([c['net_after_modelled_cost'] for c in self.output[k][1]['candidates']] or [-1]))
            frame,report=self.output.pop(group)
        result={'at':time.time(),'block':report['block'],'execution_kind':'v4-pair-v1','status':'quoted','candidates':len(report['candidates'])}
        if executor is None or not report['candidates']:
            result['status']='awaiting_executor' if report['candidates'] else 'no_profitable_complete_quote'
        else:
            candidate=report['candidates'][0];route=Route(Key(*candidate['v4_keys'][0]),Key(*candidate['v4_keys'][1]))
            deadline=frame.requested_ns+int(self.config['policy']['snapshot_max_age_ms']*1e6)
            try:result.update(executor.execute(candidate,route,{},frame.anchor,deadline))
            except Exception as exc:result.update(status='execution_rejected',reason=safe_error(exc))
        atomic_json(self.directory/'v4-execution.json',result)
        return result


def stream_options():
    """Virtual ids keep per-V4-pool metering while using the manager log address."""
    from .events import POOL_ACTIVITY_TOPICS
    def family(member):return 'init' if member=='v4:init' else 'v4' if member.startswith('v4:') else 'cl'
    def filter_for(members):
        category=family(members[0])
        check(all(family(m)==category for m in members),'mixed protocol subscription filter')
        if category=='init':return {'address':[MANAGER],'topics':[INITIALIZE]}
        if category=='v4':return {'address':[MANAGER],'topics':[list(TOPICS[1:]),[p[3:] for p in members]]}
        return {'address':members,'topics':[list(POOL_ACTIVITY_TOPICS)]}
    def log_key(log):
        if log.get('address','').lower()!=MANAGER:return log.get('address','').lower()
        topics=log.get('topics',[])
        return 'v4:init' if topics and topics[0]==INITIALIZE else 'v4:'+topics[1] if len(topics)>1 else ''
    return dict(filter_for=filter_for,log_key=log_key,family=family)
