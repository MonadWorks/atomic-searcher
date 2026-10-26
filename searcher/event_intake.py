"""Independent, bounded log pages. A failed page never rolls back another.

These events only discover identities and prioritize reads; execution always
revalidates current state. Every address retains its own contiguous checkpoint.
"""
from collections import OrderedDict
import time
from urllib.parse import urlsplit
import httpx
from .core import number, check, safe_error, RPC, RPCError, RPCUnavailable, RPCAccessDenied


class EventRPC(RPC):
    """One network attempt, retaining its FIFO place during provider cooldown.

    Event discovery has no expiring trading opportunity. HTTP timeout and
    cancellation still bound network work; do not expire the pacing ticket.
    """
    read_attempts=1
    # Both sources served the formerly denied historical page on 2026-10-18.
    FALLBACKS=('https://base.gateway.tenderly.co','https://base.public.blockpi.network/v1/rpc/public')
    preferred={}

    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.peers=[self];self.verified_peers=set();self.peer_errors={}
        for url in self.FALLBACKS:
            if url==self.url:continue
            peer=RPC(url,chain_id=self.chain_id,timeout=self.timeout,cancel_event=self.cancel_event)
            peer.read_attempts=1;peer.minimum_request_interval=.5;self.peers.append(peer)
        self.peer_index=next((i for i,p in enumerate(self.peers) if p.url==self.preferred.get(self.url)),0)

    @property
    def active_host(self):return urlsplit(self.peers[self.peer_index].url).hostname

    @staticmethod
    def retryable(exc):
        # Public dRPC returns HTTP 400/code 35 for free-plan historical
        # limits, even for a small valid page. Other providers can serve it.
        if isinstance(exc,httpx.HTTPStatusError) and exc.response.status_code==400:
            try:
                error=exc.response.json().get('error',{})
                message=error.get('message','').lower()
                if error.get('code')==35 and 'range' in message and 'block' in message:return True
            except (ValueError,AttributeError,TypeError):pass
        if isinstance(exc,RPCError) and exc.code==35 and 'range' in exc.rpc_message.lower() and 'block' in exc.rpc_message.lower():return True
        return (isinstance(exc,(httpx.TransportError,RPCUnavailable,RPCAccessDenied)) or
            isinstance(exc,httpx.HTTPStatusError) and exc.response.status_code in (403,408,429,500,502,503,504) or
            isinstance(exc,RPCError) and exc.code in (-32005,-32007) and 'limit' in exc.rpc_message.lower())

    def request(self,method,params):
        # Some transport tests construct the base object without connections.
        if not hasattr(self,'peers'):return super().request(method,params)
        check(method in RPC.METHODS,'event reader permits read-only RPC methods')
        last=None
        for step in range(len(self.peers)):
            index=(self.peer_index+step)%len(self.peers);peer=self.peers[index]
            self.check_cancelled()
            budget=peer.provider_budget
            if budget is not None and budget.cooldown_until>time.monotonic():
                last=RPCUnavailable('event provider cooling down');continue
            peer.deadline_ns=self.deadline_ns
            peer.provider_urgent=bool(getattr(self,'live_events',False))
            try:
                if index not in self.verified_peers:
                    chain=RPC.request(peer,'eth_chainId',[])
                    try:valid_chain=number(chain)==self.chain_id
                    except (TypeError,ValueError):valid_chain=False
                    if not valid_chain:raise RPCUnavailable('event reader chain mismatch')
                    self.verified_peers.add(index)
                result=RPC.request(peer,method,params)
                self.peer_index=index;self.preferred[self.url]=peer.url
                if peer is not self and peer.measurements:self.measurements.append(dict(peer.measurements[-1]))
                return result
            except Exception as exc:
                if not self.retryable(exc):raise
                self.peer_errors[urlsplit(peer.url).hostname]=safe_error(exc);last=exc
        raise last or RPCUnavailable('event reader unavailable')

    def close(self):
        for peer in getattr(self,'peers',[])[1:]:peer.close()
        super().close()


class EventIntake:
    def __init__(self, cursors=None, legacy_cursor=None, clock=time.monotonic, pending=None):
        self.cursors = dict(cursors or {})
        self.legacy_cursor = legacy_cursor
        self.clock = clock
        self.initialized = False
        self.turn = 0
        self.offset = {'factories': 0, 'oracles': 0}
        self.width = {'morpho': 1, 'factories': 8, 'oracles': 16}
        self.span = {'morpho': 32, 'factories': 32, 'oracles': 16}
        self.due = {}
        self.seen = OrderedDict()
        self.pages = {}
        self.successes = {}
        self.pending = OrderedDict((self.key(row['log']), dict(row)) for row in (pending or []))
        self.recent_cursor = None
        self.recent_cursors = {}
        self.width_successes = {}

    @staticmethod
    def key(log): return (log['blockHash'], log['transactionHash'], log['logIndex'])

    def retry_identity(self, rpc, apply):
        for key, row in list(self.pending.items()):
            if row.get('next_retry_at', 0)>time.time(): continue
            self.pending.move_to_end(key)
            try:
                log=row['log']
                if rpc.header(number(log['blockNumber']))['hash']==log['blockHash']:
                    apply(rpc,log)
                    self.seen[key]=None
                    while len(self.seen)>4096:self.seen.popitem(last=False)
                self.pending.pop(key)
            except Exception as exc:
                row.update(error=safe_error(exc),attempts=row.get('attempts',0)+1,next_retry_at=time.time()+30)
            break

    def sync(self, groups, height):
        active = {a for values in groups.values() for a in values}
        for a in active:
            if a not in self.cursors:
                self.cursors[a] = (self.legacy_cursor if not self.initialized and self.legacy_cursor is not None
                                   else max(0, height - 32))
        self.cursors = {a: n for a, n in self.cursors.items() if a in active}
        self.recent_cursors = {a:n for a,n in self.recent_cursors.items() if a in active}
        self.initialized = True

    def selected(self, kind, addresses):
        if not addresses: return []
        if kind == 'morpho': return addresses[:1]
        offset = self.offset[kind] % len(addresses)
        selected = (addresses[offset:] + addresses[:offset])[:self.width[kind]]
        self.offset[kind] = (offset + len(selected)) % len(addresses)
        return selected

    @staticmethod
    def oversized(exc):
        if isinstance(exc, httpx.TimeoutException): return True
        if isinstance(exc, RPCUnavailable): return 'total request deadline' in str(exc).lower() or 'timeout' in str(exc).lower()
        if isinstance(exc, httpx.HTTPStatusError): return exc.response.status_code in (408, 413, 504)
        return isinstance(exc, RPCError) and exc.method=='eth_getLogs' and any(s in str(exc).lower() for s in
            ('too many results', 'response size', 'block range too large', 'query timeout'))

    def read(self, rpc, kind, addresses, head, apply, topics=None, recent=False):
        now = self.clock()
        label=kind+'_recent' if recent else kind
        if not addresses or now < self.due.get(label, 0): return False
        height = number(head['number'])
        start = max(0, min(height, min(self.cursors[a] for a in addresses) - 2))
        if recent:
            window=8 if kind=='morpho' else 31
            previous=min((self.recent_cursors.get(a,0) for a in addresses),default=0)
            start=max(0,min(height,max(height-window,previous-2)))
        end = height if recent else min(height, start + self.span[kind] - 1)
        stage = 'anchor'
        started = time.monotonic()
        detail = dict(kind=kind, addresses=addresses, from_block=start, to_block=end)
        try:
            anchor = head if end == height else rpc.header(end)
            query = {'address': addresses, 'fromBlock': hex(start), 'toBlock': hex(end)}
            if topics: query['topics'] = [list(topics)]
            stage = 'logs'
            logs = rpc.request('eth_getLogs', [query])
            check(isinstance(logs, list), 'invalid event page')
            for log in logs:
                check(not log.get('removed') and log['address'].lower() in addresses and
                      start <= number(log['blockNumber']) <= end, 'event page mismatch')
                check(not topics or log.get('topics', [None])[0] in topics, 'event topic mismatch')
            stage = 'canonical'
            check(rpc.header(end)['hash'] == anchor['hash'], 'event head changed')
            stage = 'apply'
            applied = 0
            apply_errors = []
            for log in sorted(logs, key=lambda l: (number(l['blockNumber']), number(l['logIndex']))):
                key = self.key(log)
                if key in self.seen or key in self.pending: continue
                try: apply(rpc, log)
                except Exception as exc:
                    if len(self.pending)>=256: apply_errors.append(exc)
                    else:self.pending[key]=dict(log=log,error=safe_error(exc),attempts=1,next_retry_at=time.time()+30)
                    continue
                self.seen[key] = None
                applied += 1
                while len(self.seen) > 4096: self.seen.popitem(last=False)
            if apply_errors: raise apply_errors[0]
            if recent:
                for a in addresses:self.recent_cursors[a]=end+1
                if kind=='morpho':self.recent_cursor=end+1
            else:
                for a in addresses:self.cursors[a] = max(self.cursors[a], end + 1)
            self.due[label] = 0
            # Recent success says nothing about the size of a historical
            # query. Adapt them separately; recover address fan-out after a
            # transient timeout instead of remaining permanently at width 1.
            if not recent:
                self.successes[kind] = self.successes.get(kind, 0)+1
                self.width_successes[kind] = self.width_successes.get(kind,0)+1
                if self.successes[kind] >= 4:
                    self.span[kind] = min(32 if kind=='morpho' else 1000, self.span[kind]*2)
                    self.successes[kind] = 0
                if self.width_successes[kind]>=8:
                    self.width[kind]=min({'morpho':1,'factories':8,'oracles':16}[kind],self.width[kind]*2)
                    self.width_successes[kind]=0
            self.pages[label] = dict(at=time.time(), state='connected', **detail, events=len(logs),
                applied=applied, lag_blocks=max(0, height-end), last_success_at=time.time(),last_success_block=end,
                elapsed_ms=(time.monotonic()-started)*1000)
            return True
        except Exception as exc:
            # No synchronous fan-out on throttles. The next rotation makes
            # bounded, smaller progress after expensive-query failures.
            if stage == 'logs' and self.oversized(exc):
                if self.width[kind] > 1: self.width[kind] = max(1, self.width[kind] // 2)
                else: self.span[kind] = max(8, self.span[kind] // 2)
            self.successes[kind] = 0
            self.width_successes[kind] = 0
            self.due[label] = now + (5 if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 429 else 1)
            previous=self.pages.get(label,{})
            measurements=getattr(rpc,'measurements',[])
            timing=measurements[-1] if measurements else {}
            self.pages[label] = dict(at=time.time(), state='retrying', **detail, stage=stage,
                error=safe_error(exc), next_page_addresses=self.width[kind],
                last_success_at=previous.get('last_success_at'),last_success_block=previous.get('last_success_block'),
                read_timing={k:timing.get(k) for k in ('method','elapsed_ms','network_total_ms','public_limiter_wait_ms','http_status')},
                next_page_blocks=self.span[kind], elapsed_ms=(time.monotonic()-started)*1000)
            return False

    def poll(self, rpc, groups, apply, factory_topics=()):
        head = rpc.header()
        height = number(head['number'])
        groups = {k: sorted(set(a.lower() for a in v)) for k, v in groups.items()}
        self.sync(groups, height)
        # A restored historical checkpoint must not hide new borrowers while
        # older pages catch up. The recent cursor never advances the backfill.
        if any(self.cursors[a]<height-self.span['morpho'] for a in groups['morpho']):
            self.read(rpc,'morpho',groups['morpho'],head,apply,recent=True)
        else:
            self.pages.pop('morpho_recent',None)
            self.recent_cursor=None
        # Morpho position changes get a turn on every poll. Slow factory or
        # feed pages retain their checkpoint and alternate independently.
        selected = [('morpho', self.selected('morpho', groups['morpho']))]
        extras = [k for k in ('factories', 'oracles') if groups[k]]
        if extras:
            kind = extras[self.turn % len(extras)]
            self.turn += 1
            selected.append((kind, self.selected(kind, groups[kind])))
        for kind, addresses in selected:
            rpc.check_cancelled()
            if kind!='morpho':
                # New pools/feed updates must not queue behind old history.
                # A recent page is only a hint: contiguous cursors stay intact.
                if any(self.cursors[a]<height-31 for a in addresses):
                    self.read(rpc,kind,addresses,head,apply,
                        factory_topics if kind=='factories' else None,recent=True)
                if all(self.cursors[a]>=height-31 for a in groups[kind]):
                    self.pages.pop(kind+'_recent',None)
            self.read(rpc, kind, addresses, head, apply, factory_topics if kind == 'factories' else None)
        self.retry_identity(rpc,apply)
        covered = min(self.cursors.values(), default=height+1) - 1
        return dict(at=time.time(), state='connected' if all(p['state']=='connected' for p in self.pages.values()) else 'degraded',
            block=covered, head=height, lag_blocks=max(0, height-covered),
            read_source=getattr(rpc,'active_host',None),
            recent_lag_by_group={k:max((max(0,height-max(self.cursors[a],self.recent_cursors.get(a,0))+1)
                for a in values),default=0) for k,values in groups.items()},
            primary_block=max(min((self.cursors[a] for a in groups['morpho']), default=height+1),self.recent_cursor or 0)-1,
            pages=dict(self.pages), pending_identities=len(self.pending),
            market_scope='independent Morpho, factory and oracle event pages')
