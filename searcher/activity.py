"""Filtered event delivery with bounded usage; executable prices are reread.

Provider API: https://www.alchemy.com/docs/reference/base-flashblocks-api-quickstart
Only chain identity and subscriptions are requested over this connection.
"""
import json
import os
import threading
import time
from collections import OrderedDict
from pathlib import Path
from urllib.parse import urlsplit

from .core import ROOT, Reject, address, atomic_json, check, number
from .events import V3_SWAP, SLIP_SWAP, V2_SYNC, POOL_ACTIVITY_TOPICS
from .universe import ActivityHints


class CycleSubscriptions:
    """Reuse the complete route index to spend push capacity on usable pools.

    Until that topology has an index, keep every known pool. Object identity
    prevents a slow reader from publishing an older topology's smaller set.
    This only selects event subscriptions; discovery and prices stay intact.
    """
    def __init__(self, saved):
        self.lock = threading.Lock()
        self.replace(saved)

    def replace(self, saved):
        from .cycle_groups import CycleGroups
        known = {p['address'] for p in saved.get('known_pools', saved.get('pools', [])) if p.get('address')}
        groups = saved.get('cycle_groups')
        # prepare() already built this authenticated full index. Use it in the
        # same replacement to avoid expanding/shrinking filters each refresh.
        # Disk snapshots and legacy JSON lists have no trusted derived index.
        covered = set(groups.members) if isinstance(groups, CycleGroups) else None
        if covered is not None: check(covered <= known, 'subscription index contains unknown pools')
        with self.lock:
            self.topology, self.known = saved, known
            self.selected = set(known) if covered is None else covered
            self.indexed = covered is not None

    def use_index(self, saved, pages):
        covered = getattr(pages, 'covered', None)
        if covered is None: return False
        covered = set(covered)
        with self.lock:
            if saved is not self.topology: return False
            check(covered <= self.known, 'subscription index contains unknown pools')
            self.selected, self.indexed = covered, True
            return True

    def snapshot(self):
        with self.lock: return set(self.selected)

    def details(self):
        with self.lock:
            return {'subscription_scope': 'executable_cycle_members' if self.indexed else 'known_pools_until_indexed',
                    'known_universe_pools':len(self.known), 'cycle_subscription_pools':len(self.selected),
                    'noncycle_subscription_pools_skipped':len(self.known-self.selected)}


class ActivityInbox:
    """Coalesce bursts and wake readers for newly published generations.

    Generations describe delivery, not successful state reads or prices. A
    consumer acknowledges its own successful reads and retains failed work.
    """
    def __init__(self):
        self.lock, self.pending = threading.Lock(), OrderedDict()
        self.condition = threading.Condition(self.lock)
        self._generation = 0

    def publish(self, pools, observed_at):
        with self.condition:
            changed = False
            for pool in pools:
                self.pending[pool] = observed_at
                self.pending.move_to_end(pool)
                while len(self.pending) > 4096: self.pending.popitem(last=False)
                changed = True
            if changed:
                self._generation += 1
                self.condition.notify_all()

    @property
    def generation(self):
        with self.lock:
            return self._generation

    def drain_batch(self, now):
        """Atomically capture the delivered generation and its fresh events."""
        with self.lock:
            result = [(pool,at) for pool,at in self.pending.items() if 0 <= now-at <= 5]
            self.pending.clear()
            return self._generation, result

    def drain(self, now):
        return self.drain_batch(now)[1]

    def wait(self, timeout, *, after=None, stop=None):
        """Wait for delivery after a captured generation, without consuming it.

        Pass the generation from drain_batch to avoid a drain/wait race.
        With no cursor, an already queued batch returns immediately. The
        returned generation is unchanged on timeout; cancellation is checked
        at least every 50 ms when a stop event is supplied.
        """
        deadline = time.monotonic()+max(0, timeout)
        with self.condition:
            if after is None:
                after = self._generation-int(bool(self.pending))
            while self._generation <= after and not (stop is not None and stop.is_set()):
                remaining = deadline-time.monotonic()
                if remaining <= 0: break
                self.condition.wait(min(remaining, .05) if stop is not None else remaining)
            return self._generation


class ActivityJournal:
    """Local reception times, with bounded disk use and no provider credentials."""
    def __init__(self, path, max_bytes=8*1024**2):
        self.path, self.max_bytes, self.lock = Path(path), max_bytes, threading.Lock()

    def append(self, logs, source, received_utc):
        if not logs: return
        # Retain only public event fields needed to match later sealed receipts.
        fields = ('address','topics','data','blockNumber','blockHash','transactionHash','transactionIndex','logIndex')
        rows = [{k:entry[k] for k in fields if k in entry} for entry in logs]
        line = json.dumps({'received_utc':received_utc, 'source':source, 'logs':rows}, separators=(',',':'))+'\n'
        if len(line.encode()) > self.max_bytes: return
        with self.lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if self.path.exists() and self.path.stat().st_size+len(line.encode()) > self.max_bytes:
                self.path.replace(self.path.with_suffix('.previous.jsonl'))
            with self.path.open('a') as f: f.write(line)


def websocket_url():
    """Optional local endpoint never appears in saved session config or errors."""
    value = os.environ.get('BASE_ACTIVITY_WS_URL')
    if not value:
        path = ROOT/'config/node.local.json'
        if path.exists(): value = json.loads(path.read_text()).get('activity_ws_url')
    if not value: return None
    validate_url(value)
    return value


def validate_url(value):
    parsed = urlsplit(value)
    check(parsed.scheme == 'wss' and parsed.hostname and not parsed.username and not parsed.fragment,
          '事件节点必须使用有效的 wss 地址')


def configure():
    from getpass import getpass
    print('填写 Base Flashblocks 的 WebSocket 节点地址，格式为 wss://…。')
    print('地址只保存在本机。留空取消，原配置保持不变。')
    value = getpass('节点地址（隐藏输入）: ').strip()
    if not value: return
    validate_url(value)
    print('正在核对 Base 主网和 Flashblocks 订阅能力…')
    verify_endpoint(value)
    path = ROOT/'config/node.local.json'
    import tempfile
    check(not path.parent.is_symlink(), '节点配置目录不可使用符号链接')
    fd,name = tempfile.mkstemp(prefix='.node-',dir=path.parent)
    temp=Path(name)
    try:
        with os.fdopen(fd,'w') as f:
            os.fchmod(f.fileno(),0o600)
            json.dump({'activity_ws_url':value},f)
            f.flush(); os.fsync(f.fileno())
        os.replace(temp,path)
    finally:
        if temp.exists():temp.unlink()
    print('订阅验证通过，已保存。下次启动时自动使用推送；连接失败时继续使用免费 HTTP 数据。')
    return True


def verify_endpoint(url):
    import tempfile
    stop=threading.Event();ready=[]
    def health(state,**detail):
        if state=='connected':ready.append(True);stop.set()
    with tempfile.TemporaryDirectory(prefix='base-stream-check-') as directory:
        budget=StreamBudget(Path(directory)/'usage.json',65536)
        try:
            stream_session(url,8453,lambda:{'0x1ef035205f94c7734827961c438474d089a334a0'},
                           stop,time.monotonic()+8,lambda *args:None,health,budget)
        except InterruptedError:pass
    check(ready, '事件节点订阅未通过验证')


class StreamBudget:
    """Persist a received-byte ceiling across reconnects and restarts.

    This limits local data volume; provider accounting can differ.
    """
    def __init__(self, path, limit):
        self.path, self.limit = Path(path), limit
        self.state = {'started_at':time.time(), 'bytes':0}
        if self.path.exists():
            loaded = json.loads(self.path.read_text())
            check(isinstance(loaded.get('bytes'),int) and loaded['bytes'] >= 0, 'invalid stream usage journal')
            self.state = loaded
        self._refresh()

    def _refresh(self):
        if time.time()-self.state['started_at'] >= 3600:
            self.state = {'started_at':time.time(), 'bytes':0}

    def remaining_seconds(self):
        self._refresh()
        return max(0, self.state['started_at']+3600-time.time()) if self.state['bytes'] >= self.limit else 0

    def add(self, size, kind='unknown'):
        self._refresh(); self.state['bytes'] += size
        check(self.state['bytes'] < self.limit, 'stream usage limit reached')

    def save(self): atomic_json(self.path, self.state)


def stream_session(url, chain_id, pools, stop, finish, publish, health, budget, connect=None, selector=None, roster=None, filter_for=None, log_key=None, family=None):
    """One authenticated subscription session, including dynamic pool updates."""
    if connect is None:
        from websockets.sync.client import connect
    if roster is None:
        from .stream_usage import SubscriptionRoster
        roster = SubscriptionRoster(confirm_admissions=True)
    from .stream_usage import StreamPacingPause
    identifiers, active, request_id = {}, set(), 0
    fully_subscribed = False
    next_request = 0
    def detail():
        result = budget.details() if hasattr(budget, 'details') else {'received_bytes':budget.state['bytes'], 'budget_kind':'local_bytes'}
        if selector is not None: result.update(selector.details())
        result.update(roster.details())
        result['http_only_pools'] = result.get('http_only_pools',0)+len(roster.waiting)
        return result
    def alive(): return not stop.is_set() and time.monotonic() < finish
    with connect(url, open_timeout=3, close_timeout=1, ping_interval=5, ping_timeout=3,
                 max_size=65536, max_queue=32) as ws:
        last_health = 0
        def receive(deadline=None, reconfigure=False):
            nonlocal last_health
            while alive():
                check(not budget.remaining_seconds(), 'stream request budget reached')
                if deadline is not None and time.monotonic() >= deadline: raise Reject('event subscription timed out')
                try: raw = ws.recv(timeout=.2)
                except TimeoutError: continue
                size = len(raw.encode() if isinstance(raw,str) else raw)
                try: obj = json.loads(raw)
                except Exception:
                    budget.add(size,kind='invalid')
                    raise
                kind = ('notification' if isinstance(obj,dict) and obj.get('method') == 'eth_subscription'
                        else 'control' if isinstance(obj,dict) else 'invalid')
                budget.add(size,kind=kind)
                check(isinstance(obj,dict), 'invalid event envelope')
                check('error' not in obj, 'event provider rejected request')
                if obj.get('method') == 'eth_subscription':
                    params = obj.get('params',{})
                    log = params.get('result')
                    if selector is not None and isinstance(log,dict) and log.get('address'):
                        selector.observe(log_key(log) if log_key else log['address'].lower())
                    if params.get('subscription') in identifiers and isinstance(log,dict):
                        if (log_key(log) if log_key else log.get('address','').lower()) in active: publish([log], 'pendingLogs')
                    if fully_subscribed and time.monotonic()-last_health >= 1:
                        health('connected', filtered_pools=len(active), **detail())
                        budget.save(); last_health = time.monotonic()
                    # The notification is metered and delivered before pacing
                    # can close the socket. In-transit provider cost is unknown.
                    # Unsubscribe replies can follow already-buffered logs.
                    # Meter and deliver those logs; the request deadline and
                    # actual budgets bound a stalled provider without a second
                    # arbitrary eight-message shutdown of every subscription.
                    check(not budget.remaining_seconds(), 'stream request budget reached')
                    if hasattr(budget,'pacing_delay') and budget.pacing_delay():
                        raise StreamPacingPause('stream pacing pause')
                    if reconfigure and selector is not None and selector.changed:
                        return {'selection_changed':True}
                    continue
                if hasattr(budget,'pacing_delay') and budget.pacing_delay():
                    raise StreamPacingPause('stream pacing pause')
                return obj
            raise InterruptedError

        def request(method, params):
            nonlocal request_id, next_request
            # Keep filter changes below the free-plan 25 requests/second ceiling.
            if stop.wait(max(0,next_request-time.monotonic())) or not alive(): raise InterruptedError
            next_request = time.monotonic()+.06
            check(not budget.remaining_seconds(), 'stream request budget reached')
            if hasattr(budget,'pacing_delay') and budget.pacing_delay():
                raise StreamPacingPause('stream pacing pause')
            request_id += 1
            ws.send(json.dumps({'jsonrpc':'2.0','id':request_id,'method':method,'params':params}))
            deadline = time.monotonic()+3
            while alive():
                obj = receive(deadline)
                if obj.get('id') != request_id: continue
                check('error' not in obj and 'result' in obj, 'event provider rejected request')
                return obj['result']
            raise InterruptedError

        check(number(request('eth_chainId',[])) == chain_id, 'event provider chain mismatch')
        subscription_announced = False
        while alive():
            desired = set(pools())
            if selector is not None: desired = selector.select(desired)
            if hasattr(budget,'filter_capacity'):
                roster.capacity = min(8192,128*budget.filter_capacity())
                roster.rotation_seconds = budget.rotation_seconds()
            desired = roster.select(desired,individual=selector.individual_pools() if selector is not None else (),
                                    **({'priority':selector.priority} if getattr(selector,'priority',None) else {}))
            if desired != active or not subscription_announced:
                # HTTP fallback stays active while subscription filters change.
                fully_subscribed = False
                health('subscribing')
                # Preserve every unaffected subscription. A new pool no longer
                # tears down the full stream and recreates thousands of filters.
                retained = set()
                for sid,members in list(identifiers.items()):
                    isolate=bool(len(members)>1 and members & getattr(selector,'priority',set()))
                    if members <= desired and not isolate: retained.update(members)
                    else:
                        check(request('eth_unsubscribe',[sid]) is True, 'event unsubscribe not confirmed')
                        identifiers.pop(sid,None)
                        if selector is not None: selector.unsubscribed(members)
                active = set(retained)
                addresses = desired-retained
                individual = addresses & (selector.individual_pools() if selector is not None else set())
                ordering = {pool:i for i,pool in enumerate(roster.selected)}
                grouped = sorted(addresses-individual,key=lambda pool:ordering[pool])
                if family:
                    groups = {}
                    for pool in grouped: groups.setdefault(family(pool), []).append(pool)
                    chunks = [group[offset:offset+128] for group in groups.values() for offset in range(0,len(group),128)]
                else:
                    chunks = [grouped[offset:offset+128] for offset in range(0,len(grouped),128)]
                chunks += [[pool] for pool in sorted(individual,key=lambda pool:ordering[pool])]
                chunks.sort(key=lambda members:min(ordering[pool] for pool in members))
                filter_limit = budget.filter_capacity() if hasattr(budget,'filter_capacity') else 64
                for members in chunks:
                    if len(identifiers) >= filter_limit:
                        roster.defer(set(members))
                        continue
                    # A pool cooled while another control reply was pending
                    # must not be admitted from this stale selection.
                    if selector is not None:
                        members = [pool for pool in members if pool not in selector.cool_until or pool == selector.probe_candidate]
                    if not members: continue
                    subscription_filter = filter_for(members) if filter_for else {'address':members,'topics':[list(POOL_ACTIVITY_TOPICS)]}
                    sid = request('eth_subscribe',['pendingLogs',subscription_filter])
                    check(isinstance(sid,str) and sid, 'invalid event subscription identity')
                    check(sid not in identifiers, 'duplicate event subscription identity')
                    identifiers[sid] = set(members)
                    active.update(members)
                    roster.confirmed(members)
                    if selector is not None: selector.subscribed(members)
                fully_subscribed = bool(active)
                health('connected' if active else 'waiting_for_pools', filtered_pools=len(active), **detail())
                subscription_announced = True
            if hasattr(budget,'pacing_delay') and budget.pacing_delay():
                raise StreamPacingPause('stream pacing pause')
            try: receive(time.monotonic()+1,reconfigure=True)
            except Reject as exc:
                if str(exc) != 'event subscription timed out': raise
            # A quiet pool is normal. WebSocket ping/pong detects a dead socket.
            if active: health('connected', filtered_pools=len(active), **detail())


def run_stream(url, chain_id, pools, stop, finish, publish, health, budget, selector=None, filter_for=None, log_key=None, family=None):
    from .stream_usage import SubscriptionRoster, StreamPacingPause
    roster = SubscriptionRoster(confirm_admissions=True)
    failures = 0
    try:
        while not stop.is_set() and time.monotonic() < finish:
            delay = budget.remaining_seconds()
            if delay:
                detail = budget.details() if hasattr(budget,'details') else {'received_bytes':budget.state['bytes'],'budget_kind':'local_bytes'}
                health('budget_exhausted', retry_at=time.time()+delay, **detail)
                stop.wait(min(1,delay,max(0,finish-time.monotonic()))); continue
            pacing = budget.pacing_delay() if hasattr(budget,'pacing_delay') else 0
            if pacing:
                health('budget_pacing',retry_at=time.time()+pacing,**budget.details())
                stop.wait(min(1,pacing,max(0,finish-time.monotonic()))); continue
            try:
                health('connecting')
                stream_session(url,chain_id,pools,stop,finish,publish,health,budget,selector=selector,roster=roster,filter_for=filter_for,log_key=log_key,family=family)
                failures = 0
            except InterruptedError: break
            except StreamPacingPause:
                # stream_session's context has physically closed the socket.
                # Wait for a useful batch of credit, not a reconnect per reply.
                if hasattr(budget,'pause'): budget.pause()
                failures = 0
            except Exception as exc:
                if stop.is_set(): break
                failures += 1
                # Only exact local error strings are eligible for classification;
                # provider messages and URLs can contain credentials.
                reasons = {'event provider rejected request':'provider_rejected',
                    'event provider chain mismatch':'wrong_chain',
                    'event subscription timed out':'subscription_timeout',
                    'stream request budget reached':'local_budget',
                    'stream usage limit reached':'local_budget',
                    'invalid event envelope':'invalid_envelope',
                    'invalid event subscription identity':'invalid_subscription',
                    'duplicate event subscription identity':'duplicate_subscription',
                    'event unsubscribe not confirmed':'unsubscribe_unconfirmed'}
                health('retrying', error=type(exc).__name__, reason=reasons.get(str(exc),'transport_error'))
                stop.wait(min(30,2**min(failures,5),max(0,finish-time.monotonic())))
            finally:
                if selector is not None: selector.disconnected()
                budget.save()
    finally:
        budget.save(); health('stopped')


if __name__ == '__main__':
    try: configure()
    except Exception:
        raise SystemExit('节点配置未完成；请检查 Base 主网 wss 地址、账号访问权限及网络连接。原配置保持不变。') from None
