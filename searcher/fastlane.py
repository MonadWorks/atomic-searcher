"""Bounded event reads; hints choose complete routes, never executable prices."""
from collections import OrderedDict, deque
import copy
import threading
import time
import queue

from .core import check
from .pools import Pool
from .cycle_groups import CycleGroups
from .compatibility import eligible_pools


def endpoint_url():
    """Private node selection stays outside saved strategy/session settings."""
    import os
    import json
    from urllib.parse import urlsplit
    from .core import ROOT
    value = os.environ.get('BASE_HOT_RPC_URL')
    path = ROOT/'config/node.local.json'
    if not value and path.exists(): value = json.loads(path.read_text()).get('hot_rpc_url')
    if value:
        parsed = urlsplit(value)
        check(parsed.scheme == 'https' and parsed.hostname and not parsed.username and not parsed.fragment,
              'hot node must use an HTTPS endpoint')
    return value


class LatestFrames:
    """Separate replaceable slots, with bounded preference for hot frames."""
    def __init__(self):
        self.condition = threading.Condition()
        self.slots = {}
        self.hot_streak = 0

    def publish(self, lane, value):
        check(lane in ('ordinary', 'hot'), 'unknown frame lane')
        with self.condition:
            self.slots[lane] = value
            self.condition.notify()

    def get(self, timeout):
        deadline = time.monotonic()+timeout
        with self.condition:
            while not self.slots:
                remaining = deadline-time.monotonic()
                if remaining <= 0: raise queue.Empty
                self.condition.wait(remaining)
            lane = ('ordinary' if 'ordinary' in self.slots and
                    (self.hot_streak >= 2 or 'hot' not in self.slots) else 'hot')
            self.hot_streak = self.hot_streak+1 if lane == 'hot' else 0
            return lane, self.slots.pop(lane)


class FrameSequence:
    """Unique numeric IDs across independently owned evidence databases."""
    def __init__(self, initial=0):
        self.lock, self.value = threading.Lock(), initial

    def next(self):
        with self.lock:
            self.value += 1
            return self.value


class HotDispatchBudget:
    """One shared rolling budget; an event may use the second slot early.

    The rolling interval is (now - 2I, now]. Failures consume the same slots as
    successful reads. Only new event work may bypass the ordinary I spacing;
    neither kind bypasses a retry deadline or the two-dispatch window.
    """
    def __init__(self, interval):
        self.interval = interval
        self.recent = deque()
        self.last_dispatch = None
        self.retry_after = 0.0

    def _prune(self, now):
        while self.recent and self.recent[0]+2*self.interval <= now:
            self.recent.popleft()

    def mode(self, now):
        self._prune(now)
        if now < self.retry_after or len(self.recent) >= 2:
            return None
        if self.last_dispatch is None or now >= self.last_dispatch+self.interval:
            return 'regular'
        return 'fresh_event'

    def record(self, now, newest_event_at=None):
        mode = self.mode(now)
        check(mode is not None, 'hot dispatch budget exhausted')
        if mode == 'fresh_event':
            check(newest_event_at is not None and self.last_dispatch < newest_event_at <= now,
                  'early hot dispatch requires a new event')
        previous = self.last_dispatch
        self.recent.append(now)
        self.last_dispatch = now
        return dict(mode=mode, dispatched_at=now, interval_seconds=self.interval, window_seconds=2*self.interval,
                    dispatches_in_window=len(self.recent), previous_dispatch_at=previous,
                    fixed_gate_wait_avoided_ms=max(0, (previous+self.interval-now)*1000)
                        if previous is not None else 0)

    def backoff(self, until):
        self.retry_after = max(self.retry_after, until)

    def next_wake(self, now):
        self._prune(now)
        if len(self.recent) >= 2:
            due = self.recent[0]+2*self.interval
        else:
            due = self.last_dispatch+self.interval if self.last_dispatch is not None else now
        return max(self.retry_after, due)


class HotRoutes:
    """One changed leg arms a complete cycle, with bounded family rotation.

    Every selection is metadata only. StateFeed rereads all selected pools in
    one EVM call. Cold coverage is performed by the ordinary reader.
    """
    def __init__(self, saved, config, clock=time.monotonic, group_allowed=None):
        self.clock = clock
        pools = eligible_pools(config, [Pool.from_json(p) for p in saved.get('known_pools', saved.get('pools', []))])
        self.pools = {p.address:p for p in pools}
        self.settlements = [t['address'] for t in config['settlement_tokens']]
        self.max_hops = min(3, config['policy']['max_hops'])
        self.groups = CycleGroups(pools, self.settlements, self.max_hops)
        self.active, self.offsets, self.choices = OrderedDict(), OrderedDict(), OrderedDict()
        self.pair_offsets = OrderedDict()
        self.turns = {}
        self.group_allowed = group_allowed
        self.coactive_key, self.coactive = None, ()
        self.coactive_cursor = 0
        self.pending, self.read_versions = OrderedDict(), OrderedDict()
        # Entries belong to bounded in-flight reads. Keep old and new versions
        # separately so a late result releases only its own reservation.
        self.reserved_versions = {}
        self.remembered = OrderedDict()
        self.memory_seconds = 120
        self.priority_streak = 0
        self.stats = dict(coactive_read=0, coactive_read_after_ttl=0,
                          tracked_expired_unselected=0, tracked_expired_unread=0,
                          tracking_evicted=0, active_evicted=0, read_failures=0,
                          transfer_deferred_groups=0, quality_deferred_groups=0, topology_invalidated=0,
                          memory_expired=0, memory_evicted=0)
        self.expired_examples = []
        self.pool_limit = config['policy'].get('hot_lane_max_pools', 12)
        self.group_limit = config['policy'].get('hot_lane_max_groups', 4)
        self.ttl = config['policy'].get('hot_lane_event_ttl_seconds', 1.5)
        check(5 <= self.pool_limit <= 24 and 3 <= self.group_limit <= 12 and 0 < self.ttl <= 5,
              'invalid hot route bounds')

    def add(self, events):
        now = self.clock()
        for pool, stamp in events:
            if pool in self.groups.members and 0 <= now-stamp <= self.ttl:
                self.active[pool] = max(stamp, self.active.get(pool, stamp))
        while len(self.active) > 192:
            self.active.popitem(last=False)
            self.stats['active_evicted'] += 1

    def resume(self, previous):
        self.add(previous.active.items())
        self.offsets = previous.offsets.copy()
        self.pair_offsets = previous.pair_offsets.copy()
        self.priority_streak = previous.priority_streak
        self.stats = previous.stats.copy()
        def unchanged(group):
            try: self.groups.index(group)
            except (ValueError, IndexError): return False
            fields = ('kind','token0','token1','factory','spacing')
            return all(pool in previous.pools and
                       all(getattr(self.pools[pool],f) == getattr(previous.pools[pool],f) for f in fields)
                       for pool in group)
        self.read_versions = OrderedDict((group, version) for group, version in previous.read_versions.items()
                                         if unchanged(group))
        self.reserved_versions = {group:set(versions) for group, versions in previous.reserved_versions.items()
                                  if unchanged(group)}
        self.pending = OrderedDict((group, dict(item)) for group, item in previous.pending.items()
                                   if unchanged(group))
        self.remembered = OrderedDict((group, dict(item)) for group, item in previous.remembered.items()
                                      if unchanged(group))
        self.stats['topology_invalidated'] += len(previous.pending)-len(self.pending)
        self.coactive_cursor = previous.coactive_cursor
        self.expired_examples = list(previous.expired_examples)

    def _signature(self, group):
        return tuple((pool, self.active[pool]) for pool in group if pool in self.active)

    @staticmethod
    def _stamp(version):
        return max((stamp for _, stamp in version), default=float('-inf'))

    def _unread(self, group):
        latest = self._stamp(self.read_versions.get(group, ()))
        for version in self.reserved_versions.get(group, ()):
            latest = max(latest, self._stamp(version))
        return self._stamp(self._signature(group)) > latest

    def _coactive_work(self, now):
        # At most 192 active pools and 128 enumerated combinations per turn.
        # This is a reception-time signal, not a price or a same-tx assertion.
        for group, memory in list(self.remembered.items()):
            if now-memory['last_coactive_at'] > self.memory_seconds:
                del self.remembered[group]
                self.stats['memory_expired'] += 1
        key = tuple(sorted(self.active))
        if key != self.coactive_key:
            self.coactive = CycleGroups([self.pools[a] for a in key], self.settlements, self.max_hops)
            self.coactive_key = key
        total = len(self.coactive)
        for i in range(min(total, 128)):
            group = self.coactive[(self.coactive_cursor+i) % total]
            signature = self._signature(group)
            stamp = self._stamp(signature)
            # Learn topology only from complete coactivity already observed.
            # Neither reads nor subsequent single-leg activity renew its life.
            if stamp > self.remembered.get(group, {}).get('last_coactive_at', float('-inf')):
                self.remembered[group] = {'learned_at':now, 'last_coactive_at':stamp}
                self.remembered.move_to_end(group)
        while len(self.remembered) > 128:
            self.remembered.popitem(last=False)
            self.stats['memory_evicted'] += 1
        for group, item in list(self.pending.items()):
            signature = self._signature(group)
            if not signature or group not in self.remembered:
                selected = item['selected']
                key = 'tracked_expired_unread' if selected else 'tracked_expired_unselected'
                self.stats[key] += 1
                self.expired_examples.append({'pools':list(group), 'selected':selected,
                    'reason':'event_expired' if not signature else 'memory_expired_or_evicted'})
                self.expired_examples = self.expired_examples[-8:]
                del self.pending[group]
            elif signature != item['version']:
                item.update(selected=item['selected'] if self._stamp(signature) == self._stamp(item['version']) else False,
                            version=signature)
        for group in self.remembered:
            signature = self._signature(group)
            if self._stamp(signature) <= self._stamp(self.read_versions.get(group, ())): continue
            if group not in self.pending:
                self.pending[group] = {'version':signature, 'selected':False}
        self.coactive_cursor = (self.coactive_cursor+min(total,128)) % max(1,total)
        while len(self.pending) > 256:
            self.pending.popitem(last=False)
            self.stats['tracking_evicted'] += 1

    def summary(self):
        return dict(self.stats, active_pools=len(self.active), coactive_eligible_groups=len(self.coactive),
                    coactive_pending=len(self.pending), coactive_enumeration_capped=len(self.coactive)>128,
                    remembered_groups=len(self.remembered), memory_seconds=self.memory_seconds,
                    reserved_groups=len(self.reserved_versions),
                    expired_examples=list(self.expired_examples),
                    scope='bounded tracked coactivity; read is separate from evaluation and execution')

    def reserve(self, selection):
        """Reserve only captured versions, immediately before dispatching a read."""
        for group, version in selection['_group_versions'].items():
            self.reserved_versions.setdefault(group,set()).add(version)

    def release(self, selection):
        """Release a discarded read without acknowledging newer event work."""
        for group, version in selection.get('_group_versions', {}).items():
            reserved = self.reserved_versions.get(group)
            if reserved is not None:
                reserved.discard(version)
                if not reserved: del self.reserved_versions[group]

    def acknowledge(self, selection):
        """Only a successfully recorded atomic read consumes group versions."""
        self.release(selection)
        now = self.clock()
        for group, version in selection['_group_versions'].items():
            if self._stamp(version) > self._stamp(self.read_versions.get(group, ())):
                self.read_versions[group] = version
            self.read_versions.move_to_end(group)
            if (group in self.pending and
                    self._stamp(self.pending[group]['version']) <= self._stamp(version)):
                del self.pending[group]
        for group, version in selection['_coactive_versions'].items():
            self.stats['coactive_read'] += 1
            if any(now-stamp > self.ttl for _,stamp in version):
                self.stats['coactive_read_after_ttl'] += 1
        while len(self.read_versions) > 512: self.read_versions.popitem(last=False)

    def failed(self, selection):
        # Retain pending priority versions until success or reception TTL expiry.
        self.release(selection)
        self.stats['read_failures'] += 1

    def take(self):
        return self._take()

    def take_fresh(self, after):
        """Commit rotation only if a complete group with a newer event is found.

        A failed early probe must not skip unserved ordinary variants. All copied
        containers are bounded; topology/index objects are immutable and shared.
        """
        now = self.clock()
        if not any(after < stamp <= now and now-stamp <= self.ttl
                   for stamp in self.active.values()):
            return None
        trial = copy.copy(self)
        for name in ('active', 'offsets', 'choices', 'pair_offsets', 'read_versions'):
            setattr(trial, name, getattr(self, name).copy())
        trial.turns = {pool:list(turn) for pool, turn in self.turns.items()}
        trial.pending = OrderedDict((group, dict(item)) for group, item in self.pending.items())
        trial.remembered = OrderedDict((group, dict(item)) for group, item in self.remembered.items())
        trial.reserved_versions = {group:set(versions) for group, versions in self.reserved_versions.items()}
        trial.stats, trial.expired_examples = self.stats.copy(), list(self.expired_examples)
        selection = trial._take(fresh_after=after)
        if not selection['groups']:
            return None
        self.__dict__.update(trial.__dict__)
        return selection

    def _take(self, fresh_after=None):
        now = self.clock()
        for pool, stamp in list(self.active.items()):
            if not 0 <= now-stamp <= self.ttl:
                del self.active[pool]
        for pool in list(self.turns):
            if pool not in self.active: self.turns.pop(pool)
        self._coactive_work(now)
        selected, addresses, stamps, triggers = [], set(), [], []
        coactive_selected, allowed_cache = [], {}
        def allowed(group):
            if group not in allowed_cache:
                allowed_cache[group] = (self._unread(group) and
                    (fresh_after is None or self._stamp(self._signature(group)) > fresh_after))
                if allowed_cache[group] and self.group_allowed is not None:
                    allowed_cache[group] = self.group_allowed(group, self.pools)
                    if not allowed_cache[group]:
                        self.stats['quality_deferred_groups'] += 1
                        # Legacy counter name retained for existing readers.
                        # The callback now combines route and transfer evidence.
                        self.stats['transfer_deferred_groups'] += 1
            return allowed_cache[group]
        # Current coactivity and a remembered single-leg trigger each get at
        # most one slot. Retained memory cannot queue ahead of a fresh complete
        # group. Every third priority turn starts with ordinary rotation, even
        # when the pool (not group) cap binds; at least one group slot is spare.
        if self.priority_streak < 2:
            for complete in (True, False):
                for group in self.pending:
                    active = [p for p in group if p in self.active]
                    if (len(active)==len(group)) != complete or not allowed(group): continue
                    if (len(selected) >= self.group_limit-1 or
                            len(addresses | set(group)) > self.pool_limit): continue
                    selected.append(group); addresses.update(group)
                    coactive_selected.append(group)
                    stamps.extend(self.active[p] for p in active); triggers.extend(active)
                    break
        self.priority_streak = self.priority_streak+1 if coactive_selected else 0
        # Round-robin across changed pools and token-cycle families; a busy
        # hub's Cartesian product cannot fill every read indefinitely.
        for pool in list(self.active):
            if fresh_after is not None and self.active[pool] <= fresh_after:
                continue
            if pool not in self.choices:
                self.choices[pool] = self.groups.containing(pool)
                while len(self.choices) > 256: self.choices.popitem(last=False)
            families = self.choices[pool].families
            if not families: continue
            pairs = [f for f in families if len(f.legs) == 2]
            longer = [f for f in families if len(f.legs) > 2]
            # Preserve the unserved tail of a turn. Recreating two pair slots
            # after each capacity cutoff would starve its triangle indefinitely.
            if pool not in self.turns:
                proposed = []
                if pairs:
                    family = pairs[0]
                    start = self.pair_offsets.get(pool, 0)
                    proposed += [('pair', family.at((start+i) % family.size))
                                 for i in range(min(2, family.size))]
                if longer:
                    step = self.offsets.get(pool, 0)
                    family = longer[step % len(longer)]
                    proposed.append(('longer', family.at((step//len(longer)) % family.size)))
                self.turns[pool] = proposed
            proposed = self.turns[pool]
            admitted = 0
            for kind, group in proposed:
                include = allowed(group)
                if include and group not in selected:
                    if len(selected) >= self.group_limit or len(addresses | set(group)) > self.pool_limit:
                        break
                    selected.append(group); addresses.update(group)
                if include:
                    stamps.append(self.active[pool]); triggers.append(pool)
                admitted += 1
                offsets = self.pair_offsets if kind == 'pair' else self.offsets
                offsets[pool] = offsets.get(pool, 0)+1
                offsets.move_to_end(pool)
            # A partially served pool stays at the front for the next read.
            # Advance only the variants actually included in an atomic call.
            if admitted == len(proposed):
                self.active.move_to_end(pool)
                self.turns.pop(pool, None)
            else: self.turns[pool] = proposed[admitted:]
            if len(selected) >= self.group_limit: break
        while len(self.offsets) > 4096: self.offsets.popitem(last=False)
        while len(self.pair_offsets) > 4096: self.pair_offsets.popitem(last=False)
        versions = {group:self.pending[group]['version'] for group in selected if group in self.pending}
        for group in versions: self.pending[group]['selected'] = True
        # Ordinary rotation may also include an eligible coactive group.
        coactive_selected = [group for group in selected if group in versions]
        priority_evidence = [dict(self.remembered[group], pools=list(group),
                                 triggers=dict(version), selected_at=now,
                                 source='current_coactivity' if len(version)==len(group) else 'remembered_coactivity')
                             for group, version in versions.items()]
        return {'groups':selected, 'pools':[self.pools[a] for a in sorted(addresses)],
                'coactive_priority_groups':coactive_selected, '_coactive_versions':versions,
                '_group_versions':{group:self._signature(group) for group in selected},
                'priority_evidence':priority_evidence,
                'selection_summary':self.summary(),
                'trigger_pools':sorted(set(triggers)), 'oldest_event_at':min(stamps) if stamps else None,
                'newest_event_at':max(stamps) if stamps else None}


def run_hot_reader(rpc_factory, config, universe, inbox, stop, finish,
                   directory, sequence, publish, health, feed_factory=None, fallback_factory=None,
                   group_allowed=None):
    """Two bounded reads; one coordinator owns selection, evidence and publication."""
    from .statefeed import StateFeed
    from .recorder import Recorder
    from .core import safe_error, atomic_json
    feed_factory = feed_factory or StateFeed
    recorder = None
    shutdown = threading.Event()
    completed = queue.Queue(maxsize=2)
    workers, busy = [], set()
    local = dict(config, policy=dict(config['policy'], cycle_pages=False,
                 snapshot_ticks_per_side=config['policy'].get('hot_lane_ticks_per_side', 16)))

    class ReadCancellation:
        def __init__(self, prior): self.prior = prior
        def is_set(self):
            return shutdown.is_set() or stop.is_set() or bool(self.prior and self.prior.is_set())
        def wait(self, timeout=None):
            deadline = float('inf') if timeout is None else time.monotonic()+timeout
            while not self.is_set():
                remaining = deadline-time.monotonic()
                if remaining <= 0: break
                shutdown.wait(min(.025, remaining))
            return self.is_set()

    def read_worker(index, jobs):
        # HTTP clients, asyncio loops and per-feed clock caches never cross threads.
        rpc = feed = None
        verified, failures, using_fallback, retry_primary_at = False, 0, False, 0
        try:
            while not shutdown.is_set() and not stop.is_set():
                try: job = jobs.get(timeout=.05)
                except queue.Empty: continue
                if shutdown.is_set() or stop.is_set(): break
                result = dict(job, worker=index, stage='rpc_initialization')
                try:
                    if fallback_factory and ((not using_fallback and failures >= 3) or
                            (using_fallback and time.monotonic() >= retry_primary_at)):
                        if rpc is not None: rpc.close()
                        rpc = feed = None
                        using_fallback = not using_fallback
                        verified, failures = False, 0
                        retry_primary_at = time.monotonic()+60
                    if rpc is None:
                        rpc = (fallback_factory if using_fallback else rpc_factory)()
                        rpc.cancel_event = ReadCancellation(getattr(rpc, 'cancel_event', None))
                    if not verified:
                        result['stage'] = 'chain_verification'
                        rpc.verify_chain()
                        verified = True
                    result['stage'] = 'atomic_read'
                    selection = job['selection']
                    if feed is None:
                        feed = feed_factory(rpc, local, {'pools':[p.json() for p in selection['pools']]})
                    else: feed.pools = selection['pools']
                    result['frame'] = feed.poll()
                    failures = 0
                except Exception as exc:
                    result['error'] = exc
                    result['read_timing'] = getattr(feed, 'last_transport', {}) if feed is not None else {}
                    failures += 1
                result['channel'] = 'fallback' if using_fallback else 'primary'
                result['completed_ns'] = time.monotonic_ns()
                completed.put(result)  # At most one outstanding result per worker.
        finally:
            if rpc is not None: rpc.close()

    try:
        interval = config['policy'].get('hot_lane_min_interval_seconds', .5)
        check(.2 <= interval <= 2, 'invalid hot lane request interval')
        recorder = Recorder(directory/'hot-first-seen.sqlite',
                            config['policy'].get('hot_lane_recording_mb', 128)*1024**2,
                            id_source=sequence.next)
        version, selector, failures = None, None, 0
        budget = HotDispatchBudget(interval)
        selection_health_at = 0
        job_id = last_published = 0
        last_context = None
        dropped_out_of_order = dropped_topology = 0
        inbox_generation = inbox.generation
        while not stop.is_set() and time.monotonic() < finish:
            generation, saved = universe()
            if generation != version:
                previous = selector
                selector = HotRoutes(saved, config, group_allowed=group_allowed)
                if previous is not None: selector.resume(previous)
                version = generation
            inbox_generation, events = inbox.drain_batch(time.monotonic())
            selector.add(events)
            while not completed.empty():
                result = completed.get_nowait()
                busy.discard(result['worker'])
                selection = result['selection']
                if 'error' in result:
                    exc = result['error']
                    recorder.error(exc)
                    selector.failed(selection)
                    check(str(exc) != 'wrong chain', 'wrong chain')
                    failures += 1
                    budget.backoff(time.monotonic()+min(2, .25*2**min(failures,3)))
                    health('retrying', at=time.time(), error=safe_error(exc), stage=result['stage'],
                           read_timing=result.get('read_timing', {}))
                    continue
                frame = result['frame']
                requested = result['requested']
                frame.transport.update(lane='hot', route_groups=selection['groups'],
                    activity_priority_groups=selection['groups'],
                    coactive_priority_groups=selection['coactive_priority_groups'],
                    hot_selection=selection['selection_summary'],
                    priority_evidence=selection['priority_evidence'],
                    group_event_versions=[{'pools':list(group), 'triggers':dict(event_version)}
                        for group, event_version in selection['_group_versions'].items()],
                    trigger_pools=selection['trigger_pools'],
                    event_oldest_age_at_request_ms=(requested-selection['oldest_event_at'])*1000,
                    event_newest_age_at_request_ms=(requested-selection['newest_event_at'])*1000,
                    route_selection_ms=result['selection_ms'], recorder='hot-first-seen.sqlite',
                    dispatch_id=result['id'], worker=result['worker'],
                    dispatch_pacing=result['dispatch_pacing'],
                    dispatch_to_poll_ms=max(0, (frame.requested_ns/1e9-requested)*1000),
                    completion_queue_ms=max(0, (time.monotonic_ns()-result['completed_ns'])/1e6))
                fields = ('kind', 'token0', 'token1', 'factory', 'spacing')
                valid_topology = all(p.address in selector.pools and
                    all(getattr(p, f) == getattr(selector.pools[p.address], f) for f in fields)
                    for p in selection['pools'])
                context = (frame.context_block, frame.context_time)
                obsolete = result['id'] <= last_published or (last_context is not None and
                    (context[0] < last_context[0] or context[1] < last_context[1]))
                disposition = ('topology_changed' if not valid_topology else
                               'out_of_order' if obsolete else 'published')
                frame.transport['hot_read_disposition'] = disposition
                fid = recorder.append(frame.requested_ns, frame.received_ns,
                                      'hot-atomic-pending-state', frame.payload(), frame.received_utc)
                if stop.is_set(): break
                if not valid_topology:
                    selector.release(selection)
                    dropped_topology += 1
                else:
                    if obsolete:
                        # A newer different group did not evaluate this one. Keep
                        # it eligible for a fresh read while its event is timely.
                        selector.release(selection)
                        dropped_out_of_order += 1
                    else:
                        selector.acknowledge(selection)
                        publish('hot', (fid, frame))
                        last_published, last_context = result['id'], context
                        health('connected', at=time.time(), frame_id=fid, pools=len(selection['pools']),
                               channel=result['channel'], route_groups=len(selection['groups']),
                               event_age_at_request_ms=frame.transport['event_newest_age_at_request_ms'],
                               read_ms=(frame.received_ns-frame.requested_ns)/1e6,
                               inflight=len(busy), max_inflight=2)
                failures = 0
            if stop.is_set(): break
            now = time.monotonic()
            if now >= selection_health_at:
                atomic_json(directory/'hot-selection-health.json', dict(selector.summary(), at=time.time(),
                    inflight=len(busy), max_inflight=2, dispatched=job_id,
                    dropped_out_of_order=dropped_out_of_order, dropped_topology=dropped_topology))
                selection_health_at = now+1
            mode = budget.mode(now) if len(busy) < 2 else None
            if mode is not None:
                selection_started = time.monotonic()
                selection = (selector.take_fresh(budget.last_dispatch) if mode == 'fresh_event'
                             else selector.take())
                selection_ms = (time.monotonic()-selection_started)*1000
                if selection and selection['groups'] and not stop.is_set():
                    index = next((i for i in range(len(workers)) if i not in busy), len(workers))
                    if index == len(workers):
                        jobs = queue.Queue(maxsize=1)
                        thread = threading.Thread(target=read_worker, args=(index, jobs),
                                                  name=f'hot-read-{index}', daemon=True)
                        workers.append((thread, jobs)); thread.start()
                    requested = time.monotonic()
                    pacing = budget.record(requested, selection['newest_event_at'])
                    job_id += 1
                    selector.reserve(selection)
                    busy.add(index)
                    workers[index][1].put(dict(id=job_id, selection=selection, requested=requested,
                                               selection_ms=selection_ms, dispatch_pacing=pacing))
            # New events wake this wait. The bounded timeout also drains completions
            # and retries unserved groups from an existing event generation.
            now = time.monotonic()
            next_read = budget.next_wake(now)
            inbox.wait(min(.025, max(.001, next_read-now)) if next_read > now else .025,
                       after=inbox_generation, stop=stop)
    except Exception as exc:
        if not stop.is_set(): health('failed', at=time.time(), error=safe_error(exc))
    finally:
        shutdown.set()
        for thread, _ in workers: thread.join()
        if recorder is not None: recorder.close()
