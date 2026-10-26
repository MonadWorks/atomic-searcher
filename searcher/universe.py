"""Keep complete settlement cycles together and rotate finite atomic read pages."""
from .routes import RouteIndex, iter_cycles
from .core import check, number
from .events import V3_SWAP, SLIP_SWAP, V2_SYNC, POOL_ACTIVITY_TOPICS
from collections import OrderedDict, defaultdict
from .cycle_groups import CycleGroups, PoolGroups, hot_groups, EconomicRotation
import time


class CyclePages:
    def __init__(self, pools, settlements, max_hops, last_seen=None, groups=None, excluded_tokens=()):
        excluded = set(excluded_tokens)
        pools = [p for p in pools if p.token0 not in excluded and p.token1 not in excluded]
        self.pools = {p.address: p for p in pools}
        self.settlements, self.max_hops = tuple(settlements), max_hops
        settlements = self.settlements
        if groups is None:
            groups = CycleGroups(pools, settlements, max_hops)
        elif isinstance(groups, CycleGroups):
            if not set(groups.members).issubset(self.pools):
                groups = CycleGroups(pools, settlements, max_hops)
        elif excluded:
            # Cached pages predate the registry. Never resurrect an excluded
            # leg through a saved group or a resumed activity subscription.
            groups = (g for g in groups if all(a in self.pools for a in g))
        # Reuse authenticated address objects even when loading old JSON caches.
        canonical = {a:a for a in self.pools}
        last_seen = last_seen or {}
        if isinstance(groups, CycleGroups):
            self.groups = groups
            self.max_group_width = max((2 if f.pair else len(f.legs) for f in groups.families), default=2)
            self.hot = hot_groups(groups, last_seen)
            self.covered = set(groups.members)
            self.by_pool = PoolGroups(groups)
        else:
            self.groups = sorted(tuple(canonical[a] for a in g) for g in groups)
            self.max_group_width = max((len(g) for g in self.groups), default=2)
            self.hot = sorted(self.groups, key=lambda g: (-max(last_seen.get(a, 0) for a in g),
                             -sum(last_seen.get(a, 0) for a in g), len(g), g))
            self.covered = set(a for g in self.groups for a in g)
            self.by_pool = defaultdict(list)
            for group in sorted(self.hot, key=len):
                for pool in group: self.by_pool[pool].append(group)
        self.cursor = 0
        self.selected_groups = []
        self.priority = OrderedDict()
        self.activity_cursor = {}
        self.selected_priority = []
        self.selected_coactive = []
        self.recent_activity = OrderedDict()
        self.family_offsets = OrderedDict()
        self.family_cursor = 0
        self.economic_rotation = EconomicRotation(self.groups) if isinstance(self.groups, CycleGroups) else None
        self.selection_summary = {}

    def resume(self, previous):
        # A stable group key survives insertions and changes in hot ranking.
        from bisect import bisect_left
        if previous and previous.groups and self.groups:
            key = previous.groups[previous.cursor % len(previous.groups)]
            if isinstance(self.groups, CycleGroups):
                try: self.cursor = self.groups.index(key)
                except ValueError: self.cursor = previous.cursor % len(self.groups)
            else: self.cursor = bisect_left(self.groups, key) % len(self.groups)
            for group, stamp in previous.priority.items():
                if all(a in self.pools for a in group): self.priority[group] = stamp
            self.activity_cursor = dict(previous.activity_cursor)
            self.recent_activity = OrderedDict((a,t) for a,t in previous.recent_activity.items()
                                               if a in self.covered)
            self.family_offsets = previous.family_offsets.copy()
            self.family_cursor = previous.family_cursor
            if self.economic_rotation is not None and getattr(previous, 'economic_rotation', None) is not None:
                self.economic_rotation.resume(previous.economic_rotation)

    def prioritize(self, pools, observed_at=None):
        """Activity is a scheduling hint only; every selected cycle is reread.

        Bound fanout for busy hub pools and rotate their affected groups so one
        frequently traded pair cannot permanently monopolize the event lane.
        """
        stamp = time.monotonic() if observed_at is None else observed_at
        for pool in sorted(set(pools)):
            groups = self.by_pool.get(pool, ())
            if not groups: continue
            if stamp >= self.recent_activity.get(pool, float('-inf')):
                self.recent_activity[pool] = stamp
                self.recent_activity.move_to_end(pool)
            start = self.activity_cursor.get(pool, 0) % len(groups)
            for offset in range(min(128, len(groups))):
                group = groups[(start+offset) % len(groups)]
                # Preserve original arrival time and FIFO position under repeats.
                self.priority.setdefault(group, stamp)
            self.activity_cursor[pool] = (start+min(128, len(groups))) % len(groups)
        while len(self.priority) > 4096: self.priority.popitem(last=False)
        while len(self.recent_activity) > 192: self.recent_activity.popitem(last=False)

    def coactive_groups(self, now, maximum):
        """Bounded event lane: rotate token cycles before parallel pool variants.

        Only authenticated, recently changed pools participate. Events decide
        what to read together, never the price/state used to authorize a trade.
        General rotation still covers longer and partly inactive cycles.
        """
        for pool, stamp in list(self.recent_activity.items()):
            if now-stamp > 5: del self.recent_activity[pool]
        if len(self.recent_activity) < 2: return []
        active = CycleGroups([self.pools[a] for a in self.recent_activity],
                             self.settlements, min(3, self.max_hops))
        families = active.families
        if not families: return []
        result, seen = [], set()
        # One family/variant per step avoids a hub's Cartesian product filling
        # the whole priority page. The amount of work is bounded per page.
        for _ in range(min(maximum*2, len(active))):
            family = families[self.family_cursor % len(families)]
            self.family_cursor += 1
            key = (family.pair, tuple(tuple(sorted((self.pools[leg[0]].token0,
                                                    self.pools[leg[0]].token1)))
                                      for leg in family.legs))
            offset = self.family_offsets.get(key, 0)
            self.family_offsets[key] = offset+1
            self.family_offsets.move_to_end(key)
            group = family.at(offset % family.size)
            if group in seen: continue
            try: self.groups.index(group)
            except ValueError: continue
            seen.add(group); result.append(group)
            if len(result) >= maximum: break
        while len(self.family_offsets) > 2048: self.family_offsets.popitem(last=False)
        return result

    def take(self, limit, required=(), group_limit=96, group_allowed=None):
        check(limit >= self.max_group_width and group_limit >= 2, 'atomic page too small for a cycle')
        chosen = set(required)
        check(chosen.issubset(self.pools) and len(chosen) <= limit, 'invalid required route page')
        selected = set()
        visited = 0
        checked = {}
        counts = dict(quality_deferred_groups=0, eligibility_checks=0, linear_groups=0,
                      economic_groups=0)
        work_limit = max(64, group_limit*8)
        def allowed(group):
            if group not in checked:
                if len(checked) >= work_limit: return None
                checked[group] = group_allowed is None or group_allowed(group, self.pools)
                counts['eligibility_checks'] += 1
                if not checked[group]: counts['quality_deferred_groups'] += 1
            return checked[group]
        # Guarantee progress even on a very small page under constant activity.
        # Rejected cold prefixes must leave eligibility work for new events
        # and other economic families in this same read turn.
        while self.groups and visited < min(len(self.groups), max(1, work_limit//4)):
            cold = self.groups[self.cursor]
            eligibility = allowed(cold)
            if eligibility is None: break
            if not eligibility:
                self.cursor = (self.cursor+1) % len(self.groups)
                visited += 1
                continue
            if len(chosen | set(cold)) <= limit:
                chosen.update(cold); selected.add(cold)
                self.cursor = (self.cursor+1) % len(self.groups)
                visited += 1
                counts['linear_groups'] += 1
            break
        now = time.monotonic()
        self.selected_priority = []
        self.selected_coactive = []
        for group in self.coactive_groups(now, max(1, group_limit//4)):
            if len(selected) >= group_limit: break
            eligibility = allowed(group)
            if eligibility is None: break
            if not eligibility: continue
            if len(chosen | set(group)) > limit*3//4: continue
            chosen.update(group); selected.add(group)
            self.selected_priority.append(group)
            self.selected_coactive.append(group)
        for priority_attempt, (group, stamp) in enumerate(list(self.priority.items())):
            if priority_attempt >= work_limit//2: break
            if now-stamp > 5:
                del self.priority[group]
                continue
            if len(self.selected_priority) >= group_limit//2: break
            if group in selected:
                del self.priority[group]
                continue
            eligibility = allowed(group)
            if eligibility is None: break
            if not eligibility:
                del self.priority[group]
                continue
            if len(selected) >= group_limit: break
            # Reserve half the page for ongoing coverage of all known cycles.
            if len(chosen | set(group)) > limit*3//4: continue
            chosen.update(group); selected.add(group)
            self.selected_priority.append(group)
            del self.priority[group]
        # Include every complete route represented by a deeper-read request.
        # The required set is tiny; only scan bounded hot groups here. Deep
        # route groups themselves are supplied separately by StateFeed below.
        for group in self.hot[:group_limit//4 if self.selected_priority else group_limit//2]:
            if len(selected) >= group_limit: break
            eligibility = allowed(group)
            if eligibility is None: break
            if not eligibility: continue
            if len(chosen | set(group)) <= limit//2:
                chosen.update(group); selected.add(group)
        attempts = 0
        while self.groups and attempts < min(len(self.groups), work_limit) and len(selected) < group_limit:
            if self.economic_rotation is not None:
                group = self.economic_rotation.next()
            else:
                group = self.groups[self.cursor]
                self.cursor = (self.cursor+1) % len(self.groups)
            attempts += 1
            if group is None: break
            if group in selected: continue
            eligibility = allowed(group)
            if eligibility is None: break
            if not eligibility: continue
            needed = set(group)-chosen
            # Another complete family can still fit in the remaining slots.
            if len(chosen)+len(needed) > limit: continue
            chosen.update(group); selected.add(group)
            counts['economic_groups'] += 1
        self.selected_groups = sorted(selected)
        self.selection_summary = dict(counts, groups_by_hops={str(h):sum(len(g)==h for g in selected)
                                      for h in (2,3,4)}, work_limit=work_limit,
                                      eligibility_budget_exhausted=len(checked)>=work_limit,
                                      scope='selected topology for fresh reads; not evaluated or profitable routes')
        return [self.pools[a] for a in sorted(chosen)]


class ActivityHints:
    """Deduplicate recent swap and liquidity logs without treating them as executable state."""
    def __init__(self):
        self.seen = OrderedDict()

    def accept(self, logs):
        return {entry['address'].lower() for entry in self.accept_logs(logs)}

    def accept_logs(self, logs):
        check(isinstance(logs, list), 'activity logs unavailable')
        accepted = []
        for entry in logs:
            if not isinstance(entry, dict) or entry.get('removed'): continue
            topics = entry.get('topics', [])
            if not topics or topics[0] not in POOL_ACTIVITY_TOPICS: continue
            pool, tx = entry.get('address', ''), entry.get('transactionHash', '')
            from .core import address
            try:
                pool = address(pool)
                check(len(tx) == 66 and tx.startswith('0x'), 'invalid activity transaction')
                int(tx[2:], 16)
                check(number(entry['logIndex']) >= 0, 'invalid activity index')
                key = (tx.lower(), number(entry['logIndex']), pool)
            except (KeyError, ValueError, TypeError, RuntimeError): continue
            if key in self.seen: continue
            self.seen[key] = None
            accepted.append(entry)
        while len(self.seen) > 16384: self.seen.popitem(last=False)
        return accepted


def swap_activity(rpc, lower, height, anchor_hash, chunk_size=10):
    """Discovery hints only; every admitted pool is separately authenticated.

    Filter logs on the server rather than downloading every unrelated receipt.
    The caller rechecks the sealed end anchor before publishing its universe.
    Errors never advance the cursor, and no price is taken from these hints.
    """
    from collections import Counter
    activity, last_seen, seen = Counter(), {}, set()
    for start in range(lower, height+1, chunk_size):
        end = min(height, start+chunk_size-1)
        logs = rpc.request('eth_getLogs', [{'fromBlock':hex(start), 'toBlock':hex(end),
                                          'topics':[list(POOL_ACTIVITY_TOPICS)]}])
        check(isinstance(logs, list), 'discovery logs unavailable')
        for log in logs:
            n = number(log['blockNumber'])
            check(start <= n <= end and not log.get('removed'), 'invalid discovery log range')
            check(log.get('topics') and log['topics'][0] in POOL_ACTIVITY_TOPICS, 'wrong discovery topic')
            if n == height: check(log['blockHash'] == anchor_hash, 'discovery anchor changed')
            identity = (log['blockHash'], log['transactionHash'], number(log['logIndex']))
            if identity in seen: continue
            seen.add(identity)
            pool = log['address'].lower()
            activity[pool] += 1
            last_seen[pool] = max(n, last_seen.get(pool, 0))
    return activity, last_seen


class PageProgress:
    """Advance only after strategy consumption; newer states may replace a page."""
    def __init__(self):
        from queue import SimpleQueue
        self.acknowledged = SimpleQueue()
        self.current = None
        self.serial = 0
        self.continuation = []

    def begin(self, timeout=0, cancel_event=None):
        import queue
        deadline = time.monotonic()+max(0, timeout)
        while True:
            try: key, continuation = self.acknowledged.get_nowait()
            except queue.Empty:
                remaining = deadline-time.monotonic()
                if self.current is None or remaining <= 0 or (cancel_event is not None and cancel_event.is_set()): break
                try: key, continuation = self.acknowledged.get(timeout=min(.02, remaining))
                except queue.Empty: continue
            if key == self.current:
                self.current = None
                self.continuation = continuation
        if self.current is not None: return False, self.current
        self.serial += 1
        self.current = self.serial
        return True, self.current

    def consumed(self, key, continuation=()):
        if key is not None: self.acknowledged.put((key, list(continuation)))

    def take_continuation(self):
        result, self.continuation = self.continuation, []
        return result
