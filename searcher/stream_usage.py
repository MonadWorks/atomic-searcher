"""Local Chainstack full-node estimates, not an account-wide billing meter.

One WebSocket reply/notification is one request unit. Other applications and
messages buffered in transit are outside this meter; provider billing wins.
https://docs.chainstack.com/docs/request-units
"""
from collections import Counter, deque, OrderedDict
from datetime import datetime, timezone
import json
import math
import time

from .core import atomic_json, check


class StreamPacingPause(RuntimeError):
    """Close the socket before a bounded pacing pause; HTTP continues."""


class RequestBudget:
    def __init__(self, path, daily_limit, clock=time.time, *, pacing_mode="daily"):
        check(pacing_mode in ("daily", "session"), "invalid stream pacing mode")
        self.pacing_mode = pacing_mode
        self.path, self.limit, self.clock = path, daily_limit, clock
        self.capacity = min(128, daily_limit)
        self.state = {'day': self.day(), 'units': 0, 'bytes': 0}
        if path.exists():
            loaded = json.loads(path.read_text())
            check(isinstance(loaded.get('day'), str), 'invalid stream usage day')
            datetime.strptime(loaded['day'], '%Y-%m-%d')
            for key in ('units', 'bytes'):
                check(type(loaded.get(key)) is int and loaded[key] >= 0, 'invalid stream usage journal')
            self.state = loaded
        self._refresh()
        # Persist a one-time legacy migration before connecting. Restarts do
        # not grant another burst or remove debt incurred before disconnection.
        if 'pace_credit' not in self.state:
            self.state.update(pace_credit=float(min(self.capacity,max(0,self.limit-self.state['units']))),
                              pace_at=self.clock(), kinds={})
            self.save()
        for key in ('pace_credit','pace_at'):
            check(type(self.state.get(key)) in (int,float) and math.isfinite(self.state[key]), 'invalid stream pacing journal')
        check(self.state['pace_credit'] <= self.capacity, 'invalid stream pacing credit')
        kinds = self.state.get('kinds', {})
        check(isinstance(kinds,dict) and all(k in ('control','notification','invalid','unknown') and
              type(v) is int and v >= 0 for k,v in kinds.items()), 'invalid stream message accounting')
        self.state['kinds'] = kinds
        self.state.setdefault('pace_paused',False)
        check(type(self.state['pace_paused']) is bool, 'invalid stream pacing pause')

    def day(self):
        return datetime.fromtimestamp(self.clock(), timezone.utc).strftime('%Y-%m-%d')

    def _refresh(self):
        if self.day() > self.state['day']:
            self.state = {'day':self.day(), 'units':0, 'bytes':0,
                          'pace_credit':float(self.capacity), 'pace_at':self.clock(), 'kinds':{}, 'pace_paused':False}

    def seconds_left(self):
        self._refresh()
        day = datetime.strptime(self.state['day'], '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp()
        return max(1,day+86400-self.clock())

    def sustainable_rate(self):
        # Small safety reserve; the remaining allocation adapts after every
        # reply, including control churn. This is never a provider quota claim.
        self._refresh()
        return .9*max(0,self.limit-self.state['units'])/self.seconds_left()

    def _refill(self):
        self._refresh()
        now = self.clock()
        elapsed = max(0,now-self.state['pace_at'])
        self.state['pace_credit'] = min(self.capacity,self.state['pace_credit']+elapsed*self.sustainable_rate())
        self.state['pace_at'] = max(now,self.state['pace_at'])

    def remaining_seconds(self):
        self._refresh()
        return self.seconds_left() if self.state['units'] >= self.limit else 0

    def pacing_delay(self):
        self._refill()
        # Session mode spends the same durable daily allowance continuously.
        # It changes scheduling only; remaining_seconds/add still enforce the
        # shared daily total, including HTTP debits and restart recovery.
        if self.pacing_mode == "session":
            if self.state["pace_paused"]:
                self.state["pace_paused"] = False
                self.save()
            return 0
        if self.state['pace_credit'] <= 0 and not self.state['pace_paused']:
            self.state['pace_paused'] = True
            self.save()
        if not self.state['pace_paused']: return 0
        target = min(self.capacity,max(1,self.limit-self.state['units']))
        if self.state['pace_credit'] >= target:
            self.state['pace_paused'] = False
            self.save()
            return 0
        return min(self.seconds_left(),max(1,(target-self.state['pace_credit'])/max(self.sustainable_rate(),1e-12)))

    def pause(self):
        if self.pacing_mode == "session": return
        self._refill()
        self.state['pace_credit'] = min(0,self.state['pace_credit'])
        self.state['pace_paused'] = True
        self.save()

    def filter_capacity(self):
        # With little quota left, rebuild a small fair roster, not 64 filters.
        return max(1,min(64,max(0,self.limit-self.state['units'])//128))

    def rotation_seconds(self):
        return max(30,10/max(self.sustainable_rate(),1e-12))

    def add(self, size, kind='unknown'):
        self._refill()
        self.state['units'] += 1
        self.state['bytes'] += size
        self.state['pace_credit'] -= 1
        if self.pacing_mode == 'session':
            # Returning to daily pacing waits for credit, without accumulating
            # a day-long artificial debt while session pacing was disabled.
            self.state['pace_credit'] = max(0, self.state['pace_credit'])
        self.state['kinds'][kind] = self.state['kinds'].get(kind,0)+1
        # Persist even control-only startup and the final allowed notification.
        self.save()
        check(self.state['units'] <= self.limit, 'stream request budget reached')

    def save(self): atomic_json(self.path, self.state)

    def details(self):
        self._refill()
        return {'budget_kind':'local_request_units', 'local_units':self.state['units'],
                'local_daily_limit':self.limit, 'usage_day_utc':self.state['day'],
                'received_bytes':self.state['bytes'], 'account_quota_known':False,
                'message_units':dict(self.state['kinds']), 'pacing_credit':self.state['pace_credit'],
                'sustainable_units_per_second':self.sustainable_rate(),
                'pacing_burst_units':self.capacity, 'pacing_mode':self.pacing_mode}


class SubscriptionRoster:
    """Bound per-connection subscriptions without rejecting a growing universe.

    Overflow rotates FIFO; pools outside this roster remain in HTTP discovery.
    The object survives reconnects so an unstable connection cannot permanently
    starve pools sorted after the initial capacity boundary.
    """
    def __init__(self, capacity=8192, batch_size=128, rotation_seconds=30, clock=time.monotonic, confirm_admissions=False):
        check(capacity > 0 and batch_size > 0 and rotation_seconds > 0, 'invalid stream roster limits')
        self.capacity, self.batch_size = capacity, min(capacity, batch_size)
        self.rotation_seconds, self.clock = rotation_seconds, clock
        self.next_rotation = clock()+rotation_seconds
        self.selected, self.waiting = OrderedDict(), OrderedDict()
        self.confirm_admissions, self.admitted = confirm_admissions, set()

    def select(self, pools, individual=(), priority=()):
        pools, now, individual = set(pools), self.clock(), set(individual)
        priority=set(priority)&pools
        self.selected = OrderedDict((p,None) for p in self.selected if p in pools)
        self.waiting = OrderedDict((p,None) for p in self.waiting if p in pools)
        self.admitted.intersection_update(self.selected)
        for p in sorted(pools-self.selected.keys()-self.waiting.keys()): self.waiting[p] = None
        weight = lambda p: self.batch_size if p in individual else 1
        occupied = sum(weight(p) for p in self.selected)
        for p in reversed(sorted(priority)):
            if p in self.waiting:self.waiting.move_to_end(p,last=False)
        reserve=sum(weight(p) for p in priority if p not in self.selected)
        while occupied+reserve>self.capacity and any(p not in priority for p in self.selected):
            p=next(p for p in reversed(self.selected) if p not in priority)
            self.selected.pop(p);self.waiting[p]=None;self.admitted.discard(p);occupied-=weight(p)
        while occupied > self.capacity:
            p,_ = self.selected.popitem(last=True)
            self.waiting[p] = None
            occupied -= weight(p)
            self.admitted.discard(p)
        if now >= self.next_rotation:
            released = 0
            while self.waiting and self.selected and released < self.batch_size:
                p = next((p for p in self.selected if p not in priority and (not self.confirm_admissions or p in self.admitted)),None)
                if p is None: break
                self.selected.pop(p); self.admitted.discard(p)
                self.waiting[p] = None
                occupied -= weight(p); released += weight(p)
            self.next_rotation = now+self.rotation_seconds
        while self.waiting:
            p = next(iter(self.waiting))
            if occupied+weight(p) > self.capacity: break
            self.waiting.pop(p)
            self.selected[p] = None
            occupied += weight(p)
        self.selected=OrderedDict([(p,None) for p in sorted(priority) if p in self.selected]+
                                  [(p,None) for p in self.selected if p not in priority])
        return set(self.selected)

    def confirmed(self, pools):
        for pool in pools:
            if pool in self.selected:
                self.admitted.add(pool)
                self.selected.move_to_end(pool)

    def defer(self, pools):
        # Unconfirmed subscriptions retain their place ahead of new arrivals;
        # only actually admitted work may advance the round-robin window.
        deferred = [p for p in self.selected if p in pools]
        for p in deferred:
            self.selected.pop(p); self.admitted.discard(p)
        self.waiting = OrderedDict([(p,None) for p in deferred]+list(self.waiting.items()))

    def details(self):
        return {'subscription_capacity':self.capacity,
                'capacity_deferred_pools':len(self.waiting),
                'subscription_rotation_seconds':self.rotation_seconds}


class StreamSelection:
    """Bound noisy probes by actual subscription exposure and received messages.

    Metadata/history survive reconnection; a selected-but-unadmitted probe has
    no running lease. Every removed pool remains in the independent HTTP path.
    """
    def __init__(self, units_per_day, clock=time.monotonic, budget=None, priority_groups=None):
        self.clock, self.started, self.budget = clock, clock(), budget
        self.target_rate = units_per_day/86400*.70
        self.samples = deque(maxlen=20000)
        self.cool_until, self.history = {}, OrderedDict()
        self.live = {}
        self.last_check = self.started
        self.last_probe = self.started
        self.probe_candidate = None
        self.probe_limit, self.probe_seconds = 4, 10
        self.selected, self.universe = set(), set()
        self.changed, self.overrun = False, 0
        self.probes_started, self.probes_limited, self.history_evicted = 0, 0, 0
        self.priority_groups=priority_groups
        self.priority=set();self.priority_rate=0.;self.priority_changed_at=0.

    def _rate(self):
        return self.budget.sustainable_rate()*.70 if self.budget is not None else self.target_rate

    def _probe_interval(self):
        return max(30,30/max(self.budget.sustainable_rate(),1e-12)) if self.budget is not None else 30

    def _cool(self, pool, rate):
        now = self.clock()
        previous = self.history.get(pool,{})
        strikes = min(5,previous.get('strikes',0)+1)
        self.history[pool] = dict(strikes=strikes,rate=max(rate,previous.get('rate',0)),last_probe=now,isolated=True)
        self.history.move_to_end(pool)
        self.cool_until[pool] = now+min(3600,120*2**(strikes-1))
        if self.probe_candidate == pool: self.probe_candidate = None
        self.changed = True
        while len(self.history) > 8192:
            # The unique pending/live probe must retain the history needed by
            # its later disconnect and retry. Prefer the oldest other entry.
            old = next((p for p in self.history if p != self.probe_candidate and
                        not self.live.get(p,{}).get('probe')), next(iter(self.history)))
            self.history.pop(old)
            self.cool_until.pop(old,None)
            # Defensive consistency if handed an impossible all-probe history;
            # never exceed the bound or leave an orphaned probe behind.
            if old in self.live: self.live[old]['probe'] = False
            if self.probe_candidate == old: self.probe_candidate = None
            self.history_evicted += 1

    def observe(self, pool):
        now = self.clock()
        if pool not in self.live: return
        self.samples.append((now,pool))
        item = self.live[pool]
        item['messages'] += 1
        if pool in self.priority:return
        if pool in self.cool_until and not item['probe']:
            self.overrun += 1
            return
        rate = item['messages']/max(.05,now-item['since'])
        if item['probe'] and item['messages'] >= self.probe_limit:
            item['probe'] = False; self.probes_limited += 1
            self._cool(pool,rate)
        elif item['messages'] >= 4 and rate > self._rate():
            item['probe'] = False; self._cool(pool,rate)

    def subscribed(self, pools):
        now = self.clock()
        for pool in pools:
            if pool in self.live: continue
            probe = pool == self.probe_candidate
            self.live[pool] = dict(since=now,messages=0,probe=probe)
            if probe:
                self.last_probe = now; self.probes_started += 1

    def unsubscribed(self, pools):
        for pool in pools: self.live.pop(pool,None)
        self.overrun = 0

    def disconnected(self):
        for pool,item in list(self.live.items()):
            if item['probe'] and pool in self.history:
                self.cool_until[pool] = max(self.cool_until.get(pool,0),self.clock()+120)
        self.live.clear(); self.probe_candidate = None; self.overrun = 0

    def individual_pools(self):
        return self.priority | {p for p,item in self.history.items() if item.get('isolated',True)} | ({self.probe_candidate} if self.probe_candidate else set())

    def select(self, pools):
        now = self.clock()
        self.universe = set(pools)
        if self.priority_groups is not None:
            groups=[(score,set(members)&self.universe) for score,members in self.priority_groups()]
            groups=[(score,members) for score,members in groups if len(members)>=2]
            current_score=next((score for score,members in groups if members==self.priority),None)
            stronger=(groups and current_score is not None and isinstance(groups[0][0],tuple)
                      and isinstance(current_score,tuple) and groups[0][0][0]>current_score[0])
            # Keep one fully prepared route together. Pinning all high-volume
            # routes would burn the same finite budget faster, not add capacity.
            # A route retains its slot while still among the top choices; refresh
            # is bounded to avoid repeated unsubscribe/subscribe control costs.
            if (not self.priority or stronger or not any(m==self.priority for _,m in groups)
                    or now-self.priority_changed_at>=60):
                self.priority=groups[0][1] if groups else set()
                self.priority_changed_at=now
            for pool in self.priority:self.cool_until.pop(pool,None)
            self.priority_rate=sum(self.live[p]['messages']/max(1,now-self.live[p]['since'])
                                   for p in self.priority if p in self.live)
        self.history = OrderedDict((p,v) for p,v in self.history.items() if p in self.universe)
        self.cool_until = {p:t for p,t in self.cool_until.items() if p in self.history}
        if self.probe_candidate not in self.cool_until: self.probe_candidate = None
        for pool,item in self.live.items():
            if pool not in self.history: item['probe'] = False
        while self.samples and self.samples[0][0] < now-60: self.samples.popleft()
        # A quiet probe graduates only after actual successful subscription.
        for pool,item in self.live.items():
            if item['probe'] and now-item['since'] >= self.probe_seconds:
                item['probe'] = False
                self.cool_until.pop(pool,None)
                if pool in self.history: self.history[pool]['isolated'] = False
                if self.probe_candidate == pool: self.probe_candidate = None
        if now-self.last_check >= 1:
            self.last_check = now
            counts = Counter(p for _,p in self.samples if p in self.universe and p not in self.cool_until)
            span = min(60,max(1,now-self.started))
            total = sum(counts.values())
            for pool,count in counts.most_common():
                if total/span <= self._rate(): break
                if pool in self.priority:continue
                self._cool(pool,count/span); total -= count
        if self.probe_candidate is None and now-self.last_probe >= self._probe_interval():
            ready = [p for p,t in self.cool_until.items() if t <= now]
            if ready: self.probe_candidate = min(ready,key=lambda p:(self.history[p]['last_probe'],p))
        self.selected = self.universe-set(self.cool_until)
        if self.probe_candidate is not None: self.selected.add(self.probe_candidate)
        self.changed = False
        return set(self.selected)

    def details(self):
        return {'http_only_pools':len(self.universe-self.selected), 'known_pools':len(self.universe),
                'selection':'prepared_route_priority' if self.priority_groups else 'budget_paced_event_volume',
                'priority_pools':sorted(self.priority), 'priority_observed_messages_per_second':self.priority_rate,
                'priority_exceeds_sustainable_rate':self.priority_rate>self._rate(),
                'probe_message_limit':self.probe_limit,
                'probes_started':self.probes_started, 'probes_message_limited':self.probes_limited,
                'noisy_history_pools':len(self.history), 'noisy_history_evicted':self.history_evicted,
                'pending_probe_admission':self.probe_candidate is not None and self.probe_candidate not in self.live}
