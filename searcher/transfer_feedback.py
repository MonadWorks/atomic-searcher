"""Learn exact-receipt incompatibilities away from the execution deadline.

A sampled shortfall or failed token call gives related directed paths a bounded scheduling backoff.
Real whole-executor retries remain available: amount-dependent taxes and token
hooks can behave differently in the helper. Samples never authorize a trade.
"""
import json
import copy
import uuid
import hashlib
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass

from eth_abi import decode

from .core import ROOT, RPC, address, atomic_json, check, data, number, safe_error, unhex


@dataclass(frozen=True)
class TransferSample:
    token: str
    source: str
    destination: str
    amount: int

    @property
    def key(self):
        return (self.token, self.source, self.destination)


def route_edges(route, executor):
    for i, pool in enumerate(route.pools):
        yield (route.tokens[i], executor, pool)
        yield (route.tokens[i+1], pool, executor)


def measure_sample(rpc, sample, executor, owner, runtime):
    """Canonical sealed-state evidence only; this transport cannot broadcast."""
    token, source, destination = map(address, sample.key)
    executor, owner = address(executor), address(owner)
    check(len({token, source, destination, executor}) == 4 and 0 < sample.amount < 2**256,
          'invalid transfer diagnostic')
    started_utc = time.time()
    head = rpc.header()
    raw = rpc.request('eth_call', [
        {'from':owner, 'to':source, 'gas':hex(3_000_000),
         'data':data('sampleTransferStatus(address,address,address,uint256)',
                     ['address','address','address','uint256'],
                     [token,executor,destination,sample.amount])},
        head['number'], {source:{'code':runtime}, executor:{'code':runtime}}])
    funded, debited, credited, stage, selector = decode(['uint256']*3+['uint8','bytes4'], unhex(raw))
    check(stage in (0,1,2), 'invalid transfer diagnostic stage')
    check(rpc.header(number(head['number']))['hash'] == head['hash'], 'transfer diagnostic anchor changed')
    return {'token':token,'source':source,'destination':destination,'executor':executor,
            'amount':sample.amount,'funded':funded,'debited':debited,'credited':credited,
            'failure_stage':stage,'failure_selector':'0x'+selector.hex(),
            'block':number(head['number']),'block_hash':head['hash'],'canonical':True,
            'sample_started_utc':started_utc,'sample_finished_utc':time.time(),
            'scope':'sampled directed transfers; helper code at source/executor; real token code/storage; no broadcast'}


class TransferFeedback:
    def __init__(self, executor, owner, clock=time.monotonic, ttl=300, capacity=2048, queue_limit=64, retry_seconds=30, queue_seconds=60, max_retry_seconds=300, history_seconds=3600, quarantine=None):
        self.executor, self.owner = address(executor), address(owner)
        self.clock, self.ttl, self.capacity, self.queue_limit = clock, ttl, capacity, queue_limit
        self.retry_seconds = retry_seconds
        self.max_retry_seconds = max(retry_seconds, min(ttl, max_retry_seconds))
        self.queue_seconds = queue_seconds
        self.history_seconds = max(ttl, history_seconds)
        self.lock = threading.RLock()
        self.pending, self.retry_at, self.edges = OrderedDict(), OrderedDict(), OrderedDict()
        self.in_flight, self.last_passed = {}, OrderedDict()
        self.failures = OrderedDict()
        self.results = OrderedDict()
        # Helper validity and already-scheduled whole-call retries have separate
        # lifetimes. Expiring a sample must not cancel a confirmed retry delay.
        self.history = OrderedDict()
        self.probe_slots = OrderedDict()
        # Cache only tiny authenticated group topologies, never eligibility,
        # prices or sampled evidence. Every hot-read decision rechecks time.
        self.group_routes = OrderedDict()
        self.quarantine = quarantine
        if quarantine is not None:
            check(quarantine.identity['executor'] == self.executor and quarantine.identity['owner'] == self.owner,
                  'transfer quarantine owner mismatch')
        self.recovery_turn = True
        # These are bounded scheduling reservations, never durable witnesses.
        self.revalidations, self.fresh_samples, self.claimed_samples = OrderedDict(), OrderedDict(), OrderedDict()
        self.revalidation_limit = max(1, min(64, capacity, queue_limit))
        self.last_passed_wall = OrderedDict()
        self.wall_clock = quarantine.clock if quarantine is not None else time.time
        self.prewarm_namespace, self.prewarm_sequence = uuid.uuid4().hex[:16], 0
        self.prewarm_counts = dict(scheduled=0, offered=0, queued=0, dispatched=0, consumed=0,
                                   released=0, lease_expired=0, claimed=0, expired=0, timed_out=0, passed=0)

    @staticmethod
    def modeled_samples(route, states, amount, settlements):
        output, samples = amount, []
        try:
            for i, pool in enumerate(route.pools[:-1]):
                output, _ = states[pool].quote(route.tokens[i], output)
                if output <= 0: return ()
                if route.tokens[i+1] not in settlements:
                    samples.append(TransferSample(route.tokens[i+1], pool, route.pools[i+1], output))
        except Exception:
            return ()
        return tuple(samples)

    def _sample_current(self, sample, now):
        item = self.fresh_samples.get((sample.key, sample.amount))
        return item if item is not None and item['until'] > now else None

    def _samples_ready(self, samples, now):
        """A complete clean check or one exact anomaly can schedule a new call.

        A healthy unrelated leg may continuously change its intermediate size.
        It must not delay testing a still-exact anomalous direction. Only each
        individually fresh/matching sample is later pinned as failure evidence.
        """
        if not samples: return False
        complete = True
        for sample in samples:
            fresh = self._sample_current(sample, now)
            if fresh is None:
                complete = False
                continue
            evidence = fresh['evidence']
            if (evidence.get('failure_stage', 0) in (1, 2) or evidence['funded'] < sample.amount or
                    evidence['funded'] > 0 and evidence['credited'] < evidence['funded']):
                return True
        return complete

    def _prune_revalidations(self):
        now = self.clock()
        for key, item in list(self.revalidations.items()):
            if item['until'] <= now:
                del self.revalidations[key]
                self.prewarm_counts['expired'] += 1
                continue
            lease = item.get('lease')
            if lease is not None and not lease.get('held') and now-lease['updated_at'] >= self.queue_seconds:
                item.pop('lease', None)
                item['offer_after'] = now+min(1, self.retry_seconds)
                self.prewarm_counts['lease_expired'] += 1
        for key, item in list(self.fresh_samples.items()):
            if item['until'] <= now: del self.fresh_samples[key]
        for key, item in list(self.claimed_samples.items()):
            if item['until'] <= now: del self.claimed_samples[key]

    def schedule_revalidation(self, route, states, amount, settlements, failure, retry_after=0):
        """A new real failure schedules future evidence; it cannot consume it."""
        if (failure.get('validation_deadline_reached') or failure.get('validation_outcome') or
                failure.get('anchor_mismatch')): return False
        name = failure.get('revert_name')
        if name != 'BadCallback' and not (name in ('TransferFailed', 'V2TransferFailed') and
                len(set(failure.get('gas_attempts', ()))) >= 2 and failure.get('revert_selector') and
                failure.get('previous_gas_failure', {}).get('revert_selector') == failure['revert_selector'] and
                (name != 'V2TransferFailed' or failure.get('previous_gas_failure', {}).get('revert_name') == name)):
            return False
        probe = failure.get('execution_probe', {})
        call = probe.get('unsigned_call', {})
        stamp = probe.get('started_utc')
        if (probe.get('stage') not in ('initial_call', 'gas_estimate', 'final_call_batch') or
                probe.get('state_tag') != 'pending' or type(stamp) not in (int, float) or
                not 0 < stamp < float('inf') or call.get('from', '').lower() != self.owner or
                call.get('to', '').lower() != self.executor): return False
        samples = self.modeled_samples(route, states, amount, settlements)
        if not samples: return False
        with self.lock:
            self._prune_revalidations()
            for edge in route_edges(route, self.executor):
                passed = self.last_passed_wall.get(edge)
                if passed is not None and passed['until'] > self.clock() and stamp <= passed['at']:
                    return False
            key = (route.key, int(amount))
            if self.quarantine is not None and self.quarantine.blocked(route_edges(route, self.executor)):
                self.revalidations.pop(key, None)
                return False
            previous = self.revalidations.get(key)
            if previous is not None and previous['failure_stamp'] >= stamp: return False
            now = self.clock()
            self.revalidations[key] = dict(route=route, amount=int(amount), samples=samples,
                failure_stamp=stamp, until=now+self.history_seconds,
                due=now+max(self.retry_seconds, min(self.max_retry_seconds, retry_after)),
                dispatched_at=None, dispatches=0, claimed=False)
            self.revalidations.move_to_end(key)
            while len(self.revalidations) > self.revalidation_limit:
                self.revalidations.popitem(last=False)
            self.prewarm_counts['scheduled'] += 1
            return True

    def _prewarm_needed(self, sample):
        now = self.clock()
        return any(not row['claimed'] and row['due'] <= now < row['until'] and
                   sample in row['samples'] and self._sample_current(sample, now) is None
                   for row in self.revalidations.values())

    def _queue_prewarm(self):
        self._prune_revalidations()
        now = self.clock()
        for row in self.revalidations.values():
            if row['claimed'] or row['due'] > now or row['dispatches'] >= 3: continue
            for sample in row['samples']:
                if self._sample_current(sample, now) is not None: continue
                if sample.key in self.in_flight or sample.key in self.pending: continue
                # A failed network diagnostic retains its original backoff.
                # A successful sample at another size is not this evidence.
                if self.failures.get(sample.key) and self.retry_at.get(sample.key, 0) > now: continue
                if len(self.pending) >= self.queue_limit: return
                self.pending[sample.key] = (sample, now)

    def revalidation_requests(self):
        """Lease one ready group. Offering/queuing never consumes a retry."""
        with self.lock:
            self._prune_revalidations()
            now = self.clock()
            for row in self.revalidations.values():
                if (row['claimed'] or row['due'] > now or row['dispatches'] >= 3 or row.get('lease') or
                        row.get('offer_after', 0) > now or
                        row['dispatched_at'] is not None and now-row['dispatched_at'] < self.retry_seconds): continue
                if not self._samples_ready(row['samples'], now): continue
                if self.deferred(row['route']): continue
                self.prewarm_sequence += 1
                lease_id = self.prewarm_namespace+'-'+str(self.prewarm_sequence)
                row['lease'] = dict(id=lease_id, state='offered', updated_at=now)
                self.prewarm_counts['offered'] += 1
                return [dict(pools=tuple(sorted(row['route'].pools)), route_keys=[row['route'].key],
                             deep=False, score=0, since=now, source='transfer_prewarm',
                             prewarm_lease_id=lease_id, prewarm_route_key=row['route'].key,
                             prewarm_amount=row['amount'])]
            return []

    def revalidation_lease_ids(self):
        """Live IDs for queue/slice reconciliation; no execution authority."""
        with self.lock:
            self._prune_revalidations()
            return {row['lease']['id'] for row in self.revalidations.values()
                    if not row['claimed'] and row.get('lease')}

    def revalidation_queued(self, lease_ids):
        """A queue or pending slice still owns these leases; renew the handoff."""
        with self.lock:
            self._prune_revalidations()
            wanted, now, count = set(lease_ids), self.clock(), 0
            for row in self.revalidations.values():
                lease = row.get('lease')
                if row['claimed'] or lease is None or lease['id'] not in wanted: continue
                if lease['state'] == 'offered': self.prewarm_counts['queued'] += 1
                lease.update(state='queued', updated_at=now)
                count += 1
            return count

    def revalidation_amounts(self, lease_ids):
        """Return trusted fixed principals for live, diagnostically ready leases.

        Frame metadata only names leases. Amounts come from current reservations,
        and one direction is quoted at one principal per new snapshot. These
        hints neither reuse prices nor authorize an executor call.
        """
        with self.lock:
            self._prune_revalidations()
            wanted, now, amounts = set(lease_ids), self.clock(), {}
            for row in self.revalidations.values():
                lease = row.get('lease')
                if (row['claimed'] or lease is None or lease['state'] != 'queued' or
                        lease['id'] not in wanted or row['due'] > now or
                        not self._samples_ready(row['samples'], now)):
                    continue
                amounts.setdefault(row['route'].key, row['amount'])
            return amounts

    def revalidation_held(self, lease_ids):
        """Reconcile actual reader/page ownership, bounded by reservation TTL.

        Only an accepted live lease may be held. A synchronous read or a page
        awaiting consumption can exceed the queue-heartbeat window; their
        owner must explicitly return/release the lease. Ordinary queued work
        retains its short lost-handoff timeout.
        """
        with self.lock:
            self._prune_revalidations()
            wanted, now, held = set(lease_ids), self.clock(), 0
            for row in self.revalidations.values():
                lease = row.get('lease')
                if row['claimed'] or lease is None or lease['state'] != 'queued': continue
                holding = lease['id'] in wanted
                if lease.get('held') and not holding: lease['updated_at'] = now
                lease['held'] = holding
                held += holding
            return held

    def revalidation_released(self, lease_ids, reason='queue_rejected'):
        """Eviction/read failure permits another admission without spending it."""
        with self.lock:
            wanted, now, count = set(lease_ids), self.clock(), 0
            for row in self.revalidations.values():
                lease = row.get('lease')
                if lease is None or lease['id'] not in wanted: continue
                row.pop('lease')
                row['offer_after'] = now+min(1, self.retry_seconds)
                row['last_release'] = reason if reason in (
                    'queue_rejected', 'queue_evicted', 'read_failed', 'stale_lease',
                    'incomplete_quote', 'changed_amount', 'stale_helper', 'stopped') else 'released'
                self.prewarm_counts['released'] += 1
                count += 1
            return count

    def revalidation_consumed(self, lease_ids, route_keys, *, candidates=None, states=None, settlements=(), quoted_amounts=None):
        """Count only a current lease whose direction completed new evaluation.

        The consumer excludes unfinished/depth-deferred directions. A current
        positive candidate must also match this reservation's principal and
        freshly modeled evidence sizes. An exact fresh anomalous leg suffices;
        an unrelated unsampled leg never acquires evidence from this admission.
        Absence of a positive candidate may count a completed no-profit quote.
        """
        with self.lock:
            self._prune_revalidations()
            wanted, considered, now, count = set(lease_ids), set(route_keys), self.clock(), 0
            positive = {c['route_key']: c for c in candidates or ()}
            for row in self.revalidations.values():
                lease = row.get('lease')
                if (row['claimed'] or lease is None or lease['state'] != 'queued' or lease['id'] not in wanted or
                        row['route'].key not in considered): continue
                # A complete no-profit quote at A does not consume a distinct
                # reservation at B when both leases shared the same pool group.
                if quoted_amounts is not None and quoted_amounts.get(row['route'].key) != row['amount']:
                    self.revalidation_released([lease['id']], 'changed_amount')
                    continue
                candidate = positive.get(row['route'].key)
                if candidate is not None:
                    if candidate['amount_in'] != row['amount']:
                        self.revalidation_released([lease['id']], 'changed_amount')
                        continue
                    samples = self.modeled_samples(row['route'], states or {}, row['amount'], settlements)
                    changed = samples != row['samples']
                    row.update(samples=samples)
                    if not self._samples_ready(samples, now):
                        self.revalidation_released([lease['id']], 'changed_amount')
                        if not changed: row['last_release'] = 'stale_helper'
                        continue
                row.pop('lease')
                row['dispatches'] += 1
                row['dispatched_at'] = now
                self.prewarm_counts['dispatched'] += 1
                self.prewarm_counts['consumed'] += 1
                count += 1
            return count

    def _prewarm_claim_ready(self, route, amount, states, settlements):
        self._prune_revalidations()
        row = self.revalidations.get((route.key, amount))
        if row is None: return True
        now = self.clock()
        if row['claimed']: return now >= row.get('claim_until', 0)
        samples = self.modeled_samples(route, states, amount, settlements)
        if samples != row['samples']:
            row.update(samples=samples, dispatches=0, dispatched_at=None)
            row.pop('lease', None)
        ready = self._samples_ready(samples, now)
        if not ready and now < row['due']+self.queue_seconds: return False
        return True

    def failure_history(self, edge, now):
        item = self.history.get(edge)
        if item is not None and item['until'] <= now:
            del self.history[edge]
            return None
        return item

    def active_edges(self, route):
        # Caller holds the registry lock.
        now = self.clock()
        active = []
        for key in route_edges(route, self.executor):
            item = self.edges.get(key)
            if item is not None:
                if item['until'] > now: active.append((key,item))
                else: del self.edges[key]
        return active

    def scheduled_edges(self, route):
        """Scheduling memory, never a fresh transfer-compatibility assertion."""
        now = self.clock()
        active = dict(self.active_edges(route))
        for edge in route_edges(route, self.executor):
            history = self.failure_history(edge, now)
            slot = self.probe_slots.get(edge)
            if slot is not None and slot['until'] <= now:
                del self.probe_slots[edge]
                slot = None
            if history is not None or slot is not None:
                active.setdefault(edge, {})
        return active

    def deferred(self, route):
        with self.lock:
            if self.quarantine is not None and self.quarantine.blocked(route_edges(route, self.executor)):
                return True
            now = self.clock()
            return any(max(item.get('next_probe', 0),
                           self.history.get(edge, {}).get('next_probe', 0),
                           self.probe_slots.get(edge, {}).get('until', 0)) > now
                       for edge,item in self.scheduled_edges(route).items())

    def group_deferred(self, group, pools, settlements, route_keys=None):
        """Skip a hot read only while every complete direction is deferred.

        A group has at most four pools; enumeration is local to those pools.
        One eligible direction, an unknown topology, or an expired retry keeps
        the group available. Depth requests may name only the directions that
        actually requested extra data. Unknown keys remain eligible. This does
        not claim a profitable or safe route.
        """
        with self.lock:
            if not (self.edges or self.history or self.probe_slots or
                    (self.quarantine is not None and self.quarantine.records)) or not 2 <= len(group) <= 4 or len(set(group)) != len(group):
                return False
            try:
                part = [pools[a] for a in sorted(group)]
            except KeyError:
                return False
            if any(p.address != a for a, p in zip(sorted(group), part)):
                return False
            tokens = {t for p in part for t in (p.token0, p.token1)}
            starts = tuple(sorted(tokens.intersection(settlements)))
            key = (tuple((p.address, p.token0, p.token1, p.kind) for p in part), starts)
            if key not in self.group_routes:
                from .routes import Route, iter_cycles
                self.group_routes[key] = tuple(Route(tokens, path) for tokens, path in
                    iter_cycles(part, starts, len(part)) if len(path) == len(part))
            self.group_routes.move_to_end(key)
            routes = self.group_routes[key]
            while len(self.group_routes) > max(1, min(512, self.capacity)):
                self.group_routes.popitem(last=False)
            if route_keys is not None:
                requested = set(route_keys)
                if not requested or not requested.issubset({route.key for route in routes}):
                    return False
                routes = tuple(route for route in routes if route.key in requested)
            # Do not cache this result: next_probe, helper TTL and passed()
            # can restore a direction without changing pool metadata.
            return bool(routes) and all(self.deferred(route) for route in routes)

    def priority(self, route):
        with self.lock:
            scheduled = self.scheduled_edges(route)
            return (1+int(any(item.get('last_route') == route.key or
                             self.probe_slots.get(edge, {}).get('last_route') == route.key
                             for edge,item in scheduled.items()))) if scheduled else 0

    def claim_probe(self, route, amount=None, states=None, settlements=()):
        """Consume the retry slot immediately before the real whole-call check.

        Quotes, low-profit candidates and preflight timeouts never consume it.
        """
        with self.lock:
            if self.deferred(route): return False
            if amount is not None and not self._prewarm_claim_ready(route, amount, states or {}, settlements):
                return False
            if self.quarantine is not None and not self.quarantine.claim(tuple(route_edges(route, self.executor))):
                return False
            now = self.clock()
            if amount is not None:
                row = self.revalidations.get((route.key, amount))
                if row is not None:
                    if not self._samples_ready(row['samples'], now):
                        self.prewarm_counts['timed_out'] += 1
                    row.update(claimed=True, claim_until=now+self.retry_seconds)
                    row.pop('lease', None)
                    self.prewarm_counts['claimed'] += 1
                pinned = {}
                for sample in self.modeled_samples(route, states or {}, amount, settlements):
                    fresh = self._sample_current(sample, now)
                    if fresh is None: continue
                    evidence = fresh['evidence']
                    stage = evidence.get('failure_stage', 0)
                    incoming = (sample.token, sample.source, self.executor)
                    outgoing = (sample.token, self.executor, sample.destination)
                    bad = []
                    if stage == 1 or evidence['funded'] < sample.amount:
                        bad.append((incoming, stage == 1))
                    if stage == 2 or (stage == 0 and evidence['funded'] > 0 and evidence['credited'] < evidence['funded']):
                        bad.append((outgoing, stage == 2))
                    for edge, call_failed in bad:
                        if self.last_passed.get(edge, float('-inf')) >= fresh['sampled_at']: continue
                        pinned[edge] = dict(until=fresh['until'], sampled_at=fresh['sampled_at'],
                            evidence=copy.deepcopy(evidence),
                            sample_kind='call_failure' if call_failed else 'receipt_shortfall',
                            repeated_reverts=max(self.edges.get(edge, {}).get('repeated_reverts', 0),
                                self.history.get(edge, {}).get('repeated_reverts', 0)))
                self.claimed_samples[(route.key, amount)] = dict(items=pinned, edges=tuple(route_edges(route, self.executor)),
                    at=now, until=now+self.ttl)
                self.claimed_samples.move_to_end((route.key, amount))
                while len(self.claimed_samples) > self.revalidation_limit:
                    self.claimed_samples.popitem(last=False)
            until = now+self.retry_seconds
            for edge in self.scheduled_edges(route):
                self.probe_slots[edge] = {'until':until, 'last_route':route.key}
                self.probe_slots.move_to_end(edge)
            while len(self.probe_slots) > self.capacity:
                self.probe_slots.popitem(last=False)
            for _,item in self.active_edges(route):
                item.update(next_probe=until, last_route=route.key, probe_pending=True)
            return True

    def rejected(self, route, failure, amount=None):
        """Slow repeated whole-call reverts on a sampled shortfall direction.

        Only an attempted whole-executor call can confirm the retry failure.
        Timeouts and stale reads add no evidence. A repeated wrapped transfer
        failure can slow a direction only when a fresh helper call also failed.
        This remains a temporary scheduling hint, not a token blacklist.
        """
        with self.lock:
            now = self.clock()
            delay = 0
            if amount is None:
                observations = self.active_edges(route)
            else:
                claim = self.claimed_samples.pop((route.key, amount), None)
                observations = list(claim['items'].items()) if claim is not None and claim['until'] > now else []
            for edge,item in observations:
                if amount is None:
                    if item.get('last_route') != route.key or not item.pop('probe_pending', False): continue
                elif (item['until'] <= now or self.last_passed.get(edge, float('-inf')) >= claim['at']):
                    continue
                sampled_call_failed = item.get('sample_kind') == 'call_failure'
                repeated_call_failed = (sampled_call_failed and failure.get('revert_name') in ('TransferFailed','V2TransferFailed')
                                        and len(set(failure.get('gas_attempts', ()))) >= 2
                                        and failure.get('revert_selector') is not None
                                        and failure.get('previous_gas_failure', {}).get('revert_selector')
                                            == failure.get('revert_selector')
                                        and (failure.get('revert_name') != 'V2TransferFailed' or
                                            failure.get('previous_gas_failure', {}).get('revert_name') == 'V2TransferFailed'))
                if ((failure.get('revert_name') not in ('BadCallback', 'UniswapV2Invariant', 'PoolInvariant') and not repeated_call_failed) or
                        failure.get('validation_deadline_reached') or
                        failure.get('validation_outcome') == 'call_succeeded_after_deadline'):
                    continue
                streak = min(16, item.get('repeated_reverts', 0)+1)
                seconds = min(self.max_retry_seconds, self.retry_seconds * 2**streak)
                item.update(repeated_reverts=streak, next_probe=now+seconds)
                live = self.edges.get(edge)
                if live is not None:
                    live.update(repeated_reverts=streak, next_probe=now+seconds, probe_pending=False)
                # A generic executor revert is not a fresh transfer sample.
                # Only record() may renew the diagnostic evidence lifetime.
                self.history[edge] = {'repeated_reverts':streak, 'until':now+self.history_seconds,
                                      'next_probe':now+seconds}
                self.history.move_to_end(edge)
                while len(self.history) > self.capacity: self.history.popitem(last=False)
                if self.quarantine is not None:
                    self.quarantine.observe_failure(edge, item['evidence'], route, failure,
                        max(0, now-item.get('sampled_at', now)))
                delay = max(delay, seconds)
            return delay

    def passed(self, route):
        """A successful real executor call outranks helper-based suspicion."""
        with self.lock:
            if self.quarantine is not None: self.quarantine.passed(route_edges(route, self.executor))
            passed_edges = set(route_edges(route, self.executor))
            for key, row in list(self.revalidations.items()):
                if passed_edges.intersection(route_edges(row['route'], self.executor)):
                    del self.revalidations[key]
                    self.prewarm_counts['passed'] += 1
            for key, (sample, _) in list(self.pending.items()):
                if passed_edges & {(sample.token,sample.source,self.executor),
                                   (sample.token,self.executor,sample.destination)}:
                    del self.pending[key]
            # In-flight diagnostics remain transport work, but results started
            # before this pass cannot recreate a prewarm certificate.
            for key, item in list(self.fresh_samples.items()):
                sample_key = key[0]
                sample_edges = {(sample_key[0], sample_key[1], self.executor),
                                (sample_key[0], self.executor, sample_key[2])}
                if passed_edges & sample_edges: del self.fresh_samples[key]
            for key, item in list(self.claimed_samples.items()):
                if passed_edges.intersection(item['edges']): del self.claimed_samples[key]
            for edge in route_edges(route, self.executor):
                self.edges.pop(edge, None)
                self.history.pop(edge, None)
                self.probe_slots.pop(edge, None)
                self.last_passed[edge] = self.clock()
                self.last_passed.move_to_end(edge)
                self.last_passed_wall[edge] = dict(at=self.wall_clock(), until=self.clock()+self.history_seconds)
                self.last_passed_wall.move_to_end(edge)
            while len(self.last_passed) > self.capacity: self.last_passed.popitem(last=False)
            while len(self.last_passed_wall) > self.capacity: self.last_passed_wall.popitem(last=False)

    def offer(self, route, states, amount, settlements):
        """Queue bounded samples at the candidate's modeled intermediate sizes."""
        accepted = 0
        with self.lock:
            output = amount
            for i, pool in enumerate(route.pools[:-1]):
                try: output, _ = states[pool].quote(route.tokens[i], output)
                except Exception: break  # Missing depth is not transfer evidence.
                if output <= 0: break
                if route.tokens[i+1] in settlements: continue
                sample = TransferSample(route.tokens[i+1], pool, route.pools[i+1], output)
                if sample.key in self.in_flight or self.retry_at.get(sample.key, 0) > self.clock(): continue
                if sample.key in self.pending:
                    if not self._prewarm_needed(self.pending[sample.key][0]):
                        self.pending[sample.key] = (sample,self.clock())
                    continue
                if len(self.pending) >= self.queue_limit: break
                self.pending[sample.key] = (sample,self.clock())
                accepted += 1
        return accepted

    def take(self):
        with self.lock:
            self._queue_prewarm()
            def recovery():
                if self.quarantine is None: return None
                item = self.quarantine.take_due(self.in_flight)
                if item is None: return None
                sample = TransferSample(item['token'], item['source'], item['destination'], item['amount'])
                self.pending.pop(sample.key, None)
                self.in_flight[sample.key] = self.clock()
                self.recovery_turn = False
                return sample
            # One shared worker/interval; alternate due recoveries with fresh
            # candidate diagnostics. No candidate is needed to restore a lane.
            if self.recovery_turn:
                sample = recovery()
                if sample is not None: return sample
            while self.pending:
                key, (sample,offered) = self.pending.popitem(last=False)
                now = self.clock()
                if now-offered > self.queue_seconds: continue
                edges = ((sample.token,sample.source,self.executor),(sample.token,self.executor,sample.destination))
                if any(self.last_passed.get(edge,float('-inf'))+self.ttl > now for edge in edges): continue
                if not self._prewarm_needed(sample) and any(self.edges.get(edge,{}).get('until',0) > now for edge in edges): continue
                self.in_flight[key] = now
                self.recovery_turn = True
                return sample
            return recovery()

    def finish_sample(self, sample, succeeded):
        with self.lock:
            self.in_flight.pop(sample.key, None)
            if self.quarantine is not None: self.quarantine.finish_sample(sample.key)
            if succeeded: self.failures.pop(sample.key,None)
            else:
                self.failures[sample.key] = min(5,self.failures.get(sample.key,0)+1)
                self.failures.move_to_end(sample.key)
                while len(self.failures) > self.capacity: self.failures.popitem(last=False)
            delay = self.ttl if succeeded else min(self.ttl,self.retry_seconds*2**(self.failures[sample.key]-1))
            self.retry_at[sample.key] = self.clock()+delay
            self.retry_at.move_to_end(sample.key)
            while len(self.retry_at) > self.capacity: self.retry_at.popitem(last=False)

    def record(self, sample, result):
        check(result.get('failure_stage', 0) in (0,1,2), 'invalid transfer diagnostic stage')
        check(result.get('canonical') is True and
              (result['token'],result['source'],result['destination']) == sample.key and
              result['executor'] == self.executor and result['amount'] == sample.amount,
              'transfer diagnostic identity mismatch')
        with self.lock:
            now = self.clock()
            sample_edges = ((sample.token,sample.source,self.executor),(sample.token,self.executor,sample.destination))
            if not any(self.last_passed.get(edge, float('-inf'))+self.ttl > now or
                       self.last_passed.get(edge, float('-inf')) >= self.in_flight.get(sample.key, now)
                       for edge in sample_edges):
                self.fresh_samples[(sample.key, sample.amount)] = dict(evidence=copy.deepcopy(result),
                    sampled_at=now, until=now+self.ttl)
                self.fresh_samples.move_to_end((sample.key, sample.amount))
                while len(self.fresh_samples) > self.capacity: self.fresh_samples.popitem(last=False)
            if self.quarantine is not None:
                recovered = self.quarantine.observe_sample(result)
                # Exact new helper evidence only releases one persistent trial;
                # stale temporary hints must not obscure that trial slot.
                for edge in recovered:
                    self.edges.pop(edge, None)
                    self.history.pop(edge, None)
                    self.probe_slots.pop(edge, None)
            found = []
            stage = result.get('failure_stage', 0)
            check(stage in (0,1,2), 'invalid transfer diagnostic stage')
            failed_edge = ((sample.token, sample.source, self.executor) if stage == 1 else
                           (sample.token, self.executor, sample.destination) if stage == 2 else None)
            if stage == 1:
                found.append((sample.token, sample.source, self.executor))
            elif result['funded'] < sample.amount:
                found.append((sample.token, sample.source, self.executor))
            if stage == 2 or (stage == 0 and result['funded'] > 0 and result['credited'] < result['funded']):
                found.append((sample.token, self.executor, sample.destination))
            found = [edge for edge in found if self.last_passed.get(edge, float('-inf'))+self.ttl <= now]
            for edge in found:
                previous = self.edges.get(edge, {})
                if previous.get('until', 0) <= now: previous = {}
                history = self.failure_history(edge, now) or {}
                streak = max(previous.get('repeated_reverts', 0), history.get('repeated_reverts', 0))
                retry = min(self.max_retry_seconds, self.retry_seconds * 2**streak)
                self.edges[edge] = {'until':now+self.ttl,'sampled_at':now,'evidence':dict(result),
                    'sample_kind':'call_failure' if edge == failed_edge else 'receipt_shortfall',
                    'next_probe':previous.get('next_probe', history.get('next_probe', now+retry)),
                    'last_route':previous.get('last_route'),
                    'probe_pending':previous.get('probe_pending', False),
                    'repeated_reverts':streak}
                # A helper refresh preserves the due whole-call slot. Only a
                # real claimed retry may consume it, avoiding helper starvation.
                self.edges.move_to_end(edge)
            while len(self.edges) > self.capacity: self.edges.popitem(last=False)
            # Exact receipt in one sample is not a universal compatibility
            # certificate. Recheck later; the real whole-cycle call stays required.
            self.retry_at[sample.key] = now+self.ttl
            self.retry_at.move_to_end(sample.key)
            while len(self.retry_at) > self.capacity: self.retry_at.popitem(last=False)
            receipt_shortfall = ((stage in (0,2) and result['funded'] < sample.amount) or
                                 (stage == 0 and result['funded'] > 0 and result['credited'] < result['funded']))
            self.results[sample.key] = dict(result, receipt_shortfall=receipt_shortfall,
                                           transfer_call_failed=bool(stage))
            self.results.move_to_end(sample.key)
            while len(self.results) > 128: self.results.popitem(last=False)
            return found

    def prewarm_summary(self, include_rows=False):
        with self.lock:
            self._prune_revalidations()
            now = self.clock()
            status = dict(self.prewarm_counts, reservations=len(self.revalidations),
                          ready=sum(bool(row['samples']) and not row['claimed'] and row['due'] <= now and row['dispatches'] < 3
                                    and self._samples_ready(row['samples'], now)
                                    and not self.deferred(row['route'])
                                    for row in self.revalidations.values()))
            if include_rows:
                status['rows'] = [dict(route_key=row['route'].key, amount=row['amount'],
                    due_in_seconds=max(0,row['due']-now), expires_in_seconds=max(0,row['until']-now),
                    dispatches=row['dispatches'], claimed=row['claimed'],
                    lease_id=row.get('lease', {}).get('id'), lease_state=row.get('lease', {}).get('state'),
                    lease_held=bool(row.get('lease', {}).get('held')),
                    last_release=row.get('last_release')) for row in self.revalidations.values()]
            return status

    def summary(self):
        with self.lock:
            now = self.clock()
            return {'prewarm':self.prewarm_summary(include_rows=True),
                    'queued':len(self.pending),
                    'in_flight':len(self.in_flight),
                    'deferred_edges':[{'token':k[0],'sender':k[1],'recipient':k[2],
                                      'retry_after_seconds':max(0,v['next_probe']-now),
                                      'sample_kind':v.get('sample_kind','receipt_shortfall'),
                                      'repeated_reverts':v.get('repeated_reverts',0),'evidence':v['evidence']}
                                     for k,v in self.edges.items() if v['until'] > now],
                    'scheduled_retries':[{'token':k[0],'sender':k[1],'recipient':k[2],
                                          'retry_after_seconds':max(0, max(v['next_probe'],
                                              self.probe_slots.get(k, {}).get('until',0))-now),
                                          'repeated_reverts':v['repeated_reverts']}
                                         for k,v in self.history.items() if v['until'] > now],
                    'recent_samples':list(self.results.values()),
                    **({'quarantine':self.quarantine.summary()} if self.quarantine is not None else {})}

    def run(self, url, chain_id, stop, finish, path, interval=10):
        rpc = None
        try:
            rpc = RPC(url, chain_id, timeout=4, cancel_event=stop)
            artifact = (ROOT/'out/CycleProbe.sol/CycleProbe.json').read_bytes()
            if self.quarantine is not None:
                check(self.quarantine.identity['chain_id'] == chain_id and
                      self.quarantine.identity['helper_sha256'] == hashlib.sha256(artifact).hexdigest(),
                      'transfer quarantine diagnostic identity mismatch')
            runtime = json.loads(artifact)['deployedBytecode']['object']
            verified = False
            while not stop.is_set() and time.monotonic() < finish:
                if not verified:
                    try:
                        rpc.verify_chain()
                        verified = True
                        atomic_json(path, dict(self.summary(), state='idle', at=time.time(),
                            event='worker_verified', sample_completed=False))
                    except Exception as exc:
                        if stop.is_set(): break
                        atomic_json(path, {'state':'retrying','error':safe_error(exc),'at':time.time()})
                        stop.wait(min(10, max(0, finish-time.monotonic())))
                        continue
                sample = self.take()
                if sample is None:
                    stop.wait(.25); continue
                started = time.monotonic()
                succeeded = False
                try:
                    result = measure_sample(rpc, sample, self.executor, self.owner, runtime)
                    if stop.is_set(): break
                    self.record(sample, result)
                    succeeded = True
                    state = {'state':'updated', 'event':'sample_finished', 'sample_completed':True}
                except Exception as exc:
                    if stop.is_set(): break
                    state = {'state':'retrying','error':safe_error(exc)}
                finally:
                    self.finish_sample(sample, succeeded)
                atomic_json(path, dict(self.summary(), at=time.time(), **state))
                stop.wait(min(max(0, finish-time.monotonic()), max(0, interval-(time.monotonic()-started))))
        except Exception as exc:
            # Diagnostics can lose their node without stopping ordinary scans or
            # changing signing rules. Durable isolation survives diagnostics
            # outages and process restarts; only fresh evidence grants a trial.
            if not stop.is_set(): atomic_json(path, {'state':'unavailable','error':safe_error(exc),'at':time.time()})
        finally:
            if rpc is not None: rpc.close()
