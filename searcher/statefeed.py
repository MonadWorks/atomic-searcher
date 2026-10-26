"""Atomic pending-state snapshots: constant RPC count, no per-tx receipt fanout."""
import copy
import hashlib
import json
import time
from dataclasses import dataclass, field
from collections import OrderedDict, deque

from eth_abi import decode

from .core import ROOT, Reject, check, data, number, unhex, batch_request_digest
from .context import simulation_context
from .pools import Pool
from .compatibility import eligible_pools

LENS = "0x000000000000000000000000000000000000c1c3"
STATE = "(address,bool,uint160,int24,uint128,uint24,int16[],uint256[],(int24,uint128,int128)[])"


def decode_packed(raw):
    """Decode exact-width fields; reject truncation, extra data and ambiguity."""
    packed = decode(['bytes'], unhex(raw))[0]
    pos = 0
    def take(width, signed=False):
        nonlocal pos
        check(pos+width <= len(packed), 'truncated packed snapshot')
        value = int.from_bytes(packed[pos:pos+width], 'big', signed=signed)
        pos += width
        return value
    version = take(1)
    check(version in (1, 2), 'unsupported packed snapshot version')
    block, stamp, count = take(32), take(32), take(2)
    check(count <= 300, 'packed snapshot pool limit')
    states = []
    for _ in range(count):
        address, ok = '0x'+take(20).to_bytes(20, 'big').hex(), take(1)
        check(ok in ((0, 1, 2) if version == 2 else (0, 1)), 'invalid packed snapshot status')
        if not ok:
            states.append((address, False, 0, 0, 0, 0, [], [], []))
            continue
        if ok == 2:
            r0, r1, fee = take(14), take(14), take(3)
            check(r0 > 0 and r1 > 0 and fee == 3000, 'invalid packed V2 reserves or fee')
            states.append((address, True, 0, 0, 0, fee, [], [], [], r0, r1))
            continue
        price, tick, liquidity, fee = take(20), take(3, True), take(16), take(3)
        check(price > 0 and fee < 1_000_000 and -887272 <= tick <= 887272, 'invalid packed pool state')
        nwords = take(1)
        check(1 <= nwords <= 5, 'invalid packed bitmap count')
        words, bitmap = [], []
        for _ in range(nwords):
            words.append(take(2, True)); bitmap.append(take(32))
        check(len(set(words)) == len(words), 'duplicate packed bitmap word')
        nticks = take(1)
        check(nticks <= 128, 'invalid packed tick count')
        ticks = [(take(3, True), take(16), take(16, True)) for _ in range(nticks)]
        check(len({t[0] for t in ticks}) == nticks and all(-887272 <= t[0] <= 887272 for t in ticks),
              'invalid packed tick indices')
        states.append((address, True, price, tick, liquidity, fee, words, bitmap, ticks))
    check(pos == len(packed), 'trailing packed snapshot data')
    return states, block, stamp


class DeepReadScheduler:
    """Economic priority with a reserved FIFO lane; scores never authorize trades."""
    def __init__(self):
        self.pending, self.last = OrderedDict(), OrderedDict()
        self.waiting = OrderedDict()
        self.resuming = OrderedDict()
        self.selected_requests = []
        self.selected_deep_pools = set()
        # Diagnostic reservations have their own bounded FIFO. Ordinary depth
        # scores cannot evict an accepted reservation before any actual read.
        self.prewarm = OrderedDict()
        self._prewarm_owned = set()
        self._prewarm_released = set()

    @staticmethod
    def lease_ids(row):
        values = list(row.get('prewarm_lease_ids', ()))
        if row.get('prewarm_lease_id'): values.append(row['prewarm_lease_id'])
        return {value for value in values if isinstance(value, str) and value}

    def prewarm_lease_ids(self):
        return self._prewarm_owned & set().union(*(self.lease_ids(row) for row in
            list(self.prewarm.values())+list(self.pending.values())+
            list(self.resuming.values())+self.selected_requests))

    def prune_prewarm(self, live_ids):
        """Release stale queued leases; current frames remain independently owned."""
        live_ids = set(live_ids)
        self._prewarm_owned.intersection_update(live_ids)
        released, self._prewarm_released = self._prewarm_released, set()
        for group,row in list(self.prewarm.items()):
            ids = self.lease_ids(row)
            released.update(ids-live_ids)
            row['prewarm_lease_ids'] = sorted(ids & live_ids)
            if not row['prewarm_lease_ids']: del self.prewarm[group]
        return sorted(released)

    @classmethod
    def merge_leases(cls, target, incoming):
        ids = cls.lease_ids(target) | cls.lease_ids(incoming)
        if ids: target['prewarm_lease_ids'] = sorted(ids)

    def _offer_prewarm(self, route, now):
        ids = self.lease_ids(route)
        key = tuple(sorted(set(route['pools'])))
        if not ids or not 2 <= len(key) <= 4 or not route.get('route_keys'):
            return set(), ids
        if len(self._prewarm_owned | ids) > 64: return set(), ids
        self._prewarm_owned.update(ids)
        row = self.prewarm.get(key)
        if row is None:
            row = self.prewarm[key] = dict(since=now, score=0, deep=False,
                route_keys=list(route['route_keys']), prewarm_lease_ids=[], continuation_lane='prewarm')
        row['since'] = min(row['since'], route.get('since',now))
        row['deep'] = row['deep'] or bool(route.get('deep',False))
        self.merge_directions(row, route)
        self.merge_leases(row, route)
        return ids, set()

    @staticmethod
    def merge_directions(target, incoming):
        # An old/unscoped request means all directions, so a later directional
        # request must not silently narrow its coverage.
        if target.get('route_keys') is not None and incoming.get('route_keys') is not None:
            target['route_keys'] = sorted(set(target['route_keys']) | set(incoming['route_keys']))
        else:
            target.pop('route_keys', None)

    def add(self, routes, now):
        admitted, rejected = set(), set()
        for route in routes:
            if isinstance(route, dict) and self.lease_ids(route):
                accepted, denied = self._offer_prewarm(route, now)
                admitted.update(accepted); rejected.update(denied)
                continue
            score = max(0, min(10**9, int(route.get('score', 0)))) if isinstance(route, dict) else 0
            deep = bool(route.get('deep', True)) if isinstance(route, dict) else True
            key = tuple(sorted(set(route['pools'] if isinstance(route, dict) else route)))
            if not 2 <= len(key) <= 4: continue
            scope = {'route_keys':list(route['route_keys'])} if isinstance(route, dict) and route.get('route_keys') is not None else {}
            if key in self.resuming:
                self.resuming[key]['deep'] = self.resuming[key]['deep'] or deep
                self.resuming[key]['score'] = max(score, self.resuming[key]['score'])
                self.merge_directions(self.resuming[key], scope)
                continue
            if deep and now-self.last.get(key, -100) < 15: continue
            since = self.waiting.setdefault(key, now)
            self.waiting.move_to_end(key)
            if key in self.pending:
                self.pending[key]['score'] = score
                self.pending[key]['deep'] = self.pending[key].get('deep', True) or deep
                self.merge_directions(self.pending[key], scope)
            else: self.pending[key] = {'score':score, 'since':since, 'deep':deep, **scope}
        while len(self.waiting) > 4096: self.waiting.popitem(last=False)
        if len(self.pending) > 128:
            # Protect the oldest quarter even when higher scores keep arriving.
            # Age survives eviction, so a repeatedly requested cold route can
            # enter this lane instead of losing admission to every new event.
            protected = set(sorted(self.pending,key=lambda k:self.pending[k]['since'])[:32])
            ranked = sorted((k for k in self.pending if k not in protected),
                            key=lambda k: (-self.pending[k]['score'], self.pending[k]['since']))
            keep = protected | set(ranked[:96])
            self.pending = OrderedDict((k,v) for k,v in self.pending.items() if k in keep)
        released, self._prewarm_released = self._prewarm_released, set()
        return dict(accepted=sorted(admitted), rejected=sorted(rejected), released=sorted(released))

    def resume(self, requests, now):
        """Unfinished work is not a completed read subject to retry cooldown."""
        unadmitted = []
        for row in requests:
            if row.get('continuation_lane') == 'prewarm':
                # This work owns a separate accepted lease, not a vacancy in
                # the 1024 ordinary continuation queue. Rejection is explicit
                # so its origin can retry without consuming a quote attempt.
                _, rejected = self._offer_prewarm(row, now)
                self._prewarm_released.update(rejected)
                continue
            key = tuple(sorted(set(row['pools'])))
            if not 2 <= len(key) <= 4: continue
            prior = self.resuming.get(key)
            if prior is None:
                prior = self.pending.get(key, {})
                if prior:
                    prior = dict(prior)
                    prior.setdefault('deep', True)
            since = min(row.get('since', now), prior.get('since', now))
            value = {'since':since, 'score':max(row.get('score', 0), prior.get('score', 0)),
                     'deep':bool(row.get('deep', False) or prior.get('deep', False))}
            if row.get('route_keys') is not None and (not prior or prior.get('route_keys') is not None):
                value['route_keys'] = sorted(set(row['route_keys']) | set(prior.get('route_keys', ())))
            self.merge_leases(value, prior)
            self.merge_leases(value, row)
            if key not in self.resuming and len(self.resuming) >= 1024:
                unadmitted.append(dict(value, pools=key))
                continue
            self.pending.pop(key, None)
            self.resuming[key] = value
        # The caller retains unadmitted work and applies backpressure; capacity
        # is never an acknowledgement that a pending task was completed.
        self.resuming = OrderedDict(sorted(self.resuming.items(), key=lambda x:x[1]['since']))
        return unadmitted

    def take(self, available, now, deferred=None):
        selected = set()
        self.selected_requests, self.selected_deep_pools = [], set()
        for route in list(self.resuming):
            if not set(route).issubset(available):
                self._prewarm_released.update(self.lease_ids(self.resuming.pop(route)))
        for route in list(self.pending):
            if not set(route).issubset(available):
                del self.pending[route]
                self.waiting.pop(route,None)
        for route in list(self.prewarm):
            if not set(route).issubset(available):
                self._prewarm_released.update(self.lease_ids(self.prewarm.pop(route)))
        def eligible(route, row):
            return deferred is None or not deferred(dict(row, pools=route))
        pending = [k for k,v in self.pending.items() if eligible(k,v)]
        oldest = sorted(pending,key=lambda k:self.pending[k]['since'])[:2]
        ranked = sorted((k for k in pending if k not in oldest),
                        key=lambda k: (-self.pending[k]['score'], self.pending[k]['since']))
        resume = [k for k,v in self.resuming.items() if eligible(k,v)]
        warmed = [k for k,v in self.prewarm.items() if eligible(k,v)]
        # One accepted warm group has bounded service even while old depth
        # continuations keep returning. Its <=4 pools leave room for old work;
        # its separate lease owns any return, including when all 1024 ordinary
        # return slots are occupied. All remaining positions keep the old order.
        order = list(dict.fromkeys(warmed[:1]+resume+oldest+ranked))
        # Every selected group may return unfinished. A continuation owns its
        # return slot even after being popped: only capacity free at entry may
        # be reserved by new work, including when other old work is deferred.
        new_slots = max(0, 1024-len(self.resuming))
        for route in order:
            if len(self.selected_requests) >= 12: break
            continuing = route in self.resuming
            independent = not continuing and route in self.prewarm and (route not in self.pending or not new_slots)
            if not continuing and not independent and not new_slots: continue
            if len(selected | set(route)) > 24: continue
            row = (self.resuming.pop(route) if continuing else self.prewarm.pop(route) if independent
                   else self.pending.pop(route))
            row.setdefault('deep', True)
            warmed_row = self.prewarm.pop(route, None)
            if warmed_row is not None:
                row['deep'] = row['deep'] or warmed_row['deep']
                self.merge_directions(row, warmed_row)
                self.merge_leases(row, warmed_row)
            if not continuing and not independent: new_slots -= 1
            self.selected_requests.append(dict(row, pools=route))
            if row['deep']: self.selected_deep_pools.update(route)
            self.waiting.pop(route,None)
            if row['deep']:
                self.last[route] = now
                self.last.move_to_end(route)
            selected.update(route)
        while len(self.last) > 4096: self.last.popitem(last=False)
        return selected


def complete_batch_observation_start(measurement, calls, started_ns, received_ns):
    """Accept only this complete batch's earliest actual send, including retries.

    A reused, unrelated or partially timed result keeps the poll's conservative
    start. A later HTTP fragment never makes an earlier fragment younger.
    """
    if not isinstance(measurement, dict): return None
    if (measurement.get('method') != 'batch' or measurement.get('outcome') != 'ok'
            or measurement.get('timing_complete') is not True
            or measurement.get('methods') != [method for method, _ in calls]
            or measurement.get('batch_request_digest') != batch_request_digest(calls)):
        return None
    batch_started = measurement.get('batch_started_ns')
    first, last = measurement.get('network_started_ns'), measurement.get('network_finished_ns')
    if (not all(type(value) is int for value in (batch_started, first, last))
            or not started_ns <= batch_started <= first <= last <= received_ns):
        return None
    attempts = measurement.get('attempt_timings')
    if (not isinstance(attempts, list) or not attempts
            or type(measurement.get('attempts')) is not int or measurement['attempts'] != len(attempts)):
        return None
    previous, starts, ends = batch_started, [], []
    for attempt in attempts:
        if not isinstance(attempt, dict) or attempt.get('timing_complete') is not True:
            return None
        sent, ended = attempt.get('network_started_ns'), attempt.get('network_finished_ns')
        if (type(sent) is not int or type(ended) is not int
                or not previous <= sent <= ended <= received_ns):
            return None
        starts.append(sent); ends.append(ended); previous = ended
    return first if first == min(starts) and last == max(ends) else None


def observation_start_ns(frame):
    """Live validated snapshots may use their bound send time; legacy stays old.

    Reconstructed/unknown frames cannot acquire provenance from a timestamp in
    serialized data. Explicit historical reads keep their original age too.
    """
    observed = getattr(frame, 'observation_started_ns', None)
    if (isinstance(frame, StateFrame) and frame._observation_validated
            and frame.state_tag == 'pending' and type(observed) is int
            and frame.requested_ns <= observed <= frame.received_ns):
        return observed
    return frame.requested_ns


@dataclass
class StateFrame:
    header: dict
    latest: dict
    snapshot_pools: list
    requested_ns: int
    received_ns: int
    received_utc: float
    context_block: int
    context_time: int
    preconfirmed: object = None  # pending tag alone is not timing proof
    transport: dict = field(default_factory=dict)
    state_tag: str = "pending"
    observation_started_ns: int | None = None
    _observation_validated: bool = field(default=False, init=False, repr=False)

    @property
    def identity(self):
        return "0x"+hashlib.sha256(json.dumps([self.header, self.snapshot_pools], sort_keys=True).encode()).hexdigest()

    def payload(self):
        return {"header": self.header, "snapshot_pools": self.snapshot_pools,
                "context_block": self.context_block, "context_time": self.context_time,
                "observation_id": self.identity, "state_tag": self.state_tag, "preconfirmed": None,
                "atomic_state_read": True, "transport": self.transport, "receipts": [],
                "observation_started_ns": self.observation_started_ns,
                "observation_freshness_origin": ('atomic_snapshot_network_send' if
                    self._observation_validated and self.state_tag == 'pending' else 'poll_start')}


class StateFeed:
    # Bound one EVM observation, not the economic universe. A route's entire
    # pool set must fit in one call; no prices are combined across slices.
    SNAPSHOT_POOL_LIMIT = 24

    def __init__(self, rpc, config, saved, group_allowed=None):
        self.rpc, self.config = rpc, config
        self.group_allowed = group_allowed
        self.pools = eligible_pools(config, [Pool.from_json(p) for p in saved["pools"]])
        self.runtime = json.loads((ROOT/"out/StateLens.sol/StateLens.json").read_text())["deployedBytecode"]["object"]
        self.anchor = None
        self.pending = None
        self.clock_requested_ns = self.clock_received_ns = 0
        self.pages = None
        self.page_deep = set()
        self.page_groups = []
        self.page_requests = []
        self._slices = deque()
        self._logical_page = self._slice_number = self._slice_count = 0
        self._logical_groups = 0
        self._slice_priority = []
        self._slice_coactive = []
        if config['policy'].get('cycle_pages'):
            from .universe import CyclePages
            self.pages = CyclePages([Pool.from_json(p) for p in saved.get('known_pools', saved['pools'])],
                                   [t['address'] for t in config['settlement_tokens']],
                                   config['policy']['max_hops'], saved.get('last_seen'), saved.get('cycle_groups'),
                                   excluded_tokens=config.get('unsupported_transfer_tokens', {}))

    @property
    def has_pending_slices(self):
        """Acknowledge this slice before continuing its still-owned page tail."""
        return bool(self._slices)

    def prewarm_lease_ids(self):
        return set().union(*(DeepReadScheduler.lease_ids(row) for row in
            self.page_requests+[row for part in self._slices for row in part['requests']]))

    def _slice_page(self, pools, groups, requests, deep_pools):
        """Keep complete groups and exact request scopes in a bounded FIFO.

        The caller advances the economic cursor once. A failed/read-overwritten
        slice remains current; only advance_page=True may pop the next slice.
        Consequently a universe refresh must wait for this queue to drain.
        """
        by_address = {pool.address:pool for pool in pools}
        groups = list(dict.fromkeys(tuple(sorted(set(group))) for group in groups))
        check(all(2 <= len(group) <= 4 and set(group) <= by_address.keys() for group in groups),
              'invalid snapshot route group')
        # A fresh diagnostic/depth request should not wait behind an entire
        # cold page. The retained FIFO tail still guarantees ordinary coverage
        # before the next economic page can admit another priority batch.
        priority = list(dict.fromkeys(tuple(sorted(set(group))) for group in
            [row['pools'] for row in requests]+list(self.pages.selected_priority)))
        rank = {group:i for i,group in enumerate(priority)}
        groups.sort(key=lambda group:rank.get(group,len(rank)))
        limit = self.SNAPSHOT_POOL_LIMIT
        slices, addresses, selected = [], set(), []
        def append_slice():
            if addresses:
                selected_set = set(selected)
                slices.append(dict(pools=[by_address[a] for a in sorted(addresses)], groups=list(selected),
                    requests=[copy.deepcopy(row) for row in requests
                              if tuple(sorted(set(row['pools']))) in selected_set],
                    deep=set(deep_pools) & addresses,
                    priority=[group for group in self.pages.selected_priority if tuple(group) in selected_set],
                    coactive=[group for group in self.pages.selected_coactive if tuple(group) in selected_set]))
        for group in groups:
            if addresses and len(addresses | set(group)) > limit:
                append_slice(); addresses, selected = set(), []
            addresses.update(group); selected.append(group)
        # Preserve explicitly required standalone pools too; they do not create
        # new route groups or authorize combining independent observations.
        represented = {a for group in groups for a in group}
        for address in sorted(by_address.keys()-represented):
            if len(addresses) >= limit:
                append_slice(); addresses, selected = set(), []
            addresses.add(address)
        append_slice()
        if not slices:
            slices = [dict(pools=[], groups=[], requests=[], deep=set(), priority=[], coactive=[])]
        self._slices = deque(slices)
        self._logical_page += 1
        self._slice_number, self._slice_count = 0, len(slices)
        self._logical_groups = len(groups)

    def _advance_slice(self):
        current = self._slices.popleft()
        self.pools, self.page_groups = current['pools'], current['groups']
        self.page_requests, self.page_deep = current['requests'], current['deep']
        self._slice_priority, self._slice_coactive = current['priority'], current['coactive']
        self._slice_number += 1

    def poll(self, block="pending", deep_pools=(), advance_page=True, retry_shallow=False, requested_groups=None):
        p = self.config["policy"]
        check(block == "pending" or isinstance(block, int), "snapshot requires pending or an explicit block")
        tag = hex(block) if isinstance(block, int) else block
        started = time.monotonic_ns()
        self.last_transport = {'rpc_batches': []}
        clock_cache = getattr(self.rpc, 'clock_hints', None)
        clock_key = (getattr(self.rpc, 'url', None), getattr(self.rpc, 'chain_id', None))
        if None in clock_key: clock_cache = None
        borrowed = None
        batch_started_ns = batch_received_ns = 0
        shared_clock_used = False
        shared_clock_hint = None
        admission_clock_updated = False
        observation_started_ns = None
        def timed_batch(calls, role, attempt, prepare=None):
            nonlocal batch_started_ns, batch_received_ns, observation_started_ns
            before = batch_started_ns = time.monotonic_ns()
            measurements = getattr(self.rpc, 'measurements', ())
            prior = measurements[-1] if measurements else None
            ok = False
            try:
                if prepare is None:
                    value = self.rpc.batch(calls)
                else:
                    value, calls = self.rpc.batch_prepared(calls, prepare)
                batch_received_ns = time.monotonic_ns()
                if role == 'atomic_snapshot':
                    measurements = getattr(self.rpc, 'measurements', ())
                    current = measurements[-1] if measurements and measurements[-1] is not prior else None
                    observation_started_ns = complete_batch_observation_start(current, calls, before, batch_received_ns)
                ok = True
                return value
            finally:
                measurements = getattr(self.rpc, 'measurements', ())
                measurement = (dict(measurements[-1]) if measurements and
                               measurements[-1] is not prior else {})
                self.last_transport['rpc_batches'].append(dict(measurement, role=role,
                    clock_attempt=attempt+1, succeeded=ok,
                    batch_wall_ms=(time.monotonic_ns()-before)/1e6))
        deep_pools = set(deep_pools)
        check(len(deep_pools) <= 24, "deep snapshot pool cap exceeded")
        if self.pages is not None:
            if advance_page:
                if self.has_pending_slices:
                    check(not deep_pools and not requested_groups, 'new depth work before page tail completed')
                else:
                    requests = list(requested_groups or [])
                    required = deep_pools | {a for row in requests for a in row['pools']}
                    check(len(required) <= 24 and all(2 <= len(set(row['pools'])) <= 4 for row in requests),
                          'invalid requested route groups')
                    pools = self.pages.take(p['max_pools'], required, p.get('route_groups_per_page', 96),
                                            group_allowed=self.group_allowed)
                    groups = list(self.pages.selected_groups)
                    if requested_groups is not None:
                        groups = sorted(set(map(tuple, groups)) |
                                        {tuple(sorted(row['pools'])) for row in requests})
                    elif deep_pools:
                        from .routes import RouteIndex
                        deep_index = RouteIndex([self.pages.pools[a] for a in deep_pools],
                                               [t['address'] for t in self.config['settlement_tokens']], p['max_hops'])
                        groups += list({tuple(sorted(r.pools)) for r in deep_index.routes.values()})
                    self._slice_page(pools, groups, requests, deep_pools)
                self._advance_slice()
            if retry_shallow: self.page_deep = set()
            deep_pools = self.page_deep
        addresses, spacings = [x.address for x in self.pools], [x.spacing for x in self.pools]
        depth = p.get("snapshot_ticks_per_side", 4)
        mixed = any(x.kind == 'v2' for x in self.pools)
        packed = mixed or p.get('snapshot_encoding', 'abi') == 'packed-v1'
        if packed:
            calldata = data(('readMixedPacked' if mixed else 'readPacked')+'(address[],int24[],uint8,uint8[])', ['address[]', 'int24[]', 'uint8', 'uint8[]'],
                            [addresses, spacings, p['bitmap_radius'],
                             [p.get('deep_ticks_per_side', 16) if a in deep_pools else depth for a in addresses]])
        elif deep_pools:
            calldata = data("readWithDepths(address[],int24[],uint8,uint8[])", ["address[]", "int24[]", "uint8", "uint8[]"],
                            [addresses, spacings, p["bitmap_radius"], [p.get('deep_ticks_per_side', 16) if a in deep_pools else depth for a in addresses]])
        else:
            calldata = data("read(address[],int24[],uint8,uint8)", ["address[]", "int24[]", "uint8", "uint8"],
                            [addresses, spacings, p["bitmap_radius"], depth])
        call = {"to": LENS, "data": calldata, "gas": hex(p.get("snapshot_read_gas", 90_000_000))}
        for attempt in range(2):
            params = [call, tag, {LENS: {"code": self.runtime}}]
            if tag == 'pending':
                if attempt == 0 and clock_cache is not None:
                    hint = clock_cache.get(*clock_key)
                    if hint is not None and hint['requested_ns'] > self.clock_requested_ns:
                        self.anchor, self.pending = hint['anchor'], hint['pending']
                        self.clock_requested_ns, self.clock_received_ns = hint['requested_ns'], hint['received_ns']
                        borrowed, shared_clock_used = hint, True
                        shared_clock_hint = {'anchor_block':number(hint['anchor']['number']),
                                             'age_ms':(clock_cache.clock()-hint['received_ns'])/1e6}
                    elif not 0 <= time.monotonic_ns()-self.clock_received_ns <= clock_cache.ttl_ns:
                        self.anchor = self.pending = None
                if self.anchor is None or self.pending is None:
                    self.anchor, self.pending = timed_batch([
                        ('eth_getBlockByNumber', ['latest', False]),
                        ('eth_getBlockByNumber', ['pending', False])], 'clock_bootstrap', attempt)
                    selected_clock_requested_ns = batch_started_ns
                else:
                    selected_clock_requested_ns = self.clock_requested_ns
                anchor, pending = self.anchor, self.pending
                check(anchor is not None and pending is not None, 'snapshot clock unavailable')
                # A cached clock can become unusable after inconsistent node
                # responses. Clear it before rejecting so the next poll recovers.
                self.anchor = self.pending = None
                try:
                    expected = simulation_context(anchor, pending)
                except Reject:
                    if clock_cache is not None: clock_cache.discard(*clock_key, borrowed)
                    raise
                params.append(expected)
            calls = [
                ('eth_call', params),
                ('eth_getBlockByNumber', ['latest' if tag == 'pending' else tag, False])]
            if tag == 'pending': calls.append(('eth_getBlockByNumber', ['pending', False]))
            prepare = None
            if tag == 'pending' and clock_cache is not None and callable(getattr(self.rpc, 'batch_prepared', None)):
                def prepare_snapshot():
                    nonlocal anchor, pending, expected, borrowed, shared_clock_used, shared_clock_hint
                    nonlocal admission_clock_updated, selected_clock_requested_ns
                    hint = clock_cache.get(*clock_key)
                    if hint is not None and hint['requested_ns'] > selected_clock_requested_ns:
                        # Admission may have waited across a block boundary.
                        # Borrow headers only, then still verify this complete
                        # atomic read against the heads returned in its batch.
                        borrowed = hint
                        try:
                            following = simulation_context(hint['anchor'], hint['pending'])
                        except Reject:
                            clock_cache.discard(*clock_key, borrowed)
                            raise
                        anchor, pending, expected = hint['anchor'], hint['pending'], following
                        selected_clock_requested_ns = hint['requested_ns']
                        self.clock_requested_ns, self.clock_received_ns = hint['requested_ns'], hint['received_ns']
                        shared_clock_used = admission_clock_updated = True
                        shared_clock_hint = {'anchor_block':number(anchor['number']),
                                             'age_ms':(clock_cache.clock()-hint['received_ns'])/1e6}
                    return [('eth_call', [call, tag, {LENS:{'code':self.runtime}}, expected]),
                            ('eth_getBlockByNumber', ['latest', False]),
                            ('eth_getBlockByNumber', ['pending', False])]
                prepare = prepare_snapshot
            values = timed_batch(calls, 'atomic_snapshot', attempt, prepare)
            raw, head = values[:2]
            check(head is not None, 'snapshot anchor unavailable')
            if tag != 'pending': break
            self.anchor, self.pending = head, values[2]
            check(self.pending is not None, 'snapshot pending clock unavailable')
            try:
                following = simulation_context(head, self.pending)
            except Reject:
                self.anchor = self.pending = None
                if clock_cache is not None: clock_cache.discard(*clock_key, borrowed)
                if attempt == 0: continue
                raise
            self.clock_requested_ns, self.clock_received_ns = batch_started_ns, batch_received_ns
            if clock_cache is not None:
                clock_cache.publish(*clock_key, head, self.pending,
                                    requested_ns=batch_started_ns, received_ns=batch_received_ns)
            if (head['hash'] == anchor['hash'] and number(head['number']) == number(anchor['number'])
                    and following == expected): break
            # A boundary or reorg invalidates the clock used for this quote.
            # Refresh once immediately; never publish it with a newer header.
            check(attempt == 0, 'snapshot anchor changed during read')
        received, utc = time.monotonic_ns(), time.time()
        states, context_block, context_time = decode_packed(raw) if packed else decode([STATE+"[]", "uint256", "uint256"], unhex(raw))
        if tag == 'pending':
            check(context_block == number(expected['number']) and context_time == number(expected['time']),
                  'snapshot execution clock mismatch')
        check(len(states) == len(self.pools), "incomplete atomic snapshot")
        pools = []
        for original, row in zip(self.pools, states):
            address, ok, price, tick, liquidity, fee, words, bitmap, ticks = row[:9]
            check(not ok or (len(row) == 11) == (original.kind == 'v2'), 'snapshot protocol layout mismatch')
            check(address.lower() == original.address, "snapshot pool order mismatch")
            pool = copy.copy(original)
            pool.valid = ok
            pool.sqrt_price, pool.tick, pool.liquidity, pool.fee = price, tick, liquidity, fee
            pool.reserve0, pool.reserve1 = row[9:] if ok and original.kind == 'v2' else (0, 0)
            pool.bitmap, pool.ticks = dict(zip(words, bitmap)), {t: (gross, net) for t, gross, net in ticks}
            pools.append(pool.json())
        measurements = getattr(self.rpc, "measurements", ())
        transport = dict(measurements[-1]) if measurements else {}
        batches = self.last_transport['rpc_batches']
        transport.update(rpc_batches=batches, clock_attempts=attempt+1,
                         clock_bootstraps=sum(row['role']=='clock_bootstrap' for row in batches),
                         shared_clock_used=shared_clock_used, shared_clock_hint=shared_clock_hint,
                         admission_clock_updated=admission_clock_updated,
                         poll_rpc_elapsed_ms=sum(row['batch_wall_ms'] for row in batches))
        for field in ('minimum_interval_wait_ms', 'public_limiter_wait_ms', 'network_headers_ms',
                      'network_body_ms', 'network_total_ms', 'retry_wait_ms'):
            values = [row.get(field) for row in batches]
            transport['poll_'+field] = sum(values) if all(v is not None for v in values) else None
        transport["deep_pools"] = sorted(deep_pools)
        transport['snapshot_encoding'] = 'packed-v2' if mixed else ('packed-v1' if packed else 'abi')
        transport['decode_ms'] = (time.monotonic_ns()-received)/1e6
        if self.pages is not None:
            transport['cycle_pages'] = {'known_pools':len(self.pages.pools), 'cyclic_pools':len(self.pages.covered),
                                       'groups':len(self.pages.groups), 'cursor':self.pages.cursor,
                                       'selection':copy.deepcopy(self.pages.selection_summary)}
            transport['route_groups'] = self.page_groups
            transport['activity_priority_groups'] = self._slice_priority
            transport['coactive_priority_groups'] = self._slice_coactive
            transport['requested_groups'] = self.page_requests
            transport['page_slice'] = dict(logical_page=self._logical_page, index=self._slice_number,
                count=self._slice_count, pool_limit=self.SNAPSHOT_POOL_LIMIT, pools=len(self.pools),
                groups=len(self.page_groups), logical_groups=self._logical_groups,
                remaining_slices=len(self._slices), remaining_groups=sum(len(row['groups']) for row in self._slices),
                remaining_requested_groups=sum(len(row['requests']) for row in self._slices))
        if tag == 'pending': transport['execution_context'] = expected
        # The previous attempt was discarded in full. Only after every clock,
        # hash, protocol and pool-layout check may this last attempt set age.
        if tag != 'pending': observation_started_ns = None
        frame = StateFrame(head, head, pools, started, received, utc, context_block, context_time,
                           transport=transport, state_tag=tag, observation_started_ns=observation_started_ns)
        frame._observation_validated = observation_started_ns is not None
        transport['observation_freshness_origin'] = ('atomic_snapshot_network_send' if
                                                    frame._observation_validated else 'poll_start')
        transport['observation_age_at_receipt_ms'] = (received-observation_start_ns(frame))/1e6
        return frame
