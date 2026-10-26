"""Bounded recent discovery on the existing private node; history stays public.

The journals count local request-unit estimates, not provider account quota.
Endpoints stay in memory. A failed private round is discarded in its entirety.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import math
import os
from pathlib import Path
import threading
import time
from urllib.parse import urlsplit, urlunsplit

from .core import ROOT, RPC, RPCUnavailable, Reject, atomic_json, check, number
from .stream_usage import RequestBudget

_LOCKS, _LOCKS_GUARD = {}, threading.Lock()


class DiscoveryBudgetUnavailable(RPCUnavailable):
    pass


class RecentDiscoveryExpired(RPCUnavailable):
    pass


def split_settings(config, *, websocket=None):
    """Return an in-memory endpoint and fixed sublimits only when enabled."""
    if os.environ.get('BASE_DISCOVERY_RPC_URL'):
        return None
    if websocket is None:
        from .activity import websocket_url
        websocket = websocket_url
    try:
        value = websocket()
        if not value: return None
        parsed = urlsplit(value)
        host = parsed.hostname or ''
        if (parsed.scheme != 'wss' or parsed.username or parsed.password or parsed.fragment
                or not (host.endswith('.chainstack.com') or host.endswith('.p2pify.com'))):
            return None
        # Accessing .port also rejects malformed URL ports without serializing it.
        parsed.port
        total = config['policy'].get('activity_ws_daily_request_limit', 80_000)
        check(type(total) is int and 100 <= total <= 80_000, 'invalid discovery usage allocation')
        half = total // 2
        return {'url':urlunsplit(('https', parsed.netloc, parsed.path, parsed.query, '')),
                'total':total, 'ws':half, 'http':half}
    except (ValueError, TypeError, Reject):
        return None


class DiscoveryHTTPBudget:
    """Process/file-serialized pre-send debits with persistent pacing credit.

    Each instance reloads the journal under both locks. No instance-local token
    balance can multiply the allowance across workers or restarts.
    """
    def __init__(self, path, ws_path, total, limit, *, clock=time.time, monotonic=time.monotonic):
        self.path, self.ws_path = Path(path), Path(ws_path)
        self.total, self.limit = total, limit
        check(type(total) is int and type(limit) is int and 0 < limit <= total <= 80_000,
              'invalid discovery HTTP budget')
        self.clock, self.monotonic = clock, monotonic
        self.capacity = min(128, limit)
        with _LOCKS_GUARD:
            self.lock = _LOCKS.setdefault(str(self.path.resolve()), threading.Lock())

    @staticmethod
    def _usage(path):
        if not path.exists(): return None
        state = json.loads(path.read_text())
        check(isinstance(state, dict) and isinstance(state.get('day'), str), 'invalid discovery usage journal')
        datetime.strptime(state['day'], '%Y-%m-%d')
        check(type(state.get('units')) is int and state['units'] >= 0, 'invalid discovery usage units')
        return state

    @staticmethod
    def _cancel(stop):
        check(stop is None or not stop.is_set(), 'RPC cancelled')

    @contextmanager
    def _locked(self, stop=None, deadline=None):
        self._cancel(stop)
        while not self.lock.acquire(timeout=.05):
            self._cancel(stop)
            if deadline is not None and self.monotonic() >= deadline:
                raise DiscoveryBudgetUnavailable('discovery HTTP admission deadline exceeded')
        handle = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            handle = open(self.path.with_suffix(self.path.suffix+'.lock'), 'a')
            while True:
                self._cancel(stop)
                if deadline is not None and self.monotonic() >= deadline:
                    raise DiscoveryBudgetUnavailable('discovery HTTP admission deadline exceeded')
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if stop is None: time.sleep(.025)
                    else: stop.wait(.025)
            yield
        finally:
            if handle is not None:
                fcntl.flock(handle, fcntl.LOCK_UN)
                handle.close()
            self.lock.release()

    def _load(self):
        now = self.clock()
        day = datetime.fromtimestamp(now, timezone.utc).strftime('%Y-%m-%d')
        clock_day = day
        ws = self._usage(self.ws_path)
        state = self._usage(self.path)
        # A backward clock never grants another day's allowance. A future-day
        # WS journal is also a durable high-water mark for the shared allocation.
        day = max(day, (state or {}).get('day', day), (ws or {}).get('day', day))
        if state is None:
            state = {'day':day, 'units':0}
        elif day > state['day']:
            if day > clock_day:
                # Only another journal advanced before a clock rollback. Carry
                # our existing debit instead of granting a new allowance now.
                state['day'] = day
            else:
                state = {'day':day, 'units':0}
        ws_used = ws['units'] if ws is not None and ws['day'] == day else 0
        remaining = max(0, min(self.limit-state['units'], self.total-ws_used-state['units']))
        start = datetime.strptime(day, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp()
        state.setdefault('pace_credit', float(min(self.capacity, remaining)))
        state.setdefault('pace_at', max(now, start))
        if day > clock_day: state['pace_at'] = max(state['pace_at'], start)
        state.setdefault('round_units', min(8, self.capacity))
        check(all(type(state[k]) in (int, float) and math.isfinite(state[k])
                  for k in ('pace_credit', 'pace_at')), 'invalid discovery HTTP pacing')
        check(0 <= state['pace_credit'] <= self.capacity, 'invalid discovery HTTP pacing credit')
        check(type(state['round_units']) is int and 1 <= state['round_units'] <= self.capacity,
              'invalid discovery HTTP round estimate')
        seconds = max(1, start+86400-now)
        rate = .9*remaining/seconds
        state['pace_credit'] = min(self.capacity, remaining,
            state['pace_credit']+max(0, now-state['pace_at'])*rate)
        state['pace_at'] = max(now, state['pace_at'])
        return state, remaining, rate, ws_used

    def ready(self, stop=None):
        with self._locked(stop, self.monotonic()+2):
            state, remaining, rate, ws_used = self._load()
            atomic_json(self.path, state)
            return remaining >= state['round_units'] and state['pace_credit'] >= state['round_units']

    def acquire(self, units, stop=None, *, deadline=None):
        check(type(units) is int and 0 < units <= self.capacity, 'invalid discovery HTTP debit')
        while True:
            self._cancel(stop)
            with self._locked(stop, deadline):
                state, remaining, rate, ws_used = self._load()
                if remaining < units:
                    atomic_json(self.path, state)
                    raise DiscoveryBudgetUnavailable('discovery HTTP daily allocation exhausted')
                if state['pace_credit'] >= units:
                    state['pace_credit'] -= units
                    state['units'] += units
                    atomic_json(self.path, state)
                    return
                delay = (units-state['pace_credit'])/max(rate, 1e-12)
                atomic_json(self.path, state)
            if deadline is not None and self.monotonic()+delay > deadline:
                raise DiscoveryBudgetUnavailable('discovery HTTP pacing allocation unavailable')
            self._cancel(stop)
            if stop is None: time.sleep(min(.05, delay))
            else: stop.wait(min(.05, delay))

    def round_finished(self, units, *, complete, stop=None):
        with self._locked(stop, self.monotonic()+2):
            state, _, _, _ = self._load()
            estimate = max(1, units) if complete else max(state['round_units'], units+1)
            state['round_units'] = min(self.capacity, estimate)
            atomic_json(self.path, state)

    def details(self, stop=None):
        with self._locked(stop, self.monotonic()+2):
            state, remaining, rate, ws_used = self._load()
            return {'budget_kind':'local_request_units', 'local_units':state['units'],
                    'local_daily_limit':self.limit, 'combined_daily_limit':self.total,
                    'ws_units_same_day':ws_used, 'remaining_units':remaining,
                    'usage_day_utc':state['day'], 'pacing_credit':state['pace_credit'],
                    'sustainable_units_per_second':rate, 'round_admission_units':state['round_units'],
                    'account_quota_known':False}


class DiscoveryStreamBudget(RequestBudget):
    """Preserve today's shared allocation even when private HTTP is disabled.

    The legacy WebSocket journal remains authoritative for its received units.
    On every refresh/day change, today's HTTP debit caps the WS allowance.
    """
    def __init__(self, path, daily_limit, http_path, *, split_active, clock=time.time, pacing_mode="daily"):
        self.total, self.http_path, self.split_active = daily_limit, Path(http_path), split_active
        super().__init__(path, daily_limit, clock=clock, pacing_mode=pacing_mode)

    def _refresh(self):
        super()._refresh()
        usage = DiscoveryHTTPBudget._usage(self.http_path)
        if usage is not None and usage['day'] > self.state['day']:
            # Clock rollback: preserve both journals' debits conservatively.
            self.state['day'] = usage['day']
            start = datetime.strptime(usage['day'], '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp()
            if 'pace_at' in self.state: self.state['pace_at'] = max(self.state['pace_at'], start)
        used = usage['units'] if usage is not None and usage['day'] == self.state['day'] else 0
        allocation = self.total//2 if self.split_active or used else self.total
        self.limit = max(0, min(allocation, self.total-used))

    def details(self):
        result = super().details()
        result.update(combined_daily_limit=self.total, split_discovery_enabled=self.split_active)
        return result


class _DeadlineStop:
    """Event interface for existing HTTP/limiter cancellation; no timer thread."""
    def __init__(self, stop, deadline):
        self.stop, self.deadline = stop, deadline

    def is_set(self):
        return self.stop.is_set() or time.monotonic() >= self.deadline()

    def wait(self, timeout=None):
        remaining = max(0, self.deadline()-time.monotonic())
        self.stop.wait(remaining if timeout is None else min(max(0, timeout), remaining))
        return self.is_set()


class RecentDiscoveryRPC(RPC):
    """Same-provider recent reads with an admission debit for every attempt."""
    MAX_ROUND_SECONDS = 60
    discovery_log_chunk_size = 32

    def __init__(self, *args, budget, **kwargs):
        super().__init__(*args, **kwargs)
        self.discovery_budget = budget
        self.anchor_height = None
        self.round_deadline = 0
        self.round_units = 0

    def begin_round(self):
        self.anchor_height = None
        self.round_deadline = time.monotonic()+self.MAX_ROUND_SECONDS
        self.round_units = 0

    def _units(self, payload):
        if time.monotonic() >= self.round_deadline:
            raise RecentDiscoveryExpired('private discovery recent anchor expired')
        rows = payload if isinstance(payload, list) else [payload]
        cost = 0
        for row in rows:
            method, params = row.get('method'), row.get('params', [])
            check(method in RPC.METHODS, 'discovery transport is read only')
            tags = []
            if method == 'eth_getLogs':
                tags = [params[0].get('fromBlock'), params[0].get('toBlock')]
            elif method in ('eth_getBlockByNumber', 'eth_getBlockReceipts'):
                tags = [params[0]]
            elif method in ('eth_call', 'eth_getCode', 'eth_getBalance', 'eth_getTransactionCount', 'eth_getStorageAt'):
                tags = [params[2] if method == 'eth_getStorageAt' else params[1]] if len(params) >= (3 if method == 'eth_getStorageAt' else 2) else []
            elif method == 'eth_chainId':
                cost += 1
                continue
            if tags and all(tag in ('latest', 'pending') for tag in tags):
                cost += 1
                continue
            if tags and self.anchor_height is not None and all(isinstance(tag, str) and tag.startswith('0x') for tag in tags):
                heights = [number(tag) for tag in tags]
                # At most 64 already-sealed blocks plus a 60-second round at
                # Base's two-second cadence remains below the 127-block tier.
                if not all(0 <= self.anchor_height-h <= 64 for h in heights):
                    raise RecentDiscoveryExpired('private discovery block outside recent window')
                cost += 1
            else:
                # Unclassified/unknown-age RPCs never receive the recent 1-RU assumption.
                cost += 2
        return cost

    def _safe_read(self, operation):
        try:
            return operation()
        except (DiscoveryBudgetUnavailable, RecentDiscoveryExpired):
            raise
        except Exception as exc:
            if self.cancel_event is not None and self.cancel_event.is_set():
                raise Reject('RPC cancelled') from None
            # Provider text can repeat a credential outside a URL. Preserve only
            # its exception category, never that text, in topology diagnostics.
            raise RPCUnavailable('private discovery read failed: '+type(exc).__name__) from None

    def request(self, method, params):
        return self._safe_read(lambda: super(RecentDiscoveryRPC, self).request(method, params))

    def batch(self, calls):
        return self._safe_read(lambda: super(RecentDiscoveryRPC, self).batch(calls))

    def batch_prepared(self, calls, prepare):
        return self._safe_read(lambda: super(RecentDiscoveryRPC, self).batch_prepared(calls, prepare))

    def header(self, tag='latest', full=False):
        header = super().header(tag, full)
        if tag == 'latest':
            stamp = number(header.get('timestamp', 0))
            if not 0 <= time.time()-stamp <= 10:
                raise RecentDiscoveryExpired('private discovery latest header is stale')
            self.anchor_height = number(header['number'])
            # Enforce absolute sealed-header age too, not only local request age.
            self.round_deadline = min(self.round_deadline, time.monotonic()+max(0, 60-(time.time()-stamp)))
        return header

    def post(self, payload, *, prepare=None):
        units = self._units(payload)
        started = time.monotonic()
        try:
            self.discovery_budget.acquire(units, self.cancel_event,
                deadline=min(self.round_deadline, started+2))
        except Exception:
            self._last_post_timing = {'minimum_interval_wait_ms':0.0, 'public_limiter_wait_ms':0.0,
                'network_headers_ms':0.0, 'network_body_ms':0.0, 'network_total_ms':0.0,
                'network_started_ns':None, 'network_finished_ns':None, 'timing_complete':True,
                'post_total_ms':(time.monotonic()-started)*1000,
                'discovery_budget_wait_ms':(time.monotonic()-started)*1000,
                'discovery_rpc_role':'private_recent', 'discovery_units_debited':0}
            raise
        waited = (time.monotonic()-started)*1000
        self.round_units += units
        def admitted_payload():
            actual = prepare() if prepare is not None else payload
            check(self._units(actual) <= units, 'prepared discovery debit changed')
            return actual
        try:
            return super().post(payload, prepare=admitted_payload)
        finally:
            timing = self._last_post_timing
            timing.update(discovery_budget_wait_ms=waited, discovery_rpc_role='private_recent',
                          discovery_units_debited=units)
            timing['post_total_ms'] += waited


class DiscoveryReads:
    """One entry point for startup/recurring discovery, with whole-round fallback."""
    def __init__(self, config, base_rpc, stop, *, root=ROOT, websocket=None, rpc_factory=RPC,
                 private_factory=RecentDiscoveryRPC, prepare_fn=None):
        self.config, self.stop = config, stop
        self.prepare_fn = prepare_fn
        endpoint = os.environ.get('BASE_DISCOVERY_RPC_URL') or config['policy'].get('discovery_rpc_url') or base_rpc.url
        self.public = rpc_factory(endpoint, base_rpc.chain_id, timeout=10, cancel_event=stop)
        self.public.minimum_request_interval = config['policy'].get('discovery_request_interval_seconds', 0)
        self.private, self.budget = None, None
        self.retry_at = 0
        self.last_private_error = None
        self.private_factory = private_factory
        self.chain_id = base_rpc.chain_id
        self.settings = split_settings(config, websocket=websocket)
        if self.settings:
            self.budget = DiscoveryHTTPBudget(Path(root)/'evidence/discovery-http-requests.json',
                Path(root)/'evidence/activity-stream-requests.json', self.settings['total'], self.settings['http'])
            self._connect_private()

    def _connect_private(self):
        try:
            self.private = self.private_factory(self.settings['url'], self.chain_id, timeout=10,
                cancel_event=self.stop, budget=self.budget)
            self.private.minimum_request_interval = self.public.minimum_request_interval
        except Exception as exc:
            self.private = None
            self.last_private_error = type(exc).__name__
            self.retry_at = time.monotonic()+30

    @contextmanager
    def _private_deadline(self):
        event = _DeadlineStop(self.stop, lambda:self.private.round_deadline)
        prior = [(rpc, rpc.cancel_event) for rpc in (self.private, self.public)]
        try:
            for rpc, _ in prior: rpc.cancel_event = event
            yield event
        finally:
            for rpc, original in prior: rpc.cancel_event = original

    def prepare(self, recent_blocks=6, previous=None):
        from .engine import prepare
        prepare = self.prepare_fn or prepare
        if self.settings is None:
            return prepare(self.public, self.config, recent_blocks, previous=previous)
        started = time.monotonic()
        if self.settings and self.private is None and time.monotonic() >= self.retry_at:
            self._connect_private()
        use_private = self.private is not None and time.monotonic() >= self.retry_at
        reason = self.last_private_error if not use_private else None
        attempt = None
        if use_private:
            try:
                use_private = self.budget.ready(self.stop)
                if not use_private: reason = 'local_budget_pacing'
            except Exception as exc:
                if self.stop.is_set(): raise
                reason = type(exc).__name__
                use_private = False
        if use_private:
            self.private.begin_round()
            private_started = time.monotonic()
            try:
                with self._private_deadline() as deadline_stop:
                    result = prepare(self.private, self.config, recent_blocks, previous=previous, history_rpc=self.public)
                    if deadline_stop.is_set():
                        raise RecentDiscoveryExpired('private discovery request deadline exceeded')
                self.budget.round_finished(self.private.round_units, complete=True, stop=self.stop)
                result['discovery_read_route'] = {'recent':'private_recent', 'history':'public_history',
                    'private_units':self.private.round_units, 'budget':self.budget.details(self.stop),
                    'total_round_ms':(time.monotonic()-started)*1000}
                self.last_private_error = None
                return result
            except Exception as exc:
                if self.stop.is_set(): raise
                reason = type(exc).__name__
                self.last_private_error = reason
                attempt = {'outcome':'discarded', 'error_type':reason,
                           'elapsed_ms':(time.monotonic()-private_started)*1000,
                           'units_debited':self.private.round_units}
                # No URL, provider body, payload or credential is persisted.
                try: self.budget.round_finished(self.private.round_units, complete=False, stop=self.stop)
                except Exception:
                    if self.stop.is_set(): raise
                self.retry_at = time.monotonic()+30
        # Every state/header/auth read in this retry uses the public connection.
        public_started = time.monotonic()
        result = prepare(self.public, self.config, recent_blocks, previous=previous)
        result['discovery_read_route'] = {'recent':'public', 'history':'public_history',
            'private_enabled':self.settings is not None, 'fallback_reason':reason,
            'discarded_private_attempt':attempt, 'total_round_ms':(time.monotonic()-started)*1000,
            'public_round_ms':(time.monotonic()-public_started)*1000,
            'private_retry_in_seconds':max(0, self.retry_at-time.monotonic())}
        return result

    def close(self):
        try:
            if self.private is not None: self.private.close()
        finally:
            self.public.close()
