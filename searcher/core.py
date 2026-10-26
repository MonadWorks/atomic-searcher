from __future__ import annotations

import json
import copy
import hashlib
import math
import os
import re
import time
import asyncio
import threading
from urllib.parse import urlsplit
from collections import deque
from pathlib import Path

import httpx
from eth_abi import decode, encode
from eth_utils import is_address, keccak

ROOT = Path(__file__).resolve().parents[1]
WETH = "0x4200000000000000000000000000000000000006"
USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
ZERO = "0x" + "00" * 20
MULTICALL = "0xca11bde05977b3631167028862be2a173976ca11"
MULTICALL_HASH = "0xd5c15df687b16f2ff992fc8d767b4216323184a2bbc6ee2f9c398c318e770891"


def batch_request_digest(calls):
    """Bind timing metadata to a complete read batch without retaining its data."""
    return hashlib.sha256(json.dumps(calls, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


class Reject(RuntimeError):
    pass


class RPCError(Reject):
    """Preserve revert data separately from the bounded, sanitized message."""
    def __init__(self, method, error, *, batch=False):
        self.method = method
        self.code = error.get('code') if isinstance(error, dict) else None
        message = error.get('message') if isinstance(error, dict) else None
        self.rpc_message = message if isinstance(message, str) else ''
        raw = error.get('data') if isinstance(error, dict) else None
        # Providers use both direct data and a nested data/result object.
        for _ in range(3):
            if not isinstance(raw, dict): break
            raw = raw.get('data', raw.get('result', raw.get('return')))
        self.revert_data = raw if isinstance(raw, str) and re.fullmatch(r'0x(?:[0-9a-fA-F]{2})*', raw) else None
        prefix = 'RPC batch '+method if batch else 'RPC '+method
        super().__init__(prefix+': '+safe_error(error))


class Incomplete(Reject):
    """Missing data is not a zero-profit quote."""


class RPCUnavailable(Reject):
    """A bounded transport deadline or temporary provider limit was reached."""


class RPCAccessDenied(Reject):
    """A provider permission error, never a missing receipt or retryable send."""


def check_rpc_status(response):
    if response.status_code in (401, 403):
        try:
            message = response.json().get('error', {}).get('message', '')
        except (ValueError, AttributeError, TypeError):
            message = ''
        if not isinstance(message, str): message = ''
        if 'archive' in message.lower() and 'token' in message.lower():
            detail = '该节点的历史交易查询需要个人令牌；请使用支持历史回执的节点，或正确配置该节点的凭证。'
        else:
            detail = '该节点拒绝访问；请检查节点地址及凭证。'
        raise RPCAccessDenied(f'节点访问被拒绝（HTTP {response.status_code}）。{detail}')
    response.raise_for_status()


def check(condition, reason):
    if not condition:
        raise Reject(reason)


def address(value):
    check(is_address(value) and value.lower() != ZERO, "invalid address")
    return value.lower()


def number(value):
    return int(value, 16) if isinstance(value, str) and value.startswith("0x") else int(value)


def data(signature, types=(), values=()):
    return "0x" + (keccak(text=signature)[:4] + encode(types, values)).hex()


def unhex(value):
    return bytes.fromhex(value.removeprefix("0x"))


def safe_error(error):
    return re.sub(r"(?:https?|wss?)://[^\s\x27\x22]+", "[endpoint]", str(error))[:300]


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    with open(temp, "w") as f:
        json.dump(value, f, indent=2)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)


class PublicRateLimiter:
    """Shared request-count budget; batches consume one unit per RPC method."""
    def __init__(self):
        self.lock = threading.Lock()
        self.sent = deque()
        self.critical_waiters = 0
        self.critical_queue = deque()

    def acquire(self, count, cancelled=None, submission=False, critical=False, deadline_ns=None):
        # Critical reads go first once queued; small reserved headroom also
        # lets a candidate recheck start without waiting for a large discovery
        # batch. Keep enough background capacity for both state readers.
        limit = 24 if submission else 21 if critical else 18
        check(0 < count <= limit, 'public RPC batch exceeds pacing capacity')
        ticket = object()
        if critical:
            with self.lock:
                self.critical_waiters += 1
                self.critical_queue.append(ticket)
        try:
            while True:
                check(cancelled is None or not cancelled.is_set(), 'RPC cancelled')
                if deadline_ns is not None and time.monotonic_ns()>=deadline_ns:raise RPCUnavailable('RPC opportunity deadline exceeded')
                now = time.monotonic()
                with self.lock:
                    while self.sent and self.sent[0] <= now-1.05: self.sent.popleft()
                    capacity = len(self.sent)+count <= limit
                    turn = (self.critical_queue[0] is ticket if critical else not self.critical_waiters)
                    if capacity and (submission or turn):
                        self.sent.extend([now]*count)
                        return
                    wait = (.01 if capacity else
                            max(.001, self.sent[len(self.sent)+count-limit-1]+1.05-now))
                if cancelled is not None: cancelled.wait(min(wait,.1))
                else: time.sleep(min(wait,.1))
        finally:
            if critical:
                with self.lock:
                    self.critical_waiters -= 1
                    self.critical_queue.remove(ticket)


_PUBLIC_BASE_LIMITER = PublicRateLimiter()


class PublicProviderBudget:
    """Conservative local pacing, not an assertion of a provider's quota.

    One shared queue and in-flight request prevent independent workers from
    retrying a public endpoint together. Each method consumes pacing time,
    including every method in an HTTP batch. Recovery is deliberately gradual.
    """
    max_batch = 3

    def __init__(self, rate=2.0, hostname="base.drpc.org"):
        check(math.isfinite(rate) and rate > 0, 'invalid provider pacing rate')
        self.maximum_rate = self.rate = rate
        self.hostname=hostname
        self.lock, self.queue = threading.Lock(), deque()
        self.active = None
        self.urgent_units = 0
        self.next_at = self.cooldown_until = 0.0
        self.throttles = self.successes = 0

    @staticmethod
    def retry_after(value):
        if value is None: return None
        try:
            seconds = float(value)
        except (TypeError, ValueError):
            try:
                from email.utils import parsedate_to_datetime
                from datetime import timezone
                date = parsedate_to_datetime(value)
                if date.tzinfo is None: date = date.replace(tzinfo=timezone.utc)
                seconds = max(0, date.timestamp()-time.time())
            except (TypeError, ValueError, OverflowError):
                return None
        return seconds if math.isfinite(seconds) and seconds >= 0 else None

    def acquire(self, count, cancelled=None, deadline_ns=None, urgent=False):
        check(type(count) is int and 0 < count <= self.max_batch, 'provider batch exceeds pacing capacity')
        ticket = (object(), bool(urgent))
        with self.lock: self.queue.append(ticket)
        acquired = False
        try:
            while True:
                check(cancelled is None or not cancelled.is_set(), 'RPC cancelled')
                if deadline_ns is not None and time.monotonic_ns()>=deadline_ns:raise RPCUnavailable('RPC opportunity deadline exceeded')
                with self.lock:
                    now = time.monotonic()
                    delay = max(self.next_at, self.cooldown_until)-now
                    live = next((t for t in self.queue if t[1]), None)
                    background = next((t for t in self.queue if not t[1]), None)
                    # One live method may pass historical work, then the oldest
                    # background request gets a turn. Rate/cooldown are shared.
                    chosen = (background if background is not None and self.urgent_units >= 1
                              else live if live is not None else background)
                    if self.active is None and chosen is ticket and delay <= 0:
                        self.queue.remove(ticket)
                        self.urgent_units = self.urgent_units+count if urgent else 0
                        self.active, acquired = ticket, True
                        self.next_at = now+count/self.rate
                        return ticket
                # Even a long Retry-After remains cancellable. Only the FIFO
                # head can probe recovery; new requests obey the same deadline.
                wait = min(.05, max(.001, delay))
                if cancelled is None: time.sleep(wait)
                else: cancelled.wait(wait)
        finally:
            if not acquired:
                with self.lock: self.queue.remove(ticket)

    def finish(self, ticket, status=None, retry_after=None):
        seconds = self.retry_after(retry_after)
        throttled = status == 429 or (status == 503 and seconds is not None)
        with self.lock:
            check(self.active is ticket, 'provider request ownership lost')
            now = time.monotonic()
            if throttled:
                self.throttles = min(16, self.throttles+1)
                self.successes = 0
                self.rate = max(self.maximum_rate/8, self.rate/2)
                backoff = min(30, 2**min(self.throttles, 5))
                self.cooldown_until = max(self.cooldown_until, now+max(backoff, seconds or 0))
            elif status is not None and 200 <= status < 300:
                self.successes += 1
                if self.successes >= 8:
                    self.successes = 0
                    self.throttles = max(0, self.throttles-1)
                    self.rate = min(self.maximum_rate, self.rate*1.25)
            else:
                self.successes = 0
            self.active = None
            return throttled

    def snapshot(self):
        with self.lock:
            return {'hostname':self.hostname, 'local_method_rate':self.rate,
                    'cooldown_seconds':max(0, self.cooldown_until-time.monotonic()),
                    'queued_requests':len(self.queue), 'consecutive_throttle_level':self.throttles,
                    'quota_claim':False}


_PUBLIC_DRPC_BUDGET = PublicProviderBudget()
_DISCOVERY_PROVIDER_BUDGETS = {host:PublicProviderBudget(2,host) for host in
    ('base.gateway.tenderly.co','base.public.blockpi.network')}

class RPC:
    # Read-only transport; a typo or configuration change cannot enable sends.
    METHODS = {"eth_chainId", "eth_getBlockByNumber", "eth_getBlockByHash", "eth_getBlockReceipts",
               "eth_getTransactionByHash", "eth_getTransactionReceipt", "eth_getCode", "eth_call",
               "eth_getLogs", "eth_getBalance", "eth_getTransactionCount", "eth_estimateGas",
               "eth_gasPrice", "eth_feeHistory", "eth_getStorageAt"}

    def __init__(self, url=None, chain_id=8453, timeout=15, cancel_event=None):
        self.url = url or os.environ.get("BASE_RPC_URL", "https://mainnet.base.org")
        self.chain_id = chain_id
        self.timeout = timeout
        self.loop = asyncio.new_event_loop()
        try:
            self.session = httpx.AsyncClient(timeout=timeout,limits=httpx.Limits(keepalive_expiry=120))
        except Exception:
            self.loop.close()
            raise
        self.multicall_verified = False
        self.measurements = deque(maxlen=10000)
        self.cancel_event = cancel_event
        self.public_rate_limiter = _PUBLIC_BASE_LIMITER if urlsplit(self.url).hostname == "mainnet.base.org" else None
        host=urlsplit(self.url).hostname
        self.provider_budget = (_PUBLIC_DRPC_BUDGET if host=='base.drpc.org'
                                else _DISCOVERY_PROVIDER_BUDGETS.get(host))
        self.critical_reads = False
        self.deadline_ns = None
        self.minimum_request_interval = 0
        self.last_post_at = 0

    def check_cancelled(self):
        if getattr(self,'deadline_ns',None) is not None and time.monotonic_ns()>=self.deadline_ns:
            raise RPCUnavailable('RPC opportunity deadline exceeded')
        check(self.cancel_event is None or not self.cancel_event.is_set(), "RPC cancelled")

    def close(self):
        if not self.loop.is_closed():
            self.loop.run_until_complete(self.session.aclose())
            self.loop.close()

    def post(self, payload, *, prepare=None):
        """Bound the complete request, including a peer dripping headers/body."""
        post_started = time.monotonic()
        timing = dict(minimum_interval_wait_ms=0.0, public_limiter_wait_ms=0.0,
                      network_headers_ms=0.0, network_body_ms=0.0,
                      network_total_ms=0.0, network_started_ns=None, network_finished_ns=None, timing_complete=True)
        response = None
        provider_ticket = None
        transport_complete = False
        headers_received = False
        async def operation():
            started = time.monotonic()
            async def fetch():
                nonlocal response, headers_received
                async with self.session.stream("POST", self.url, json=payload) as received:
                    response, headers_received = received, True
                    timing['network_headers_ms'] = (time.monotonic()-started)*1000
                    response.extensions["headers_elapsed_ms"] = timing['network_headers_ms']
                    body_started = time.monotonic()
                    try:
                        await response.aread()
                        return response
                    finally:
                        timing['network_body_ms'] = (time.monotonic()-body_started)*1000
            task = asyncio.create_task(fetch())
            try:
                while not task.done():
                    self.check_cancelled()
                    if time.monotonic()-started >= self.timeout:
                        raise RPCUnavailable("RPC total request deadline exceeded")
                    await asyncio.wait({task}, timeout=min(.025, max(.001, self.timeout-(time.monotonic()-started))))
                self.check_cancelled()
                if time.monotonic()-started > self.timeout:
                    raise RPCUnavailable("RPC total request deadline exceeded")
                return task.result()
            finally:
                if not task.done(): task.cancel()
                await asyncio.gather(task, return_exceptions=True)

        try:
            self.check_cancelled()
            delay = self.minimum_request_interval - (time.monotonic()-self.last_post_at)
            if getattr(self,'deadline_ns',None) is not None:
                delay=min(delay,max(0,(self.deadline_ns-time.monotonic_ns())/1e9))
            if delay > 0:
                wait_started = time.monotonic()
                try:
                    if self.cancel_event is not None: self.cancel_event.wait(delay)
                    else: time.sleep(delay)
                finally:
                    timing['minimum_interval_wait_ms'] = (time.monotonic()-wait_started)*1000
                self.check_cancelled()
            if self.public_rate_limiter is not None:
                submission = isinstance(payload, dict) and payload.get('method') == 'eth_sendRawTransaction'
                wait_started = time.monotonic()
                try:
                    self.public_rate_limiter.acquire(len(payload) if isinstance(payload, list) else 1,
                                                     self.cancel_event, submission=submission, critical=self.critical_reads,
                                                     **({'deadline_ns':self.deadline_ns} if getattr(self,'deadline_ns',None) is not None else {}))
                finally:
                    timing['public_limiter_wait_ms'] = (time.monotonic()-wait_started)*1000
            if self.provider_budget is not None:
                wait_started = time.monotonic()
                try:
                    provider_ticket = self.provider_budget.acquire(
                        len(payload) if isinstance(payload, list) else 1, self.cancel_event,
                        **({'deadline_ns':self.deadline_ns} if getattr(self,'deadline_ns',None) is not None else {}),
                        **({'urgent':True} if getattr(self,'provider_urgent',False) else {}))
                finally:
                    timing['public_limiter_wait_ms'] += (time.monotonic()-wait_started)*1000
            self.check_cancelled()
            if prepare is not None:
                # Preparation is local only and retains the already-admitted
                # method count. A clock selected before this wait may be old.
                payload = prepare()
                self.check_cancelled()
            self.last_post_at = time.monotonic()
            network_started = time.monotonic()
            timing['network_started_ns'] = time.monotonic_ns()
            try:
                result = self.loop.run_until_complete(operation())
                transport_complete = True
                return result
            finally:
                timing['network_finished_ns'] = time.monotonic_ns()
                timing['network_total_ms'] = (time.monotonic()-network_started)*1000
                if not headers_received:
                    timing['network_headers_ms'] = timing['network_total_ms']
        finally:
            if provider_ticket is not None:
                status = response.status_code if response is not None else None
                if status is not None and 200 <= status < 300 and not transport_complete:
                    # Receiving successful headers alone is not a successful
                    # request. Body failure/cancellation must not raise pace.
                    status = None
                throttled = self.provider_budget.finish(provider_ticket,
                    status,
                    response.headers.get('Retry-After') if response is not None else None)
                timing['provider_budget'] = self.provider_budget.snapshot()
                if response is not None: response.extensions['provider_cooldown_started'] = throttled
            timing['post_total_ms'] = (time.monotonic()-post_started)*1000
            self._last_post_timing = dict(timing)
            if response is not None: response.extensions['rpc_timing'] = dict(timing)

    def _timed_post(self, payload, timings, *, prepare=None):
        # A replaced post() may return an ordinary httpx.Response without our
        # extensions. Keep its unavailable phases unknown, never infer node
        # latency from a wall time that may include a local limiter.
        self._last_post_timing = None
        started, response = time.monotonic(), None
        try:
            response = self.post(payload) if prepare is None else self.post(payload, prepare=prepare)
            return response
        finally:
            timing = self._last_post_timing
            if timing is None:
                extensions = getattr(response, 'extensions', {})
                timing = dict(minimum_interval_wait_ms=None, public_limiter_wait_ms=None,
                              network_headers_ms=extensions.get('headers_elapsed_ms'),
                              network_body_ms=None, network_total_ms=None,
                              post_total_ms=(time.monotonic()-started)*1000, timing_complete=False)
            timing = dict(timing)
            if response is not None:
                timing.update(response_bytes=len(response.content), http_status=response.status_code)
            timings.append(timing)

    def _record_timing(self, method, started, timings, retry_wait_ms, succeeded, methods=None):
        result = {'method':method, 'elapsed_ms':(time.monotonic_ns()-started)/1e6,
                  'retry_wait_ms':retry_wait_ms, 'attempts':len(timings),
                  'outcome':'ok' if succeeded else 'error', 'attempt_timings':timings,
                  'timing_complete':all(t['timing_complete'] for t in timings)}
        for field in ('minimum_interval_wait_ms', 'public_limiter_wait_ms', 'network_headers_ms',
                      'network_body_ms', 'network_total_ms', 'post_total_ms'):
            values = [t.get(field) for t in timings]
            result[field] = sum(values) if all(v is not None for v in values) else None
        starts = [t.get('network_started_ns') for t in timings]
        ends = [t.get('network_finished_ns') for t in timings]
        result['network_started_ns'] = min(starts) if starts and all(isinstance(t,int) for t in starts) else None
        result['network_finished_ns'] = max(ends) if ends and all(isinstance(t,int) for t in ends) else None
        if methods is not None: result['methods'] = methods
        if timings:
            result['response_headers_ms'] = timings[-1].get('network_headers_ms')
            result['response_bytes'] = sum(t.get('response_bytes', 0) for t in timings)
            if 'http_status' in timings[-1]: result['http_status'] = timings[-1]['http_status']
            if 'provider_budget' in timings[-1]: result['provider_budget'] = timings[-1]['provider_budget']
        self.measurements.append(result)

    def request(self, method, params):
        check(method in self.METHODS, "write/unknown RPC method forbidden")
        started = time.monotonic_ns()
        timings, retry_wait_ms, succeeded = [], 0.0, False
        attempts=1 if getattr(self,'deadline_ns',None) is not None else max(1,min(4,getattr(self,'read_attempts',4)))
        try:
            for attempt in range(attempts):
                self.check_cancelled()
                response = self._timed_post({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timings)
                # Sending/recovery is journal-owned, never a transport replay.
                if method not in RPC.METHODS or getattr(self,'deadline_ns',None) is not None: break
                if self.provider_budget is not None and response.extensions.get('provider_cooldown_started'):
                    # Resume this idempotent read in place. Every attempt must
                    # rejoin the shared FIFO/cooldown; no second local sleep.
                    if attempt+1 < attempts: continue
                    break
                if response.status_code not in (429, 502, 503, 504) or attempt+1 == attempts:
                    break
                try:
                    wait = max(2**attempt, float(response.headers.get("Retry-After", 0)))
                except ValueError:
                    # Do not ignore an HTTP-date Retry-After value.
                    from email.utils import parsedate_to_datetime
                    wait = max(2**attempt, parsedate_to_datetime(response.headers["Retry-After"]).timestamp()-time.time())
                if wait > 30:
                    raise RPCUnavailable("RPC throttled; Retry-After exceeds bounded retry window")
                wait_started = time.monotonic()
                try:
                    if self.cancel_event is None: time.sleep(wait)
                    else:
                        self.cancel_event.wait(wait)
                        self.check_cancelled()
                finally:
                    retry_wait_ms += (time.monotonic()-wait_started)*1000
            self.check_cancelled()
            check_rpc_status(response)
            body = response.json()
            check(isinstance(body, dict) and body.get("id") == 1, "invalid RPC response")
            if body.get("error"):
                raise RPCError(method, body['error'])
            check("result" in body, "missing RPC result")
            succeeded = True
            return body["result"]
        finally:
            self._record_timing(method, started, timings, retry_wait_ms, succeeded)

    def verify_chain(self):
        check(number(self.request("eth_chainId", [])) == self.chain_id, "wrong chain")

    def batch(self, calls):
        """Read-only batch. Results follow request order, never response order."""
        return self._batch(calls)[0]

    def batch_prepared(self, calls, prepare):
        """Prepare one read batch after local admission; return values and calls.

        The zero-argument callback performs no I/O. It may replace parameters,
        but must retain the exact read-method sequence and count. This bounded
        path never splits a batch. Each bounded retry reacquires admission and
        prepares fresh parameters; timing retains the earliest attempt's age.
        """
        check(callable(prepare), 'invalid RPC batch preparation')
        return self._batch(calls, prepare)

    def _batch(self, calls, prepare=None):
        if prepare is not None: calls = copy.deepcopy(calls)
        check(0 < len(calls) <= 128, "invalid batch size")
        self.check_cancelled()
        allowed = RPC.METHODS if prepare is not None else self.METHODS
        check(all(m in allowed for m, _ in calls), "write/unknown RPC method forbidden")
        batch_limit = 21 if self.critical_reads else 18
        if self.provider_budget is not None: batch_limit = min(batch_limit, self.provider_budget.max_batch)
        check(prepare is None or len(calls) <= batch_limit, 'prepared RPC batch exceeds pacing capacity')
        methods = tuple(m for m, _ in calls)
        actual_calls = calls
        started = time.monotonic_ns()
        request_digest = batch_request_digest(calls)
        timings, succeeded = [], False
        def send(group):
            nonlocal actual_calls, request_digest
            self.check_cancelled()
            def payload_for(rows):
                return [{"jsonrpc": "2.0", "id": i, "method": m, "params": p}
                        for i, (m, p) in enumerate(rows)]
            def prepare_payload():
                nonlocal actual_calls, request_digest
                prepared = prepare()
                check(isinstance(prepared, (list, tuple)) and len(prepared) == len(group)
                      and all(isinstance(row, (list, tuple)) and len(row) == 2
                              and isinstance(row[1], (list, tuple)) for row in prepared),
                      'prepared RPC batch shape changed')
                check(tuple(row[0] for row in prepared) == methods, 'prepared RPC batch methods changed')
                actual_calls = copy.deepcopy(prepared)
                request_digest = batch_request_digest(actual_calls)
                return payload_for(actual_calls)
            payload = payload_for(group)
            retryable_read = all(method in RPC.METHODS for method in methods)
            for attempt in range(1 if getattr(self,'deadline_ns',None) is not None else 4):
                self.check_cancelled()
                try:
                    response = (self._timed_post(payload, timings) if prepare is None else
                                self._timed_post(payload, timings, prepare=prepare_payload))
                finally:
                    # Each attempt is independently bound; a prepared clock
                    # may change while waiting for the next shared admission.
                    if timings:
                        timings[-1]['batch_request_digest'] = batch_request_digest(
                            actual_calls if prepare is not None else group)
                self.check_cancelled()
                if not (getattr(self,'deadline_ns',None) is None and self.provider_budget is not None and retryable_read and attempt < 3
                        and response.extensions.get('provider_cooldown_started')):
                    break
            self.check_cancelled()
            check_rpc_status(response)
            body = response.json()
            check(isinstance(body, list) and len(body) == len(group), "incomplete RPC batch")
            indexed = {v.get("id"): v for v in body}
            check(set(indexed) == set(range(len(group))), "invalid RPC batch ids")
            for i, value in indexed.items():
                if value.get('error'): raise RPCError(group[i][0], value['error'], batch=True)
                check("result" in value, "missing batch result")
            return [indexed[i]["result"] for i in range(len(group))]
        try:
            if (self.public_rate_limiter is not None or self.provider_budget is not None) and len(calls) > batch_limit:
                values = [value for start in range(0, len(calls), batch_limit)
                          for value in send(calls[start:start+batch_limit])]
            else:
                values = send(calls)
            succeeded = True
            return values, actual_calls
        finally:
            self._record_timing('batch', started, timings, 0.0, succeeded, [m for m, _ in calls])
            self.measurements[-1].update(batch_started_ns=started, batch_request_digest=request_digest)

    def header(self, tag="latest", full=False):
        h = self.request("eth_getBlockByNumber", [hex(tag) if isinstance(tag, int) else tag, full])
        check(h is not None, "missing block")
        return h

    def call(self, to, calldata, block):
        return unhex(self.request("eth_call", [{"to": address(to), "data": calldata},
                                               hex(block) if isinstance(block, int) else block]))

    def views(self, calls, block):
        """Explicit read calls aggregated at ONE pinned block, with per-call failures."""
        if not self.multicall_verified:
            self.verify_chain()
            code = self.request("eth_getCode", [MULTICALL, hex(block)])
            check("0x" + keccak(unhex(code)).hex() == MULTICALL_HASH, "unverified Multicall bytecode")
            self.multicall_verified = True
        result = []
        for offset in range(0, len(calls), 120):
            chunk = calls[offset:offset+120]
            payload = [(address(a), True, unhex(d)) for a, d in chunk]
            raw = self.call(MULTICALL, data("aggregate3((address,bool,bytes)[])",
                            ["(address,bool,bytes)[]"], [payload]), block)
            decoded = decode(["(bool,bytes)[]"], raw)[0]
            check(len(decoded) == len(chunk), "incomplete aggregate")
            result.extend(b if ok else None for ok, b in decoded)
        return result

    def batch_contract_reads(self, calls, approved_reads=()):
        """Combine explicitly approved getters, preserving original result order.

        Uses the authenticated deployed Multicall3; no wallet or executor state
        overrides. Multicall changes msg.sender and shares temporary state, so
        callers must approve only sender-independent, nonpersistent operations.
        Non-approved calls stay as separate methods in the same HTTP batch.
        """
        approved_reads = set(approved_reads)
        indices = [i for i, (method, params) in enumerate(calls)
                   if method == 'eth_call' and len(params) == 2 and params[1] == 'pending'
                   and set(params[0]) == {'to', 'data'}
                   and (params[0]['to'].lower(), params[0]['data'][:10]) in approved_reads]
        if len(indices) < 2: return self.batch(calls)
        if not self.multicall_verified:
            self.verify_chain()
            code = self.request('eth_getCode', [MULTICALL, 'latest'])
            check('0x'+keccak(unhex(code)).hex() == MULTICALL_HASH, 'unverified Multicall bytecode')
            self.multicall_verified = True
        payload = [(address(calls[i][1][0]['to']), True, unhex(calls[i][1][0]['data'])) for i in indices]
        aggregate = ('eth_call', [{'to': MULTICALL, 'data': data('aggregate3((address,bool,bytes)[])',
                      ['(address,bool,bytes)[]'], [payload])}, 'pending'])
        remaining = [i for i in range(len(calls)) if i not in indices]
        values = self.batch([calls[i] for i in remaining]+[aggregate])
        decoded = decode(['(bool,bytes)[]'], unhex(values[-1]))[0]
        check(len(decoded) == len(indices) and all(ok for ok, _ in decoded), 'context contract read failed')
        result = [None]*len(calls)
        for i, value in zip(remaining, values[:-1]): result[i] = value
        for i, (_, value) in zip(indices, decoded): result[i] = '0x'+value.hex()
        return result


def load_config(path=None):
    config = json.loads(Path(path or ROOT / "config/base.json").read_text())
    check('_executor_version' not in config and '_migration_source' not in config, 'internal migration fields in user configuration')
    check(config["chain_id"] == 8453, "Base configuration required")
    check(0 < config["policy"]["max_hops"] <= 4, "unsupported route length")
    check(0 < config["policy"]["max_gas"] <= 6_000_000, "gas ceiling exceeded")
    check(config["policy"]["mode"] == "shadow", "funded mode requires a separate verified release")
    for token in config["settlement_tokens"]:
        address(token["address"])
        check(0 < int(token["max_input_raw"]) < 1 << 255 and int(token["min_net_raw"]) > 0, "invalid settlement cap/floor")
    structural=config.get('structural',{})
    check(isinstance(structural,dict) and isinstance(structural.get('enabled',False),bool),'invalid structural configuration')
    if structural.get('enabled'):
        check(any(t['address']==USDC for t in config['settlement_tokens']),'structural strategies require USDC settlement')
    suite=config.get('strategy_suite',{})
    check(isinstance(suite,dict) and isinstance(suite.get('enabled',False),bool),'invalid strategy suite configuration')
    if suite.get('enabled'):
        check(structural.get('enabled') and structural.get('market_universe'),'strategy suite requires shared universe')
        check(isinstance(suite.get('read_methods_per_second',4),(int,float)) and 1<=suite.get('read_methods_per_second',4)<=6,'invalid suite read budget')
    venues=config.get('venues',{})
    check(isinstance(venues,dict) and isinstance(venues.get('enabled',False),bool),'invalid venue configuration')
    if venues.get('enabled'):check(suite.get('enabled') and structural.get('enabled'),'venue cycles require shared suite executor')
    v4 = config.get('v4', {})
    check(isinstance(v4, dict) and isinstance(v4.get('enabled', False), bool), 'invalid V4 configuration')
    if v4.get('enabled'):
        check(any(t['address'] == USDC for t in config['settlement_tokens']), 'V4 requires USDC settlement')
        check(isinstance(v4.get('bitmap_radius',16),int) and 0 <= v4.get('bitmap_radius',16) <= 32, 'invalid V4 radius')
        check(isinstance(v4.get('tick_limit',128),int) and 2 <= v4.get('tick_limit',128) <= 256, 'invalid V4 tick limit')
        check(isinstance(v4.get('poll_seconds',1),(int,float)) and .5 <= v4.get('poll_seconds',1) <= 30, 'invalid V4 poll interval')
    address(config["owner"])
    excluded = config.get('unsupported_transfer_tokens', {})
    check(isinstance(excluded, dict), 'unsupported transfer registry must be an object')
    for token, reason in excluded.items():
        check(token == address(token) and isinstance(reason, str) and 0 < len(reason) <= 256,
              'invalid unsupported transfer token')
    check(not any(t['address'] in excluded for t in config['settlement_tokens']),
          'settlement token has unsupported transfers')
    from .protocols import KINDS
    for factory in config['factories']:
        address(factory['address'])
        check(factory['kind'] in KINDS, 'unsupported pool protocol')
        if factory['kind'] == 'v2':
            check(factory['address'] == '0x8909dc15e40173ff4699343b6eb8132c65e18ec6',
                  'V2 adapter requires the verified Base Uniswap V2 factory')
    policy = config["policy"]
    check(int(policy.get('min_net_profit_usdc_raw', 0)) >= 0, 'invalid net profit floor')
    check(isinstance(policy.get('min_net_profit_fee_bps', 0), int) and policy.get('min_net_profit_fee_bps', 0) >= 0,
          'invalid profit-to-fee floor')
    check(policy.get("feed_mode", "atomic-state") in ("atomic-state", "receipts"), "unsupported feed mode")
    check(policy["max_hops"] <= policy["max_pools"] <= 300, "pool limit exceeds atomic reader capacity")
    check(0 <= policy["bitmap_radius"] <= 2, "invalid bitmap radius")
    check(1 <= policy.get("snapshot_ticks_per_side", 4) <= 16, "invalid tick cache size")
    check(policy.get('sizing_mode', 'fixed') in ('fixed', 'liquidity'), 'invalid sizing mode')
    check(policy.get('discovery_mode', 'receipts') in ('receipts', 'logs'), 'invalid discovery mode')
    check(policy.get('snapshot_encoding', 'abi') in ('abi', 'packed-v1'), 'invalid snapshot encoding')
    check(.5 <= policy.get('activity_poll_seconds', 1) <= 30, 'invalid activity polling interval')
    check(0 <= policy.get('page_ack_wait_ms', 100) <= 100, 'invalid page acknowledgement wait')
    check(65536 <= policy.get('activity_ws_hourly_byte_limit', 500_000) <= 100_000_000,
          'invalid event-stream usage budget')
    check(type(policy.get('activity_ws_daily_request_limit',80_000)) is int
          and 100 <= policy.get('activity_ws_daily_request_limit',80_000) <= 80_000,
          'invalid free-plan event request budget')
    check(policy.get('activity_ws_pacing_mode', 'daily') in ('daily', 'session'),
          'invalid stream pacing mode')
    check(1 <= policy.get('deep_ticks_per_side', 16) <= 64, 'invalid deep tick cache size')
    check(2 <= policy.get('route_groups_per_page', 96) <= 512, 'invalid route page size')
    check(0 <= policy.get('discovery_request_interval_seconds', 0) <= 5, 'invalid discovery pacing')
    check(0 < policy.get('bid_profit_share_bps', 2500) <= 10000, 'invalid bidding profit allocation')
    check(0 <= policy.get('priority_fee_percentile', 90) <= 100, 'invalid fee percentile')
    check(policy.get('executor_migration') in (None, 'mixed-v1'), 'unknown executor migration')
    check(policy.get('cap_migration') in (None, 'liquidity-v2'), 'unknown migration')

    check(1_000_000 <= policy.get("snapshot_read_gas", 90_000_000) <= 150_000_000, "invalid read-only snapshot gas allowance")
    check(policy.get("pending_poll_interval_seconds", .2) >= .2, "public polling interval must be at least 0.2 seconds")
    check(policy.get("universe_refresh_seconds", 15) >= 1, "invalid discovery refresh interval")
    check(1 <= policy.get("discovery_catchup_blocks", 32) <= 32, "invalid discovery catch-up window")
    check(0 < policy.get("final_validation_max_age_ms", 500) <= policy["snapshot_max_age_ms"], "invalid final validation age")
    check(16 <= policy.get("recording_max_mb", 512) <= 4096, "invalid recording size limit")
    check(128 <= policy.get("decision_retention", 2048) <= 100000, "invalid decision retention")
    check(16 <= policy.get("opportunity_retention_mb", 128) <= 4096, "invalid opportunity size limit")
    check(1 <= policy.get("opportunity_retention_count", 512) <= 100000, "invalid opportunity count limit")
    for key in ("snapshot_max_age_ms", "max_quote_ms", "poll_interval_seconds", "max_pools",
                "max_consecutive_failures", "max_total_fee_wei", "max_daily_fee_wei", "eth_usdc_ceiling"):
        check(int(policy[key]) > 0, "invalid positive policy: "+key)
    check(int(policy["max_total_fee_wei"]) <= int(policy["max_daily_fee_wei"]), "attempt fee exceeds daily ceiling")
    return config
