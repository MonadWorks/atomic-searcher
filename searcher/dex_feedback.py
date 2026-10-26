"""Feed complete-call outcomes back into DEX scheduling, independently of quotes.

Exact route failures and sampled directed transfers have bounded retry windows.
Neither native quotes nor ordinary pool-price changes clear this memory. Only a
successful complete call clears it early. No sample permits signing a trade.
"""
from collections import OrderedDict, Counter
import threading
import time

from eth_abi import decode
from .core import atomic_json, data, number, safe_error, unhex
from .routes import Route
from .route_feedback import RouteFeedback
from .transfer_feedback import TransferFeedback, TransferSample, measure_sample
from .structural import artifact, HOPS


def route_for(path):
    return Route((path[0][1], *(h[2] for h in path)), tuple(h[0] for h in path))


class DexFeedback:
    def __init__(self, owner, contracts, output, clock=time.monotonic):
        self.clock, self.output = clock, output
        self.lock = threading.RLock()
        self.routes = RouteFeedback(clock=clock)
        self.transfers = {name: TransferFeedback(target, owner, clock=clock)
                          for name, target in contracts.items()
                          if name in ('AtomicCycle', 'StructuralExecutor') and target}
        self.pending, self.retry_at = OrderedDict(), OrderedDict()
        self.in_flight = set()
        self.counts = Counter()
        self.sequence = 0

    @staticmethod
    def scope(path):
        return 'StructuralExecutor' if any(h[3] == 4 for h in path) else 'AtomicCycle'

    def key(self, path, execution_key=None):
        return (self.scope(path), execution_key or route_for(path).key)

    def allowed(self, path, execution_key=None):
        if self.routes.blocked(self.key(path, execution_key)): return False
        transfers = self.transfers.get(self.scope(path))
        return transfers is None or not transfers.deferred(route_for(path))

    def blocked_hops(self):
        # Pool state reads must not invalidate token-transfer evidence. Return
        # immutable snapshots so the price beam performs no lock per edge.
        result = {}
        for name, feedback in self.transfers.items():
            with feedback.lock:
                now = self.clock()
                keys = set(feedback.edges) | set(feedback.history) | set(feedback.probe_slots)
                blocked = set()
                for token, sender, recipient in keys:
                    key = (token, sender, recipient)
                    sample = feedback.edges.get(key, {})
                    history = feedback.history.get(key, {})
                    slot = feedback.probe_slots.get(key, {})
                    deadline = max(sample.get('next_probe', 0) if sample.get('until', 0) > now else 0,
                                   history.get('next_probe', 0) if history.get('until', 0) > now else 0,
                                   slot.get('until', 0))
                    if deadline > now:
                        if sender == feedback.executor: blocked.add((recipient, token, 'in'))
                        if recipient == feedback.executor: blocked.add((sender, token, 'out'))
                result[name] = frozenset(blocked)
        return result

    def admit(self, path, execution_key=None):
        if not self.allowed(path, execution_key):
            with self.lock: self.counts['complete_calls_deferred'] += 1
            return False
        feedback = self.transfers.get(self.scope(path))
        return feedback is None or feedback.claim_probe(route_for(path))

    def rejected(self, path, amount, failure, *, block, gross, floor, execution_key=None):
        delay = self.routes.record(self.key(path, execution_key), failure)
        feedback = self.transfers.get(self.scope(path))
        if feedback is not None:
            delay = max(delay, feedback.rejected(route_for(path), failure))
        if delay and failure.get('revert_name') in ('BadCallback', 'UniswapV2Invariant', 'PoolInvariant'):
            self.queue_samples(path, amount)
        self.event(path, amount, 'rejected', block=block, native_gross=gross,
                   required_gross=floor, failure=failure, retry_seconds=delay, execution_key=execution_key)
        return delay

    def passed(self, path, amount, *, block, gross, floor, execution_key=None):
        self.routes.passed(self.key(path, execution_key))
        feedback = self.transfers.get(self.scope(path))
        if feedback is not None: feedback.passed(route_for(path))
        self.event(path, amount, 'passed', block=block, gross_surplus=gross, required_gross=floor, execution_key=execution_key)

    def event(self, path, amount, outcome, **details):
        with self.lock:
            self.sequence += 1
            self.counts['complete_calls_' + outcome] += 1
            if outcome == 'rejected':
                self.counts['rejected_' + details['failure'].get('revert_name', 'other')] += 1
            value = dict(at=time.time(), sequence=self.sequence, stage='whole_cycle_eth_call',
                         scope=self.scope(path), pools=[h[0] for h in path],
                         tokens=[path[0][1], *(h[2] for h in path)], amount_in=amount,
                         outcome=outcome, **details)
            atomic_json(self.output / f'dex-execution-{self.sequence % 64}.json', value)

    def queue_samples(self, path, amount):
        scope = self.scope(path)
        feedback = self.transfers.get(scope)
        if feedback is None: return
        with self.lock:
            now = self.clock()
            # Each intermediate transfer keeps its actual sender/recipient;
            # another first or last pool cannot erase a shared bad transfer.
            for i, hop in enumerate(path[:-1]):
                key = (scope, hop[2], hop[0], path[i+1][0])
                if key in self.in_flight or key in self.pending or self.retry_at.get(key, 0) > now: continue
                if len(self.pending) >= 32: break
                self.pending[key] = (tuple(path[:i+1]), int(amount), now)

    def diagnose_one(self, rpc):
        with self.lock:
            job = None
            while self.pending:
                key, job = self.pending.popitem(last=False)
                if self.clock()-job[2] < 120: break
                job = None
            if job is None: return False
            self.in_flight.add(key)
        scope, token, source, destination = key
        feedback = self.transfers[scope]
        prefix, amount, _ = job
        sample = None
        succeeded = False
        try:
            head = rpc.header()
            lens = '0x000000000000000000000000000000000000c1d6'
            raw = rpc.request('eth_call', [
                {'to':lens, 'gas':hex(4_000_000), 'data':data('quotePath('+HOPS+',uint256)',
                    [HOPS, 'uint256'], [prefix, amount])}, head['number'],
                {lens:{'code':artifact('CycleQuoteLens')['deployedBytecode']['object']}}])
            output = decode(['uint256'], unhex(raw))[0]
            if output <= 0: return True
            sample = TransferSample(token, source, destination, output)
            with feedback.lock: feedback.in_flight[sample.key] = self.clock()
            result = measure_sample(rpc, sample, feedback.executor, feedback.owner,
                                    artifact('CycleProbe')['deployedBytecode']['object'])
            feedback.record(sample, result)
            succeeded = True
            with self.lock: self.counts['transfer_samples'] += 1
        except Exception as exc:
            with self.lock:
                self.counts['diagnostic_errors'] += 1
                self.last_diagnostic_error = safe_error(exc)
        finally:
            if sample is not None: feedback.finish_sample(sample, succeeded)
            with self.lock:
                self.in_flight.discard(key)
                self.retry_at[key] = self.clock() + (300 if succeeded else 30)
                self.retry_at.move_to_end(key)
                while len(self.retry_at) > 2048: self.retry_at.popitem(last=False)
        return True

    def publish(self):
        with self.lock:
            counts, queued, active = dict(self.counts), len(self.pending), len(self.in_flight)
            # atomic_json uses a deterministic temporary name; both the scan
            # lane and diagnostic worker publish under this same lock.
            atomic_json(self.output/'dex-feedback.json', dict(at=time.time(), counts=counts,
                queued_diagnostics=queued, in_flight=active,
                transfers={name: feedback.summary() for name, feedback in self.transfers.items()},
                scope='Temporary scheduling evidence; full execution, fees and signing checks remain required.'))
