"""Bounded, temporary feedback from actual execution failures, never quote guesses."""
import time
import threading
from collections import OrderedDict


class RouteFeedback:
    def __init__(self, clock=time.monotonic, seconds=30, limit=4096, max_seconds=300, history_seconds=3600):
        self.clock, self.seconds, self.limit = clock, seconds, limit
        self.failures = OrderedDict()
        self.history = OrderedDict()
        self.max_seconds, self.history_seconds = max(seconds, max_seconds), history_seconds
        self.lock = threading.RLock()
        self.group_routes = OrderedDict()

    def blocked(self, key):
        with self.lock:
            deadline = self.failures.get(key)
            if deadline is None: return False
            if self.clock() < deadline: return True
            del self.failures[key]
            return False

    def priority(self, key):
        with self.lock:
            previous = self.history.get(key)
            if previous is None: return 0
            if previous['until'] <= self.clock():
                del self.history[key]
                return 0
            return 1

    def group_deferred(self, group, pools, settlements, route_keys=None, transfers=None):
        """Read a group whenever any requested direction remains eligible.

        A route failure belongs only to that exact direction. Combining it
        with sampled transfer feedback must not blacklist a token or the
        reverse direction. Unknown topology or scope stays eligible.
        """
        if not 2 <= len(group) <= 4 or len(set(group)) != len(group): return False
        try: part = [pools[a] for a in sorted(group)]
        except KeyError: return False
        if any(p.address != a for a,p in zip(sorted(group),part)): return False
        starts = tuple(sorted({t for p in part for t in (p.token0,p.token1)}.intersection(settlements)))
        key = (tuple((p.address,p.token0,p.token1,p.kind) for p in part),starts)
        with self.lock:
            if key not in self.group_routes:
                from .routes import Route, iter_cycles
                self.group_routes[key] = tuple(Route(tokens,path) for tokens,path in
                    iter_cycles(part,starts,len(part)) if len(path)==len(part))
            self.group_routes.move_to_end(key)
            routes = self.group_routes[key]
            while len(self.group_routes) > min(512,max(1,self.limit)):
                self.group_routes.popitem(last=False)
        if route_keys is not None:
            requested = set(route_keys)
            if not requested or not requested.issubset({r.key for r in routes}): return False
            routes = tuple(r for r in routes if r.key in requested)
        return bool(routes) and all(self.blocked(r.key) or
            (transfers is not None and transfers.deferred(r)) for r in routes)

    def record(self, key, failure):
        if (failure.get('validation_deadline_reached') or failure.get('validation_outcome')
                or failure.get('anchor_mismatch')):
            return 0
        # A repeated revert under larger gas budgets also earns a short route
        # retry delay. It is not evidence that any token/pool is incompatible.
        # A lone wrapped out-of-gas, transport failure or changed price does not.
        previous = failure.get('previous_gas_failure', {})
        repeated = (failure.get('revert_name') in ('TransferFailed','unclassified') and
                    failure.get('revert_selector') not in (None,'0x08c379a0') and
                    previous.get('revert_selector') == failure.get('revert_selector') and
                    len(set(failure.get('gas_attempts', []))) >= 2)
        repeated = repeated or (failure.get('revert_name') == 'V2TransferFailed' and
                    previous.get('revert_name') == 'V2TransferFailed' and
                    len(set(failure.get('gas_attempts', []))) >= 2)
        if not repeated and failure.get('revert_name') not in ('UniswapV2Invariant', 'PoolInvariant', 'BadCallback', 'InvalidPool'):
            return 0
        signature = (failure.get('revert_name'),failure.get('revert_selector'))
        with self.lock:
            now = self.clock()
            previous = self.history.get(key, {})
            streak = (previous.get('streak',0) if previous.get('until',0)>now
                      and previous.get('signature')==signature else 0)
            streak = min(16,streak+1)
            delay = min(self.max_seconds,self.seconds*2**(streak-1))
            self.history[key] = dict(signature=signature,streak=streak,until=now+self.history_seconds)
            self.history.move_to_end(key)
            self.failures[key] = now+delay
            self.failures.move_to_end(key)
            while len(self.failures) > self.limit: self.failures.popitem(last=False)
            while len(self.history) > self.limit: self.history.popitem(last=False)
            return delay

    def passed(self, key):
        with self.lock:
            self.failures.pop(key, None)
            self.history.pop(key, None)
