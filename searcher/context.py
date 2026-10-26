"""One block environment for pending quotes and transaction validation."""
from .core import check, number, unhex


def simulation_context(anchor, pending=None):
    """Keep Flashblocks' pending state, with the next block's execution clock.

    Some nodes overlay recent state on an older EVM environment. Time-dependent
    fees must use the same clock as execute(), not that older environment. The
    anchor hash is observed canonical history and is checked again by callers.
    """
    height, timestamp = number(anchor['number']), number(anchor['timestamp'])
    check(len(unhex(anchor['hash'])) == 32 and any(unhex(anchor['hash'])), 'invalid execution anchor hash')
    target, stamp = height+1, timestamp+2
    if pending is not None:
        target, stamp = number(pending['number']), number(pending['timestamp'])
        check(height < target <= height+2 and timestamp < stamp <= timestamp+6,
              'pending execution clock outside anchor window')
        if target == height+1:
            check(pending['parentHash'] == anchor['hash'], 'pending parent differs from anchor')
    return {'number': hex(target), 'time': hex(stamp), 'blockHash': {str(height): anchor['hash']}}


class SharedClockHints:
    """Short-lived header hints shared only by readers of one endpoint/chain.

    Headers select an initial pending EVM clock, never prices or an executable
    state. Every borrower must still compare its complete atomic read with the
    heads returned by that same RPC batch. Entries are process-lifetime only.
    """
    def __init__(self, *, ttl_seconds=1.5, capacity=8, stop=None, clock=None):
        import threading
        import time
        from collections import OrderedDict
        check(0 < ttl_seconds <= 2 and 1 <= capacity <= 32, 'invalid shared clock bounds')
        self.ttl_ns, self.capacity = int(ttl_seconds*1e9), capacity
        self.stop, self.clock = stop, clock or time.monotonic_ns
        self.lock, self.entries, self.closed = threading.Lock(), OrderedDict(), False

    @staticmethod
    def _key(url, chain_id):
        import hashlib
        # Endpoint credentials must never appear in timing/evidence metadata.
        return hashlib.sha256(str(url).encode()).digest(), number(chain_id)

    def _stopped(self):
        return self.closed or (self.stop is not None and self.stop.is_set())

    @staticmethod
    def _copy(hint):
        return dict(hint, anchor=dict(hint['anchor']), pending=dict(hint['pending']))

    def get(self, url, chain_id):
        key = self._key(url, chain_id)
        with self.lock:
            now = self.clock()
            if self._stopped(): return None
            hint = self.entries.get(key)
            if hint is None or hint.get('invalidated'): return None
            if not 0 <= now-hint['received_ns'] <= self.ttl_ns:
                # Retain the bounded request watermark after the hint expires:
                # a slow earlier request must not revive an obsolete clock.
                return None
            self.entries.move_to_end(key)
            return self._copy(hint)

    def publish(self, url, chain_id, anchor, pending, *, requested_ns, received_ns):
        from .core import Reject
        try:
            # Keep no transaction lists, prices, logs, fee history or authority.
            anchor = {k:anchor[k] for k in ('number','timestamp','hash','parentHash') if k in anchor}
            pending = {k:pending[k] for k in ('number','timestamp','hash','parentHash') if k in pending}
            simulation_context(anchor, pending)
            height = number(anchor['number'])
            now = self.clock()
            if not (0 <= requested_ns <= received_ns <= now and now-received_ns <= self.ttl_ns):
                return False
        except (KeyError, TypeError, ValueError, Reject):
            return False
        key = self._key(url, chain_id)
        with self.lock:
            # Take the clock under the same lock as expiry/version comparison:
            # another thread may have published while we were acquiring it.
            now = self.clock()
            if self._stopped() or not 0 <= now-received_ns <= self.ttl_ns: return False
            previous = self.entries.get(key)
            if previous is not None and requested_ns <= previous['requested_ns']:
                return False
            if previous is not None and not previous.get('invalidated') and 0 <= now-previous['received_ns'] <= self.ttl_ns:
                # A fresh higher head cannot be replaced by a lagging response.
                # A later same-height reorg remains admissible; after TTL a
                # genuinely newer request may recover a lower reorganized head.
                if height < number(previous['anchor']['number']): return False
                if anchor['hash'] == previous['anchor']['hash'] and (
                        number(pending['number']) < number(previous['pending']['number']) or
                        number(pending['timestamp']) < number(previous['pending']['timestamp'])):
                    return False
            self.entries[key] = dict(anchor=anchor, pending=pending,
                                     requested_ns=requested_ns, received_ns=received_ns)
            self.entries.move_to_end(key)
            while len(self.entries) > self.capacity: self.entries.popitem(last=False)
            return True

    def discard(self, url, chain_id, hint):
        """Invalidate only the borrowed generation, not another reader's update."""
        if hint is None: return
        key = self._key(url, chain_id)
        with self.lock:
            if self.entries.get(key) == hint:
                # Keep the request watermark, while requiring a fresh query.
                self.entries[key]['invalidated'] = True

    def close(self):
        with self.lock:
            self.closed = True
            self.entries.clear()
