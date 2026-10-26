"""Bounded hints for where a new real gas validation should start.

Remembering a completed revert skips repeated low-budget probes. It never
stores prices, authorizes execution or supplies a past failure as a new one.
"""
from collections import OrderedDict
import threading
import time


class GasValidationProgress:
    NAMES = frozenset(('TransferFailed', 'V2TransferFailed'))

    def __init__(self, chain_id, owner, *, ttl=3600, capacity=256, clock=None):
        if ttl <= 0 or capacity < 1: raise ValueError('invalid gas progress bounds')
        self.scope = (chain_id, owner.lower())
        self.ttl, self.capacity = ttl, capacity
        self.clock = clock or time.monotonic
        self.records = OrderedDict()
        self.lock = threading.RLock()

    def key(self, target, route_key, amount):
        return (*self.scope, target.lower(), route_key, int(amount))

    def start(self, key, initial, maximum):
        with self.lock:
            item = self.records.get(key)
            if item is not None and item['until'] <= self.clock():
                self.records.pop(key)
                item = None
            start = min(initial, maximum)
            if item is not None:
                # Keep two real current-attempt ceilings available. Use ceil
                # division: floor(maximum/2) needs an extra one-gas final read
                # when the configured maximum is odd (currently 5,217,391).
                # Start at the next unresolved budget. Repeating the last
                # timely failure would stall forever when only one RPC fits
                # each observation window and its following RPC returns late.
                start = min(maximum, max(start, min(item['gas']*2, (maximum+1)//2)))
                self.records.move_to_end(key)
            return start, dict(model_initial_gas=initial, validation_start_gas=start,
                validation_maximum_gas=maximum, remembered_failed_gas=item['gas'] if item else None,
                remembered_revert_name=item['name'] if item else None,
                reused=start > min(initial, maximum),
                scope='new read-only validation starting hint; no cached execution authority')

    def record(self, key, gas, name):
        if name not in self.NAMES or not isinstance(gas, int) or gas <= 0: return
        with self.lock:
            now = self.clock()
            old = self.records.get(key)
            if old is not None and old['until'] > now and old['name'] == name:
                gas = max(gas, old['gas'])
            self.records[key] = dict(gas=gas, name=name, until=now+self.ttl)
            self.records.move_to_end(key)
            while len(self.records) > self.capacity: self.records.popitem(last=False)

    def clear(self, key):
        with self.lock: self.records.pop(key, None)
