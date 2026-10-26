"""Economic results and bounded retry scheduling for exact execution probes."""
import time
from .core import Reject


class UnprofitableQuote(Reject):
    def __init__(self, candidate):
        self.gross = int(candidate['gross_surplus'])
        self.fee = int(candidate['model_fee_raw'])
        self.required = int(candidate['minimum_gross_surplus'])
        super().__init__('complete quote below floor')

    def evidence(self):
        result = {'status': 'not_profitable', 'gross_surplus': self.gross,
                'fee_reservation': self.fee, 'required_gross_surplus': self.required,
                'net_after_fee_reservation': self.gross-self.fee,
                'evidence': 'whole_execution_measurement', 'submitted': False}
        if hasattr(self, 'anchor_refresh'): result['anchor_refresh'] = self.anchor_refresh
        return result


class ProbeSchedule:
    """A result belongs to one route AND amount, never to an entire market.

    Changed observed state bypasses cooldown. Unobserved changes (including
    liquidity outside the current tick) get another probe within 30 seconds.
    Transport and opaque contract failures retain the short ordinary retry.
    """
    def __init__(self, clock=time.monotonic, capacity=512):
        self.clock, self.capacity, self.entries = clock, capacity, {}

    @staticmethod
    def key(route, amount):
        return route.key, int(amount)

    def ready(self, route, amount, epoch):
        key = self.key(route, amount)
        prior = self.entries.get(key)
        if prior is None:
            return True
        if prior['epoch'] != epoch:
            self.entries.pop(key, None)
            return True
        return self.clock() >= prior['until']

    def reject(self, route, amount, epoch, *, economic=False, refresh=False):
        key = self.key(route, amount)
        prior = self.entries.pop(key, None)
        streak = (prior['streak'] if prior and prior['epoch'] == epoch else 0)+1 if economic else 0
        delay = min(30, 5 * 2**min(3, streak-1)) if economic else .05 if refresh else 2
        self.entries[key] = {'epoch': epoch, 'streak': streak, 'until': self.clock()+delay}
        while len(self.entries) > self.capacity:
            self.entries.pop(next(iter(self.entries)))
        return delay

    def clear(self, route, amount):
        self.entries.pop(self.key(route, amount), None)
