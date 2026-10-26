"""Select prepared positions against an arriving oracle report, before swap work.

All amounts here are proposals. Only the complete executor call establishes
eligibility, the realized sale proceeds, and the ability to repay.
"""
from itertools import zip_longest
from contextlib import nullcontext
from collections import OrderedDict
from concurrent.futures import Future
import threading
import time


class AuctionScreens:
    """Coalesce twin-auction proposal reads, never executable quotes or bids.

    Keys bind the oracle override, canonical anchor, execution clock and exact
    borrower requests. A completed screen is reusable for just 100 ms. Each
    auction still measures and fully simulates its own authorized operation.
    """
    def __init__(self,ttl=.1,clock=time.monotonic):
        self.lock=threading.Lock();self.entries=OrderedDict();self.ttl=ttl;self.clock=clock

    def read(self,key,operation,timeout):
        with self.lock:
            entry=self.entries.get(key)
            if entry and entry['completed'] is not None and self.clock()-entry['completed']>self.ttl:
                self.entries.pop(key);entry=None
            owner=entry is None
            if owner:
                entry={'future':Future(),'completed':None};self.entries[key]=entry
                while len(self.entries)>4:
                    old=next((k for k,v in self.entries.items() if v['completed'] is not None),None)
                    if old is None:break
                    self.entries.pop(old)
        if owner:
            try:value=operation()
            except BaseException as exc:
                with self.lock:
                    if self.entries.get(key) is entry:self.entries.pop(key)
                entry['future'].set_exception(exc);raise
            with self.lock:entry['completed']=self.clock()
            entry['future'].set_result(value)
        result=entry['future'].result(timeout=max(0,timeout))
        return {id:(p,t,[dict(r) for r in rows]) for id,(p,t,rows) in result.items()},not owner


def select_positions(book, price, observed, lltv, limit=32):
    rows = (book.snapshot() if hasattr(book, 'snapshot') else
            book.proposals(limit) if hasattr(book, 'proposals') else list(observed))
    original = {r['borrower']: r for r in rows}
    # Morpho debt/share exchange rate is shared by the market. The largest
    # observed share position bounds rounding error when repricing cached ones;
    # using a change in a dust position's rounded debt would exaggerate growth.
    reference=max((r for r in observed if r['shares'] and r['debt']),
                  key=lambda r:r['shares'],default=None)
    adjusted = {}
    for row in rows:
        if not row['shares'] or not row['collateral']:
            continue
        debt = ((row['shares'] * reference['debt'] + reference['shares'] - 1) // reference['shares']
                if reference else row['debt'])
        threshold = (debt * 10**18 + lltv - 1) // lltv
        boundary = (threshold * 10**36 + row['collateral'] - 1) // row['collateral']
        adjusted[row['borrower']] = {**row, 'debt': debt, 'boundary': boundary}
    adjusted.update((r['borrower'], r) for r in observed)
    incentive = min(115 * 10**16, 10**36 // (10**18 - 3 * 10**17 * (10**18 - lltv) // 10**18))
    eligible = [r for r in adjusted.values() if r['shares'] and r['collateral'] and price < r['boundary']]
    def potential(row):
        affordable = row['collateral'] * price // 10**36 * 10**18 // incentive
        return min(row['debt'], affordable) * (incentive - 10**18) // 10**18
    eligible.sort(key=lambda r: (-potential(r), -r['boundary'], r['borrower']))
    selected=eligible[:limit]
    if len(eligible)>limit:
        explore=max(1,limit//4);head=eligible[:limit-explore];tail=eligible[limit-explore:]
        with getattr(book,'lock',nullcontext()):
            cursor=getattr(book,'auction_cursor',0)%len(tail)
            book.auction_cursor=cursor+explore
        selected=head+[tail[(cursor+i)%len(tail)] for i in range(explore)]
    return selected, {'known_positions': len(original), 'eligible_hints': len(eligible),
                             'selected_positions': min(limit, len(eligible)),
                             'selection_basis': 'report_price_and_repayable_incentive',
                             'execution_evidence': False}


def reference_borrower(book, fallback):
    rows=book.snapshot() if hasattr(book,'snapshot') else fallback
    return max((r for r in rows if r['shares'] and r['collateral']),
               key=lambda r:r['shares'],default=fallback[0])['borrower']


def interleave(groups):
    """Every borrower group gets a full-size trial before alternate sizes."""
    return [item for row in zip_longest(*groups) for item in row if item is not None]
