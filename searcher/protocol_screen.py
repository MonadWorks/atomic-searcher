"""Size proposals by quoted USDC proceeds, never by raw token-unit equality.

Advisory quotes can admit work, not authorize transactions. Unknown quotes keep
a rotating bounded full-call audit, and no result permanently excludes a path.
"""
from collections import OrderedDict
import time
from .core import USDC, safe_error
from .route_preparation import quote_paths


class ProtocolScreen:
    def __init__(self, config, report, clock=time.monotonic):
        self.floor = int(config['policy']['min_net_profit_usdc_raw'])
        self.report, self.clock = report, clock
        self.audit_at, self.audit_cursor = OrderedDict(), {}

    def quote(self, rpc, rows):
        values = []
        for start in range(0, len(rows), 24):
            values.extend(quote_paths(rpc, rows[start:start+24]))
        return values

    def compound(self, scanner, comet, asset, loan, sizes):
        proposals = []
        for amount, received in sizes:
            paths = scanner.paths(asset, loan, received, loan_amount=amount)
            for unwind in dict.fromkeys(tuple(a) for a,b in paths):
                proposals.append((unwind, amount, received))
        if not proposals: return []
        unknown, priced, admitted = [], [], []
        error = None
        try:
            outputs = self.quote(scanner.rpc, [(a,n) for a,_,n in proposals])
            for (unwind,amount,received),output in zip(proposals, outputs):
                if scanner.u.route_book: scanner.u.route_book.record(unwind, received, output)
                if not output:
                    unknown.append((unwind,amount,received))
                    continue
                # The unwind output and debt are in the SAME loan-token units.
                surplus = output - amount
                if surplus <= 0: continue
                if loan == USDC:
                    if surplus >= self.floor: admitted.append((unwind,(),amount,surplus))
                    continue
                rb = scanner.u.route_book
                if rb: settlements = rb.ranked(loan, USDC, surplus, 2)
                else:
                    with scanner.u.lock: settlements = scanner.u.graph.direct(loan, USDC)
                for settlement in settlements:
                    priced.append((unwind,tuple(settlement),amount,received,surplus))
            if priced:
                profits = self.quote(scanner.rpc, [(b,n) for _,b,_,_,n in priced])
                for (a,b,amount,received,_),profit in zip(priced, profits):
                    if profit >= self.floor: admitted.append((a,b,amount,profit))
                    elif profit == 0 or set(h[0] for h in a).intersection(h[0] for h in b):
                        # Separate quote calls do not model a reused pool's
                        # post-unwind price. Keep occasional complete audits.
                        unknown.append((a,amount,received))
        except Exception as exc:
            error = safe_error(exc)
            unknown = proposals
        # An unpriced path is not a zero-profit fact. Audit a rotating path,
        # bounded per inventory asset, instead of flooding all strategy lanes.
        key = (comet,asset,loan)
        audit = None
        now = self.clock()
        if unknown and now >= self.audit_at.get(key, 0):
            i = self.audit_cursor.get(key, 0)
            a,amount,received = unknown[i % len(unknown)]
            choices = scanner.paths(asset,loan,received,loan_amount=amount)
            settlements = [b for x,b in choices if tuple(x)==a]
            if settlements:
                audit = (a,tuple(settlements[i % len(settlements)]),amount,None)
                self.audit_at[key] = now+30
                self.audit_cursor[key] = i+1
                self.audit_at.move_to_end(key)
                while len(self.audit_at)>256:
                    old,_=self.audit_at.popitem(last=False);self.audit_cursor.pop(old,None)
        # Strongest expected USDC surplus first; the full call prices actual gas.
        admitted.sort(key=lambda x: -x[3])
        if audit and not any(x[:3]==audit[:3] for x in admitted): admitted.append(audit)
        self.report(state='scanning',detail='inventory_profit_screened',market=comet,asset=asset,loan=loan,
            native_proposals=len(proposals),admitted_proposals=len(admitted),unpriced_proposals=len(unknown),
            complete_audit=bool(audit),screen_error=error,screen_is_execution_evidence=False)
        return [(a,b,amount) for a,b,amount,_ in admitted]
