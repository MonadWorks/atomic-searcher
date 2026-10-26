"""Persistent account discovery; public logs are hints, current state decides.

Recent pages and historical backfill advance separately. Accounts are never
excluded for being healthy: they remain in the rotating current-state reads.
"""
from pathlib import Path
import json
import time
from eth_abi import decode
from eth_utils import keccak
from .core import ROOT, ZERO, atomic_json, data, number, safe_error, check
from .event_intake import EventRPC, EventIntake
from .morpho_universe import optional_views

EVENTS = {
    'Withdraw(address,address,uint256)': (1,),
    'SupplyCollateral(address,address,address,uint256)': (2,),
    'WithdrawCollateral(address,address,address,uint256)': (1,),
    'TransferCollateral(address,address,address,uint256)': (1, 2),
}
TOPICS = {'0x'+keccak(text=s).hex(): indices for s,indices in EVENTS.items()}


class CompoundAccounts:
    def __init__(self, comets, config, stop, report, path=None, clock=time.monotonic):
        self.comets=set(comets);self.config=config;self.stop=stop;self.report=report;self.clock=clock
        self.path=Path(path) if path is not None else ROOT/'evidence/compound-accounts-8453.json'
        self.accounts={c:{} for c in comets};self.cursor=None;self.backfill=None;self.gaps=[]
        self.offset={};self.hot={};self.next_read=0;self.page_size=100;self.page_successes=0
        try:
            cached=json.loads(self.path.read_text());check(cached['chain_id']==8453,'account catalog chain')
            for c,rows in cached['accounts'].items():
                if c in self.comets:
                    self.accounts[c]={a:int(n) for a,n in rows.items() if self.valid_address(a)}
            self.cursor=cached.get('cursor');self.backfill=cached.get('backfill');self.gaps=cached.get('gaps',[])
            self.page_size=max(8,min(1000,int(cached.get('page_size',100))))
        except FileNotFoundError:pass
        except Exception as exc:self.report(detail='borrower_catalog_reload_failed',catalog_error=safe_error(exc))

    @staticmethod
    def valid_address(a):
        try:return isinstance(a,str) and a.startswith('0x') and len(a)==42 and len(bytes.fromhex(a[2:]))==20 and a!=ZERO
        except ValueError:return False

    def save(self):
        atomic_json(self.path,dict(chain_id=8453,at=time.time(),accounts=self.accounts,
            cursor=self.cursor,backfill=self.backfill,gaps=self.gaps,page_size=self.page_size,
            historical_catalog_complete=self.backfill==0 and not self.gaps))

    def read_page(self,rpc,start,end):
        try:
            logs=rpc.request('eth_getLogs',[{'address':sorted(self.comets),'topics':[list(TOPICS)],
                'fromBlock':hex(start),'toBlock':hex(end)}])
        except Exception as exc:
            if EventIntake.oversized(exc):self.page_size=max(8,self.page_size//2)
            self.page_successes=0
            raise
        check(isinstance(logs,list),'invalid Compound event page')
        hints=[]
        for log in logs:
            c=log['address'].lower();topics=log['topics'];n=number(log['blockNumber'])
            check(c in self.comets and start<=n<=end and topics and topics[0] in TOPICS,
                'Compound event page mismatch')
            if log.get('removed'):continue
            for index in TOPICS[topics[0]]:
                check(len(topics)>index and len(topics[index])==66,'Compound account topic')
                a='0x'+topics[index][-40:].lower()
                if self.valid_address(a):hints.append((c,a,n))
        for c,a,n in hints:self.accounts[c][a]=max(n,self.accounts[c].get(a,0))
        self.page_successes+=1
        if self.page_successes>=4:
            # Fast providers can cover old borrowers without spending weeks
            # in 100-block pages. Timeouts still shrink the next attempt.
            self.page_size=min(1000,self.page_size*2);self.page_successes=0

    def refresh(self,rpc=None):
        if self.clock()<self.next_read or self.stop.is_set():return
        own=rpc is None
        if own:rpc=EventRPC(self.config['policy'].get('identity_rpc_url'),timeout=2,cancel_event=self.stop)
        try:
            head=number(rpc.header()['number'])
            start=max(0,head-self.page_size+1,self.cursor-2 if self.cursor is not None else 0)
            if start>head:start=max(0,head-self.page_size+1)
            self.read_page(rpc,start,head)
            if self.cursor is not None and self.cursor<start:self.gaps.append([self.cursor,start-1])
            self.cursor=head+1
            if self.backfill is None:self.backfill=start
            # One extra historical page, independent of the recent checkpoint.
            if self.gaps:
                lo,hi=self.gaps[0];end=min(hi,lo+self.page_size-1);self.read_page(rpc,lo,end)
                if end==hi:self.gaps.pop(0)
                else:self.gaps[0]=[end+1,hi]
            elif self.backfill>0:
                lo=max(0,self.backfill-self.page_size);self.read_page(rpc,lo,self.backfill-1);self.backfill=lo
            self.report(detail='borrower_catalog_advanced',borrower_identities=sum(map(len,self.accounts.values())),
                borrower_recent_block=head,borrower_backfill_before=self.backfill,catalog_error=None,
                historical_catalog_complete=self.backfill==0 and not self.gaps)
        except Exception as exc:
            self.report(detail='borrower_catalog_retrying',catalog_error=safe_error(exc))
        finally:
            self.save();self.next_read=self.clock()+10
            if own:rpc.close()

    def eligible(self,rpc,comet):
        identities=sorted(self.accounts[comet],key=lambda a:-self.accounts[comet][a])
        if not identities:return []
        offset=self.offset.get(comet,0)%len(identities)
        rotated=identities[offset:]+identities[:offset];self.offset[comet]=offset+24
        # Recently near the borrowing boundary first; every other identity
        # still rotates, including accounts that have been quiet for months.
        hot=self.hot.setdefault(comet,[])
        batch=list(dict.fromkeys([*hot[:8],*rotated[:24]]))
        if hot:self.hot[comet]=hot[8:]+hot[:8]
        values=optional_views(rpc,[(comet,data(s,['address'],[a])) for a in batch for s in
            ('isLiquidatable(address)','isBorrowCollateralized(address)','borrowBalanceOf(address)')])
        eligible=[];unavailable=0
        for i,a in enumerate(batch):
            liquid,healthy,debt=values[3*i:3*i+3]
            if not all(v is not None and len(v)>=32 for v in (liquid,healthy,debt)):
                unavailable+=1;continue
            owed=int.from_bytes(debt,'big')
            near=owed>0 and not decode(['bool'],healthy)[0]
            if near and a not in self.hot[comet]:self.hot[comet].append(a)
            elif not near and a in self.hot[comet]:self.hot[comet].remove(a)
            if owed>0 and decode(['bool'],liquid)[0]:eligible.append((owed,a))
        self.report(detail='borrower_eligibility_checked',market=comet,borrowers_checked=len(batch),
            eligible_borrowers=len(eligible),unread_borrowers=unavailable)
        return [a for _,a in sorted(eligible,reverse=True)[:8]]
