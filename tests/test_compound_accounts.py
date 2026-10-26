import threading
from eth_abi import encode
import pytest
from searcher.compound_accounts import CompoundAccounts,TOPICS
from searcher.core import Reject

C='0x'+'11'*20;A='0x'+'22'*20;B='0x'+'33'*20
TOPIC=next(iter(TOPICS))

def log(a,block,c=C):
    return dict(address=c,topics=[TOPIC,'0x'+a[2:].rjust(64,'0'),'0x'+'00'*32],blockNumber=hex(block))

class RPC:
    def __init__(self,head=1000,fail=None):self.head=head;self.fail=fail;self.queries=[]
    def header(self):return {'number':hex(self.head)}
    def request(self,method,params):
        q=params[0];lo=int(q['fromBlock'],16);hi=int(q['toBlock'],16);self.queries.append((lo,hi))
        if self.fail and self.fail(lo,hi):raise RuntimeError('page unavailable')
        return [log(A,hi)]

def catalog(tmp_path):return CompoundAccounts([C],{'policy':{}},threading.Event(),lambda **v:None,path=tmp_path/'accounts.json',clock=lambda:1000)

def test_failed_backfill_keeps_recent_progress_and_survives_restart(tmp_path):
    c=catalog(tmp_path);r=RPC(fail=lambda lo,hi:hi<1000);c.refresh(r)
    assert c.cursor==1001 and c.backfill==901 and A in c.accounts[C]
    restored=catalog(tmp_path)
    assert restored.cursor==1001 and restored.backfill==901 and A in restored.accounts[C]
    r=RPC(head=1300);restored.refresh(r)
    assert r.queries==[(1201,1300),(1001,1100)]
    assert restored.cursor==1301 and restored.gaps==[[1101,1200]] and restored.backfill==901

def test_recent_failure_does_not_claim_coverage(tmp_path):
    c=catalog(tmp_path);c.refresh(RPC(fail=lambda *args:True))
    assert c.cursor is None and not c.accounts[C]

def test_expensive_page_shrinks_without_advancing_cursor_or_retry_fanout(tmp_path):
    import httpx
    c=catalog(tmp_path);r=RPC();calls=[]
    def fail(method,params):
        calls.append(params)
        raise httpx.ReadTimeout('slow log page')
    r.request=fail;c.refresh(r)
    assert c.page_size==50 and c.cursor is None and len(calls)==1
    c.next_read=0;r=RPC();c.refresh(r)
    assert r.queries==[(951,1000),(901,950)] and c.cursor==1001 and c.backfill==901

def test_out_of_scope_page_cannot_add_accounts(tmp_path):
    c=catalog(tmp_path);r=RPC();r.request=lambda *args:[log(A,1000,c=B)]
    with pytest.raises(Reject):c.read_page(r,901,1000)
    assert not c.accounts[C]

def test_healthy_accounts_keep_rotating_and_newly_eligible_is_selected(tmp_path,monkeypatch):
    import searcher.compound_accounts as module
    c=catalog(tmp_path);addresses=['0x'+format(i+1,'040x') for i in range(70)]
    c.accounts[C]={a:i for i,a in enumerate(addresses)};seen=set();eligible=set()
    def views(rpc,calls):
        values=[]
        for i in range(0,len(calls),3):
            a='0x'+calls[i][1][-40:];seen.add(a)
            values.extend([encode(['bool'],[a in eligible]),encode(['bool'],[a not in eligible]),encode(['uint256'],[10**6])])
        return values
    monkeypatch.setattr(module,'optional_views',views)
    for _ in range(3):assert c.eligible(None,C)==[]
    assert seen==set(addresses)
    eligible.add(addresses[0]);picked=[]
    for _ in range(3):picked.extend(c.eligible(None,C))
    assert addresses[0] in picked and addresses[0] in c.hot[C]

def test_unknown_state_is_not_liquidatable(tmp_path,monkeypatch):
    import searcher.compound_accounts as module
    c=catalog(tmp_path);c.accounts[C][A]=1
    monkeypatch.setattr(module,'optional_views',lambda *args:[None,None,None])
    assert c.eligible(None,C)==[] and A in c.accounts[C]

def test_backfill_expands_on_success_persists_and_shrinks_on_timeout(tmp_path):
    import httpx
    c=catalog(tmp_path);r=RPC(head=100_000)
    for _ in range(40):c.read_page(r,90_001,100_000)
    assert c.page_size==1000
    c.cursor=100_001;c.backfill=90_001;c.save()
    restored=catalog(tmp_path)
    assert restored.page_size==1000
    def fail_old(lo,hi):
        if hi<100_000:raise httpx.ReadTimeout('historical page too slow')
        return False
    restored.refresh(RPC(head=100_010,fail=fail_old))
    assert restored.cursor==100_011 and restored.backfill==90_001
    assert restored.page_size==500
    restored.next_read=0;rpc=RPC(head=100_011);restored.refresh(rpc)
    assert rpc.queries[-1]==(89_501,90_000)
    assert restored.backfill==89_501
