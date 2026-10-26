import threading
import time
from types import SimpleNamespace
from searcher.strategy_suite import ReadBudget
from searcher.core import Reject
import pytest


def test_fresh_reads_bypass_old_scan_queue_without_starving_it_or_raising_rate():
    budget=ReadBudget(100);budget.next=time.monotonic()+.2
    done=[];lock=threading.Lock();threads=[]
    def run(name,urgent):
        budget.acquire(SimpleNamespace(suite_urgent=urgent,check_cancelled=lambda:None),1)
        with lock:done.append((name,time.monotonic()))
    for name,urgent in [('cold-a',False),('cold-b',False),('hot-a',True),('hot-b',True),('hot-c',True),('hot-d',True)]:
        t=threading.Thread(target=run,args=(name,urgent));threads.append(t);t.start()
        deadline=time.monotonic()+1
        while len(budget.queue)<len(threads) and time.monotonic()<deadline:time.sleep(.001)
    for t in threads:t.join(timeout=2);assert not t.is_alive()
    assert [n for n,_ in done]==['hot-a','hot-b','cold-a','hot-c','hot-d','cold-b']
    assert all(b-a>=.009 for (_,a),(_,b) in zip(done,done[1:]))
    assert not budget.queue


def test_cancelled_priority_ticket_releases_background_reader():
    budget=ReadBudget(100)
    def cancel():raise Reject('cancelled')
    try:budget.acquire(SimpleNamespace(suite_urgent=True,check_cancelled=cancel),1)
    except Reject:pass
    budget.acquire(SimpleNamespace(check_cancelled=lambda:None),1)
    assert not budget.queue and budget.urgent_units==0


def test_chain_identity_rechecked_on_expiry_or_endpoint_change_and_failures_not_cached(monkeypatch):
    from searcher.strategy_suite import SuiteRPC
    from searcher.core import RPC
    import searcher.strategy_suite as module
    now=[100.];calls=[];wrong=[False]
    def verify(rpc):
        calls.append((rpc.url,rpc.chain_id))
        if wrong[0]:raise Reject('wrong chain')
    monkeypatch.setattr(module.time,'monotonic',lambda:now[0]);monkeypatch.setattr(RPC,'verify_chain',verify)
    rpc=SuiteRPC.__new__(SuiteRPC);rpc.url='https://example.invalid/one';rpc.chain_id=8453
    for _ in range(100):rpc.verify_chain()
    assert len(calls)==1
    now[0]+=60;rpc.verify_chain();assert len(calls)==2
    rpc.url='https://example.invalid/two';rpc.verify_chain();assert len(calls)==3
    now[0]+=60;wrong[0]=True
    for _ in range(2):
        with pytest.raises(Reject,match='wrong chain'):rpc.verify_chain()
    assert len(calls)==5
