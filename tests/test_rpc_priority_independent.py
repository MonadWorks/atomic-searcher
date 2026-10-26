"""Independent quota and batching regressions. All traffic is synthetic."""
import threading
import time

import httpx
import pytest

from searcher.core import PublicRateLimiter, RPC, Reject


def wait_for(predicate, timeout=1):
    deadline = time.monotonic()+timeout
    while not predicate():
        assert time.monotonic() < deadline, 'condition did not arrive'
        time.sleep(.001)


def acquire_thread(limiter, count, *, critical=False, submission=False):
    cancelled, finished = threading.Event(), threading.Event()
    errors = []
    def run():
        try:
            limiter.acquire(count, cancelled=cancelled, critical=critical, submission=submission)
        except Exception as exc:
            errors.append(exc)
        finally:
            finished.set()
    worker = threading.Thread(target=run)
    worker.start()
    return worker, cancelled, finished, errors


def stop_threads(*workers):
    for thread,cancelled,finished,errors in workers:
        cancelled.set()
    for thread,cancelled,finished,errors in workers:
        thread.join(1)
        assert not thread.is_alive()


def test_cancelling_critical_waiter_restores_background_admission():
    limiter = PublicRateLimiter()
    limiter.acquire(12, critical=True)
    critical = acquire_thread(limiter, 10, critical=True)
    background = None
    try:
        wait_for(lambda: limiter.critical_waiters == 1)
        # There is background capacity (12+1<=18), but a queued critical batch
        # must receive it first. Its cancellation must release priority state.
        background = acquire_thread(limiter, 1)
        assert not background[2].wait(.03)
        critical[1].set()
        assert critical[2].wait(.3)
        assert critical[3] and isinstance(critical[3][0], Reject)
        assert limiter.critical_waiters == 0
        assert background[2].wait(.3)
        assert not background[3]
        assert len(limiter.sent) == 13
    finally:
        stop_threads(*(x for x in (critical,background) if x is not None))


def test_waiting_context_gets_the_next_whole_slot_before_background():
    limiter = PublicRateLimiter()
    limiter.acquire(18)
    critical = acquire_thread(limiter, 7, critical=True)
    background = None
    try:
        wait_for(lambda: limiter.critical_waiters == 1)
        background = acquire_thread(limiter, 1)
        assert not critical[2].wait(.02)  # 18+7 exceeds21; no false instant guarantee.
        assert not background[2].is_set()
        with limiter.lock:
            # Four old method units expire. Both1background and7critical now
            # fit individually, but only the queued context can claim them.
            for _ in range(4): limiter.sent.popleft()
        assert critical[2].wait(.3)
        assert not critical[3]
        assert len(limiter.sent) == 21
        assert not background[2].is_set()
        assert limiter.critical_waiters == 0
    finally:
        stop_threads(*(x for x in (critical,background) if x is not None))


def test_submission_keeps_three_units_after_read_limit_and_still_has_a_cap():
    limiter = PublicRateLimiter()
    limiter.acquire(18)
    limiter.acquire(3, critical=True)
    limiter.acquire(3, submission=True, critical=True)
    assert len(limiter.sent) == 24
    extra = acquire_thread(limiter, 1, submission=True, critical=True)
    try:
        wait_for(lambda: limiter.critical_waiters == 1)
        assert not extra[2].wait(.02)
        extra[1].set()
        assert extra[2].wait(.3)
        assert len(limiter.sent) == 24
        assert limiter.critical_waiters == 0
    finally:
        stop_threads(extra)


def test_invalid_or_pre_cancelled_critical_acquire_leaves_no_priority_leak():
    limiter = PublicRateLimiter()
    with pytest.raises(Reject):
        limiter.acquire(22, critical=True)
    cancelled = threading.Event(); cancelled.set()
    with pytest.raises(Reject, match='cancelled'):
        limiter.acquire(7, cancelled, critical=True)
    assert limiter.critical_waiters == 0 and not limiter.sent
    limiter.acquire(18)


@pytest.mark.parametrize('critical,total,expected', [
    (False,31,[18,13]), (True,31,[21,10]),
    (False,3,[3]), (True,7,[7]), (True,10,[10]),
])
def test_batch_order_and_atomic_context_group_survive_quota_chunking(monkeypatch,critical,total,expected):
    rpc = RPC('https://mainnet.base.org',timeout=1)
    rpc.critical_reads = critical
    observed = []
    def post(payload):
        observed.append(payload)
        response = httpx.Response(200,json=[
            {'jsonrpc':'2.0','id':row['id'],'result':row['params'][0]}
            for row in reversed(payload)],request=httpx.Request('POST','https://example.invalid'))
        response.extensions['headers_elapsed_ms'] = 0
        return response
    monkeypatch.setattr(rpc,'post',post)
    calls = [('eth_getBlockByNumber',[hex(i),False]) for i in range(total)]
    try:
        assert rpc.batch(calls) == [hex(i) for i in range(total)]
        assert [len(batch) for batch in observed] == expected
        flattened = [(row['method'],row['params']) for batch in observed for row in batch]
        assert flattened == calls
    finally:
        rpc.close()


def test_pending_lens_and_clock_triplet_is_kept_in_one_http_batch(monkeypatch):
    rpc = RPC('https://mainnet.base.org',timeout=1)
    seen = []
    calls = [('eth_call',[{'to':'0x'+'11'*20,'data':'0x00'},'pending',{},
                          {'number':'0x2','time':'0x3'}]),
             ('eth_getBlockByNumber',['latest',False]),
             ('eth_getBlockByNumber',['pending',False])]
    def post(payload):
        seen.append(payload)
        response = httpx.Response(200,json=[{'jsonrpc':'2.0','id':i,'result':str(i)}
                                           for i in range(3)],
                                  request=httpx.Request('POST','https://example.invalid'))
        response.extensions['headers_elapsed_ms'] = 0
        return response
    monkeypatch.setattr(rpc,'post',post)
    try:
        assert rpc.batch(calls) == ['0','1','2']
        assert len(seen) == 1
        assert [(row['method'],row['params']) for row in seen[0]] == calls
    finally:
        rpc.close()


def test_later_small_critical_request_cannot_bypass_waiting_context_batch():
    limiter = PublicRateLimiter()
    limiter.acquire(12, critical=True)
    context = acquire_thread(limiter, 10, critical=True)
    small = None
    try:
        wait_for(lambda: limiter.critical_waiters == 1)
        small = acquire_thread(limiter, 1, critical=True)
        # 12+1 fits, 12+10 does not: this is the starvation counterexample.
        assert not small[2].wait(.03), 'small critical request bypassed queued context'
        assert not context[2].is_set()
        with limiter.lock:
            limiter.sent.clear()  # deterministic expiration of the occupied window
        assert context[2].wait(.3) and small[2].wait(.3)
        assert not context[3] and not small[3]
        assert limiter.critical_waiters == 0
        assert len(limiter.sent) == 11
    finally:
        stop_threads(*(x for x in (context,small) if x is not None))


def test_cancelled_fifo_head_does_not_hold_the_next_critical_batch():
    limiter = PublicRateLimiter()
    limiter.acquire(12, critical=True)
    first = acquire_thread(limiter, 10, critical=True)
    second = None
    try:
        wait_for(lambda: limiter.critical_waiters == 1)
        second = acquire_thread(limiter, 7, critical=True)
        assert not second[2].wait(.02)
        first[1].set()
        assert first[2].wait(.3) and second[2].wait(.3)
        assert first[3] and not second[3]
        assert limiter.critical_waiters == 0 and not limiter.critical_queue
        assert len(limiter.sent) == 19
    finally:
        stop_threads(*(x for x in (first,second) if x is not None))


def test_submission_uses_reserved_capacity_even_with_a_waiting_context():
    limiter = PublicRateLimiter()
    limiter.acquire(21, critical=True)
    context = acquire_thread(limiter, 7, critical=True)
    submission = None
    try:
        wait_for(lambda: limiter.critical_waiters == 1)
        submission = acquire_thread(limiter, 3, critical=True, submission=True)
        assert submission[2].wait(.3)
        assert not submission[3] and not context[2].is_set()
        assert len(limiter.sent) == 24
        assert limiter.critical_waiters == 1
    finally:
        stop_threads(*(x for x in (context,submission) if x is not None))
    assert limiter.critical_waiters == 0 and not limiter.critical_queue
