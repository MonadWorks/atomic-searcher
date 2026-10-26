"""Prewarming schedules a new observation; it never joins future state backward."""
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from searcher.core import WETH
from searcher.quarantine import DirectionQuarantine
from searcher.routes import Route
from searcher.statefeed import DeepReadScheduler
from searcher.transfer_feedback import TransferFeedback, TransferSample
from test_direction_quarantine import IDENTITY, failure
from test_transfer_feedback import EXECUTOR, OWNER, OTHER_POOL, sample_result
from test_receipt_eligibility import BAD, pools


class Quote:
    def __init__(self, multiplier=2): self.multiplier = multiplier
    def quote(self, token, amount): return amount*self.multiplier, 0


def environment(tmp_path, **kwargs):
    now = [1000.]
    q = DirectionQuarantine(tmp_path/'q.json', IDENTITY, clock=lambda:now[0])
    f = TransferFeedback(EXECUTOR, OWNER, clock=lambda:now[0], quarantine=q, **kwargs)
    route = Route((WETH, BAD, WETH), tuple(p.address for p in pools()[:2]))
    states = {p:Quote() for p in route.pools}
    sample = TransferSample(BAD, *route.pools, 100)
    edge = (BAD, sample.source, EXECUTOR)
    return now, q, f, route, states, sample, edge


def schedule(f, route, states, now, amount=50, delay=30):
    return f.schedule_revalidation(route, states, amount, [WETH], failure(route, now), delay)


def record(f, sample, loss=True):
    f.record(sample, sample_result(sample, funded=sample.amount*95//100 if loss else sample.amount))
    f.finish_sample(sample, True)


def claim(f, route, states, amount=50):
    return f.claim_probe(route, amount=amount, states=states, settlements=[WETH])


def test_late_helper_schedules_new_read_and_only_new_calls_become_witnesses(tmp_path):
    now,q,f,route,states,sample,edge = environment(tmp_path)
    assert claim(f,route,states)
    original = failure(route,now[0])
    assert f.rejected(route,original,amount=50)==0
    assert schedule(f,route,states,now[0])
    now[0]+=10
    record(f,sample)
    assert not q.records  # The helper is later than the failed whole call.
    assert f.revalidation_requests()==[]
    now[0]+=31
    rows=f.revalidation_requests()
    assert len(rows)==1 and rows[0]['route_keys']==[route.key]
    assert rows[0]['deep'] is False
    deep=DeepReadScheduler()
    deep.add(rows,now[0])
    assert deep.take(set(route.pools),now[0])==set(route.pools)
    assert deep.selected_deep_pools==set()  # Complete shallow read, no extra depth budget.
    assert claim(f,route,states)
    new=failure(route,now[0])
    delay=f.rejected(route,new,amount=50)
    assert delay==60 and len(q.records[edge]['witnesses'])==1
    assert q.records[edge]['witnesses'][0]['started_utc']==now[0]
    assert schedule(f,route,states,now[0],delay=delay)
    now[0]+=61
    assert f.revalidation_requests()
    assert claim(f,route,states)
    f.rejected(route,failure(route,now[0]),amount=50)
    assert q.blocked([edge])
    assert not schedule(f,route,states,now[0])
    assert not f.revalidations  # Durable isolation owns recovery from here.


def test_exact_helper_is_pinned_before_claim_not_replaced_by_future_sample(tmp_path):
    now,q,f,route,states,sample,edge=environment(tmp_path)
    record(f,sample)
    old=dict(f.edges[edge]['evidence'])
    now[0]+=31
    assert claim(f,route,states)
    newer=sample_result(sample,funded=80);newer['block']=101
    now[0]+=1;f.record(sample,newer)
    f.rejected(route,failure(route,now[0]),amount=50)
    assert q.records[edge]['sample']['funded']==old['funded']
    assert q.records[edge]['sample']['block']==old['block']


@pytest.mark.parametrize('different',['amount','reverse','pool','intermediate_amount'])
def test_other_identity_or_amount_does_not_consume_sample_as_evidence(tmp_path,different):
    now,q,f,route,states,sample,edge=environment(tmp_path)
    record(f,sample);now[0]+=31
    amount=50
    if different=='amount': amount=51
    if different=='reverse': route=Route(route.tokens,tuple(reversed(route.pools)))
    if different=='pool': route=Route(route.tokens,(OTHER_POOL,route.pools[1]));states[OTHER_POOL]=Quote()
    if different=='intermediate_amount': states[route.pools[0]]=Quote(3)
    assert claim(f,route,states,amount)
    f.rejected(route,failure(route,now[0]),amount=amount)
    assert not q.records


def test_same_amount_changed_state_prewarms_new_size_without_blocking_other_amount(tmp_path):
    now,q,f,route,states,sample,edge=environment(tmp_path)
    assert schedule(f,route,states,now[0])
    now[0]+=31;record(f,sample,loss=False)
    states[route.pools[0]]=Quote(3)
    assert not claim(f,route,states)
    # Different sized healthy candidate has no inherited reservation.
    assert claim(f,route,states,amount=51)
    warmed=f.take()
    assert warmed==TransferSample(*sample.key,150)
    record(f,warmed,loss=False)
    assert claim(f,route,states)
    assert not q.records


def test_clean_helper_only_readies_full_validation_not_quarantine_or_trade(tmp_path):
    now,q,f,route,states,sample,edge=environment(tmp_path)
    assert schedule(f,route,states,now[0])
    now[0]+=31
    assert f.take()==sample
    record(f,sample,loss=False)
    assert f.revalidation_requests()
    assert claim(f,route,states)
    f.rejected(route,failure(route,now[0]),amount=50)
    assert not q.records
    assert f.summary()['prewarm']['claimed']==1


@pytest.mark.parametrize('kind',['deadline','late_success','transport','quote','wrong_sender','wrong_target','gas_single'])
def test_unexecuted_or_inconclusive_failure_does_not_schedule(tmp_path,kind):
    now,q,f,route,states,sample,edge=environment(tmp_path)
    info=failure(route,now[0])
    if kind=='deadline':info['validation_deadline_reached']=True
    if kind=='late_success':info['validation_outcome']='call_succeeded_after_deadline'
    if kind=='transport':info={'cause':'HTTP 429'}
    if kind=='quote':info.pop('execution_probe')
    if kind=='wrong_sender':info['execution_probe']['unsigned_call']['from']=OTHER_POOL
    if kind=='wrong_target':info['execution_probe']['unsigned_call']['to']=OTHER_POOL
    if kind=='gas_single':info.update(revert_name='TransferFailed',gas_attempts=[1])
    assert not f.schedule_revalidation(route,states,50,[WETH],info)
    assert f.take() is None and not f.revalidations


def test_success_clears_reservations_pending_and_invalidates_inflight_evidence(tmp_path):
    now,q,f,route,states,sample,edge=environment(tmp_path)
    assert schedule(f,route,states,now[0])
    now[0]+=31
    assert f.take()==sample
    f.passed(route)
    now[0]+=1
    record(f,sample)
    assert not f.revalidation_requests() and not f.fresh_samples and not q.records
    assert claim(f,route,states)
    f.rejected(route,failure(route,now[0]),amount=50)
    assert not q.records


def test_failed_diagnostic_waits_original_backoff_and_route_wait_is_bounded(tmp_path):
    now,q,f,route,states,sample,edge=environment(tmp_path)
    assert schedule(f,route,states,now[0])
    now[0]+=31
    assert f.take()==sample
    f.finish_sample(sample,False)
    assert not claim(f,route,states)
    assert f.take() is None
    now[0]+=30
    assert f.take()==sample
    f.finish_sample(sample,False)
    now[0]=1091
    assert claim(f,route,states)  # Original whole-call path may retry; no helper authority.
    f.rejected(route,failure(route,now[0]),amount=50)
    assert not q.records
    assert f.summary()['prewarm']['timed_out']==1


def test_two_lanes_cannot_duplicate_ready_dispatch_or_claim(tmp_path):
    now,q,f,route,states,sample,edge=environment(tmp_path)
    assert schedule(f,route,states,now[0])
    now[0]+=31;record(f,sample,loss=False)
    with ThreadPoolExecutor(max_workers=2) as pool:
        dispatched=list(pool.map(lambda _:f.revalidation_requests(),range(2)))
        claims=list(pool.map(lambda _:claim(f,route,states),range(2)))
    assert sum(bool(row) for row in dispatched)==1
    assert sorted(claims)==[False,True]


def test_capacity_ttl_no_network_storm_and_restart_has_no_prevalidated_authority(tmp_path):
    now,q,f,route,states,sample,edge=environment(tmp_path,queue_limit=2,capacity=3)
    for amount in range(50,55):assert schedule(f,route,states,now[0],amount)
    assert len(f.revalidations)==2
    now[0]+=31
    first=f.take();assert first is not None
    assert f.take() is None  # Same source/destination is single flight even for distinct sizes.
    f.finish_sample(first,False)
    assert f.take() is None
    restarted=TransferFeedback(EXECUTOR,OWNER,clock=lambda:now[0],quarantine=q)
    assert not restarted.revalidations and not restarted.fresh_samples
    assert restarted.revalidation_requests()==[]
    now[0]+=4000
    assert f.revalidation_requests()==[] and not f.revalidations
    assert f.summary()['prewarm']['expired']==2


def test_quote_only_repeated_readiness_is_capped_and_never_claims_trial(tmp_path):
    now,q,f,route,states,sample,edge=environment(tmp_path)
    assert schedule(f,route,states,now[0])
    now[0]+=31;record(f,sample,loss=False)
    count=0
    for _ in range(8):
        rows=f.revalidation_requests()
        count+=bool(rows)
        if rows:
            leases=[row['prewarm_lease_id'] for row in rows]
            f.revalidation_queued(leases)
            # A complete new no-profit quote consumes a read opportunity;
            # merely offering or queueing it no longer does.
            f.revalidation_consumed(leases,[route.key],candidates=[])
        now[0]+=31
    assert count==3 and not q.records
    assert f.summary()['prewarm']['claimed']==0


def test_late_failure_handler_cannot_recreate_reservation_after_success(tmp_path):
    now,q,f,route,states,sample,edge=environment(tmp_path)
    original=failure(route,now[0])
    assert claim(f,route,states)
    now[0]+=1
    f.passed(route)
    now[0]+=1
    assert f.rejected(route,original,amount=50)==0
    assert not f.schedule_revalidation(route,states,50,[WETH],original)
    assert not f.revalidations


def test_two_amount_samples_remain_independently_usable(tmp_path):
    now,q,f,route,states,sample,edge=environment(tmp_path)
    record(f,sample)
    record(f,TransferSample(*sample.key,200))
    now[0]+=31
    assert claim(f,route,states)
    f.rejected(route,failure(route,now[0]),amount=50)
    assert q.records[edge]['sample']['amount']==100


def test_real_executor_late_helper_then_new_calls_learn_and_clean_recovery_passes(market):
    import json
    import time
    from searcher.core import ROOT, RPCError, data
    from searcher.execution import Executor, execution_failure_details
    from searcher.transfer_feedback import measure_sample
    rpc,config,contract,signer,directory,index,candidate,_,send=market
    route=index.routes[candidate['route_key']]
    token=route.tokens[1]
    send(token,data('setBad(bool,uint256)',['bool','uint256'],[False,400]))
    identity=dict(IDENTITY,owner=signer.address.lower(),executor=contract)
    now=[1000.]
    q=DirectionQuarantine(directory/'prewarm-q.json',identity,clock=lambda:now[0])
    f=TransferFeedback(contract,signer.address,clock=lambda:now[0],quarantine=q)
    settlements=[row['address'] for row in config['settlement_tokens']]
    executor=Executor(rpc,config,contract,signer,directory)
    executor.transfer_feedback=f
    executor.gas_estimate_context_verified=False
    runtime=json.loads((ROOT/'out/CycleProbe.sol/CycleProbe.json').read_text())['deployedBytecode']['object']
    try:
        assert f.offer(route,index.states,candidate['amount_in'],settlements)==1
        executor.refresh_context()
        with pytest.raises(RPCError) as first:
            executor.execute(candidate,route,index.states,rpc.header(),time.monotonic_ns()+2_000_000_000,submit=False)
        original=execution_failure_details(first.value)
        assert original['revert_name']=='BadCallback' and not q.records
        assert f.schedule_revalidation(route,index.states,candidate['amount_in'],settlements,original,30)
        now[0]+=10
        sample=f.take();assert sample is not None
        f.record(sample,measure_sample(rpc,sample,contract,signer.address,runtime));f.finish_sample(sample,True)
        assert not q.records  # Real helper happened after the original real revert.
        for offset in (31,61):
            now[0]+=offset
            assert f.revalidation_requests()
            # This fixture supplies synthetic CL quote states; its contract
            # lacks bitmap/liquidity getters. Requote those supplied states,
            # then use a newly read real account/header and real whole call.
            # Complete snapshot transport is covered separately by StateFeed.
            import copy
            head=rpc.header()
            fresh=copy.deepcopy(list(index.states.values()))
            fresh_states={p.address:p for p in fresh}
            from searcher.engine import evaluate
            from searcher.routes import RouteIndex
            import copy
            quote_config=copy.deepcopy(config);quote_config['policy']['max_quote_ms']=10_000
            fresh_report=evaluate(RouteIndex(fresh,settlements,config['policy']['max_hops']),quote_config,head)
            current=next(c for c in fresh_report['candidates'] if c['route_key']==route.key)
            assert current['amount_in']==candidate['amount_in']
            executor.refresh_context()
            with pytest.raises(RPCError) as caught:
                executor.execute(current,route,fresh_states,head,time.monotonic_ns()+2_000_000_000,submit=False)
            details=execution_failure_details(caught.value)
            f.schedule_revalidation(route,fresh_states,current['amount_in'],settlements,details,
                                    details.get('transfer_retry_after_seconds',30))
        assert q.summary()['quarantined']>=1
        assert executor.db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0]==0
        send(token,data('setBad(bool,uint256)',['bool','uint256'],[False,0]))
        now[0]+=301
        sample=f.take();assert sample is not None
        f.record(sample,measure_sample(rpc,sample,contract,signer.address,runtime));f.finish_sample(sample,True)
        executor.refresh_context()
        outcome=executor.execute(candidate,route,index.states,rpc.header(),time.monotonic_ns()+2_000_000_000,submit=False)
        assert outcome['signed'] is False and outcome['broadcast'] is False
        assert not q.records and not f.revalidations
    finally: executor.close()


from test_execution import market


def test_incomplete_repriced_depth_never_counts_as_ready_helper(tmp_path):
    now,q,f,route,states,sample,edge=environment(tmp_path)
    assert schedule(f,route,states,now[0])
    now[0]+=31;record(f,sample,loss=False)
    class Missing:
        def quote(self,*args): raise ValueError('incomplete depth')
    states[route.pools[0]]=Missing()
    assert not claim(f,route,states)
    assert f.revalidation_requests()==[]
    assert f.prewarm_summary()['ready']==0
