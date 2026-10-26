from concurrent.futures import ThreadPoolExecutor
import pytest

from searcher.core import WETH
from searcher.transfer_feedback import TransferFeedback
from test_transfer_feedback import EXECUTOR, OWNER
from test_transfer_prewarm import environment, schedule, record, claim, Quote


def ready(tmp_path):
    now,q,f,route,states,sample,edge=environment(tmp_path)
    assert schedule(f,route,states,now[0])
    now[0]+=31
    record(f,sample,loss=False)
    return now,q,f,route,states,sample,edge


def offer(f):
    rows=f.revalidation_requests()
    assert len(rows)==1
    return rows[0],rows[0]['prewarm_lease_id']


def test_queue_rejects_and_evictions_never_spend_three_real_chances(tmp_path):
    now,q,f,route,states,sample,edge=ready(tmp_path)
    old=[]
    for i in range(8):
        row,lease=offer(f);old.append(lease)
        assert row['prewarm_amount']==50 and row['prewarm_route_key']==route.key
        assert f.prewarm_summary()['consumed']==0
        assert f.revalidation_queued([lease])==1
        assert f.revalidation_released([lease],'queue_evicted')==1
        now[0]+=2
    assert len(set(old))==8
    assert next(iter(f.revalidations.values()))['dispatches']==0
    row,lease=offer(f)
    f.revalidation_queued([lease])
    assert f.revalidation_consumed(old,[route.key])==0
    assert f.revalidation_consumed([lease],[route.key])==1
    assert f.revalidation_consumed([lease],[route.key])==0
    assert f.prewarm_summary()['consumed']==1


def test_offer_cannot_be_acked_without_acceptance_and_unfinished_quote_keeps_budget(tmp_path):
    now,q,f,route,states,sample,edge=ready(tmp_path)
    row,lease=offer(f)
    assert f.revalidation_consumed([lease],[route.key])==0
    f.revalidation_queued([lease])
    assert f.revalidation_consumed([lease],[])==0
    assert f.revalidation_consumed([lease],['not-the-route'])==0
    assert f.revalidation_lease_ids()=={lease}
    assert f.revalidation_consumed([lease],[route.key])==1


def test_queued_heartbeat_survives_wait_but_lost_handoff_expires_without_consumption(tmp_path):
    now,q,f,route,states,sample,edge=ready(tmp_path)
    row,lease=offer(f)
    f.revalidation_queued([lease])
    for _ in range(4):
        now[0]+=31
        assert f.revalidation_queued([lease])==1
        assert not f.revalidation_requests()
    assert f.prewarm_summary()['queued']==1
    now[0]+=61
    assert f.revalidation_lease_ids()==set()
    now[0]+=2
    _,next_lease=offer(f)
    assert next_lease!=lease and f.prewarm_summary()['consumed']==0
    assert f.revalidation_consumed([lease],[route.key])==0


def test_three_completed_new_quotes_limit_reads_and_no_profit_can_count(tmp_path):
    now,q,f,route,states,sample,edge=ready(tmp_path)
    for _ in range(3):
        _,lease=offer(f)
        f.revalidation_queued([lease])
        assert f.revalidation_consumed([lease],[route.key],candidates=[],states=states,settlements=[WETH])==1
        now[0]+=31
    assert f.revalidation_requests()==[]
    assert f.prewarm_summary()['consumed']==3
    assert not q.records


def test_new_positive_amount_or_intermediate_does_not_use_old_chance(tmp_path):
    now,q,f,route,states,sample,edge=ready(tmp_path)
    _,lease=offer(f);f.revalidation_queued([lease])
    candidate={'route_key':route.key,'amount_in':51}
    assert f.revalidation_consumed([lease],[route.key],candidates=[candidate],states=states,settlements=[WETH])==0
    now[0]+=2
    _,lease=offer(f);f.revalidation_queued([lease])
    states[route.pools[0]]=Quote(3)
    candidate['amount_in']=50
    assert f.revalidation_consumed([lease],[route.key],candidates=[candidate],states=states,settlements=[WETH])==0
    assert f.prewarm_summary()['consumed']==0
    assert f.revalidation_requests()==[]  # Needs the actual new helper size.
    newer=f.take();assert newer.amount==150
    record(f,newer,loss=False);now[0]+=2
    _,lease=offer(f);f.revalidation_queued([lease])
    assert f.revalidation_consumed([lease],[route.key],candidates=[candidate],states=states,settlements=[WETH])==1


def test_success_generation_restart_and_concurrency_cannot_spend_old_lease(tmp_path):
    now,q,f,route,states,sample,edge=ready(tmp_path)
    _,lease=offer(f);f.revalidation_queued([lease])
    f.passed(route)
    assert f.revalidation_consumed([lease],[route.key])==0

    restarted=TransferFeedback(EXECUTOR,OWNER,clock=lambda:now[0],quarantine=q)
    assert restarted.revalidation_queued([lease])==0
    now[0]+=301
    assert schedule(f,route,states,now[0])
    now[0]+=31;record(f,sample,loss=False)
    _,newlease=offer(f);f.revalidation_queued([newlease])
    assert newlease!=lease
    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes=list(pool.map(lambda _:f.revalidation_consumed([newlease],[route.key]),range(2)))
    assert sorted(outcomes)==[0,1]
    assert f.revalidation_consumed([lease],[route.key])==0


def test_fixed_principal_requires_current_accepted_lease_and_fresh_helper(tmp_path):
    now,q,f,route,states,sample,edge=ready(tmp_path)
    _,lease=offer(f)
    assert f.revalidation_amounts([lease])=={}
    f.revalidation_queued([lease])
    assert f.revalidation_amounts([lease])=={route.key:50}
    # A newly optimized amount cannot silently redefine the fixed reservation.
    for new_amount in (51,52,53):
        assert f.revalidation_amounts([lease])=={route.key:50}
        assert new_amount != f.revalidation_amounts([lease])[route.key]
    now[0]+=301
    assert f.revalidation_amounts([lease])=={}


def test_same_direction_two_principals_only_actual_quoted_amount_is_consumed(tmp_path):
    now,q,f,route,states,sample,edge=ready(tmp_path)
    _,first=offer(f);f.revalidation_queued([first])
    assert schedule(f,route,states,now[0],amount=51)
    now[0]+=31
    from searcher.transfer_feedback import TransferSample
    second_sample=TransferSample(sample.token,sample.source,sample.destination,102)
    record(f,second_sample,loss=False)
    _,second=offer(f);f.revalidation_queued([second])
    amounts=f.revalidation_amounts([first,second])
    assert amounts=={route.key:50}
    assert f.revalidation_consumed([first,second],[route.key],candidates=[],quoted_amounts=amounts)==1
    assert f.revalidations[(route.key,50)]['dispatches']==1
    assert f.revalidations[(route.key,51)]['dispatches']==0
    now[0]+=2
    _,next_lease=offer(f);f.revalidation_queued([next_lease])
    assert f.revalidation_amounts([next_lease])=={route.key:51}


def test_reader_and_unacknowledged_frame_hold_is_bounded_and_cannot_resurrect(tmp_path):
    now,q,f,route,states,sample,edge=ready(tmp_path)
    _,lease=offer(f)
    assert f.revalidation_held([lease])==0  # Not yet accepted.
    f.revalidation_queued([lease])
    assert f.revalidation_held([lease])==1
    now[0]+=90
    assert f.revalidation_lease_ids()=={lease}
    assert f.prewarm_summary(include_rows=True)['rows'][0]['lease_held'] is True
    assert f.revalidation_held([])==0  # Returned to ordinary queue timeout.
    now[0]+=59
    assert f.revalidation_lease_ids()=={lease}
    assert f.revalidation_released([lease],'read_failed')==1
    assert f.revalidation_held([lease])==0
    now[0]+=2
    _,next_lease=offer(f);f.revalidation_queued([next_lease]);f.revalidation_held([next_lease])
    now[0]+=4000
    assert f.revalidation_lease_ids()==set()
    assert f.revalidation_held([next_lease,lease])==0


def test_exact_bad_first_direction_survives_unrelated_healthy_downstream_size_drift(tmp_path):
    from searcher.routes import Route
    from test_direction_quarantine import failure
    now,q,f,old_route,_,_,_=environment(tmp_path)
    tokens=(WETH,old_route.tokens[1],'0x'+'a2'*20,'0x'+'a3'*20,WETH)
    pools=old_route.pools+('0x'+'b3'*20,'0x'+'b4'*20)
    route=Route(tokens,pools)
    states={pool:Quote() for pool in pools}
    assert schedule(f,route,states,now[0])
    first,second,third=f.modeled_samples(route,states,50,[WETH])
    now[0]+=10
    record(f,first,loss=True)
    record(f,second,loss=False)
    record(f,third,loss=False)
    now[0]+=31
    for multiplier in (3,4):
        _,lease=offer(f);f.revalidation_queued([lease])
        assert f.revalidation_amounts([lease])=={route.key:50}
        states[pools[2]]=Quote(multiplier)
        candidate={'route_key':route.key,'amount_in':50}
        assert f.revalidation_consumed([lease],[route.key],candidates=[candidate],states=states,
            settlements=[WETH],quoted_amounts={route.key:50})==1
        assert claim(f,route,states)
        assert set(f.claimed_samples[(route.key,50)]['items'])=={(first.token,first.source,EXECUTOR)}
        assert f.rejected(route,failure(route,now[0]),amount=50)>0
        if multiplier==3:
            assert schedule(f,route,states,now[0],delay=60)
            now[0]+=61
    assert q.blocked([(first.token,first.source,EXECUTOR)])
    assert len(q.records)==1, 'A changed unsampled healthy edge is not a witness.'


@pytest.mark.parametrize('evidence_kind',['exact_bad','clean','wrong_amount','wrong_direction','expired'])
def test_partial_readiness_requires_an_exact_fresh_anomaly(tmp_path,evidence_kind):
    from dataclasses import replace
    from searcher.routes import Route
    now,q,f,old_route,_,_,_=environment(tmp_path)
    route=Route((WETH,old_route.tokens[1],'0x'+'a2'*20,WETH),
                old_route.pools+('0x'+'b3'*20,))
    states={pool:Quote() for pool in route.pools}
    assert schedule(f,route,states,now[0])
    first,_=f.modeled_samples(route,states,50,[WETH])
    evidence=first
    if evidence_kind=='wrong_amount':evidence=replace(first,amount=101)
    if evidence_kind=='wrong_direction':evidence=replace(first,source=first.destination,destination=first.source)
    now[0]+=10
    record(f,evidence,loss=evidence_kind!='clean')
    now[0]+=301 if evidence_kind=='expired' else 31
    if evidence_kind=='exact_bad':
        _,lease=offer(f);f.revalidation_queued([lease])
        assert f.revalidation_amounts([lease])=={route.key:50}
    else:
        assert f.revalidation_requests()==[]
    assert not q.records, 'Readiness alone never creates an executor failure witness.'


def test_production_receive_delivers_crowded_warm_lease_to_actual_snapshot_consumer(tmp_path,monkeypatch):
    """Real queue/StateFeed/strategy/ACK; deterministic local RPC state only."""
    import copy
    import threading
    import searcher.pipeline as pipeline
    import searcher.discovery_reads as discovery_reads
    import searcher.transfer_feedback as transfer_module
    import searcher.quarantine_runtime as quarantine_runtime
    from searcher.core import load_config
    from searcher.routes import Route
    from searcher.statefeed import DeepReadScheduler
    from test_receipt_eligibility import pools,HEADER
    from test_ordinary_feed_slices import RecordedRPC
    from test_direction_quarantine import failure,IDENTITY
    from searcher.quarantine import DirectionQuarantine
    config=load_config();config['v4']={'enabled':False};config['_diagnostic_executor']=EXECUTOR
    config['owner']=OWNER
    config['policy'].update(hot_lane_enabled=False,activity_priority=False,cycle_pages=True,
        max_quote_ms=1000,snapshot_encoding='packed-v1',universe_refresh_seconds=100,
        pending_poll_interval_seconds=.001)
    config['settlement_tokens']=[dict(next(t for t in config['settlement_tokens'] if t['address']==WETH))]
    ps=pools()[2:];route=Route((WETH,ps[0].token1,WETH),tuple(p.address for p in ps))
    states={p.address:p for p in ps};now=[1000.];stop=threading.Event()
    q=DirectionQuarantine(tmp_path/'q.json',IDENTITY,clock=lambda:now[0])
    feedback=TransferFeedback(EXECUTOR,OWNER,clock=lambda:now[0],quarantine=q)
    assert feedback.schedule_revalidation(route,states,10**15,[WETH],failure(route,now[0]),30)
    now[0]+=10
    for sample in feedback.modeled_samples(route,states,10**15,[WETH]):record(feedback,sample,loss=False)
    now[0]+=31
    fixture={'snapshot_pools':[p.json() for p in ps],'header':HEADER}
    observations=[];reads=[]
    class LocalRPC(RecordedRPC):
        def __init__(self,*args,**kwargs):
            super().__init__(copy.deepcopy(fixture));self.url='http://local.invalid';self.chain_id=8453
        def verify_chain(self):pass
        def close(self):pass
        def batch(self,calls):
            result=super().batch(calls)
            if calls[0][0]=='eth_call':reads.append(tuple(self.reads[-1][0]))
            return result
    class Crowded(DeepReadScheduler):
        def __init__(self):
            super().__init__()
            self.add([dict(pools=('0x'+f'{10000+2*i:040x}','0x'+f'{10001+2*i:040x}'),score=1)
                      for i in range(128)],900)
    class Discovery:
        def __init__(self,*args,**kwargs):pass
        def close(self):pass
    original=pipeline.acknowledge_prewarm
    def acknowledge(strategy,frame,report,settlements):
        original(strategy,frame,report,settlements)
        if report.get('prewarm_read_ack'):
            observations.append((copy.deepcopy(frame.transport),copy.deepcopy(report)))
            stop.set()
    monkeypatch.setattr(pipeline,'RPC',LocalRPC)
    monkeypatch.setattr(pipeline,'DeepReadScheduler',Crowded)
    monkeypatch.setattr(pipeline,'acknowledge_prewarm',acknowledge)
    monkeypatch.setattr(discovery_reads,'DiscoveryReads',Discovery)
    monkeypatch.setattr(quarantine_runtime,'open_quarantine',lambda *a,**k:q)
    monkeypatch.setattr(transfer_module,'TransferFeedback',lambda *a,**k:feedback)
    monkeypatch.setattr(feedback,'run',lambda *a,**k:stop.wait(5))
    result=pipeline.run_pending(LocalRPC(),config,
        {'header':HEADER,'pools':fixture['snapshot_pools']},5,tmp_path/'pipeline',stop_event=stop)
    assert stop.is_set() and not result['worker_errors']
    assert observations and set(route.pools) in [set(row) for row in reads]
    transport,report=observations[0]
    assert any(DeepReadScheduler.lease_ids(row) for row in transport['requested_groups'])
    assert report['prewarm_quoted_amounts'][route.key]==10**15
    assert feedback.prewarm_summary()['consumed']==1
    assert not feedback.revalidation_lease_ids() and not q.records
