"""Independent warmed-read admission/consumption regression. Local fixtures only."""
from searcher.statefeed import DeepReadScheduler
from test_transfer_prewarm import environment, schedule, record


def ready_case(tmp_path):
    now, quarantine, feedback, route, states, sample, edge = environment(tmp_path)
    assert schedule(feedback, route, states, now[0])
    now[0] += 10
    record(feedback, sample)
    now[0] = 1041.
    return now, quarantine, feedback, route, states, sample, edge


def crowded_scheduler():
    scheduler = DeepReadScheduler()
    scheduler.add([
        {'pools': [f'0x{10000 + 2*i:040x}', f'0x{10001 + 2*i:040x}'], 'score': 1}
        for i in range(128)
    ], 900.)
    return scheduler


def test_full_pending_queue_cannot_spend_unperformed_prewarm_reads(tmp_path):
    now, _, feedback, route, _, _, _ = ready_case(tmp_path)
    scheduler = crowded_scheduler()
    for stamp in (1041., 1072., 1103., 1134.):
        now[0] = stamp
        offered = feedback.revalidation_requests()
        scheduler.add(offered, now[0])
    # No take(), state read, or consumer ran. Queue offers cannot be completed
    # work even if the queue later evicts or has not yet admitted the group.
    assert feedback.prewarm_summary()['dispatched'] == 0
    assert feedback.revalidations[(route.key, 50)]['dispatches'] == 0


def lease(feedback):
    offered = feedback.revalidation_requests()
    assert len(offered) == 1
    return offered[0], offered[0]['prewarm_lease_id']


def test_queue_ownership_heartbeat_does_not_count_as_completed_read(tmp_path):
    now, _, feedback, route, _, _, _ = ready_case(tmp_path)
    _, lease_id = lease(feedback)
    for stamp in (1041., 1070., 1099., 1128.):
        now[0] = stamp
        assert feedback.revalidation_queued([lease_id]) == 1
        assert feedback.revalidation_requests() == []
    assert feedback.prewarm_summary()['dispatched'] == 0
    assert feedback.revalidation_consumed([lease_id], [route.key], candidates=[]) == 1
    assert feedback.revalidation_consumed([lease_id], [route.key], candidates=[]) == 0
    assert feedback.prewarm_summary()['dispatched'] == 1


def test_expired_and_replaced_lease_ack_cannot_consume_new_generation(tmp_path):
    now, _, feedback, route, _, _, _ = ready_case(tmp_path)
    _, old_id = lease(feedback)
    now[0] += feedback.queue_seconds + 1
    assert feedback.revalidation_consumed([old_id], [route.key]) == 0
    now[0] += 2
    _, new_id = lease(feedback)
    assert old_id != new_id
    assert feedback.revalidation_queued([old_id]) == 0
    assert feedback.revalidation_released([old_id], 'read_failed') == 0
    assert feedback.revalidation_consumed([old_id], [route.key]) == 0
    assert feedback.revalidation_lease_ids() == {new_id}
    assert feedback.revalidation_queued([new_id]) == 1
    assert feedback.revalidation_consumed([new_id], [route.key], candidates=[]) == 1


def test_new_actual_failure_replaces_reservation_without_accepting_old_ack(tmp_path):
    now, _, feedback, route, states, _, _ = ready_case(tmp_path)
    _, old_id = lease(feedback)
    now[0] += 1
    assert schedule(feedback, route, states, now[0])
    now[0] += 31
    _, new_id = lease(feedback)
    assert old_id != new_id
    assert feedback.revalidation_consumed([old_id], [route.key]) == 0
    assert feedback.revalidation_lease_ids() == {new_id}
    assert feedback.prewarm_summary()['dispatched'] == 0


def test_read_failure_release_retries_without_consuming_or_leaking_old_ids(tmp_path):
    now, _, feedback, route, _, _, _ = ready_case(tmp_path)
    ids = []
    for _ in range(6):
        _, lease_id = lease(feedback)
        assert lease_id not in ids
        ids.append(lease_id)
        assert feedback.revalidation_queued([lease_id]) == 1
        assert feedback.revalidation_released([lease_id], 'read_failed') == 1
        assert feedback.revalidation_consumed([lease_id], [route.key]) == 0
        now[0] += 2
    assert feedback.prewarm_summary()['dispatched'] == 0
    assert feedback.revalidations[(route.key, 50)]['dispatches'] == 0


def test_different_direction_unfinished_or_repriced_quote_cannot_spend_lease(tmp_path):
    _, _, feedback, route, states, _, _ = ready_case(tmp_path)
    _, lease_id = lease(feedback)
    assert feedback.revalidation_consumed([lease_id], [route.key]) == 0, 'Unaccepted offers cannot be acknowledged.'
    assert feedback.revalidation_queued([lease_id]) == 1
    for completed in ([], ['unrelated-route']):
        assert feedback.revalidation_consumed([lease_id], completed) == 0
        assert feedback.revalidation_lease_ids() == {lease_id}
    assert feedback.revalidation_consumed([lease_id], [route.key],
        candidates=[{'route_key':route.key, 'amount_in':51}], states=states,
        settlements=[route.tokens[0]]) == 0
    assert not feedback.revalidation_lease_ids()
    assert feedback.prewarm_summary()['dispatched'] == 0


def test_actual_pass_and_parallel_duplicate_consumers_cannot_reuse_ack(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    _, _, feedback, route, _, _, _ = ready_case(tmp_path)
    _, lease_id = lease(feedback)
    assert feedback.revalidation_queued([lease_id]) == 1
    with ThreadPoolExecutor(2) as pool:
        consumed = list(pool.map(lambda _:feedback.revalidation_consumed(
            [lease_id], [route.key], candidates=[]), range(2)))
    assert sorted(consumed) == [0, 1]
    feedback.passed(route)
    assert feedback.revalidation_consumed([lease_id], [route.key]) == 0
    assert not feedback.revalidations


def test_accepted_warm_survives_repeated_new_depth_pressure_until_selected(tmp_path):
    now, _, feedback, route, _, _, _ = ready_case(tmp_path)
    scheduler = crowded_scheduler()
    row, lease_id = lease(feedback)
    result = scheduler.add([row], now[0])
    assert result['accepted'] == [lease_id] and not result['rejected']
    assert feedback.revalidation_queued(result['accepted']) == 1
    for turn in range(5):
        scheduler.add([
            {'pools':[f'0x{20000+turn*1000+2*i:040x}',
                      f'0x{20001+turn*1000+2*i:040x}'], 'score':10**9}
            for i in range(160)
        ], now[0]+turn)
        assert lease_id in scheduler.prewarm_lease_ids()
        assert len(scheduler.pending) <= 128
    available = {a for group in scheduler.pending for a in group} | set(route.pools)
    scheduler.take(available, now[0]+5)
    assert any(lease_id in scheduler.lease_ids(r) for r in scheduler.selected_requests)
    assert len(scheduler.selected_requests) <= 12
    assert feedback.prewarm_summary()['dispatched'] == 0, 'Selection is not completed consumption.'


def test_continuously_returning_old_work_cannot_starve_warmed_route(tmp_path):
    now, _, feedback, route, _, _, _ = ready_case(tmp_path)
    scheduler = DeepReadScheduler()
    old = [{'pools':(f'0x{30000+2*i:040x}', f'0x{30001+2*i:040x}'),
            'route_keys':[f'old-{i}'], 'deep':True} for i in range(12)]
    assert scheduler.resume(old,900.) == []
    row, lease_id = lease(feedback)
    assert scheduler.add([row],now[0])['accepted'] == [lease_id]
    available = {a for r in old for a in r['pools']} | set(route.pools)
    found = False
    for turn in range(4):
        scheduler.take(available,now[0]+turn)
        selected = list(scheduler.selected_requests)
        found = found or any(lease_id in scheduler.lease_ids(r) for r in selected)
        returning = [r for r in selected if lease_id not in scheduler.lease_ids(r)]
        assert scheduler.resume(returning,now[0]+turn+.1) == []
        assert len(scheduler.resuming) == len(old), 'Every old continuation retains its return slot.'
        if found: break
    assert found, 'Twelve returning groups must not monopolize every available read forever.'


def test_full_return_capacity_does_not_delete_or_charge_warm_promise(tmp_path):
    now, _, feedback, route, _, _, _ = ready_case(tmp_path)
    scheduler=DeepReadScheduler()
    old=[{'pools':(f'0x{40000+2*i:040x}',f'0x{40001+2*i:040x}'),
          'route_keys':[f'old-{i}'],'deep':True} for i in range(1024)]
    assert scheduler.resume(old,900.) == []
    row, lease_id=lease(feedback)
    assert scheduler.add([row],now[0])['accepted']==[lease_id]
    available={a for r in old for a in r['pools']} | set(route.pools)
    scheduler.take(available,now[0])
    selected=list(scheduler.selected_requests)
    assert len(selected)<=12 and len({a for r in selected for a in r['pools']})<=24
    # Neither old return slots nor the warm lease may disappear even if both
    # workloads need another attempt after this read.
    assert scheduler.resume(selected,now[0]+1)==[]
    assert len(scheduler.resuming)==1024
    assert lease_id in scheduler.prewarm_lease_ids()
    assert feedback.prewarm_summary()['dispatched']==0


def snapshot_case(tmp_path, amount=10**15):
    from searcher.core import load_config, WETH
    from searcher.pipeline import PendingStrategy
    from searcher.routes import Route
    from searcher.quarantine import DirectionQuarantine
    from searcher.transfer_feedback import TransferFeedback
    from test_direction_quarantine import IDENTITY, failure
    from test_transfer_feedback import EXECUTOR, OWNER
    from test_receipt_eligibility import pools, HEADER
    config=load_config()
    config['policy'].update(max_quote_ms=1000, sizing_mode='fixed', max_hops=2)
    config['settlement_tokens']=[dict(next(t for t in config['settlement_tokens'] if t['address']==WETH),
                                      max_input_raw=str(10**18))]
    ps=pools()[2:]
    route=Route((WETH,ps[0].token1,WETH),tuple(p.address for p in ps))
    states={p.address:p for p in ps}
    now=[1000.]
    quarantine=DirectionQuarantine(tmp_path/'snapshot-q.json',IDENTITY,clock=lambda:now[0])
    feedback=TransferFeedback(EXECUTOR,OWNER,clock=lambda:now[0],quarantine=quarantine)
    assert feedback.schedule_revalidation(route,states,amount,[WETH],failure(route,now[0]),30)
    now[0]+=10
    for sample in feedback.modeled_samples(route,states,amount,[WETH]): record(feedback,sample)
    now[0]=1041.
    row,lease_id=lease(feedback)
    scheduler=DeepReadScheduler()
    admitted=scheduler.add([row],now[0])
    assert admitted['accepted']==[lease_id]
    assert feedback.revalidation_queued(admitted['accepted'])==1
    scheduler.take(set(route.pools),now[0])
    row=scheduler.selected_requests[0]
    strategy=PendingStrategy(None,config,{'header':HEADER,'pools':[p.json() for p in ps]})
    strategy.transfer_feedback=feedback
    return now,feedback,route,ps,strategy,row,lease_id


def snapshot(ps, row, page=1):
    from test_page_ownership import frame
    return frame([p.json() for p in ps],[tuple(p.address for p in ps)],page=page,requested=[row])


def test_current_lease_quotes_original_amount_on_fresh_real_pool_state(tmp_path):
    _,feedback,route,ps,strategy,row,lease_id=snapshot_case(tmp_path)
    report=strategy.on_snapshot(snapshot(ps,row))
    candidate=next(c for c in report['candidates'] if c['route_key']==route.key)
    assert candidate['amount_in']==10**15
    assert report['prewarm_quoted_amounts'][route.key]==10**15
    assert route.key in report['quote_resolved_route_keys']
    assert feedback.prewarm_summary()['dispatched']==0, 'Calculation must not self-acknowledge transport consumption.'


def test_serialized_bare_or_obsolete_amount_cannot_override_current_lease(tmp_path):
    import copy
    _,feedback,route,ps,strategy,row,lease_id=snapshot_case(tmp_path)
    forged=copy.deepcopy(row);forged['prewarm_amount']=123
    report=strategy.on_snapshot(snapshot(ps,forged))
    assert next(c for c in report['candidates'] if c['route_key']==route.key)['amount_in']==10**15
    assert feedback.revalidation_released([lease_id],'read_failed')==1
    # Old frame data cannot keep a now-released reservation's amount authority.
    report=strategy.on_snapshot(snapshot(ps,row,page=2))
    assert route.key not in report.get('prewarm_quoted_amounts',{})
    assert next(c for c in report['candidates'] if c['route_key']==route.key)['amount_in']!=10**15


def test_fixed_amount_preserves_cap_and_no_profit_rejection(tmp_path):
    _,feedback,route,ps,strategy,row,lease_id=snapshot_case(tmp_path)
    strategy.config['settlement_tokens'][0]['max_input_raw']=str(10**15-1)
    report=strategy.on_snapshot(snapshot(ps,row))
    assert not any(c['route_key']==route.key for c in report['candidates'])
    assert route.key not in report.get('quote_resolved_route_keys',[])
    strategy.config['settlement_tokens'][0]['max_input_raw']=str(10**18)
    for pool in ps:pool.reserve1=pool.reserve0
    report=strategy.on_snapshot(snapshot(ps,row,page=2))
    assert not any(c['route_key']==route.key for c in report['candidates'])
    assert route.key in report['quote_resolved_route_keys']
    assert report['prewarm_quoted_amounts'][route.key]==10**15


def test_temporary_feedback_and_unstarted_work_cannot_acknowledge_warm(tmp_path,monkeypatch):
    _,feedback,route,ps,strategy,row,lease_id=snapshot_case(tmp_path)
    strategy.execution_feedback.failures[route.key]=strategy.execution_feedback.clock()+60
    report=strategy.on_snapshot(snapshot(ps,row))
    assert route.key not in report.get('quote_resolved_route_keys',[])
    assert report['page_unfinished_requested_routes']>=1
    assert feedback.prewarm_summary()['dispatched']==0
    # A later global-budget stop before this route starts remains unconsumed.
    strategy.execution_feedback.passed(route.key)
    def unstarted(index,config,header,changed,**kwargs):
        index.last_evaluated_keys=list(index.routes)
        index.unstarted_keys=set(index.routes)
        return {'candidates':[],'routes_considered':0,'quote_resolved_route_keys':[],
                'prewarm_quoted_amounts':{}}
    monkeypatch.setattr('searcher.pipeline.evaluate',unstarted)
    report=strategy.on_snapshot(snapshot(ps,row,page=2))
    assert route.key not in report['quote_resolved_route_keys']
    assert not report['page_advance_ready']
    assert feedback.revalidation_consumed([lease_id],report['quote_resolved_route_keys'])==0


def test_incomplete_fixed_quote_preserves_lease_and_requests_real_deeper_read(tmp_path,monkeypatch):
    from searcher.core import Incomplete
    from searcher.routes import Route
    _,feedback,route,ps,strategy,row,lease_id=snapshot_case(tmp_path)
    original=Route.quote
    def missing(self,states,amount):
        if self.key==route.key:raise Incomplete('independent missing initialized tick')
        return original(self,states,amount)
    monkeypatch.setattr(Route,'quote',missing)
    for _ in range(3):
        report=strategy.on_snapshot(snapshot(ps,row))
        assert route.key not in report.get('quote_resolved_route_keys',[])
        assert route.key not in report.get('prewarm_quoted_amounts',{})
        assert feedback.revalidation_consumed([lease_id],report.get('quote_resolved_route_keys',[]))==0
    carried=[r for r in report['continuation_requests'] if route.key in r.get('route_keys',[])]
    assert len(carried)==1 and carried[0]['deep'] is True
    assert lease_id in DeepReadScheduler.lease_ids(carried[0])
    assert carried[0]['continuation_lane']=='prewarm'
    scheduler=DeepReadScheduler()
    assert scheduler.resume(carried,1042.)==[]
    scheduler.take(set(route.pools),1043.)
    assert scheduler.selected_deep_pools==set(route.pools), 'Returned incomplete warm work must actually deepen the next read.'


def test_quote_exception_cannot_emit_consumption_authority(tmp_path,monkeypatch):
    import pytest
    from searcher.routes import Route
    _,feedback,route,ps,strategy,row,lease_id=snapshot_case(tmp_path)
    original=Route.quote
    def malformed(self,states,amount):
        if self.key==route.key:raise ValueError('independent malformed state')
        return original(self,states,amount)
    monkeypatch.setattr(Route,'quote',malformed)
    with pytest.raises(ValueError,match='malformed state'):
        strategy.on_snapshot(snapshot(ps,row))
    assert route.key not in getattr(strategy.index,'quote_resolved_keys',set())
    assert feedback.prewarm_summary()['dispatched']==0
    assert feedback.revalidation_lease_ids()=={lease_id}


def test_held_page_retains_lease_but_never_revives_old_or_unaccepted_id(tmp_path):
    now,_,feedback,route,_,_,_=ready_case(tmp_path)
    _,lease_id=lease(feedback)
    assert feedback.revalidation_held([lease_id])==0
    assert feedback.revalidation_queued([lease_id])==1
    assert feedback.revalidation_held([lease_id])==1
    now[0]+=feedback.queue_seconds+10
    assert feedback.revalidation_lease_ids()=={lease_id}
    assert feedback.prewarm_summary()['dispatched']==0
    assert feedback.revalidation_released([lease_id],'read_failed')==1
    assert feedback.revalidation_held([lease_id])==0
    now[0]+=2
    _,fresh_id=lease(feedback)
    assert fresh_id!=lease_id
    assert feedback.revalidation_held([lease_id])==0
    assert feedback.revalidation_consumed([lease_id],[route.key])==0


def test_real_snapshot_consumer_ack_requires_fresh_terminal_quote(tmp_path):
    from searcher.pipeline import acknowledge_prewarm
    _,feedback,route,ps,strategy,row,lease_id=snapshot_case(tmp_path)
    f=snapshot(ps,row)
    report=strategy.on_snapshot(f)
    assert feedback.prewarm_summary()['dispatched']==0
    acknowledge_prewarm(strategy,f,report,[route.tokens[0]])
    assert report['prewarm_read_ack']==1
    assert feedback.prewarm_summary()['dispatched']==1
    acknowledge_prewarm(strategy,f,report,[route.tokens[0]])
    assert report['prewarm_read_ack']==0
    assert not feedback.quarantine.records, 'Quote consumption is not actual execution evidence.'


def test_stale_snapshot_or_non_snapshot_cannot_acknowledge_terminal_quote(tmp_path):
    from searcher.pipeline import acknowledge_prewarm
    _,feedback,route,ps,strategy,row,lease_id=snapshot_case(tmp_path)
    f=snapshot(ps,row)
    report=strategy.on_snapshot(f)
    f.requested_ns-=int((strategy.config['policy']['snapshot_max_age_ms']+1)*1e6)
    acknowledge_prewarm(strategy,f,report,[route.tokens[0]])
    assert feedback.prewarm_summary()['dispatched']==0
    acknowledge_prewarm(strategy,{'state_tag':'pending'},report,[route.tokens[0]])
    assert feedback.prewarm_summary()['dispatched']==0
    assert feedback.revalidation_lease_ids()=={lease_id}


def test_inflight_warm_still_owns_its_bounded_capacity(tmp_path):
    scheduler=DeepReadScheduler()
    rows=[{'pools':(f'0x{60000+2*i:040x}',f'0x{60001+2*i:040x}'),
           'route_keys':[f'route-{i}'], 'prewarm_lease_id':f'lease-{i}', 'deep':False}
          for i in range(65)]
    assert len(scheduler.add(rows[:64],0)['accepted'])==64
    scheduler.take({a for row in rows for a in row['pools']},1)
    assert scheduler.selected_requests
    assert scheduler.add(rows[64:],2)['rejected']==['lease-64']
    assert len(scheduler.prewarm_lease_ids())==64
    # Completion/release is the capacity boundary, not popping for a read.
    first=next(iter(scheduler.lease_ids(scheduler.selected_requests[0])))
    scheduler.prune_prewarm({f'lease-{i}' for i in range(64)}-{first})
    assert scheduler.add(rows[64:],3)['accepted']==['lease-64']
    assert len(scheduler.prewarm_lease_ids())==64


def test_one_group_quote_cannot_charge_two_distinct_principal_reservations(tmp_path):
    now,_,feedback,route,states,sample,_=ready_case(tmp_path)
    row_a,id_a=lease(feedback);feedback.revalidation_queued([id_a])
    now[0]+=1
    assert schedule(feedback,route,states,now[0],amount=51)
    from searcher.transfer_feedback import TransferSample
    record(feedback,TransferSample(sample.token,sample.source,sample.destination,102))
    now[0]+=31
    row_b,id_b=lease(feedback);feedback.revalidation_queued([id_b])
    assert feedback.revalidation_amounts([id_a,id_b])=={route.key:50}
    assert feedback.revalidation_consumed([id_a,id_b],[route.key],candidates=[],
                                          quoted_amounts={route.key:50})==1
    assert feedback.revalidations[(route.key,50)]['dispatches']==1
    assert feedback.revalidations[(route.key,51)]['dispatches']==0
    assert feedback.prewarm_summary()['dispatched']==1


def test_fixed_principal_does_not_bypass_fee_cap(tmp_path):
    _,feedback,route,ps,strategy,row,lease_id=snapshot_case(tmp_path)
    f=snapshot(ps,row)
    f.header=dict(f.header,baseFeePerGas=hex(10**20))
    f.latest=dict(f.latest,baseFeePerGas=hex(10**20))
    report=strategy.on_snapshot(f)
    assert not any(c['route_key']==route.key for c in report['candidates'])
    assert route.key not in report['quote_resolved_route_keys']
    assert feedback.prewarm_summary()['dispatched']==0


def multi_leg_case(tmp_path):
    from searcher.core import WETH
    from searcher.quarantine import DirectionQuarantine
    from searcher.routes import Route
    from searcher.transfer_feedback import TransferFeedback
    from test_direction_quarantine import IDENTITY, failure
    from test_transfer_feedback import EXECUTOR, OWNER
    from test_transfer_prewarm import Quote
    now=[1000.]
    quarantine=DirectionQuarantine(tmp_path/'multi-q.json',IDENTITY,clock=lambda:now[0])
    feedback=TransferFeedback(EXECUTOR,OWNER,clock=lambda:now[0],quarantine=quarantine)
    tokens=(WETH,'0x'+'ba'*20,'0x'+'ca'*20,WETH)
    route=Route(tokens,tuple('0x'+f'{900+i:040x}' for i in range(3)))
    states={pool:Quote() for pool in route.pools}
    assert feedback.schedule_revalidation(route,states,50,[WETH],failure(route,1000.),30)
    samples=feedback.modeled_samples(route,states,50,[WETH])
    assert [s.amount for s in samples]==[100,200]
    return now,quarantine,feedback,route,states,samples


def test_unrelated_healthy_amount_drift_cannot_block_exact_anomalous_leg(tmp_path):
    from searcher.core import WETH
    from test_transfer_prewarm import Quote
    from test_direction_quarantine import failure
    now,quarantine,feedback,route,states,samples=multi_leg_case(tmp_path)
    now[0]=1010.;record(feedback,samples[0],loss=True)
    now[0]=1041.
    row,lease_id=lease(feedback)
    feedback.revalidation_queued([lease_id])
    states[route.pools[1]]=Quote(3)  # Only healthy downstream modeled amount changes.
    assert [s.amount for s in feedback.modeled_samples(route,states,50,[WETH])]==[100,300]
    assert feedback.revalidation_consumed([lease_id],[route.key],
        candidates=[{'route_key':route.key,'amount_in':50}],states=states,
        settlements=[WETH],quoted_amounts={route.key:50})==1
    assert feedback.claim_probe(route,amount=50,states=states,settlements=[WETH])
    feedback.rejected(route,failure(route,now[0]),amount=50)
    expected={(samples[0].token,samples[0].source,feedback.executor)}
    assert set(quarantine.records)==expected
    assert next(iter(quarantine.records.values()))['state']=='watch'
    assert next(iter(quarantine.records.values()))['sample']['amount']==100


def test_wrong_amount_anomaly_or_only_healthy_partial_sample_gives_no_evidence(tmp_path):
    from searcher.core import WETH
    from searcher.transfer_feedback import TransferSample
    from test_direction_quarantine import failure
    now,quarantine,feedback,route,states,samples=multi_leg_case(tmp_path)
    now[0]=1010.
    wrong=TransferSample(samples[0].token,samples[0].source,samples[0].destination,99)
    record(feedback,wrong,loss=True)
    record(feedback,samples[1],loss=False)
    now[0]=1041.
    assert feedback.revalidation_requests()==[]
    assert not feedback.claim_probe(route,amount=50,states=states,settlements=[WETH])
    # The original bounded network-wait fallback may eventually allow a real
    # check; it still cannot pin the wrong-amount anomaly as witness evidence.
    now[0]=1091.
    assert feedback.claim_probe(route,amount=50,states=states,settlements=[WETH])
    feedback.rejected(route,failure(route,now[0]),amount=50)
    assert not quarantine.records


def test_initialize_range_adaptation_retries_once_and_retains_unread_suffix(tmp_path):
    import pytest
    from searcher.core import RPCError
    from test_v4_log_span import seed, RangeRPC, header

    class ChangingLimit(RangeRPC):
        def request(self, method, params):
            if method == 'eth_getLogs':
                lo, hi = (int(params[0][key], 16) for key in ('fromBlock', 'toBlock'))
                self.ranges.append((lo, hi))
                limit = 10 if len(self.ranges) == 1 else 2
                raise RPCError(method, {'code': -32005, 'message': f'up to a {limit} block range'})
            return super().request(method, params)

    inventory = seed(tmp_path/'inventory.db')
    rpc = ChangingLimit()
    with pytest.raises(RPCError):
        inventory.scan(rpc, header(138))
    assert rpc.ranges == [(75, 138), (75, 84)]
    assert inventory.get('cursor') == 74
    assert not inventory.registry.keys
    # The failed retry did not turn a bounded scan into recursive requests.
    next_rpc = RangeRPC(limit=2)
    inventory.scan(next_rpc, header(138))
    assert next_rpc.ranges == [(75, 84), (75, 76)]
    assert inventory.get('cursor') == 76
    inventory.close()


def test_initialize_shrunk_history_requires_page_end_and_never_skips_failed_page(tmp_path):
    import pytest
    from searcher.core import Reject
    from test_v4_log_span import seed, RangeRPC, header

    class MissingEnd(RangeRPC):
        def batch(self, calls):
            return [None for _ in calls]

    inventory = seed(tmp_path/'inventory.db', backward=True)
    rpc = MissingEnd()
    with pytest.raises(Reject, match='header missing'):
        inventory.scan(rpc, header(138))
    assert rpc.ranges == [(75, 138), (129, 138)]
    assert inventory.get('backfill') == 138
    assert inventory.get('cursor') == 138
    successful = RangeRPC()
    inventory.scan(successful, header(138))
    assert successful.ranges == [(129, 138)]
    assert inventory.get('backfill') == 128
    inventory.close()


def test_initialize_range_hint_never_replays_http_quota_failure(tmp_path):
    import httpx
    import pytest
    from test_v4_log_span import seed, RangeRPC, header

    request = httpx.Request('POST', 'https://public.invalid')
    response = httpx.Response(429, request=request)
    error = httpx.HTTPStatusError('up to a 10 block range', request=request, response=response)
    inventory = seed(tmp_path/'inventory.db')
    rpc = RangeRPC(error=error)
    with pytest.raises(httpx.HTTPStatusError):
        inventory.scan(rpc, header(138))
    assert rpc.ranges == [(75, 138)]
    assert inventory.get('cursor') == 74
    assert inventory.get('initialize_log_span') is None
    inventory.close()
