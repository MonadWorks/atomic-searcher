import copy
import hashlib
import json
import threading
import time

import pytest

from searcher.core import WETH
from searcher.quarantine import DirectionQuarantine, QuarantineStateError
from searcher.routes import Route
from searcher.transfer_feedback import TransferFeedback, TransferSample, route_edges
from test_receipt_eligibility import BAD, pools
from test_transfer_feedback import EXECUTOR, OWNER, OTHER_POOL, sample_result


IDENTITY = dict(chain_id=8453, executor=EXECUTOR, owner=OWNER,
                helper_sha256='1'*64, executor_sha256='2'*64)


def failure(route, stamp=1000, **changes):
    return dict(revert_name='BadCallback', revert_selector='0x6c25f7f8',
        execution_probe={'stage':'initial_call', 'state_tag':'pending', 'started_utc':stamp,
            'unsigned_call':{'from':OWNER,'to':EXECUTOR,'data':'secret-not-persisted'}}, **changes)


def setup(tmp_path, capacity=2048):
    clock=[1000.]
    q=DirectionQuarantine(tmp_path/'quarantine.json', IDENTITY, clock=lambda:clock[0], capacity=capacity)
    f=TransferFeedback(EXECUTOR, OWNER, clock=lambda:clock[0], quarantine=q)
    sample=TransferSample(BAD,pools()[0].address,pools()[1].address,100)
    route=Route((WETH,BAD,WETH),(sample.source,sample.destination))
    edge=(BAD,sample.source,EXECUTOR)
    return clock,q,f,sample,route,edge


def isolate(clock,q,f,sample,route,edge):
    f.record(sample, sample_result(sample,funded=95))
    for step in (31,61):
        clock[0]+=step
        assert f.claim_probe(route)
        assert f.rejected(route,failure(route,clock[0])) > 0
    assert q.blocked([edge])


def test_corroborated_direction_stays_isolated_after_ttl_and_restart(tmp_path):
    clock,q,f,sample,route,edge=setup(tmp_path)
    isolate(clock,q,f,sample,route,edge)
    clock[0]+=86400
    assert f.deferred(route)
    alternate=Route((WETH,BAD,WETH),(sample.source,OTHER_POOL))
    reverse=Route((WETH,BAD,WETH),(sample.destination,sample.source))
    assert f.deferred(alternate) and not f.deferred(reverse)
    reloaded=DirectionQuarantine(q.path, IDENTITY, clock=lambda:clock[0])
    fresh=TransferFeedback(EXECUTOR,OWNER,clock=lambda:clock[0],quarantine=reloaded)
    assert fresh.deferred(route) and fresh.deferred(alternate) and not fresh.deferred(reverse)
    assert fresh.group_deferred(route.pools,{p.address:p for p in pools()},[WETH],{route.key})
    assert not fresh.group_deferred(route.pools,{p.address:p for p in pools()},[WETH])
    text=q.path.read_text()
    assert 'secret-not-persisted' not in text and 'unsigned_call' not in text


def test_one_failure_or_duplicate_handler_does_not_create_isolation(tmp_path):
    clock,q,f,sample,route,edge=setup(tmp_path)
    f.record(sample,sample_result(sample,funded=95))
    clock[0]+=31
    assert f.claim_probe(route)
    f.rejected(route,failure(route))
    assert f.rejected(route,failure(route)) == 0
    assert not q.blocked([edge])
    assert len(q.records[edge]['witnesses']) == 1
    # Even an accidental second observation of the same whole call is one.
    assert not q.observe_failure(edge,sample_result(sample,funded=95),route,failure(route),1)
    assert len(q.records[edge]['witnesses']) == 1
    clock[0]+=10000
    assert not f.deferred(route)


@pytest.mark.parametrize('kind',['no_probe','stale_sample','timeout','deadline_success','wrong_sender','wrong_target','wrong_route','plain_transfer'])
def test_inconclusive_failures_cannot_create_durable_isolation(tmp_path,kind):
    clock,q,f,sample,route,edge=setup(tmp_path)
    bad=failure(route)
    age=1
    if kind=='no_probe': bad.pop('execution_probe')
    if kind=='stale_sample': age=301
    if kind=='timeout': bad['validation_deadline_reached']=True
    if kind=='deadline_success': bad['validation_outcome']='call_succeeded_after_deadline'
    if kind=='wrong_sender': bad['execution_probe']['unsigned_call']['from']=OTHER_POOL
    if kind=='wrong_target': bad['execution_probe']['unsigned_call']['to']=OTHER_POOL
    if kind=='wrong_route': route=Route((WETH,BAD,WETH),(sample.destination,OTHER_POOL))
    if kind=='plain_transfer': bad['revert_name']='TransferFailed'
    for _ in range(2):
        q.observe_failure(edge,sample_result(sample,funded=95),route,bad,age)
    assert not q.records


def test_expired_sample_is_not_promoted_by_real_rejection(tmp_path):
    clock,q,f,sample,route,edge=setup(tmp_path)
    f.record(sample,sample_result(sample,funded=95))
    clock[0]+=301
    assert f.claim_probe(route)
    assert f.rejected(route,failure(route)) == 0
    assert not q.records


def test_background_recovery_needs_no_candidate_and_consumes_one_trial(tmp_path):
    clock,q,f,sample,route,edge=setup(tmp_path)
    isolate(clock,q,f,sample,route,edge)
    assert not f.pending
    clock[0]+=301
    assert f.take() == sample
    assert f.take() is None
    clean=sample_result(sample);clean['block']=101
    f.record(sample,clean);f.finish_sample(sample,True)
    assert not f.deferred(route)
    assert q.summary()['trial_eligible'] == 1
    assert f.claim_probe(route)
    assert f.deferred(route) and not f.claim_probe(route)
    clock[0]+=10000
    assert f.deferred(route)  # Failed, timed-out or abandoned trial is closed.
    f.passed(route)
    assert not q.records and not f.deferred(route)
    assert not DirectionQuarantine(q.path,IDENTITY).records


@pytest.mark.parametrize('change',['same_block','different_amount','shortfall','failed_call','debited_loss','noncanonical','wrong_executor'])
def test_only_new_exact_identical_sample_grants_trial(tmp_path,change):
    clock,q,f,sample,route,edge=setup(tmp_path)
    isolate(clock,q,f,sample,route,edge)
    clean=sample_result(sample);clean['block']=101
    if change=='same_block': clean['block']=100
    if change=='different_amount': clean.update(amount=200,funded=200,debited=200,credited=200)
    if change=='shortfall': clean['funded']=95
    if change=='failed_call': clean['failure_stage']=2
    if change=='debited_loss': clean['debited']=99
    if change=='noncanonical': clean['canonical']=False
    if change=='wrong_executor': clean['executor']=OTHER_POOL
    if change in ('noncanonical','wrong_executor'):
        with pytest.raises(ValueError): q.observe_sample(clean)
    else: assert q.observe_sample(clean)==[]
    assert q.blocked([edge])


def test_recovery_certificate_cannot_replay_after_claim_or_restart(tmp_path):
    clock,q,f,sample,route,edge=setup(tmp_path)
    isolate(clock,q,f,sample,route,edge)
    clean=sample_result(sample);clean['block']=101
    assert q.observe_sample(clean)==[edge]
    assert q.claim([edge])
    assert q.observe_sample(clean)==[]
    reloaded=DirectionQuarantine(q.path,IDENTITY,clock=lambda:clock[0])
    assert reloaded.observe_sample(clean)==[] and reloaded.blocked([edge])
    clean['block']=102
    assert reloaded.observe_sample(clean)==[edge]
    restarted=DirectionQuarantine(q.path,IDENTITY,clock=lambda:clock[0])
    assert restarted.blocked([edge])
    assert restarted.observe_sample(clean)==[]


def test_fingerprint_change_preserves_isolation_and_requires_new_sample(tmp_path):
    clock,q,f,sample,route,edge=setup(tmp_path)
    isolate(clock,q,f,sample,route,edge)
    clean=sample_result(sample);clean['block']=101
    q.observe_sample(clean)
    new=DirectionQuarantine(q.path,dict(IDENTITY,helper_sha256='3'*64),clock=lambda:clock[0])
    assert new.blocked([edge])
    assert new.observe_sample(clean)==[]
    clean['block']=102
    assert new.observe_sample(clean)==[edge]


@pytest.mark.parametrize('change',['json','schema','identity','duplicate','false_witness','too_many','negative_time'])
def test_bad_persistent_state_is_explicit_not_empty(tmp_path,change):
    clock,q,f,sample,route,edge=setup(tmp_path)
    isolate(clock,q,f,sample,route,edge)
    doc=json.loads(q.path.read_text())
    if change=='schema': doc['schema']=99
    if change=='identity': doc['identity']['chain_id']=1
    if change=='duplicate': doc['records'].append(copy.deepcopy(doc['records'][0]))
    if change=='false_witness': doc['records'][0]['witnesses']=doc['records'][0]['witnesses'][:1]
    if change=='too_many': doc['records']*=2049
    if change=='negative_time': doc['last_wall']=-1
    q.path.write_text('{' if change=='json' else json.dumps(doc))
    with pytest.raises(QuarantineStateError,match='state invalid'):
        DirectionQuarantine(q.path,IDENTITY)


def test_write_error_preserves_memory_block_and_never_grants_trial(tmp_path,monkeypatch):
    import searcher.quarantine as module
    clock,q,f,sample,route,edge=setup(tmp_path)
    isolate(clock,q,f,sample,route,edge)
    original=module.os.replace
    def fail(*args): raise OSError('disk issue')
    monkeypatch.setattr(module.os,'replace',fail)
    clean=sample_result(sample);clean['block']=101
    assert q.observe_sample(clean)==[] and q.blocked([edge])
    assert q.summary()['persistence_error']=='OSError' and q.dirty
    monkeypatch.setattr(module.os,'replace',original)
    clock[0]+=6;q.flush()
    assert not q.dirty and q.blocked([edge])
    clean['block']=102
    assert q.observe_sample(clean)==[edge]
    monkeypatch.setattr(module.os,'replace',fail)
    assert not q.claim([edge]) and q.blocked([edge])


def test_capacity_never_evicts_confirmed_direction(tmp_path):
    clock,q,f,sample,route,edge=setup(tmp_path,capacity=1)
    isolate(clock,q,f,sample,route,edge)
    other=TransferSample(BAD,OTHER_POOL,sample.destination,100)
    other_route=Route((WETH,BAD,WETH),(other.source,other.destination))
    other_edge=(BAD,other.source,EXECUTOR)
    assert not q.observe_failure(other_edge,sample_result(other,funded=95),other_route,failure(other_route,2000),1)
    assert q.blocked([edge]) and len(q.records)==1
    assert q.summary()['capacity_exhausted']


def test_recovery_and_fresh_diagnostics_share_queue_fairly(tmp_path):
    clock,q,f,sample,route,edge=setup(tmp_path)
    isolate(clock,q,f,sample,route,edge)
    clock[0]+=301
    fresh=TransferSample(BAD,OTHER_POOL,sample.destination,200)
    f.pending[fresh.key]=(fresh,clock[0])
    assert f.take()==sample
    assert f.take()==fresh
    f.finish_sample(sample,False);f.finish_sample(fresh,False)
    assert f.take() is None
    clock[0]+=301
    assert f.take()==sample


def test_clock_rollback_restart_does_not_advance_due_time(tmp_path):
    clock,q,f,sample,route,edge=setup(tmp_path)
    isolate(clock,q,f,sample,route,edge)
    next_check=q.records[edge]['next_check']
    clock[0]=1
    loaded=DirectionQuarantine(q.path,IDENTITY,clock=lambda:clock[0])
    assert loaded.blocked([edge]) and loaded.take_due() is None
    clock[0]=next_check
    assert loaded.take_due()['amount']==sample.amount


def test_two_threads_cannot_consume_one_recovery_trial(tmp_path):
    clock,q,f,sample,route,edge=setup(tmp_path)
    isolate(clock,q,f,sample,route,edge)
    clean=sample_result(sample);clean['block']=101
    q.observe_sample(clean)
    barrier=threading.Barrier(3);results=[]
    def claim(): barrier.wait();results.append(q.claim([edge]))
    threads=[threading.Thread(target=claim) for _ in range(2)]
    for thread in threads: thread.start()
    barrier.wait()
    for thread in threads: thread.join()
    assert sorted(results)==[False,True]


def test_explicit_review_seed_preserves_audit_provenance(tmp_path):
    clock,q,f,sample,route,edge=setup(tmp_path)
    failures=[failure(route,1000),failure(route,2000)]
    assert q.seed_reviewed(edge,sample_result(sample,funded=95),route,failures,'a'*64)
    assert q.blocked([edge]) and q.records[edge]['provenance']=='a'*64
    loaded=DirectionQuarantine(q.path,IDENTITY,clock=lambda:clock[0])
    assert loaded.records[edge]['provenance']=='a'*64
    with pytest.raises(ValueError): q.seed_reviewed(edge,sample_result(sample),route,failures,'b'*64)


@pytest.mark.parametrize('changed',[False,True])
def test_preflight_validation_never_writes_or_allows_mutation(tmp_path,changed):
    clock,q,f,sample,route,edge=setup(tmp_path)
    isolate(clock,q,f,sample,route,edge)
    clean=sample_result(sample);clean['block']=101
    q.observe_sample(clean)
    before=q.path.read_bytes();stamp=q.path.stat().st_mtime_ns
    identity=dict(IDENTITY,helper_sha256='3'*64) if changed else IDENTITY
    readonly=DirectionQuarantine(q.path,identity,validate_only=True)
    assert readonly.summary()['validate_only'] and readonly.blocked([edge])
    actions=[lambda:readonly.claim([edge]), lambda:readonly.passed([edge]),
        lambda:readonly.observe_sample(clean), lambda:readonly.take_due(),
        lambda:readonly.finish_sample(sample.key),lambda:readonly.flush(),
        lambda:readonly.observe_failure(edge,sample_result(sample,funded=95),route,failure(route),1),
        lambda:readonly.seed_reviewed(edge,sample_result(sample,funded=95),route,[failure(route,1),failure(route,2)],'a'*64)]
    for action in actions:
        with pytest.raises(QuarantineStateError,match='read-only'): action()
    assert q.path.read_bytes()==before and q.path.stat().st_mtime_ns==stamp


def test_wrapped_transfer_failure_requires_helper_failure_on_same_edge(tmp_path):
    clock,q,f,sample,route,edge=setup(tmp_path)
    wrapped=failure(route)
    wrapped.update(revert_name='TransferFailed',gas_attempts=[1,2],
                   previous_gas_failure={'revert_selector':wrapped['revert_selector']})
    observation=sample_result(sample,funded=95)
    observation['failure_stage']=2  # Incoming shortfall, outgoing failed call.
    assert not q.observe_failure(edge,observation,route,wrapped,1)
    wrapped2=copy.deepcopy(wrapped);wrapped2['execution_probe']['started_utc']+=1
    with pytest.raises(ValueError,match='different directed edge'):
        q.seed_reviewed(edge,observation,route,[wrapped,wrapped2],'a'*64)
    outgoing=(sample.token,EXECUTOR,sample.destination)
    assert not q.observe_failure(outgoing,observation,route,wrapped,1)
    assert q.observe_failure(outgoing,observation,route,wrapped2,1)
    assert q.blocked([outgoing]) and not q.blocked([edge])


@pytest.mark.parametrize('mismatch',[False,True])
def test_worker_publishes_loaded_isolation_before_any_sample_and_checks_identity(tmp_path,monkeypatch,mismatch):
    import searcher.transfer_feedback as module
    clock,q,f,sample,route,edge=setup(tmp_path)
    artifact=tmp_path/'out/CycleProbe.sol/CycleProbe.json';artifact.parent.mkdir(parents=True)
    artifact.write_text(json.dumps({'deployedBytecode':{'object':'0x6000'}}))
    q.identity['helper_sha256']=hashlib.sha256(artifact.read_bytes()).hexdigest()
    isolate(clock,q,f,sample,route,edge)
    if mismatch: q.identity['helper_sha256']='f'*64
    calls=[]
    class RPC:
        def __init__(self,*args,**kwargs): pass
        def verify_chain(self): calls.append('verify_chain')
        def close(self): calls.append('close')
    class Stop:
        done=False
        def is_set(self): return self.done
        def wait(self,seconds): self.done=True
    def no_sample(*args): pytest.fail('recovery not yet due')
    monkeypatch.setattr(module,'ROOT',tmp_path);monkeypatch.setattr(module,'RPC',RPC)
    monkeypatch.setattr(module,'measure_sample',no_sample)
    path=tmp_path/'health.json'
    f.run('local',8453,Stop(),time.monotonic()+10,path)
    health=json.loads(path.read_text())
    if mismatch:
        assert health['state']=='unavailable' and calls==['close']
    else:
        assert health['quarantine']['quarantined']==1
        assert health['event']=='worker_verified' and health['sample_completed'] is False
        assert calls==['verify_chain','close']
    assert q.blocked([edge])
