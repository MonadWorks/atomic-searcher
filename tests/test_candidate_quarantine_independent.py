"""Independent directed quarantine tests. Synthetic identities; no network/signing."""
import copy
import json
import threading

import pytest

from searcher.quarantine import DirectionQuarantine, QuarantineStateError
from searcher.routes import Route
from searcher.transfer_feedback import TransferFeedback, TransferSample, route_edges
from searcher.quarantine_runtime import open_quarantine, quarantine_path


def addr(n): return '0x'+f'{n:040x}'
A,B,P,Q,E,O=map(addr,(1,2,11,12,90,91))
ROUTE=Route((A,B,A),(P,Q))
EDGE=(B,P,E)
IDENTITY=dict(chain_id=8453,owner=O,executor=E,helper_sha256='1'*64,executor_sha256='2'*64)


def evidence(block=100,clean=False):
    return dict(token=B,source=P,destination=Q,executor=E,amount=100,
                funded=100 if clean else 95,debited=100 if clean else 95,
                credited=100 if clean else 95,block=block,block_hash='0x'+f'{block:064x}',
                canonical=True,failure_stage=0)


def failure(stamp=1000,**extra):
    return dict(revert_name='BadCallback',revert_selector='0x6c25f7f8',gas_attempts=[580000],
                execution_probe=dict(stage='initial_call',started_utc=stamp,state_tag='pending',
                                     unsigned_call={'from':O,'to':E}),**extra)


def fixture(tmp_path):
    clock=[1000.0]
    q=DirectionQuarantine(tmp_path/'state.json',IDENTITY,clock=lambda:clock[0])
    tf=TransferFeedback(E,O,clock=lambda:clock[0],quarantine=q)
    return clock,q,tf


def seed(q):
    assert q.seed_reviewed(EDGE,evidence(),ROUTE,[failure(900),failure(901)],'3'*64)


def test_repeated_claimed_failures_persist_and_time_or_restart_never_releases(tmp_path):
    clock,q,tf=fixture(tmp_path);sample=TransferSample(B,P,Q,100)
    tf.record(sample,evidence())
    clock[0]=1031;assert tf.claim_probe(ROUTE)
    assert tf.rejected(ROUTE,failure(1031))==60
    assert q.summary()['watching']==1
    clock[0]=1092;assert tf.claim_probe(ROUTE)
    assert tf.rejected(ROUTE,failure(1092))==120
    assert q.blocked([EDGE])
    clock[0]=10**8
    restored=DirectionQuarantine(q.path,IDENTITY,clock=lambda:clock[0])
    tf2=TransferFeedback(E,O,clock=lambda:clock[0],quarantine=restored)
    assert tf2.deferred(ROUTE) and not tf2.claim_probe(ROUTE)
    assert not tf2.deferred(Route((A,B,A),(Q,P)))
    assert not tf2.deferred(Route((A,B,A),(addr(13),Q)))
    assert restored.summary()['quarantined']==1


def test_same_whole_call_and_timeouts_do_not_manufacture_second_witness(tmp_path):
    _,q,_=fixture(tmp_path)
    assert not q.observe_failure(EDGE,evidence(),ROUTE,failure(900),0)
    for _ in range(4):assert not q.observe_failure(EDGE,evidence(),ROUTE,failure(900),0)
    for f in (failure(901,validation_deadline_reached=True),
              failure(902,validation_outcome='call_succeeded_after_deadline'),
              {'cause':'RPC total request deadline exceeded'}):
        assert not q.observe_failure(EDGE,evidence(),ROUTE,f,0)
    assert q.summary()['watching']==1 and not q.blocked([EDGE])
    assert not q.observe_failure(EDGE,evidence(),ROUTE,failure(903),301)
    assert q.observe_failure(EDGE,evidence(),ROUTE,failure(904),0)


def test_clean_new_block_allows_only_one_atomic_claim_then_timeout_keeps_isolation(tmp_path):
    clock,q,tf=fixture(tmp_path);seed(q);clock[0]=1301
    assert q.observe_sample(evidence(100,True))==[]
    assert tf.deferred(ROUTE)
    tf.record(TransferSample(B,P,Q,100),evidence(101,True))
    assert not tf.deferred(ROUTE)
    gate=threading.Barrier(3);claims=[]
    def claim():gate.wait();claims.append(tf.claim_probe(ROUTE))
    workers=[threading.Thread(target=claim) for _ in range(2)]
    for t in workers:t.start()
    gate.wait()
    for t in workers:t.join(1)
    assert sorted(claims)==[False,True]
    assert tf.rejected(ROUTE,{'cause':'timeout'})==0
    assert tf.deferred(ROUTE)
    clock[0]+=10000
    assert tf.deferred(ROUTE)
    assert q.observe_sample(evidence(101,True))==[]
    assert tf.deferred(ROUTE)


def test_actual_success_clears_durable_isolation_and_old_helper_cannot_restore(tmp_path):
    clock,q,tf=fixture(tmp_path);seed(q);clock[0]=1301
    tf.record(TransferSample(B,P,Q,100),evidence(101,True))
    assert tf.claim_probe(ROUTE)
    tf.passed(ROUTE)  # Actual Executor success callback, before later fee/send checks.
    assert not q.records and not tf.deferred(ROUTE)
    assert tf.record(TransferSample(B,P,Q,100),evidence(100))==[]
    assert not DirectionQuarantine(q.path,IDENTITY,clock=lambda:clock[0]).records


def test_recovery_does_not_need_new_candidates_and_alternates_normal_diagnostics(tmp_path):
    clock,q,tf=fixture(tmp_path);seed(q);clock[0]=1301
    other=TransferSample(addr(3),addr(14),addr(15),77)
    tf.pending[other.key]=(other,clock[0])
    recovery=tf.take();assert recovery.key==(B,P,Q)
    normal=tf.take();assert normal==other
    assert tf.take() is None
    tf.finish_sample(recovery,False)
    assert q.blocked([EDGE]) and q.take_due() is None
    clock[0]+=301
    assert tf.take().key==(B,P,Q)


@pytest.mark.parametrize('changed',[{'chain_id':1},{'owner':addr(92)},{'executor':addr(93)}])
def test_account_namespace_changes_reject_instead_of_borrowing_other_account_evidence(tmp_path,changed):
    _,q,_=fixture(tmp_path);seed(q)
    with pytest.raises(QuarantineStateError):DirectionQuarantine(q.path,{**IDENTITY,**changed})


def test_restart_and_changed_code_invalidate_clean_certificate_but_keep_evidence(tmp_path):
    clock,q,_=fixture(tmp_path);seed(q);clock[0]=1301
    assert q.observe_sample(evidence(101,True))==[EDGE]
    newer=DirectionQuarantine(q.path,{**IDENTITY,'helper_sha256':'4'*64},clock=lambda:clock[0])
    assert newer.blocked([EDGE]) and not newer.claim([EDGE])
    assert newer.records[EDGE]['state']=='quarantined'


def test_failed_persistence_keeps_in_memory_isolation_visible_and_blocks_trial(tmp_path,monkeypatch):
    clock,q,_=fixture(tmp_path);seed(q);clock[0]=1301
    def no_write(*args):raise OSError('synthetic disk failure')
    with monkeypatch.context() as m:
        m.setattr('searcher.quarantine.os.replace',no_write)
        assert q.observe_sample(evidence(101,True))==[]
        assert q.blocked([EDGE]) and not q.claim([EDGE])
        assert q.summary()['persistence_dirty'] and q.summary()['persistence_error']=='OSError'
    clock[0]+=6;q.flush()
    assert not q.summary()['persistence_dirty'] and q.blocked([EDGE])


def test_corrupt_file_is_not_silently_reset(tmp_path):
    path=tmp_path/'state.json';path.write_text('{')
    with pytest.raises(QuarantineStateError):DirectionQuarantine(path,IDENTITY)
    assert path.read_text()=='{'


def test_account_path_is_stable_across_session_directories(tmp_path):
    assert quarantine_path(tmp_path/'account',E)==quarantine_path(tmp_path/'account',E.upper().replace('0X','0x'))
    assert 'session-' not in quarantine_path(tmp_path/'account',E).name


def test_corrupt_quarantine_fails_before_any_pipeline_worker_starts(tmp_path,monkeypatch):
    from types import SimpleNamespace
    import searcher.pipeline as pipeline
    from searcher.core import load_config
    from searcher.route_feedback import RouteFeedback
    config=load_config();config['owner']=O;config['v4']['enabled']=True
    config['policy']['hot_lane_enabled']=False
    path=tmp_path/'state.json';path.write_text('{')
    started=[]
    class Worker:
        def __init__(self,*args,**kwargs):pass
        def start(self):started.append('v4')
    class Strategy:
        def __init__(self,*args):self.execution_feedback=RouteFeedback()
    monkeypatch.setattr('searcher.v4_runtime.Worker',Worker)
    monkeypatch.setattr(pipeline,'PendingStrategy',Strategy)
    executor=SimpleNamespace(contract=E,transfer_quarantine_path=path)
    rpc=SimpleNamespace(url='http://127.0.0.1:1',chain_id=8453)
    with pytest.raises(QuarantineStateError):
        pipeline.run_pending(rpc,config,{},1,tmp_path/'pipeline',executor=executor)
    assert started==[]


def test_open_quarantine_readonly_preflight_preserves_eligible_file_and_metadata(tmp_path):
    artifact_root=tmp_path/'package'
    for rel,payload in [('out/CycleProbe.sol/CycleProbe.json','{}'),('out/AtomicCycle.sol/AtomicCycle.json','{"v":1}')]:
        path=artifact_root/rel;path.parent.mkdir(parents=True);path.write_text(payload)
    config={'chain_id':8453,'owner':O}
    state=tmp_path/'account/state.json'
    q=open_quarantine(config,E,state,root=artifact_root)
    seed(q);q.observe_sample(evidence(101,True))
    before=state.read_bytes();mtime=state.stat().st_mtime_ns
    (artifact_root/'out/CycleProbe.sol/CycleProbe.json').write_text('{"v":2}')
    readonly=open_quarantine(config,E,state,root=artifact_root,validate_only=True)
    assert readonly.blocked([EDGE])
    with pytest.raises(QuarantineStateError):readonly.claim([EDGE])
    assert state.read_bytes()==before and state.stat().st_mtime_ns==mtime
    assert json.loads(before)['records'][0]['state']=='eligible'


def test_slow_background_recovery_cannot_hold_candidate_admission_lock(tmp_path,monkeypatch):
    import time
    import searcher.transfer_feedback as module
    import hashlib
    clock=[1000.]
    identity={**IDENTITY,'helper_sha256':hashlib.sha256((module.ROOT/'out/CycleProbe.sol/CycleProbe.json').read_bytes()).hexdigest()}
    q=DirectionQuarantine(tmp_path/'state.json',identity,clock=lambda:clock[0])
    tf=TransferFeedback(E,O,clock=lambda:clock[0],quarantine=q)
    seed(q);clock[0]=1301
    entered=threading.Event();release=threading.Event();stop=threading.Event();checked=threading.Event()
    class LocalOnlyRPC:
        def __init__(self,*a,**kw):pass
        def verify_chain(self):pass
        def close(self):pass
    def slow(*args):
        entered.set();assert release.wait(2)
        raise TimeoutError('synthetic recovery timeout')
    monkeypatch.setattr(module,'RPC',LocalOnlyRPC)
    monkeypatch.setattr(module,'measure_sample',slow)
    worker=threading.Thread(target=tf.run,args=('http://127.0.0.1:1',8453,stop,time.monotonic()+10,tmp_path/'health.json'),kwargs={'interval':0})
    worker.start()
    try:
        assert entered.wait(1)
        def inspect():
            assert tf.deferred(ROUTE)
            assert not tf.deferred(Route((A,B,A),(Q,P)))
            checked.set()
        consumer=threading.Thread(target=inspect);consumer.start()
        assert checked.wait(1),'diagnostic RPC held the admission lock'
        consumer.join(1)
    finally:
        stop.set();release.set();worker.join(2)
    assert not worker.is_alive() and not q.in_flight and not tf.in_flight
    assert q.blocked([EDGE])
