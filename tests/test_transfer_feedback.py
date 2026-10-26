import copy
import json
import threading
import time

import pytest
from eth_abi import encode

from searcher.core import RPCError, Reject, WETH, load_config
from searcher.engine import evaluate
from searcher.routes import Route, RouteIndex
from searcher.transfer_feedback import TransferFeedback, TransferSample, measure_sample
from test_receipt_eligibility import pools, HEADER, BAD

EXECUTOR='0x'+'aa'*20
OWNER='0x'+'bb'*20
OTHER_POOL='0x'+'cc'*20


def sample_result(sample, funded=None, credited=None):
    funded=sample.amount if funded is None else funded
    return dict(token=sample.token,source=sample.source,destination=sample.destination,
        executor=EXECUTOR,amount=sample.amount,funded=funded,debited=funded,
        credited=funded if credited is None else credited,canonical=True,
        block=100,block_hash=HEADER['hash'])


def test_unknown_shortfall_frees_search_for_other_profitable_routes():
    config=load_config();config['unsupported_transfer_tokens']={}
    config['policy'].update(max_quote_ms=500,sizing_mode='fixed')
    config['settlement_tokens']=[dict(config['settlement_tokens'][0],max_input_raw=str(10**18))]
    states=pools();index=RouteIndex(states,[WETH],3)
    first=evaluate(index,config,HEADER)
    bad=next(c for c in first['candidates'] if BAD in c['tokens'])
    route=index.routes[bad['route_key']]
    now=[10];feedback=TransferFeedback(EXECUTOR,OWNER,clock=lambda:now[0])
    assert feedback.offer(route,index.states,bad['amount_in'],[WETH])==1
    sample=feedback.take()
    feedback.record(sample,sample_result(sample,funded=sample.amount*96//100))
    index.transfer_feedback=feedback
    result=evaluate(index,config,HEADER)
    assert result['rejected']['sampled_transfer_backoff']==1
    assert result['candidates'] and all(BAD not in c['tokens'] for c in result['candidates'])
    assert bad['route_key'] in index.queued_routes
    now[0]+=301
    result=evaluate(index,config,HEADER,set())
    assert any(BAD in c['tokens'] for c in result['candidates'])


def test_shortfall_blocks_related_directed_paths_without_token_blacklist():
    sample=TransferSample(BAD,pools()[0].address,pools()[1].address,100)
    feedback=TransferFeedback(EXECUTOR,OWNER)
    feedback.record(sample,sample_result(sample,funded=96,credited=96))
    # Incoming shortfall follows the emitting pool across different recipients.
    alternate=Route((WETH,BAD,WETH),(sample.source,OTHER_POOL))
    reverse=Route((WETH,BAD,WETH),(sample.destination,sample.source))
    assert feedback.deferred(alternate) and not feedback.deferred(reverse)
    outgoing=TransferFeedback(EXECUTOR,OWNER)
    outgoing.record(sample,sample_result(sample,funded=100,credited=96))
    assert outgoing.deferred(Route((WETH,BAD,WETH),(OTHER_POOL,sample.destination)))
    assert not outgoing.deferred(alternate)


def test_good_samples_never_whitelist_and_results_are_bounded():
    f=TransferFeedback(EXECUTOR,OWNER,capacity=2)
    sample=TransferSample(BAD,pools()[0].address,pools()[1].address,100)
    assert f.record(sample,sample_result(sample))==[]
    assert not f.edges
    for n in range(3):
        item=TransferSample(BAD,'0x'+f'{n+100:040x}',sample.destination,100)
        f.record(item,sample_result(item,funded=96))
    assert len(f.edges)==len(f.retry_at)==2
    assert not f.deferred(Route((WETH,BAD,WETH),('0x'+f'{100:040x}',OTHER_POOL)))


def test_queue_deduplicates_inflight_samples_and_retries_unknown_failures():
    now=[0];feedback=TransferFeedback(EXECUTOR,OWNER,clock=lambda:now[0],queue_limit=1)
    ps=pools();states={p.address:p for p in ps}
    route=Route((WETH,BAD,WETH),(ps[0].address,ps[1].address))
    assert feedback.offer(route,states,10**15,[WETH])==1
    assert feedback.offer(route,states,2*10**15,[WETH])==0
    feedback.take()
    assert feedback.offer(route,states,10**15,[WETH])==0
    now[0]=31
    assert feedback.offer(route,states,10**15,[WETH])==0
    sample=next(iter(feedback.in_flight))
    feedback.finish_sample(TransferSample(*sample,10**15),False)
    now[0]=62
    assert feedback.offer(route,states,10**15,[WETH])==1
    assert not feedback.deferred(route)


def test_sample_updates_amount_expires_and_skips_already_covered_edges():
    now=[0];feedback=TransferFeedback(EXECUTOR,OWNER,clock=lambda:now[0])
    ps=pools();states={p.address:p for p in ps};route=Route((WETH,BAD,WETH),(ps[0].address,ps[1].address))
    feedback.offer(route,states,10**15,[WETH]);feedback.offer(route,states,2*10**15,[WETH])
    item=feedback.take();assert item.amount==ps[0].quote(WETH,2*10**15)[0]
    feedback.finish_sample(item,False);now[0]=31
    feedback.offer(route,states,10**15,[WETH]);now[0]=92
    assert feedback.take() is None
    feedback.offer(route,states,10**15,[WETH])
    feedback.record(item,sample_result(item,funded=item.amount*96//100))
    assert feedback.take() is None


def test_periodic_real_probe_remains_available_and_success_overrides_old_sample():
    now=[0];f=TransferFeedback(EXECUTOR,OWNER,clock=lambda:now[0])
    sample=TransferSample(BAD,pools()[0].address,pools()[1].address,100)
    original=Route((WETH,BAD,WETH),(sample.source,sample.destination))
    alternate=Route((WETH,BAD,WETH),(sample.source,OTHER_POOL))
    f.record(sample,sample_result(sample,funded=96))
    assert f.deferred(original) and not f.claim_probe(original)
    now[0]=31
    # Merely looking/quoting never consumes the real-call retry slot.
    assert not f.deferred(original) and not f.deferred(alternate)
    assert f.claim_probe(original) and not f.claim_probe(alternate)
    now[0]=62
    assert f.priority(alternate)<f.priority(original)
    assert f.claim_probe(alternate)
    f.passed(alternate)
    assert f.priority(original)==0
    # In-flight or later helper results do not immediately undo a real pass.
    assert f.record(sample,sample_result(sample,funded=96))==[]
    assert not f.deferred(original)


def test_unknown_retry_backoff_is_bounded():
    now=[0];f=TransferFeedback(EXECUTOR,OWNER,clock=lambda:now[0])
    sample=TransferSample(BAD,pools()[0].address,pools()[1].address,100)
    delays=[]
    for i in range(8):
        f.finish_sample(sample,False);delays.append(f.retry_at[sample.key]-now[0]);now[0]+=1000
    assert delays[:3]==[30,60,120] and max(delays)==300 and not f.edges


@pytest.mark.parametrize('case',['shortfall','exact','reorg','rpc_error'])
def test_probe_uses_real_token_state_and_canonical_check(case):
    calls=[]
    class RPC:
        def header(self,n=None):
            return dict(HEADER,hash='0x'+'33'*32 if n is not None and case=='reorg' else HEADER['hash'])
        def request(self,method,params):
            calls.append((method,params))
            if case=='rpc_error': raise RPCError(method,{'code':3,'message':'execution reverted'})
            return '0x'+encode(['uint256']*3+['uint8','bytes4'],
                              ([96,96,92] if case=='shortfall' else [100]*3)+[0,bytes(4)]).hex()
    sample=TransferSample(BAD,pools()[0].address,pools()[1].address,100)
    if case in ('reorg','rpc_error'):
        with pytest.raises(Reject):measure_sample(RPC(),sample,EXECUTOR,OWNER,'0x6000')
    else:
        result=measure_sample(RPC(),sample,EXECUTOR,OWNER,'0x6000')
        assert result['canonical'] and result['funded']==(96 if case=='shortfall' else 100)
    method,params=calls[0]
    assert method=='eth_call' and params[0]['from']==OWNER and params[1]==HEADER['number']
    assert set(params[2])=={sample.source,EXECUTOR} and all(set(v)=={'code'} for v in params[2].values())
    assert BAD not in params[2]


def test_worker_recovers_from_initial_node_failure_without_blocking_route(monkeypatch,tmp_path):
    import searcher.transfer_feedback as module
    from pathlib import Path
    checks=[];closed=[]
    class RPC:
        def __init__(self,*args,**kwargs):pass
        def verify_chain(self):
            checks.append(1)
            if len(checks)==1:raise RuntimeError('temporarily offline')
        def close(self):closed.append(True)
    class Stop:
        def __init__(self):self.done=False
        def is_set(self):return self.done
        def wait(self,seconds):
            if len(checks)>1:self.done=True
    artifact=tmp_path/'out/CycleProbe.sol/CycleProbe.json';artifact.parent.mkdir(parents=True)
    artifact.write_text(json.dumps({'deployedBytecode':{'object':'0x6000'}}))
    monkeypatch.setattr(module,'ROOT',tmp_path);monkeypatch.setattr(module,'RPC',RPC)
    feedback=TransferFeedback(EXECUTOR,OWNER)
    feedback.run('test',8453,Stop(),time.monotonic()+30,tmp_path/'health.json')
    assert len(checks)==2 and closed and not feedback.edges


def test_strategy_keeps_transfer_evidence_across_page_rebuilds():
    from searcher.pipeline import PendingStrategy
    from searcher.statefeed import StateFrame
    config=load_config();config['unsupported_transfer_tokens']={};config['policy']['max_quote_ms']=500
    config['settlement_tokens']=[dict(config['settlement_tokens'][0],max_input_raw=str(10**18))]
    ps=pools();strategy=PendingStrategy(None,config,{'header':HEADER,'pools':[p.json() for p in ps]})
    feedback=TransferFeedback(EXECUTOR,OWNER);strategy.transfer_feedback=feedback
    sample=TransferSample(BAD,ps[0].address,ps[1].address,100)
    feedback.record(sample,sample_result(sample,funded=96))
    for subset in (ps,ps[:2],ps):
        now=time.monotonic_ns()
        frame=StateFrame(HEADER,HEADER,[p.json() for p in subset],now,now,time.time(),101,202)
        report=strategy.on_snapshot(frame)
        assert report['rejected']['sampled_transfer_backoff']==1
        assert all(BAD not in c['tokens'] for c in report['candidates'])


def test_running_pipeline_learns_shortfall_and_keeps_other_execution_candidates(monkeypatch,tmp_path):
    import searcher.pipeline as pipeline
    import searcher.transfer_feedback as transfers
    from test_runtime import FakeRPC
    from searcher.statefeed import StateFrame
    config=load_config();config['unsupported_transfer_tokens']={}
    config['policy'].update(max_quote_ms=500,sizing_mode='fixed',cycle_pages=False,
        activity_priority=False,universe_refresh_seconds=100,pending_poll_interval_seconds=.01)
    config['settlement_tokens']=[dict(config['settlement_tokens'][0],max_input_raw=str(10**18))]
    ps=pools();stop=threading.Event();good=[];sampled=[];writes=[]
    class Feed:
        sequence=0
        def __init__(self,*args):pass
        def poll(self):
            self.sequence+=1;now=time.monotonic_ns()
            h=dict(HEADER,baseFeePerGas=hex(1+self.sequence%2))
            return StateFrame(h,h,[p.json() for p in ps],now,now,time.time(),101,202)
    class Executor:
        contract=EXECUTOR
        context=None
        def __init__(self):self.rpc=FakeRPC()
        def bind_stop_event(self,event):pass
        def start_context_worker(self):pass
        def reconcile(self):pass
        def execute(self,candidate,route,states,anchor,deadline):
            if BAD in route.tokens:
                raise RPCError('eth_call',{'code':3,'message':'execution reverted','data':'0x6c25f7f8'})
            good.append(route.key);return {'tx_hash':'test-only'}
    def measure(rpc,sample,executor,owner,runtime):
        sampled.append(sample)
        return sample_result(sample,funded=sample.amount*96//100)
    original_write=pipeline.atomic_json
    def write(path,value):
        original_write(path,value)
        if path.name.startswith('decision-'):
            writes.append(copy.deepcopy(value))
            if value.get('rejected',{}).get('sampled_transfer_backoff') and good:stop.set()
    monkeypatch.setattr(pipeline,'RPC',FakeRPC)
    monkeypatch.setattr(transfers,'RPC',FakeRPC)
    monkeypatch.setattr(transfers,'measure_sample',measure)
    monkeypatch.setattr(pipeline,'StateFeed',Feed)
    monkeypatch.setattr(pipeline,'atomic_json',write)
    result=pipeline.run_pending(FakeRPC(),config,{'header':HEADER,'pools':[p.json() for p in ps]},
                                1.2,tmp_path,executor=Executor(),stop_event=stop)
    assert sampled and good and result['transfer_diagnostics_stopped']
    filtered=[r for r in writes if r.get('rejected',{}).get('sampled_transfer_backoff')]
    assert filtered and all(BAD not in c['tokens'] for r in filtered for c in r['candidates'])
