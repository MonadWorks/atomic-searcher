import copy
import json
import time
import threading

import pytest

from searcher.core import WETH, load_config
from searcher.routes import RouteIndex
from searcher.engine import evaluate
from searcher.statefeed import StateFrame
from searcher.candidate_refresh import refresh_candidate
from test_receipt_eligibility import pools, HEADER


@pytest.mark.parametrize('still_profitable', [True,False])
def test_refresh_recomputes_every_leg_and_keeps_first_seen_provenance(monkeypatch,still_profitable):
    config=load_config();config['policy']['max_quote_ms']=500
    config['settlement_tokens']=[dict(config['settlement_tokens'][0],max_input_raw=str(10**18))]
    original=pools()[2:];index=RouteIndex(original,[WETH],3)
    discovery=evaluate(index,config,HEADER)['candidates'][0]
    route=index.routes[discovery['route_key']]
    fresh=copy.deepcopy(original)
    if not still_profitable: fresh[0].reserve1=fresh[1].reserve1
    now=time.monotonic_ns()
    new_header=dict(HEADER,number='0x65',hash='0x'+'22'*32)
    frame=StateFrame(new_header,new_header,[p.json() for p in fresh],now,now,time.time(),102,204)
    seen=[]
    class Feed:
        def __init__(self,rpc,local,saved):
            assert not local['policy']['cycle_pages']
            self.pools=saved['pools'];assert len(self.pools)==2
        def poll(self,**kwargs): seen.append(kwargs);return frame
    monkeypatch.setattr('searcher.candidate_refresh.StateFeed',Feed)
    candidate,states,anchor,deadline,evidence=refresh_candidate(None,config,route,index.states)
    assert bool(candidate)==still_profitable
    assert anchor==new_header and deadline==now+2_000_000_000
    assert all(p.block==101 and p.block_hash==frame.identity for p in states.values())
    assert evidence['requested_ns']==now and evidence['frame']['snapshot_pools']==frame.snapshot_pools
    assert config['policy']['cycle_pages'] and index.states[original[0].address].reserve1==2*10**20
    assert seen==[{'deep_pools':[]}]


def test_prepare_cli_saves_pool_facts_without_expanding_cycle_products(monkeypatch,tmp_path):
    from searcher import __main__ as cli
    from searcher.universe import CyclePages
    ps=pools()[2:];pages=CyclePages(ps,[WETH],3)
    snapshot={'header':HEADER,'pools':[p.json() for p in ps],'cycle_groups':pages.groups,'scope':'test'}
    monkeypatch.setattr(cli,'RPC',lambda:None)
    monkeypatch.setattr('searcher.engine.prepare',lambda *args:snapshot)
    destination=tmp_path/'universe.json'
    monkeypatch.setattr('sys.argv',['searcher','prepare','--out',str(destination)])
    cli.main()
    result=json.loads(destination.read_text())
    assert 'cycle_groups' not in result and result['pools']==snapshot['pools']


@pytest.mark.parametrize('outcome',['profitable','disappeared','expired'])
def test_pipeline_passes_only_recomputed_fresh_quote_to_executor(monkeypatch,tmp_path,outcome):
    import searcher.pipeline as pipeline
    from test_runtime import FakeRPC
    config=load_config();config['policy'].update(max_quote_ms=500,cycle_pages=False,
        activity_priority=False,universe_refresh_seconds=100,pending_poll_interval_seconds=.01)
    config['settlement_tokens']=[dict(config['settlement_tokens'][0],max_input_raw=str(10**18))]
    original=pools()[2:];fresh=copy.deepcopy(original)
    fresh[0].reserve1=(3 if outcome!='disappeared' else 1)*10**20
    stop=threading.Event();executed=[];read_count=[]
    new_header=dict(HEADER,number='0x65',hash='0x'+'22'*32)
    class Feed:
        def __init__(self,*args): pass
        def poll(self):
            now=time.monotonic_ns()
            return StateFrame(HEADER,HEADER,[p.json() for p in original],now-3_000_000_000,now,time.time(),101,202)
    class RefreshFeed:
        def __init__(self,rpc,local,saved): self.pools=saved['pools']
        def poll(self,**kwargs):
            read_count.append(1);now=time.monotonic_ns()
            return StateFrame(new_header,new_header,[p.json() for p in fresh],
                now-(3_000_000_000 if outcome=='expired' else 0),now,time.time(),102,204)
    class Executor:
        context=None
        def bind_stop_event(self,event): pass
        def start_context_worker(self): pass
        def reconcile(self): pass
        def execute(self,candidate,route,states,anchor,deadline):
            executed.append((copy.deepcopy(candidate),states,anchor,deadline))
            stop.set();return {'tx_hash':'0x'+'11'*32}
    monkeypatch.setattr(pipeline,'RPC',FakeRPC)
    monkeypatch.setattr(pipeline,'StateFeed',Feed)
    monkeypatch.setattr('searcher.candidate_refresh.StateFeed',RefreshFeed)
    saved={'header':HEADER,'pools':[p.json() for p in original]}
    pipeline.run_pending(FakeRPC(),config,saved,.18,tmp_path,executor=Executor(),stop_event=stop)
    assert bool(executed)==(outcome=='profitable') and read_count
    reports=[json.loads(f.read_text()) for f in tmp_path.glob('decision-*.json')]
    assert reports
    candidate=next(c for d in reports for c in d['candidates'] if 'execution_recheck' in c)
    assert candidate['execution_recheck']['frame']['header']==new_header
    if executed:
        quote,states,anchor,deadline=executed[0]
        assert anchor==new_header and json.loads(json.dumps(quote))==candidate['execution_recheck']['decision']['candidates'][0]
        assert quote['amount_in']!=candidate['amount_in'] or quote['gross_surplus']!=candidate['gross_surplus']
        assert states[fresh[0].address].reserve1==3*10**20
    else:
        assert candidate['status']==('recheck_below_floor' if outcome=='disappeared' else 'expired_before_probe')
