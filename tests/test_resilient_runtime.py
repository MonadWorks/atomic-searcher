import json
import threading
from pathlib import Path

import httpx
import pytest
from eth_abi import encode

from searcher.core import Reject, RPCError, RPCUnavailable, RPCAccessDenied, atomic_json, load_config
from searcher.recovery import retry_connection
from searcher.snapshots import read_universe, write_universe


class ImmediateWait:
    def __init__(self, cancel_after=None): self.delays=[]; self.cancel_after=cancel_after
    def is_set(self): return self.cancel_after is not None and len(self.delays)>=self.cancel_after
    def wait(self, delay): self.delays.append(delay); return self.is_set()


def test_connection_recovers_after_long_outage_without_new_unlock():
    stop=ImmediateWait(); attempts=[]; notices=[]
    account=object()
    def recover():
        attempts.append(account)
        if len(attempts)<=9: raise RPCUnavailable('temporary timeout')
        return account
    assert retry_connection(recover,stop=stop,on_retry=lambda *x:notices.append(x)) is account
    assert len(attempts)==10 and all(a is account for a in attempts)
    assert stop.delays==[1,2,4,8,16,30,30,30,30]


@pytest.mark.parametrize('failure',[Reject('wrong chain'),RPCAccessDenied('denied'),ValueError('bad config')])
def test_semantic_failures_are_not_restarted(failure):
    stop=ImmediateWait()
    def fail(): raise failure
    with pytest.raises(type(failure)): retry_connection(fail,stop=stop)
    assert not stop.delays


def test_stop_during_retry_never_invokes_operation_again():
    stop=ImmediateWait(cancel_after=1); attempts=[]
    def fail(): attempts.append(1);raise RPCUnavailable('offline')
    with pytest.raises(InterruptedError): retry_connection(fail,stop=stop)
    assert attempts==[1]


def test_pending_setup_resumes_same_journal():
    from searcher.onboarding import SetupPending
    attempts=[];journal=object()
    def resume():
        attempts.append(journal)
        if len(attempts)==1: raise SetupPending('same hash is waiting')
        return journal
    assert retry_connection(resume,stop=ImmediateWait(),pending=(SetupPending,)) is journal
    assert attempts==[journal,journal]


def test_saved_universe_preserves_pool_facts_without_derived_groups(tmp_path):
    p=tmp_path/'universe.json'
    facts={'known_pools':[{'address':'pool'}],'pools':[{'valid':True}],
           'discovery_cursor':42,'last_seen':{'pool':42},'cycle_groups':[['pool','other']]*5000}
    write_universe(p,facts)
    assert p.stat().st_size<500
    assert read_universe(p)=={k:v for k,v in facts.items() if k!='cycle_groups'}
    assert len(facts['cycle_groups'])==5000


def test_large_legacy_cache_migration_does_not_load_derived_addresses(tmp_path):
    p=tmp_path/'universe.json'
    with p.open('w') as f:
        f.write('{\n  "known_pools": [{"address":"pool"}],\n  "cycle_groups": [\n')
        row='    ["'+'a'*150+'", "'+'b'*150+'"],\n'
        for _ in range(60000):f.write(row)
        f.write('    ["last", "group"]\n  ],\n  "discovery_cursor": 42\n}\n')
    assert p.stat().st_size>16*1024**2
    assert read_universe(p)=={'known_pools':[{'address':'pool'}],'discovery_cursor':42}


def test_page_hot_path_does_not_traverse_complete_route_table():
    from searcher.universe import CyclePages
    from types import SimpleNamespace
    pools=[SimpleNamespace(address='p'+str(n),token0='a',token1='b') for n in range(3)]
    pages=CyclePages(pools,['a'],2,groups=[['p0','p1'],['p0','p2']])
    class IndexedOnly(list):
        def __iter__(self):raise AssertionError('hot path scanned all cycles')
    pages.groups=IndexedOnly(pages.groups)
    assert pages.take(3,group_limit=2)


def test_cached_group_addresses_are_shared_with_authenticated_pool_keys():
    from searcher.universe import CyclePages
    from types import SimpleNamespace
    a='0x'+'1'*40;b='0x'+'2'*40
    pools=[SimpleNamespace(address=v,token0='a',token1='b') for v in (a,b)]
    copied=json.loads(json.dumps([[a,b],[a,b]]))
    pages=CyclePages(pools,['a'],2,groups=copied)
    for group in pages.groups:assert group[0] is a and group[1] is b


def test_explicit_v2_invariant_is_not_retried_with_more_gas():
    import time
    from searcher.execution import verified_call_gas_budget,execution_failure_details
    raw='0x08c379a0'+encode(['string'],['UniswapV2: K']).hex()
    err=RPCError('eth_call',{'code':3,'message':'execution reverted','data':raw})
    class RevertRPC:
        def __init__(self):self.calls=[]
        def request(self,method,params):self.calls.append((method,params));raise err
    rpc=RevertRPC()
    with pytest.raises(RPCError):
        verified_call_gas_budget(rpc,{}, {}, 100000,800000,time.monotonic_ns()+10**10)
    assert len(rpc.calls)==1
    assert execution_failure_details(err)['revert_name']=='UniswapV2Invariant'


def test_launch_retries_setup_with_one_local_unlock(tmp_path,monkeypatch,capsys):
    import sys
    from types import SimpleNamespace
    from searcher import __main__ as cli, execution, onboarding, runner, service, recovery
    monkeypatch.setattr(service,'ROOT',tmp_path)
    config=cli.load_config();config['v4']={'enabled':False};config['structural']={'enabled':False}
    config['owner']='0x'+'2'*40  # Explicit synthetic owner, not the bundled example.
    monkeypatch.setattr(cli,'load_config',lambda *a:config)
    calls={'unlock':0,'setup':0,'executor':0,'run':0,'close':0}; account=object()
    class NoNetwork:
        def __init__(self,**kwargs):pass
        def verify_chain(self):pass
        def close(self):calls['close']+=1
    def unlock(*args):calls['unlock']+=1;return account
    def setup(rpc,config,signer):
        assert signer is account
        calls['setup']+=1
        if calls['setup']==1:raise RPCUnavailable('offline')
        if calls['setup']==2:raise onboarding.SetupPending('existing hash')
        return {'contract':'test'}
    class FakeExecutor:
        def __init__(self,rpc,config,contract,signer,directory):
            assert signer is account
            calls['executor']+=1
            if calls['executor']==1:raise RPCUnavailable('offline')
        def close(self):pass
    def run(config,seconds,executor):calls['run']+=1;return {'state':'stopped'}
    real_retry=recovery.retry_connection
    monkeypatch.setattr(recovery,'retry_connection',lambda fn,**kw:real_retry(fn,stop=ImmediateWait(),**kw))
    monkeypatch.setattr(execution,'BroadcastRPC',NoNetwork)
    monkeypatch.setattr(execution,'verify_execution_context',lambda rpc:None)
    monkeypatch.setattr(execution,'Executor',FakeExecutor)
    monkeypatch.setattr(onboarding,'preflight_setup',lambda *args:None)
    monkeypatch.setattr(onboarding,'unlock_wallet',unlock)
    monkeypatch.setattr(onboarding,'setup_executor',setup)
    monkeypatch.setattr(runner,'run',run)
    monkeypatch.setattr(sys,'argv',['searcher','launch','--seconds','1'])
    cli.main()
    assert calls=={'unlock':1,'setup':3,'executor':2,'run':1,'close':1}
    assert not service.trading_status()['process_running']


@pytest.mark.parametrize('failure',[httpx.UnsupportedProtocol('bad scheme'),httpx.LocalProtocolError('bad request')])
def test_deterministic_transport_configuration_errors_are_not_retried(failure):
    stop=ImmediateWait()
    def fail():raise failure
    with pytest.raises(type(failure)):retry_connection(fail,stop=stop)
    assert not stop.delays


def test_large_new_metadata_cache_roundtrips_without_derived_list(tmp_path):
    p=tmp_path/'universe.json'
    facts={'known_pools':[], 'metadata':'x'*(17*1024**2), 'discovery_cursor':77}
    write_universe(p,facts)
    assert read_universe(p)==facts
