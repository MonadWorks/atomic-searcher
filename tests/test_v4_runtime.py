import copy
import json
import threading
import time
from eth_abi import encode
from searcher.core import USDC,ROOT
from searcher.v4 import MANAGER,Reader,Route
from searcher.v4_market import Key,key_from_input,KEY_TYPES
from searcher.v4_runtime import Inventory
from test_v4_integration import event

ASSET='0xff00000000000000000000000000000000000001'
class DiscoveryRPC:
    def __init__(self):self.tip=1000;self.branch='a';self.calls=[]
    def header(self,n=None):return {'number':hex(self.tip if n is None else n),'hash':f'{self.branch}-{self.tip if n is None else n}'}
    def request(self,method,params):
        self.calls.append((method,params))
        if method=='eth_getBlockByNumber':return self.header(int(params[0],16))
        if method=='eth_getLogs':return []
        raise AssertionError(method)
    def batch(self,calls):return [self.request(*c) for c in calls]

def test_inventory_backfills_while_tip_continues_and_survives_restart(tmp_path):
    rpc=DiscoveryRPC();path=tmp_path/'inventory.sqlite';inv=Inventory(path)
    for _ in range(20):inv.scan(rpc,rpc.header());rpc.tip+=2
    older=inv.get('backfill');cursor=inv.get('cursor')
    assert older<744 and cursor>=rpc.tip-10
    inv.close();inv=Inventory(path)
    assert inv.get('backfill')==older and inv.get('cursor')==cursor
    inv.scan(rpc,rpc.header());inv.close()

def test_inventory_reorg_drops_old_keys_and_restarts_discovery(tmp_path):
    rpc=DiscoveryRPC();inv=Inventory(tmp_path/'inventory.sqlite');k=Key(USDC,ASSET,500,10)
    inv.scan(rpc,rpc.header());h=rpc.header(760)
    inv.accept([event(k,h)],760,760,{760:h['hash']});assert k.id in inv.registry.keys
    rpc.branch='b';assert inv.check_reorg(rpc)
    assert not inv.registry.keys and inv.get('cursor') is None
    inv.close()

def test_public_calldata_key_recovery_is_hash_authenticated():
    k=Key(USDC,ASSET,500,10)
    raw='0x12345678'+encode(KEY_TYPES,k.values()).hex()
    assert key_from_input(raw,k.id,USDC)==k
    assert key_from_input(raw,'0x'+'ff'*32,USDC) is None
    assert key_from_input('0x00'*1,k.id,USDC) is None
    assert key_from_input('0x'+'00'*131073,k.id,USDC) is None

def test_recover_keys_from_public_reference_calldata():
    # Calldata, unlike a raw signed transaction, contains no signing material.
    fixture=json.loads((ROOT/'tests/fixtures/v4/route.json').read_text())
    for item in fixture:
        k=Key(*item['key'])
        nested='0xabcd'+encode(['bytes'],[encode(KEY_TYPES,k.values())]).hex()
        assert key_from_input(nested,k.id,USDC)==k

def test_existing_v4_deployment_can_resume_with_pending_trading_ledger(tmp_path,monkeypatch):
    from searcher import v4_onboarding as setup
    from searcher.core import load_config
    c=load_config();dep={'creation_data_hash':'0x'+'ab'*32,'creation_data':'0x1234'}
    journal=tmp_path/'v4'/('ab'*32);journal.mkdir(parents=True)
    (journal/'setup-deploy.json').write_text('{}');(journal/'setup-unpause.json').write_text('{}')
    contract='0x'+'12'*20;calls=[]
    monkeypatch.setattr(setup,'deployment',lambda c:dep)
    monkeypatch.setattr(setup,'verify_manager',lambda r:None)
    monkeypatch.setattr(setup,'verify_contract',lambda *a,**kw:{'contract':contract,'paused':False})
    monkeypatch.setattr(setup,'ensure_migration_account_idle',lambda *a:(_ for _ in ()).throw(AssertionError('pending account must enter recovery')))
    monkeypatch.setattr(setup,'submit_setup_step',lambda *a,**kw:calls.append(str(a[3])) or {'contractAddress':contract,'transactionHash':'0x123'})
    result=setup.setup_executor(None,c,None,tmp_path,require_validation=False)
    assert result['contract']==contract and len(calls)==2

def test_pending_initialize_removed_and_ttl_never_fill_canonical_inventory(tmp_path):
    inv=Inventory(tmp_path/'inventory.sqlite');k=Key(USDC,ASSET,500,10);h={'number':'0x1','hash':'a'}
    inv.pending(event(k,h),now=1);assert k.id in inv.registry.keys
    inv.pending(dict(event(k,h),removed=True),now=2);assert k.id not in inv.registry.keys
    inv.pending(event(k,h),now=3);inv.expire_pending(now=24);assert k.id not in inv.registry.keys
    assert inv.db.execute('SELECT COUNT(*) FROM pools').fetchone()[0]==0
    inv.close()

def test_backfill_completion_does_not_stop_new_blocks(tmp_path):
    rpc=DiscoveryRPC();inv=Inventory(tmp_path/'inventory.sqlite')
    inv.put('cursor',999);inv.put('backfill',0);inv.put('scan_turn',2);inv.db.commit()
    inv.scan(rpc,rpc.header());assert inv.get('cursor')==1000;inv.close()

def test_unrelated_empty_report_does_not_overwrite_executable_candidate(tmp_path):
    from types import SimpleNamespace
    from searcher.v4_runtime import Worker
    config={'policy':{'snapshot_max_age_ms':2000}}
    worker=Worker(None,config,tmp_path,threading.Event(),time.monotonic()+5)
    key=Key(USDC,ASSET,500,10);other=Key(USDC,ASSET,600,10)
    frame=lambda k:SimpleNamespace(keys=(k,),requested_ns=time.monotonic_ns(),anchor={})
    good={'block':1,'candidates':[{'net_after_modelled_cost':100}]};empty={'block':1,'candidates':[]}
    worker.offer(frame(key),good);worker.offer(frame(other),empty)
    result=worker.consume(None)
    assert result['candidates']==1 and result['status']=='awaiting_executor'

def test_shared_stream_filters_virtual_ids_and_delivers_both_protocols(tmp_path):
    from collections import deque
    import pytest
    from searcher.activity import stream_session,StreamBudget
    from searcher.v4_runtime import stream_options
    from searcher.v4_market import INITIALIZE,SWAP
    from searcher.events import V3_SWAP
    cl='0x'+'11'*20;k=Key(USDC,ASSET,500,10);stop=threading.Event();received=[]
    class Socket:
        def __init__(self):self.messages=deque();self.filters=[]
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def send(self,raw):
            obj=json.loads(raw);method=obj['method'];result='0x2105' if method=='eth_chainId' else str(obj['id'])
            self.messages.append(json.dumps({'id':obj['id'],'result':result}))
            if method=='eth_subscribe':
                filt=obj['params'][1];self.filters.append(filt)
                if filt['address']==[cl]:log={'address':cl,'topics':[V3_SWAP]}
                elif filt['topics']==[INITIALIZE]:log=event(k,{'number':'0x1','hash':'x'})
                else:log={'address':MANAGER,'topics':[SWAP,k.id]}
                self.messages.append(json.dumps({'method':'eth_subscription','params':{'subscription':result,'result':log}}))
        def recv(self,timeout):
            if self.messages:return self.messages.popleft()
            stop.set();raise TimeoutError
    socket=Socket()
    with pytest.raises(InterruptedError):
        stream_session('wss://example.invalid',8453,lambda:{cl,'v4:init','v4:'+k.id},stop,time.monotonic()+2,
            lambda logs,source:received.extend(logs),lambda *a,**kw:None,StreamBudget(tmp_path/'budget.json',20000),
            connect=lambda *a,**kw:socket,**stream_options())
    assert len(received)==3
    assert {r['topics'][0] for r in received}=={V3_SWAP,INITIALIZE,SWAP}
    assert all(all(not a.startswith('v4:') for a in f['address']) for f in socket.filters)

def test_worker_event_reads_publishes_archives_and_stops_without_wallet(tmp_path,monkeypatch):
    import searcher.v4_runtime as module
    from searcher.v4 import Frame
    from searcher.v4_market import Snapshot
    keys=[Key(USDC,ASSET,500,10),Key(USDC,ASSET,3000,60)]
    path=tmp_path/'durable.sqlite';inv=Inventory(path);h={'number':'0x1','hash':'a'}
    inv.accept([event(k,h) for k in keys],1,1,{1:'a'});inv.close()
    stop=threading.Event();calls=[]
    config={'_v4_inventory_path':str(path),'policy':{'snapshot_max_age_ms':2000},'v4':{'poll_seconds':1}}
    class ReadRPC:
        def header(self):return h
        def close(self):calls.append('closed')
    class FakeReader:
        def __init__(self,*a):pass
        def read(self,pair):
            calls.append('read');now=time.monotonic_ns()
            return Frame(h,{},Snapshot({}),tuple(pair),now,now,time.time())
        def request_missing(self,*a):pass
    monkeypatch.setattr(module,'Reader',FakeReader)
    monkeypatch.setattr(module.Inventory,'scan',lambda *a,**kw:set())
    monkeypatch.setattr(module.Inventory,'active_keys',lambda *a,**kw:set())
    monkeypatch.setattr(module,'evaluate',lambda f,c:{'block':1,'missing_slots':[],'candidates':[{'net_after_modelled_cost':100}]})
    worker=module.Worker(ReadRPC,config,tmp_path/'session',stop,time.monotonic()+4);worker.start()
    until=time.monotonic()+2
    while time.monotonic()<until and not list((tmp_path/'session/v4-evidence').glob('*.gz')):time.sleep(.01)
    assert list((tmp_path/'session/v4-evidence').glob('*.gz'))
    assert worker.consume(None)['status']=='awaiting_executor'
    started=time.monotonic();stop.set();worker.thread.join(timeout=.4)
    assert not worker.thread.is_alive() and time.monotonic()-started<.4 and calls[-1]=='closed'

def test_new_setup_waits_for_finality_and_never_reuses_orphan_nonce(tmp_path):
    import sqlite3
    import pytest
    from searcher.v4_onboarding import ensure_setup_account_ready
    from searcher.onboarding import SetupPending
    db=sqlite3.connect(tmp_path/'transactions.sqlite')
    db.execute('CREATE TABLE attempts(id TEXT,state TEXT)');db.execute('CREATE TABLE intents(attempt TEXT,outcome TEXT)')
    db.execute("INSERT INTO attempts VALUES('x','success')")
    db.execute('INSERT INTO intents VALUES(?,?)',('x',json.dumps({'block':10,'block_hash':'original'})));db.commit();db.close()
    class Headers:
        final=9;canonical='original'
        def request(self,method,params):
            assert method=='eth_getBlockByNumber'
            return {'number':hex(self.final),'hash':'final'} if params[0]=='finalized' else {'number':'0xa','hash':self.canonical}
    rpc=Headers()
    with pytest.raises(SetupPending):ensure_setup_account_ready(rpc,tmp_path)
    rpc.final=12;rpc.canonical='replacement'
    with pytest.raises(SetupPending):ensure_setup_account_ready(rpc,tmp_path)
    rpc.canonical='original';ensure_setup_account_ready(rpc,tmp_path)
