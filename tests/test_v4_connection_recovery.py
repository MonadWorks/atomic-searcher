"""A poisoned read connection recovers without restarting the trading process."""
import json
from types import SimpleNamespace
import httpx
import pytest
from searcher import v4_runtime as runtime
from searcher.core import RPCUnavailable, Reject, USDC
from searcher.v4_market import Key


@pytest.mark.parametrize('error', [RPCUnavailable('RPC total request deadline exceeded'),
                                  httpx.ReadTimeout('body stalled')])
@pytest.mark.parametrize('factory_fails_once', [False, True])
def test_worker_rebuilds_only_transport_and_keeps_depth_addresses(tmp_path, monkeypatch, error, factory_fails_once):
    class Clock:
        now=100.0
        def monotonic(self):return self.now
        def time(self):return 1700000000+self.now
    clock=Clock()
    class Stop:
        flag=False
        def is_set(self):return self.flag
        def set(self):self.flag=True
        def wait(self,n=0):clock.now+=n;return self.flag
    stop=Stop()
    class Wake:
        def set(self):pass
        def clear(self):pass
        def wait(self,n):clock.now+=n;return False
    keys=tuple(Key(USDC,'0xff00000000000000000000000000000000000091',f,1) for f in (100,500))
    path=tmp_path/'inventory.sqlite';inv=runtime.Inventory(path)
    with inv.db:
        for k in keys:inv.db.execute('INSERT INTO pools VALUES(?,?,?,?)',(k.id,json.dumps(k.values()),1,'canonical'))
    inv.close()
    class Discovery:
        def __init__(self,*a):self.thread=SimpleNamespace(join=lambda:None)
        def start(self):pass
        def take(self):return 1,{k.id:k for k in keys},set(),{}
    class Connection:
        def __init__(self,healthy):self.healthy=healthy;self.closed=False;self.measurements=[]
        def close(self):self.closed=True
    old,new=Connection(False),Connection(True);factory_calls=[];readers=[];reads=[]
    def factory():
        factory_calls.append(clock.now)
        if len(factory_calls)==1:return old
        if factory_fails_once and len(factory_calls)==2:raise httpx.ConnectError('temporary reconnect failure')
        return new
    class Reader:
        def __init__(self,rpc,c):
            self.rpc=rpc;self.verified=True;self.extra={('pair',):{123:None}};self.last_phase='atomic_state';readers.append(self)
        def read(self,selected):
            reads.append((clock.now,self.rpc));assert set(selected)==set(keys)
            if not self.rpc.healthy:raise error
            assert not self.verified and self.extra=={('pair',):{123:None}}
            stop.set()
            return SimpleNamespace(keys=keys,received_utc=clock.time(),anchor={'number':'0x64'})
        def request_missing(self,*a):pass
    monkeypatch.setattr(runtime,'time',clock);monkeypatch.setattr(runtime,'DiscoveryPump',Discovery)
    monkeypatch.setattr(runtime,'Reader',Reader)
    monkeypatch.setattr(runtime,'evaluate',lambda *a:{'block':100,'candidates':[],'missing_slots':[]})
    worker=runtime.Worker(factory,{'_v4_inventory_path':str(path),'v4':{},'policy':{}},tmp_path/'session',stop,300)
    worker.wake=Wake();offers=[];worker.offer=lambda *a:offers.append(a)
    worker.run()
    assert len(readers)==1 and len(offers)==1 and old.closed and new.closed
    assert len(reads)==(5 if factory_fails_once else 4)
    assert [t for t,_ in reads[:4]]==pytest.approx([100,102,106,114])
    assert worker.health['state']=='scanning' and worker.health['connection_rebuilds']==1
    assert worker.health['error'] is None and worker.health['transport_failures']==0


@pytest.mark.parametrize('error',[RPCUnavailable('RPC throttled; Retry-After exceeds bounded retry window'),
                                  Reject('V4 state anchor changed'),Reject('V4 inconsistent atomic slot')])
def test_quota_and_semantic_failures_never_rebuild_to_evade_limits(error):
    assert not runtime.transport_stalled(error)


def test_health_contains_transport_timing_but_never_call_or_endpoint():
    row={'method':'batch','methods':['eth_call'],'network_total_ms':3000,'outcome':'error',
         'url':'https://private.invalid/secret','params':['private'],'response':'private'}
    assert runtime.read_transport(SimpleNamespace(measurements=[row]))=={
        'method':'batch','methods':['eth_call'],'network_total_ms':3000,'outcome':'error'}


def test_reader_identifies_atomic_state_timeout_without_advancing_state(monkeypatch):
    from searcher import v4
    from searcher.core import load_config
    monkeypatch.setattr(v4,'verify_manager',lambda rpc:None)
    anchor={'number':'0x64','timestamp':'0xc8','hash':'0x'+'01'*32}
    pending={'number':'0x65','timestamp':'0xca','parentHash':anchor['hash']}
    class RPC:
        calls=0
        def batch(self,calls):
            self.calls+=1
            if self.calls==1:return anchor,pending
            raise RPCUnavailable('RPC total request deadline exceeded')
    reader=v4.Reader(RPC(),load_config())
    key=Key(USDC,'0xff00000000000000000000000000000000000091',100,1)
    with pytest.raises(RPCUnavailable):reader.read([key])
    assert reader.last_phase=='atomic_state'
