"""Independent transport recovery boundaries; local in-memory RPC, no sends."""
import json
from types import SimpleNamespace

import httpx
import pytest
from eth_abi import encode

from searcher import v4, v4_runtime as runtime
from searcher.core import ROOT,RPCUnavailable,Reject,USDC
from searcher.v4_market import Key


@pytest.mark.parametrize('error',[
    httpx.UnsupportedProtocol('unsupported local scheme'),
    httpx.LocalProtocolError('invalid local header'),
    httpx.HTTPStatusError('429',request=httpx.Request('POST','https://example.invalid'),
                          response=httpx.Response(429)),
    RPCUnavailable('RPC throttled; Retry-After exceeds bounded retry window'),
    Reject('V4 inconsistent atomic slot'),
])
def test_non_connection_errors_do_not_rebuild(error):
    assert not runtime.transport_stalled(error)


def test_worker_reconnects_real_reader_and_rechecks_protocol_without_losing_extra(tmp_path,monkeypatch):
    class Clock:
        now=100.
        def monotonic(self):return self.now
        def time(self):return 1700000000+self.now
    clock=Clock()
    class Stop:
        flag=False
        def is_set(self):return self.flag
        def set(self):self.flag=True
        def wait(self,n=0):clock.now+=n;return self.flag
    class BusyWake:
        def set(self):pass
        def clear(self):pass
        def wait(self,n):clock.now+=n;return True
    stop=Stop();keys=tuple(Key(USDC,'0xff00000000000000000000000000000000000091',f,1) for f in (100,500))
    path=tmp_path/'inventory.sqlite';inventory=runtime.Inventory(path)
    with inventory.db:
        for k in keys:inventory.db.execute('INSERT INTO pools VALUES(?,?,?,?)',(k.id,json.dumps(k.values()),1,'canonical'))
    inventory.close()
    class Discovery:
        def __init__(self,*args):self.thread=SimpleNamespace(join=lambda:None)
        def start(self):pass
        def take(self):return 1,{k.id:k for k in keys},set(),{}
    code=json.loads((ROOT/'tests/fixtures/v4/prestate.json').read_text())['response']['result'][v4.MANAGER]['code']
    anchor={'number':'0x64','timestamp':'0xc8','hash':'0x'+'01'*32}
    pending={'number':'0x65','timestamp':'0xca','parentHash':anchor['hash']}
    class RPC:
        chain_id=8453
        def __init__(self,healthy):self.healthy=healthy;self.closed=False;self.verifications=0;self.reads=[];self.measurements=[]
        def verify_chain(self):self.verifications+=1
        def request(self,method,params):
            assert method=='eth_getCode' and params==[v4.MANAGER,'latest']
            return code
        def batch(self,calls):
            if calls[0][0]=='eth_getBlockByNumber':return anchor,pending
            assert calls[0][0]=='eth_call'
            self.reads.append(clock.now)
            if not self.healthy:raise RPCUnavailable('RPC total request deadline exceeded')
            stop.set()
            return '0x'+encode(['uint256','uint256','bytes32[]','bytes32[]'],[101,202,[(123).to_bytes(32,'big')],[(456).to_bytes(32,'big')]]).hex(),anchor
        def close(self):self.closed=True
    old,new=RPC(False),RPC(True);created=[];readers=[]
    def factory():created.append(clock.now);return old if len(created)==1 else new
    class Reader(v4.Reader):
        def __init__(self,rpc,config):
            super().__init__(rpc,config);self.request_missing(keys,[123]);readers.append(self)
    monkeypatch.setattr(runtime,'time',clock)
    monkeypatch.setattr(runtime,'DiscoveryPump',Discovery)
    monkeypatch.setattr(runtime,'Reader',Reader)
    monkeypatch.setattr(runtime,'evaluate',lambda *args:{'block':100,'candidates':[],'missing_slots':[]})
    config={'_v4_inventory_path':str(path),'v4':{},'policy':{}}
    worker=runtime.Worker(factory,config,tmp_path/'session',stop,150)
    worker.wake=BusyWake();published=[];worker.offer=lambda frame,report:published.append(frame)
    worker.run()
    assert len(created)==2 and len(readers)==1
    assert old.reads==pytest.approx([100,102,106]) and new.reads==pytest.approx([114])
    assert old.verifications==new.verifications==1
    assert old.closed and new.closed and len(published)==1
    assert readers[0].extra[tuple(sorted(k.id for k in keys))]=={123:None}
    assert published[0].snapshot.storage[123]==456
    assert readers[0].last_phase=='complete' and worker.health['state']=='scanning'
