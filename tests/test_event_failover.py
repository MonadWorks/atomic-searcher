import time
import httpx
import pytest
from searcher.core import RPC,Reject,RPCError
from searcher.event_intake import EventRPC

def test_cooling_primary_switches_once_and_keeps_working_peer(monkeypatch):
    EventRPC.preferred.clear();rpc=EventRPC('https://base.drpc.org',timeout=.1);calls=[]
    from searcher.core import PublicProviderBudget
    rpc.provider_budget=PublicProviderBudget();rpc.provider_budget.cooldown_until=time.monotonic()+60
    def request(peer,method,params):
        calls.append((peer.url,method));return '0x2105' if method=='eth_chainId' else []
    monkeypatch.setattr(RPC,'request',request)
    try:
        assert rpc.request('eth_getLogs',[{}])==[]
        assert rpc.active_host=='base.gateway.tenderly.co'
        assert rpc.request('eth_getLogs',[{}])==[]
        assert len(calls)==3 and all('tenderly' in u for u,m in calls)
    finally:rpc.close();EventRPC.preferred.clear()

def test_throttle_and_wrong_chain_are_skipped_but_never_signed(monkeypatch):
    EventRPC.preferred.clear();rpc=EventRPC('https://node.invalid',timeout=.1);calls=[]
    def request(peer,method,params):
        calls.append((peer.url,method))
        if peer is rpc:
            r=httpx.Response(429,request=httpx.Request('POST',peer.url));r.raise_for_status()
        if 'tenderly' in peer.url:return '0x1'
        return '0x2105' if method=='eth_chainId' else []
    monkeypatch.setattr(RPC,'request',request)
    try:
        assert rpc.request('eth_getLogs',[{}])==[]
        assert rpc.active_host=='base.public.blockpi.network'
        assert not any('tenderly' in u and m!='eth_chainId' for u,m in calls)
        with pytest.raises(Reject):rpc.request('eth_sendRawTransaction',['secret'])
        assert not any(m=='eth_sendRawTransaction' for u,m in calls)
    finally:rpc.close();EventRPC.preferred.clear()

def test_contract_revert_is_returned_without_retry_on_other_nodes(monkeypatch):
    EventRPC.preferred.clear();rpc=EventRPC('https://node.invalid',timeout=.1);calls=[]
    def request(peer,method,params):
        calls.append((peer.url,method))
        if method=='eth_chainId':return '0x2105'
        raise RPCError(method,{'code':3,'message':'execution reverted','data':'0xdeadbeef'})
    monkeypatch.setattr(RPC,'request',request)
    try:
        with pytest.raises(RPCError):rpc.request('eth_call',[{}])
        assert len(calls)==2 and all(u=='https://node.invalid' for u,m in calls)
    finally:rpc.close();EventRPC.preferred.clear()

def test_provider_historical_range_restriction_uses_same_page_on_working_peer(monkeypatch):
    EventRPC.preferred.clear();rpc=EventRPC('https://node.invalid',timeout=.1);calls=[]
    query=[{'fromBlock':hex(52714733),'toBlock':hex(52714932)}]
    def request(peer,method,params):
        if method=='eth_chainId':return '0x2105'
        calls.append((peer.url,params))
        if peer is rpc:
            r=httpx.Response(400,json={'error':{'code':35,'message':'ranges over 10000 blocks are not supported on free plan'}},request=httpx.Request('POST',peer.url));r.raise_for_status()
        return []
    monkeypatch.setattr(RPC,'request',request)
    try:
        assert rpc.request('eth_getLogs',query)==[]
        assert rpc.active_host=='base.gateway.tenderly.co'
        assert len(calls)==2 and all(params==query for _,params in calls)
        rpc.request('eth_getLogs',query)
        assert calls[-1][0]=='https://base.gateway.tenderly.co'
    finally:rpc.close();EventRPC.preferred.clear()

def test_http_bad_input_and_contract_reverts_are_not_provider_limits():
    for error in [{'code':-32602,'message':'invalid block range'}, {'code':3,'message':'execution reverted: block range'}]:
        r=httpx.Response(400,json={'error':error},request=httpx.Request('POST','https://node.invalid'))
        try:r.raise_for_status()
        except httpx.HTTPStatusError as exc:assert not EventRPC.retryable(exc)

def test_discovery_workers_share_provider_budget_without_using_execution_budget():
    a=RPC('https://base.gateway.tenderly.co');b=EventRPC('https://base.gateway.tenderly.co')
    c=RPC('https://base.public.blockpi.network/v1/rpc/public');main=RPC('https://mainnet.base.org')
    try:
        assert a.provider_budget is b.provider_budget and a.provider_budget is not c.provider_budget
        assert main.provider_budget is None and main.public_rate_limiter is not None
        assert a.provider_budget.snapshot()['hostname']=='base.gateway.tenderly.co'
        assert c.provider_budget.maximum_rate==2 and c.provider_budget.max_batch<=10
    finally:
        for rpc in (a,b,c,main):rpc.close()
