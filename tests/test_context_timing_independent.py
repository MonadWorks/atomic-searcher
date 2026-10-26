"""Independent timing checks using real RPC parsing and no sockets or signer."""
from types import SimpleNamespace

from eth_abi import decode, encode
import httpx
import pytest

from searcher.core import MULTICALL, RPC, Reject, USDC, data, load_config, unhex
from searcher.execution import Executor, ORACLE, execution_failure_details
from test_rpc_timing import transport, Limiter, Session


class ContextSession:
    def __init__(self, clock):
        self.clock, self.payloads = clock, []
        self.bad_nonce = False

    def value(self, method, params):
        if method == 'eth_getTransactionCount':
            return 'invalid' if self.bad_nonce else '0x7'
        if method == 'eth_getBalance':
            return hex(10**18)
        if method == 'eth_getBlockByNumber':
            pending = params[0] == 'pending'
            return dict(number=hex(101 if pending else 100),
                        timestamp=hex(202 if pending else 200),
                        hash='0x'+'11'*32, parentHash='0x'+'11'*32)
        if method == 'eth_feeHistory':
            return {'reward': [['0x3'], ['0x5']]}
        assert method == 'eth_call'
        target, calldata = params[0]['to'].lower(), params[0]['data']
        if target == MULTICALL:
            rows = decode(['(address,bool,bytes)[]'], unhex(calldata)[4:])[0]
            returns = [(True, unhex(self.value('eth_call',
                       [{'to': to, 'data': '0x'+payload.hex()}, 'pending'])))
                       for to, _, payload in rows]
            return '0x'+encode(['(bool,bytes)[]'], [returns]).hex()
        if target == ORACLE:
            value = 111 if calldata[:10] == data('getL1FeeUpperBound(uint256)')[:10] else 222
            return '0x'+encode(['uint256'], [value]).hex()
        return '0x'+encode(['uint256','uint160','uint32','uint256'], [333,444,555,666]).hex()

    def stream(self, method, url, json):
        self.payloads.append(json)
        session = self
        class Stream:
            async def __aenter__(self):
                session.clock.advance(.1)
                rows = [{'id':row['id'], 'result':session.value(row['method'], row['params'])}
                        for row in reversed(json)]
                class Response(httpx.Response):
                    async def aread(self):
                        session.clock.advance(.1)
                        return self.content
                return Response(200, json=rows,
                                request=httpx.Request('POST','https://example.invalid'))
            async def __aexit__(self, *args):
                pass
        return Stream()

    async def aclose(self):
        pass


def context_executor(transport, aggregate):
    rpc, clock = transport
    rpc.session = ContextSession(clock)
    rpc.multicall_verified = True
    rpc.public_rate_limiter = Limiter(clock, delay=1.6)
    e = Executor.__new__(Executor)
    e.config = load_config()
    e.config['policy'].update(aggregate_execution_context=aggregate, profit_aware_bidding=True)
    e.owner, e.rpc = e.config['owner'], rpc
    e.context, e.context_error = {'old': True}, 'old error'
    e.verify_settlements = lambda: None
    e.db = SimpleNamespace(execute=lambda *args: SimpleNamespace(fetchone=lambda: None))
    return e, rpc, clock


@pytest.mark.parametrize('aggregate', [False, True])
def test_real_context_batch_preserves_fields_and_execute_rejects_after_1500ms(transport, aggregate):
    e, rpc, clock = context_executor(transport, aggregate)
    e.refresh_context()
    assert e.context['freshness_origin'] == 'network_send'
    assert e.context['requested_ns']-e.context['refresh_started_ns'] == 1_600_000_000
    assert e.context['received_ns']-e.context['requested_ns'] == 200_000_000
    assert e.context['nonce'] == e.context['pending_nonce'] == 7
    assert e.context['extra_fee'] == 666
    assert e.context['usdc_at_native_cap'] == e.context['weth_profit_floor'] == 333
    assert e.context['observed_priority_fee'] == 5
    assert len(rpc.session.payloads) == 1
    assert any(row['method']=='eth_call' and row['params'][0]['to']==MULTICALL
               for row in rpc.session.payloads[0]) is aggregate

    route = SimpleNamespace(key='independent-timing', pools=('a','b'), tokens=(USDC, '0x'+'22'*20, USDC))
    candidate = dict(route_key=route.key, pools=list(route.pools), tokens=list(route.tokens), amount_in=1,
                     cost_budget=dict(gas_limit=21000, max_fee_per_gas=1, priority_fee=0))
    # A deliberately different anchor stops the fresh call at the next guard,
    # without invoking an eth_call, ledger write, signer, or submission.
    anchor = {'hash': '0x'+'99'*32}
    clock.now = e.context['requested_ns']/1e9+1.5
    with pytest.raises(Reject, match='anchor expired during preparation'):
        e.execute(candidate, route, {}, anchor, round((clock.now+10)*1e9))
    clock.advance(.000001)
    with pytest.raises(Reject, match='execution context stale') as caught:
        e.execute(candidate, route, {}, anchor, round((clock.now+10)*1e9))
    diagnostics = execution_failure_details(caught.value)['context_timing']
    assert diagnostics['age_ms'] > diagnostics['max_age_ms'] == 1500
    assert diagnostics['freshness_origin'] == 'network_send'
    assert len(rpc.session.payloads) == 1


@pytest.mark.parametrize('aggregate', [False, True])
def test_real_batch_decode_failure_cannot_replace_previous_complete_context(transport, aggregate):
    e, rpc, clock = context_executor(transport, aggregate)
    e.refresh_context()
    previous = e.context
    rpc.session.bad_nonce = True
    with pytest.raises(ValueError):
        e.refresh_context()
    assert e.context is previous


def test_partial_fragment_http_failure_keeps_earliest_send_and_error_status(transport):
    rpc, clock = transport
    rpc.public_rate_limiter = Limiter(clock, delay=.6)
    rpc.session = Session(clock, statuses=(200,503))
    with pytest.raises(httpx.HTTPStatusError):
        rpc.batch([('eth_call',[i]) for i in range(40)])
    timing = rpc.measurements[-1]
    assert timing['outcome'] == 'error'
    assert timing['network_started_ns'] == timing['attempt_timings'][0]['network_started_ns']
    assert timing['network_finished_ns'] == timing['attempt_timings'][-1]['network_finished_ns']
    assert timing['network_started_ns'] < timing['attempt_timings'][1]['network_started_ns']
