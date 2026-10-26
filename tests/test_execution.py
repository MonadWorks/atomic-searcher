"""Full signed transactions on a temporary loopback-only Anvil chain."""
import copy
import json
import shutil
import socket
import subprocess
import time

import pytest
from eth_abi import decode, encode
from eth_account import Account
from eth_utils import to_checksum_address

from searcher.core import ROOT, USDC, WETH, Reject, data, load_config, number
from searcher.engine import evaluate
from searcher.execution import BroadcastRPC, Executor, runtime_matches, verify_execution_context, required_net_profit, CONTEXT_PROBE
from searcher.context import simulation_context
from searcher.math import sqrt_at_tick
from searcher.pools import Pool
from searcher.routes import RouteIndex


class LocalRPC(BroadcastRPC):
    METHODS = BroadcastRPC.METHODS | {"anvil_setCode", "anvil_setBalance", "anvil_setBlockTimestampInterval", "evm_mine", "evm_setAutomine", "evm_snapshot", "evm_revert"}
    def __init__(self, url):
        assert url.startswith("http://127.0.0.1:")
        # Temporary-chain setup can be descheduled on the shared desktop.
        # This transport allowance is separate from the unchanged production
        # context/observation/final-signing deadlines exercised by these tests.
        super().__init__(url, timeout=5)
        self.fail_send = False

    def request(self, method, params):
        if method == "eth_sendRawTransaction" and self.fail_send:
            self.fail_send = False
            raise TimeoutError("test lost connection before submission")
        result = super().request(method, params)
        if method == "eth_getTransactionReceipt" and result:
            result.update(l1Fee="0x0", operatorFee="0x0")
        return result

    def batch(self, calls):
        results = super().batch(calls)
        for (method, _), result in zip(calls, results):
            if method == "eth_getTransactionReceipt" and result:
                result.update(l1Fee="0x0", operatorFee="0x0")
        return results


def artifact(file, name):
    return json.loads((ROOT/f"out/{file}/{name}.json").read_text())


@pytest.fixture
def market(tmp_path, monkeypatch):
    anvil = shutil.which("anvil")
    if not anvil: pytest.skip("Anvil required for full transaction integration")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0)); port = sock.getsockname()[1]
    process = subprocess.Popen([anvil, "--host", "127.0.0.1", "--port", str(port), "--chain-id", "8453",
                                "--base-fee", "0", "--gas-price", "0", "--silent"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    rpc = LocalRPC(f"http://127.0.0.1:{port}")
    try:
        for _ in range(100):
            try: rpc.verify_chain(); break
            except Exception: time.sleep(.02)
        else: raise RuntimeError("local chain failed to start")
        rpc.request('anvil_setBlockTimestampInterval', [2])  # Match Base's block clock.
        signer = Account.create()
        rpc.request("anvil_setBalance", [signer.address, hex(10**23)])
        for token in (WETH, USDC):
            rpc.request("anvil_setCode", [token, artifact("AtomicCycle.t.sol", "TokenCycleMock")["deployedBytecode"]["object"]])
        # Anvil has no L1 publication fee or operator fee.
        rpc.request("anvil_setCode", ["0x420000000000000000000000000000000000000f", "0x5f5f5260205ff3"])
        import searcher.core as core
        from eth_utils import keccak
        aggregate = artifact('ContextAggregator.t.sol','ContextAggregatorMock')['deployedBytecode']['object']
        rpc.request('anvil_setCode', [core.MULTICALL, aggregate])
        monkeypatch.setattr(core, 'MULTICALL_HASH', '0x'+keccak(core.unhex(aggregate)).hex())

        def send(to=None, calldata="0x"):
            tx = {"chainId": 8453, "type": 2, "nonce": number(rpc.request("eth_getTransactionCount", [signer.address, "latest"])),
                  "value": 0, "data": calldata, "gas": 6_000_000, "maxFeePerGas": 1_000_000, "maxPriorityFeePerGas": 1_000_000}
            if to: tx["to"] = to_checksum_address(to)
            signed = signer.sign_transaction(tx)
            h = rpc.request("eth_sendRawTransaction", ["0x"+bytes(signed.raw_transaction).hex()])
            result = rpc.request("eth_getTransactionReceipt", [h])
            for _ in range(100):
                if result is not None: break
                time.sleep(.01)
                result = rpc.request("eth_getTransactionReceipt", [h])
            assert number(result["status"]) == 1
            return result

        def deploy(file, name, types=(), values=()):
            bytecode = artifact(file, name)["bytecode"]["object"]
            return send(calldata=bytecode+encode(types, values).hex())["contractAddress"].lower()

        factory = deploy("AtomicCycle.t.sol", "FactoryCycleMock")
        p1 = deploy("AtomicCycle.t.sol", "PoolCycleMock", ["address"]*3+["uint256"]*2, [factory, WETH, USDC, 2, 1])
        p2 = deploy("AtomicCycle.t.sol", "PoolCycleMock", ["address"]*3+["uint256"]*2, [factory, WETH, USDC, 1, 1])
        send(p2, data("setFee(uint24)", ["uint24"], [500]))
        for pool, fee in ((p1, 3000), (p2, 500)):
            send(factory, data("add(address,address,uint24,address)", ["address", "address", "uint24", "address"], [WETH, USDC, fee, pool]))
            for token in (WETH, USDC): send(token, data("mint(address,uint256)", ["address", "uint256"], [pool, 10**30]))
        contract = deploy("AtomicCycle.sol", "AtomicCycle", ["address", "uint256", "address[]", "uint8[]", "address[]", "uint256[]"],
                          [signer.address, 8453, [factory], [1], [WETH], [10**20]])
        # Pools are registered atomically during first execution.
        send(contract, data("setPaused(bool)", ["bool"], [False]))
        config = load_config(); config["owner"] = signer.address.lower()
        config['v4'] = {'enabled':False}  # This fixture deploys the CL/V2 protocol only.
        config['structural'] = {'enabled':False}
        config['strategy_suite'] = {'enabled':False}
        config['venues'] = {'enabled':False}  # This fixture deploys only the original executor.
        config['policy']['activity_priority'] = False  # Isolated synthetic chain.
        config['policy']['min_net_profit_usdc_raw'] = 0  # Local synthetic market has no deployed mainnet quoter.
        config["settlement_tokens"] = [config["settlement_tokens"][0]]
        config["settlement_tokens"][0]["max_input_raw"] = str(10**20)
        head = rpc.header()
        ps = [Pool(a, factory, "v3", WETH, USDC, 60, fee, sqrt_at_tick(tick), tick, 10**24,
                   {-2: 0, -1: 0, 0: 0, 1: 0, 2: 0}, {}, number(head["number"]), head["hash"])
              for a, fee, tick in ((p1, 3000, 6931), (p2, 500, 0))]
        index = RouteIndex(ps, [WETH], 3)
        # This fixture prepares a known synthetic opportunity, not a strategy
        # speed measurement. Leave the configuration yielded to tests intact.
        preparation_config = copy.deepcopy(config)
        preparation_config['policy']['max_quote_ms'] = 10_000
        report = evaluate(index, preparation_config, head)
        assert report["candidates"]
        candidate = report["candidates"][0]
        yield rpc, config, contract, signer, tmp_path, index, candidate, head, send
    finally:
        rpc.close()
        process.terminate()
        try: process.wait(timeout=3)
        except subprocess.TimeoutExpired: process.kill(); process.wait()


def test_discovered_route_signs_executes_and_accounts_real_profit(market):
    rpc, c, contract, signer, directory, index, candidate, head, _ = market
    executor = Executor(rpc, c, contract, signer, directory)
    try:
        result = executor.execute(candidate, index.routes[candidate["route_key"]], index.states, head, time.monotonic_ns()+2_000_000_000)
        assert result["status"] == "submitted"
        rpc.request("evm_mine", [])
        executor.reconcile()
        state, actual = executor.db.execute("SELECT state,actual FROM attempts").fetchone()
        outcome = json.loads(executor.db.execute("SELECT outcome FROM intents").fetchone()[0])
        assert state == "success" and int(actual) > 0
        assert outcome["net_profit_raw"] > 0
    finally: executor.close()


def test_actual_gas_estimate_repairs_underbudgeted_cycle_and_reserves_full_cost(market):
    rpc, c, contract, signer, directory, index, candidate, head, _ = market
    candidate = copy.deepcopy(candidate)
    candidate['cost_budget']['gas_limit'] = 100_000  # Too small even for first pool registration.
    e = Executor(rpc, c, contract, signer, directory)
    try:
        e.execute(candidate, index.routes[candidate['route_key']], index.states, head, time.monotonic_ns()+2_000_000_000)
        intent = json.loads(e.db.execute('SELECT payload FROM intents').fetchone()[0])
        estimated = intent['estimated_execution_gas']
        assert estimated > 100_000
        assert intent['transaction']['gas'] >= (estimated*115+99)//100
        assert intent['reserved_fee_wei'] >= intent['transaction']['gas']*intent['transaction']['maxFeePerGas']
        assert intent['minimum_profit'] >= intent['fee_in_settlement']*2
        rpc.request('evm_mine', []); e.reconcile()
        outcome = json.loads(e.db.execute('SELECT outcome FROM intents').fetchone()[0])
        assert outcome['net_profit_raw'] > 0
    finally: e.close()


def test_ignored_estimate_context_uses_exact_call_then_executes_profitably(market, monkeypatch):
    rpc, c, contract, signer, directory, index, candidate, head, _ = market
    original = rpc.request
    gas_checks = []
    def ignoring_estimate_context(method, params):
        if method == 'eth_estimateGas':
            assert params[0]['to'] == CONTEXT_PROBE  # No guarded trade is sent to this incompatible method.
            return original(method, params[:3])
        if method == 'eth_call' and params[0].get('to', '').lower() == contract and len(params) == 4:
            assert params[1:3] == ['pending', {}]
            assert params[3] == simulation_context(head)
            gas_checks.append(number(params[0]['gas']))
        return original(method, params)
    monkeypatch.setattr(rpc, 'request', ignoring_estimate_context)
    e = Executor(rpc, c, contract, signer, directory)
    candidate = copy.deepcopy(candidate)
    candidate['cost_budget']['gas_limit'] = 100_000
    try:
        assert e.gas_estimate_context_verified is False
        e.execute(candidate, index.routes[candidate['route_key']], index.states, head,
                  time.monotonic_ns()+2_000_000_000)
        intent = json.loads(e.db.execute('SELECT payload FROM intents').fetchone()[0])
        assert len(gas_checks) > 1 and gas_checks[0] == 100_000
        assert intent['gas_budget_source'] == 'successful_context_eth_call_ceiling'
        assert intent['transaction']['gas'] >= (gas_checks[-1]*115+99)//100
        assert intent['reserved_fee_wei'] >= intent['transaction']['gas']*intent['transaction']['maxFeePerGas']
        rpc.request('evm_mine', []); e.reconcile()
        outcome = json.loads(e.db.execute('SELECT outcome FROM intents').fetchone()[0])
        assert outcome['net_profit_raw'] > 0
    finally: e.close()


@pytest.mark.parametrize('fault', ['semantic_revert', 'transport', 'expired', 'bad_result'])
def test_context_call_gas_failure_never_signs_or_reserves(market, monkeypatch, fault):
    rpc, c, contract, signer, directory, index, candidate, head, _ = market
    e = Executor(rpc, c, contract, signer, directory)
    e.gas_estimate_context_verified = False
    original = rpc.request
    checks = []
    def failure(method, params):
        assert method != 'eth_sendRawTransaction'
        if method == 'eth_call' and params[0].get('to', '').lower() == contract and len(params) == 4:
            checks.append(number(params[0]['gas']))
            if fault == 'semantic_revert': raise Reject("RPC eth_call: {'message': 'execution reverted', 'data': '0x6c25f7f8'}")
            if fault == 'transport': raise Reject('RPC unavailable')
            if fault == 'expired':
                time.sleep(.04)
                return '0x'+encode(['uint256'], [candidate['gross_surplus']]).hex()
            return '0x'
        return original(method, params)
    monkeypatch.setattr(rpc, 'request', failure)
    monkeypatch.setattr(signer, 'sign_transaction', lambda _: pytest.fail('must not sign'))
    try:
        with pytest.raises(Reject):
            e.execute(candidate, index.routes[candidate['route_key']], index.states, head,
                      time.monotonic_ns()+(20_000_000 if fault == 'expired' else 2_000_000_000))
        assert e.db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0] == 0
        assert e.db.execute('SELECT COUNT(*) FROM intents').fetchone()[0] == 0
        if fault == 'semantic_revert': assert max(checks)*115 <= c['policy']['max_gas']*100
        else: assert len(checks) == 1
    finally: e.close()


@pytest.mark.parametrize('deduction_bps', [200, 800])
def test_real_token_receipt_shortfall_is_rejected_once_before_signing(market, monkeypatch, deduction_bps):
    from searcher.execution import execution_failure_details
    rpc, c, contract, signer, directory, index, candidate, _, send = market
    send(USDC, data('setBad(bool,uint256)', ['bool','uint256'], [False,deduction_bps]))
    head=rpc.header()
    e=Executor(rpc,c,contract,signer,directory)
    e.gas_estimate_context_verified=False
    request=rpc.request;checks=[]
    def observe(method,params):
        assert method!='eth_sendRawTransaction'
        if method=='eth_call' and params[0].get('to','').lower()==contract and len(params)==4:
            checks.append(number(params[0]['gas']))
        return request(method,params)
    monkeypatch.setattr(rpc,'request',observe)
    monkeypatch.setattr(signer,'sign_transaction',lambda _:pytest.fail('must not sign a receipt shortfall'))
    try:
        with pytest.raises(Reject) as caught:
            e.execute(candidate,index.routes[candidate['route_key']],index.states,head,time.monotonic_ns()+2_000_000_000)
        assert execution_failure_details(caught.value)['revert_name']=='BadCallback'
        assert len(checks)==1
        assert e.db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0]==0
        assert e.db.execute('SELECT COUNT(*) FROM intents').fetchone()[0]==0
    finally:e.close()


def test_executor_independently_rejects_excluded_token_from_stale_candidate(market,monkeypatch):
    rpc,c,contract,signer,directory,index,candidate,head,_=market
    e=Executor(rpc,c,contract,signer,directory)
    c['unsupported_transfer_tokens'][USDC]='Test: incompatible intermediate transfer'
    monkeypatch.setattr(rpc,'request',lambda *_:pytest.fail('excluded candidate must stop before network work'))
    monkeypatch.setattr(signer,'sign_transaction',lambda _:pytest.fail('must not sign'))
    try:
        with pytest.raises(Reject,match='unsupported token transfer'):
            e.execute(candidate,index.routes[candidate['route_key']],index.states,head,time.monotonic_ns()+2_000_000_000)
        assert e.db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0]==0
    finally:e.close()


@pytest.mark.parametrize('fault', ['over_cap', 'no_profit_after_reprice', 'expired', 'rpc_error'])
def test_gas_estimation_failures_never_sign_or_reserve(market, monkeypatch, fault):
    rpc, c, contract, signer, directory, index, candidate, head, _ = market
    candidate = copy.deepcopy(candidate)
    e = Executor(rpc, c, contract, signer, directory)
    original = rpc.request
    deadline = time.monotonic_ns()+2_000_000_000
    if fault == 'no_profit_after_reprice':
        # Initial model passes, but the corrected reservation must be covered.
        old = max(candidate['cost_budget']['gas_limit']*candidate['cost_budget']['max_fee_per_gas']+e.context['extra_fee'],
                  candidate['cost_budget']['total_fee_wei'])
        candidate['gross_surplus'] = old*2+1
    def request(method, params):
        assert method != 'eth_sendRawTransaction'
        if method == 'eth_estimateGas':
            assert int(params[0]['gas'], 16) == c['policy']['max_gas']
            assert params[1:3] == ['pending', {}]
            assert params[3]['blockHash'] == {str(number(head['number'])): head['hash']}
            if fault == 'over_cap': return hex(c['policy']['max_gas'])
            if fault == 'no_profit_after_reprice': return hex(1_200_000)
            if fault == 'expired':
                time.sleep(.04)
                return hex(500_000)
            raise Reject('estimate unavailable')
        return original(method, params)
    monkeypatch.setattr(rpc, 'request', request)
    monkeypatch.setattr(signer, 'sign_transaction', lambda _: pytest.fail('must not sign'))
    if fault == 'expired': deadline = time.monotonic_ns()+20_000_000
    try:
        with pytest.raises(Reject):
            e.execute(candidate, index.routes[candidate['route_key']], index.states, head, deadline)
        assert e.db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0] == 0
        assert e.db.execute('SELECT COUNT(*) FROM intents').fetchone()[0] == 0
    finally: e.close()


def test_pending_context_must_expose_real_anchor_hash(market, monkeypatch):
    rpc, _, _, _, _, _, _, _, _ = market
    assert verify_execution_context(rpc)['pending_anchor_verified']
    request = rpc.request
    def lost_anchor(method, params):
        value = request(method, params)
        if method == 'eth_call' and params[0]['to'] == CONTEXT_PROBE:
            fields = list(decode(['uint256', 'uint256', 'bytes32', 'uint256'], bytes.fromhex(value[2:])))
            fields[2] = bytes(32)
            return '0x'+encode(['uint256', 'uint256', 'bytes32', 'uint256'], fields).hex()
        return value
    monkeypatch.setattr(rpc, 'request', lost_anchor)
    with pytest.raises(Reject, match='区块校验不兼容'): verify_execution_context(rpc)


@pytest.mark.parametrize('fault', ['reorg', 'missing', 'wrong_height'])
def test_final_simulation_requires_matching_canonical_anchor_before_signing(market, monkeypatch, fault):
    rpc, c, contract, signer, directory, index, candidate, head, _ = market
    e = Executor(rpc, c, contract, signer, directory)
    request, batch = rpc.request, rpc.batch
    simulated = []
    def checked_request(method, params):
        assert method != 'eth_sendRawTransaction', 'reorganized plan must not be broadcast'
        return request(method, params)
    def checked_batch(calls):
        values = batch(calls)
        method, params = calls[0]
        if method == 'eth_call' and params[0].get('to', '').lower() == contract and len(params) == 4:
            # Only the real observed anchor is supplied to BLOCKHASH; contract
            # storage, balances and code are never overridden for execution.
            assert params[1:3] == ['pending', {}]
            assert params[3] == simulation_context(head, e.context['pending_head'])
            assert calls[1:3] == [('eth_getBlockByNumber', [head['number'], False]),
                                  ('eth_getBlockByNumber', ['pending', False])]
            simulated.append(values[0])
            if fault == 'reorg': values[1] = dict(values[1], hash='0x'+'ab'*32)
            elif fault == 'missing': values[1] = None
            else: values[1] = dict(values[1], number=hex(number(head['number'])+1))
        return values
    def forbidden_sign(_transaction):
        pytest.fail('reorganized plan must not be signed')
    monkeypatch.setattr(rpc, 'request', checked_request)
    monkeypatch.setattr(rpc, 'batch', checked_batch)
    monkeypatch.setattr(signer, 'sign_transaction', forbidden_sign)
    try:
        with pytest.raises(Reject, match='anchor not canonical before signing'):
            e.execute(candidate, index.routes[candidate['route_key']], index.states, head, time.monotonic_ns()+2_000_000_000)
        assert simulated
        assert e.db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0] == 0
        assert e.db.execute('SELECT COUNT(*) FROM intents').fetchone()[0] == 0
    finally: e.close()


def test_future_oracle_timestamp_blocks_a_simulated_profitable_trade(market, monkeypatch):
    rpc, c, contract, signer, directory, index, candidate, head, send = market
    route = index.routes[candidate['route_key']]
    send(route.pools[0], data('setObservedAt(uint32)', ['uint32'], [number(head['timestamp'])+60]))
    head = rpc.header()
    e = Executor(rpc, c, contract, signer, directory)
    monkeypatch.setattr(signer, 'sign_transaction', lambda _: pytest.fail('future-state quote must not sign'))
    try:
        with pytest.raises(Reject, match='oracle state is newer'):
            e.execute(candidate, route, index.states, head, time.monotonic_ns()+2_000_000_000)
        assert e.db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0] == 0
    finally: e.close()


def test_pending_clock_advance_after_successful_simulation_prevents_signing(market, monkeypatch):
    rpc, c, contract, signer, directory, index, candidate, head, _ = market
    e = Executor(rpc, c, contract, signer, directory)
    batch = rpc.batch
    def changed_clock(calls):
        values = batch(calls)
        if calls[0][0] == 'eth_call' and calls[0][1][0].get('to', '').lower() == contract:
            values[2] = {**values[2], 'number':hex(number(values[2]['number'])+1),
                         'timestamp':hex(number(values[2]['timestamp'])+2)}
        return values
    monkeypatch.setattr(rpc, 'batch', changed_clock)
    monkeypatch.setattr(signer, 'sign_transaction', lambda _: pytest.fail('changed clock must not sign'))
    try:
        with pytest.raises(Reject, match='pending execution clock'):
            e.execute(candidate, index.routes[candidate['route_key']], index.states, head, time.monotonic_ns()+2_000_000_000)
        assert e.db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0] == 0
    finally: e.close()


def test_execution_endpoint_override_and_readonly_feed_are_separate(monkeypatch):
    from searcher.core import RPC
    from searcher.execution import DEFAULT_EXECUTION_RPC
    monkeypatch.delenv('BASE_RPC_URL', raising=False)
    monkeypatch.delenv('BASE_EXECUTION_RPC_URL', raising=False)
    for expected in (DEFAULT_EXECUTION_RPC, 'http://127.0.0.1:1111'):
        if expected != DEFAULT_EXECUTION_RPC: monkeypatch.setenv('BASE_EXECUTION_RPC_URL', expected)
        rpc, feed = BroadcastRPC(), RPC()
        try:
            assert rpc.url == expected and feed.url == 'https://mainnet.base.org'
            assert 'eth_sendRawTransaction' not in feed.METHODS
        finally: rpc.close(); feed.close()


@pytest.mark.parametrize('usdc_floor', [10_000, 100_000])
@pytest.mark.parametrize('aggregate', [False, True])
def test_weth_profit_floor_uses_exact_output_usdc_quote_and_blocks_small_profit(market, monkeypatch, usdc_floor, aggregate):
    rpc, c, contract, signer, directory, index, candidate, head, send = market
    c['policy']['aggregate_execution_context'] = aggregate
    c['policy']['min_net_profit_usdc_raw'] = usdc_floor
    quoted_floor = candidate['gross_surplus']+1
    quoter = next(f['quoter'] for f in c['factories'] if f['kind'] == 'v3')
    rpc.request('anvil_setCode', [quoter, artifact('ContextAggregator.t.sol','ContextQuoteMock')['deployedBytecode']['object']])
    # EVM mock checks the token order, exact output amount, fee, and limit under
    # both direct and Multicall execution instead of intercepting HTTP calls.
    send(quoter, data('configure(address,address,uint256,uint256)', ['address','address','uint256','uint256'],
                     [WETH,USDC,usdc_floor,quoted_floor]))
    head = rpc.header()  # Configuration above mined a local setup transaction.
    e = Executor(rpc, c, contract, signer, directory)
    try:
        assert required_net_profit(c, c['settlement_tokens'][0], e.context) == quoted_floor
        before = rpc.request('eth_getTransactionCount', [signer.address, 'latest'])
        with pytest.raises(Reject, match='金额门槛'):
            e.execute(candidate, index.routes[candidate['route_key']], index.states, head, time.monotonic_ns()+2_000_000_000)
        assert rpc.request('eth_getTransactionCount', [signer.address, 'latest']) == before
        assert e.db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0] == 0
        with pytest.raises(Reject, match='报价不可用'):
            required_net_profit(c, c['settlement_tokens'][0], {})
        # The USDC-settled route uses the same global floor even if its token
        # entry came from a stale file with a smaller minimum.
        assert required_net_profit(c, {'address':USDC,'min_net_raw':'1000'}, e.context) == usdc_floor
    finally: e.close()


def test_net_floor_prices_failure_margin_in_the_same_settlement_units():
    c = load_config()
    assert c['policy']['min_net_profit_usdc_raw'] == '100000'
    assert c['policy']['min_net_profit_fee_bps'] == 0
    usdc = next(t for t in c['settlement_tokens'] if t['address'] == USDC)
    assert required_net_profit(c, usdc, {}, 5_000) == 100_000
    assert required_net_profit(c, usdc, {}, 300_000) == 100_000
    c['policy']['min_net_profit_fee_bps'] = 10000
    assert required_net_profit(c, usdc, {}, 300_000) == 300_000
    c['policy']['min_net_profit_fee_bps'] = 10_001
    assert required_net_profit(c, usdc, {}, 300_000) == 300_030
    c['policy']['min_net_profit_fee_bps'] = -1
    with pytest.raises(Reject, match='profit-to-fee'):
        required_net_profit(c, usdc, {}, 300_000)


@pytest.mark.parametrize('surplus_above_required', [-1, 0])
def test_small_trade_must_cover_its_fees_and_another_fee_budget(market, surplus_above_required):
    rpc, c, contract, signer, directory, index, candidate, head, _ = market
    c['policy']['min_net_profit_fee_bps'] = 10000
    candidate = copy.deepcopy(candidate)
    # Make the actual sending envelope higher than the original model, so the
    # gate must use the refreshed full reservation rather than the old quote.
    candidate['cost_budget'].update(gas_limit=1_000_000, max_fee_per_gas=10_000_000)
    e = Executor(rpc, c, contract, signer, directory)
    try:
        maximum = max(10_000_000_000_000 + e.context['extra_fee'], candidate['cost_budget']['total_fee_wei'])
        assert maximum > int(c['settlement_tokens'][0]['min_net_raw'])
        candidate['gross_surplus'] = 2*maximum + surplus_above_required
        before = rpc.request('eth_getTransactionCount', [signer.address, 'latest'])
        if surplus_above_required < 0:
            with pytest.raises(Reject, match='金额门槛'):
                e.execute(candidate, index.routes[candidate['route_key']], index.states, head, time.monotonic_ns()+2_000_000_000)
            assert e.db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0] == 0
            assert rpc.request('eth_getTransactionCount', [signer.address, 'latest']) == before
        else:
            e.execute(candidate, index.routes[candidate['route_key']], index.states, head, time.monotonic_ns()+2_000_000_000)
            intent = json.loads(e.db.execute('SELECT payload FROM intents').fetchone()[0])
            assert intent['reserved_fee_wei'] == maximum
            assert intent['minimum_profit'] == 2*maximum
            rpc.request('evm_mine', [])
            e.reconcile()
            outcome = json.loads(e.db.execute('SELECT outcome FROM intents').fetchone()[0])
            assert outcome['net_profit_raw'] >= maximum
    finally: e.close()


@pytest.mark.parametrize('reject_top', [False, True])
def test_continuous_pipeline_automatically_submits_and_reconciles(market, monkeypatch, reject_top):
    """Real loop + signer + EVM + ledger; only the synthetic market feed is supplied."""
    import threading
    import searcher.pipeline as pipeline
    from searcher.statefeed import StateFrame
    rpc, c, contract, signer, directory, index, _, head, _ = market
    c["policy"]["universe_refresh_seconds"] = 100
    saved = {"header":head, "pools":[p.json() for p in index.states.values()]}
    class LocalFeed:
        def __init__(self, feed_rpc, config, saved):
            assert feed_rpc.url.startswith("http://127.0.0.1:")
            self.rpc, self.pools = feed_rpc, [Pool.from_json(p) for p in saved["pools"]]
        def poll(self, **kwargs):
            now = time.monotonic_ns(); h = self.rpc.header()
            return StateFrame(h, h, [p.json() for p in self.pools], now, time.monotonic_ns(), time.time(),
                              number(h["number"]), number(h["timestamp"]))
    monkeypatch.setattr(pipeline, "StateFeed", LocalFeed)
    if reject_top:
        real_evaluate = pipeline.evaluate
        def with_invalid_top(*args, **kwargs):
            result = real_evaluate(*args, **kwargs)
            if result['candidates']:
                rejected = copy.deepcopy(result['candidates'][0])
                rejected['amount_in'] = 0
                result['candidates'].insert(0, rejected)
            return result
        monkeypatch.setattr(pipeline, 'evaluate', with_invalid_top)
    stop = threading.Event()
    executor = Executor(rpc, c, contract, signer, directory)
    submit, reconcile = executor.execute, executor.reconcile
    def mine_after_submission(*args, **kwargs):
        result = submit(*args, **kwargs)
        rpc.request("evm_mine", [])
        return result
    def stop_when_accounted():
        reconcile()
        if executor.db.execute("SELECT 1 FROM attempts WHERE state='success'").fetchone(): stop.set()
    monkeypatch.setattr(executor, "execute", mine_after_submission)
    monkeypatch.setattr(executor, "reconcile", stop_when_accounted)
    try:
        report = pipeline.run_pending(rpc, c, saved, 5, directory/"pipeline", executor, stop)
        row = executor.db.execute("SELECT state,outcome FROM attempts JOIN intents ON intents.attempt=attempts.id").fetchone()
        assert row[0] == "success" and json.loads(row[1])["net_profit_raw"] > 0
        assert executor.db.execute('SELECT COUNT(*) FROM attempts').fetchone()[0] == 1
        if reject_top:
            decisions=[json.loads(p.read_text()) for p in (directory/'pipeline').glob('decision-*.json')]
            assert any(d['candidates'] and d['candidates'][0].get('error')=='input cap exceeded'
                       and any(c['status']=='submitted' for c in d['candidates'][1:]) for d in decisions)
        assert report["reader_stopped"] and report["discovery_stopped"] and not report["worker_errors"]
    finally: executor.close()


@pytest.mark.parametrize('fault', ['preconfirmation', 'head_lag', 'missing_block'])
def test_receipt_visibility_lag_waits_without_recovery_or_accounting(market, monkeypatch, fault):
    rpc, c, contract, signer, directory, index, candidate, head, _ = market
    e = Executor(rpc, c, contract, signer, directory)
    try:
        result = e.execute(candidate, index.routes[candidate['route_key']], index.states, head,
                           time.monotonic_ns()+2_000_000_000)
        receipt = rpc.request('eth_getTransactionReceipt', [result['tx_hash']])
        rpc.request('evm_mine', [])
        batch, request = rpc.batch, rpc.request
        active = {'fault': True}
        def delayed_batch(calls):
            values = batch(calls)
            if active['fault'] and fault == 'preconfirmation':
                for i, (method, _) in enumerate(calls):
                    if method == 'eth_getTransactionReceipt' and values[i]:
                        values[i] = {**values[i], 'blockNumber': None, 'blockHash': None}
            return values
        def delayed_request(method, params):
            assert method != 'eth_sendRawTransaction', 'A visible pending receipt must not trigger a send'
            value = request(method, params)
            if active['fault'] and method == 'eth_getBlockByNumber':
                if fault == 'missing_block' and params[0] == receipt['blockNumber']: return None
                if fault == 'head_lag' and params[0] == 'latest':
                    return {**value, 'number': hex(number(receipt['blockNumber'])-1)}
            return value
        def unexpected_recovery(*args): pytest.fail('Visibility lag is not a dropped transaction')
        monkeypatch.setattr(rpc, 'batch', delayed_batch)
        monkeypatch.setattr(rpc, 'request', delayed_request)
        monkeypatch.setattr(e, 'recover_submission', unexpected_recovery)
        e.reconcile()
        assert e.db.execute('SELECT state,actual,outcome FROM attempts JOIN intents ON attempts.id=intents.attempt').fetchone() == ('submitted', None, None)
        active['fault'] = False
        e.reconcile()
        assert e.db.execute('SELECT state FROM attempts').fetchone()[0] == 'success'
    finally: e.close()


@pytest.mark.parametrize('visibility', ['head_lag', 'missing_block', 'canonical'])
def test_known_inclusion_without_receipt_does_not_trigger_cancellation(market, monkeypatch, visibility):
    import itertools
    from types import SimpleNamespace
    import searcher.execution as execution_module
    rpc, c, contract, signer, directory, index, candidate, head, _ = market
    e = Executor(rpc, c, contract, signer, directory)
    try:
        # This case tests receipt visibility after submission. Use a bounded
        # clock only while creating its sent transaction; production freshness
        # policy and the real RPC/asyncio clocks remain unchanged.
        real_monotonic_ns = time.monotonic_ns
        seed_started = real_monotonic_ns()
        seed_clock = SimpleNamespace(time=time.time,
            monotonic_ns=itertools.count(seed_started, 1_000_000).__next__)
        assert c['policy']['final_validation_max_age_ms'] == 500
        with monkeypatch.context() as seed:
            seed.setattr(execution_module, 'time', seed_clock)
            assert time.monotonic_ns is real_monotonic_ns
            result = e.execute(candidate, index.routes[candidate['route_key']], index.states, head,
                               seed_started+2_000_000_000)
        assert time.monotonic_ns is real_monotonic_ns
        receipt = rpc.request('eth_getTransactionReceipt', [result['tx_hash']])
        for _ in range(4): rpc.request('evm_mine', [])
        batch, request = rpc.batch, rpc.request
        def delayed_batch(calls):
            if all(m == 'eth_getTransactionReceipt' for m, _ in calls): return [None]*len(calls)
            return batch(calls)
        def delayed_request(method, params):
            assert method != 'eth_sendRawTransaction', 'Known inclusion must not trigger cancellation'
            value = request(method, params)
            if method == 'eth_getTransactionCount': return hex(number(value)-1)  # lagging nonce view
            if method == 'eth_getBlockByNumber':
                if visibility == 'missing_block' and params[0] == receipt['blockNumber']: return None
                if visibility == 'head_lag' and params[0] == 'latest':
                    return {**value, 'number': hex(number(receipt['blockNumber'])-1)}
            return value
        monkeypatch.setattr(rpc, 'batch', delayed_batch)
        monkeypatch.setattr(rpc, 'request', delayed_request)
        e.reconcile()
        payload, outcome = e.db.execute('SELECT payload,outcome FROM intents').fetchone()
        assert 'cancellation' not in json.loads(payload) and outcome is None
    finally: e.close()


@pytest.mark.parametrize("phase", ["validation", "reservation"])
def test_stop_during_execution_preparation_prevents_broadcast(market, monkeypatch, phase):
    import threading
    rpc, c, contract, signer, directory, index, candidate, head, _ = market
    e = Executor(rpc, c, contract, signer, directory)
    stop = threading.Event(); e.bind_stop_event(stop)
    calls = []; original = rpc.request; original_batch = rpc.batch
    def request(method, params):
        calls.append(method)
        result = original(method, params)
        return result
    def batch(items):
        calls.extend(method for method, _ in items)
        result = original_batch(items)
        if phase == 'validation' and any(method == 'eth_call' and len(params) == 4 for method, params in items): stop.set()
        return result
    monkeypatch.setattr(rpc, "request", request)
    monkeypatch.setattr(rpc, 'batch', batch)
    if phase == "reservation":
        reserve = e.ledger.reserve
        def stopped_reserve(*args): reserve(*args); stop.set()
        monkeypatch.setattr(e.ledger, "reserve", stopped_reserve)
    try:
        with pytest.raises(RuntimeError):
            e.execute(candidate, index.routes[candidate["route_key"]], index.states, head, time.monotonic_ns()+2_000_000_000)
        assert "eth_sendRawTransaction" not in calls
        assert not e.db.execute("SELECT 1 FROM attempts WHERE state IN ('reserved','submitted')").fetchone()
    finally: e.close()


def test_timeout_recovers_same_signed_transaction_after_restart(market):
    rpc, c, contract, signer, directory, index, candidate, head, _ = market
    e = Executor(rpc, c, contract, signer, directory)
    rpc.fail_send = True
    with pytest.raises(TimeoutError):
        e.execute(candidate, index.routes[candidate["route_key"]], index.states, head, time.monotonic_ns()+2_000_000_000)
    old = e.db.execute("SELECT tx_hash FROM attempts").fetchone()[0]; e.close()
    e = Executor(rpc, c, contract, signer, directory)
    try:
        e.reconcile(); rpc.request("evm_mine", []); e.reconcile()
        assert e.db.execute("SELECT state,tx_hash FROM attempts").fetchone() == ("success", old)
    finally: e.close()


def test_expired_unsubmitted_transaction_cancels_without_releasing_budget_early(market):
    rpc, c, contract, signer, directory, index, candidate, head, _ = market
    e = Executor(rpc, c, contract, signer, directory)
    rpc.fail_send = True
    try:
        with pytest.raises(TimeoutError):
            e.execute(candidate, index.routes[candidate["route_key"]], index.states, head, time.monotonic_ns()+2_000_000_000)
        # At the last permitted execution height the next inclusion is already
        # too late; recovery must cancel instead of sending a guaranteed revert.
        for _ in range(2): rpc.request("evm_mine", [])
        e.reconcile()
        assert e.db.execute("SELECT state FROM attempts").fetchone()[0] == "submitted"
        rpc.request("evm_mine", []); e.reconcile()
        outcome = json.loads(e.db.execute("SELECT outcome FROM intents").fetchone()[0])
        assert outcome["cancelled"] and outcome["net_profit_raw"] < 0
    finally: e.close()


def test_immutable_check_cannot_be_bypassed_by_leaving_getter_unchanged():
    a = artifact("AtomicCycle.sol", "AtomicCycle")
    raw = bytearray.fromhex(a["deployedBytecode"]["object"][2:])
    refs = next(r for r in a["deployedBytecode"]["immutableReferences"].values() if len(r) > 1)
    ref = refs[-1]; raw[ref["start"]+ref["length"]-1] = 1
    assert not runtime_matches("0x"+raw.hex(), a)


def test_underpriced_cancellation_reprices_and_keeps_old_hash(market, monkeypatch):
    rpc, c, contract, signer, directory, index, candidate, head, _ = market
    e = Executor(rpc, c, contract, signer, directory)
    rpc.fail_send = True
    try:
        with pytest.raises(TimeoutError):
            e.execute(candidate, index.routes[candidate["route_key"]], index.states, head, time.monotonic_ns()+2_000_000_000)
        for _ in range(3): rpc.request("evm_mine", [])
        original = rpc.request
        def underpriced(method, params):
            if method == "eth_sendRawTransaction": raise Reject("replacement transaction underpriced")
            return original(method, params)
        monkeypatch.setattr(rpc, "request", underpriced)
        with pytest.raises(Reject, match="underpriced"): e.reconcile()
        before = json.loads(e.db.execute("SELECT payload FROM intents").fetchone()[0])
        monkeypatch.setattr(rpc, "request", original)
        e.reconcile()
        after = json.loads(e.db.execute("SELECT payload FROM intents").fetchone()[0])
        assert after["cancellation"]["hash"] != before["cancellation"]["hash"]
        assert after["cancellation_history"][0]["hash"] == before["cancellation"]["hash"]
        rpc.request("evm_mine", []); e.reconcile()
        outcome = json.loads(e.db.execute("SELECT outcome FROM intents").fetchone()[0])
        assert outcome["cancelled"]
    finally: e.close()


def test_persistent_candidate_is_rechecked_on_unchanged_observation(market):
    _, c, _, _, _, index, _, head, _ = market
    report = evaluate(index, c, head, set())
    assert report["candidates"]


def test_orphaned_original_receipt_cannot_hide_canonical_cancellation(market, monkeypatch):
    rpc, c, contract, signer, directory, index, candidate, head, _ = market
    e = Executor(rpc, c, contract, signer, directory)
    rpc.fail_send = True
    try:
        with pytest.raises(TimeoutError):
            e.execute(candidate, index.routes[candidate["route_key"]], index.states, head, time.monotonic_ns()+2_000_000_000)
        for _ in range(3): rpc.request("evm_mine", [])
        e.reconcile(); rpc.request("evm_mine", [])
        original = rpc.batch
        def stale_original(calls):
            results = original(calls)
            if calls[0][0] == "eth_getTransactionReceipt" and len(results) == 2 and results[1]:
                results[0] = {**results[1], "transactionHash": calls[0][1][0], "blockHash": "0x"+"ab"*32}
            return results
        monkeypatch.setattr(rpc, "batch", stale_original)
        e.reconcile()
        outcome = json.loads(e.db.execute("SELECT outcome FROM intents").fetchone()[0])
        assert outcome["cancelled"] and e.db.execute("SELECT state FROM attempts").fetchone()[0] == "failed"
    finally: e.close()


@pytest.mark.parametrize("cancelled", [False, True])
def test_provisional_reorg_restores_reservation_across_restart_before_new_send(market, monkeypatch, cancelled):
    rpc, c, contract, signer, directory, index, candidate, head, _ = market
    snapshot = rpc.request("evm_snapshot", [])
    request = rpc.request
    def with_finality_lag(method, params):
        if method == "eth_getBlockByNumber" and params[0] == "finalized": return head
        return request(method, params)
    monkeypatch.setattr(rpc, "request", with_finality_lag)
    e = Executor(rpc, c, contract, signer, directory)
    try:
        rpc.fail_send = cancelled
        if cancelled:
            with pytest.raises(TimeoutError):
                e.execute(candidate, index.routes[candidate["route_key"]], index.states, head, time.monotonic_ns()+2_000_000_000)
            for _ in range(3): rpc.request("evm_mine", [])
            e.reconcile()
        else:
            e.execute(candidate, index.routes[candidate["route_key"]], index.states, head, time.monotonic_ns()+2_000_000_000)
        rpc.request("evm_mine", []); e.reconcile()
        old_state, old_hash, reserved = e.db.execute("SELECT state,tx_hash,reserved FROM attempts").fetchone()
        assert old_state == ("failed" if cancelled else "success")
        e.verify_settlements()
        assert e.db.execute("SELECT finalized FROM intents").fetchone()[0] == 0
        e.close(); e = None

        rpc.request("evm_revert", [snapshot])
        # Make a different canonical branch, then keep recovered transactions
        # pending so the reservation can be inspected before another receipt.
        rpc.request("evm_mine", [])
        rpc.request("evm_setAutomine", [False])
        e = Executor(rpc, c, contract, signer, directory)
        with pytest.raises(Reject, match="previous transaction unresolved"):
            e.execute(candidate, index.routes[candidate["route_key"]], index.states, rpc.header(), time.monotonic_ns()+2_000_000_000)
        assert e.db.execute("SELECT state,actual,reserved,tx_hash,finished_at FROM attempts").fetchone() == ("submitted", None, reserved, old_hash, None)
        assert e.db.execute("SELECT outcome FROM intents").fetchone()[0] is None
        with pytest.raises(Reject, match="unresolved prior attempt"):
            e.ledger.reserve("must-not-send", 1)
        e.reconcile()
        intent = json.loads(e.db.execute("SELECT payload FROM intents").fetchone()[0])
        recovered_hash = intent["cancellation"]["hash"] if cancelled else old_hash
        recovered = rpc.request("eth_getTransactionByHash", [recovered_hash])
        assert number(recovered["nonce"]) == intent["plan_nonce"]
        e.close(); e = None

        e = Executor(rpc, c, contract, signer, directory)
        rpc.request("evm_mine", []); rpc.request("evm_mine", [])
        e.reconcile()
        assert e.db.execute("SELECT state FROM attempts").fetchone()[0] == old_state
        outcome = json.loads(e.db.execute("SELECT outcome FROM intents").fetchone()[0])
        assert outcome["cancelled"] == cancelled and not outcome["finalized"]
    finally:
        if e: e.close()


def test_missing_finalized_tag_keeps_watch_and_new_heads_use_batched_checks(market, monkeypatch):
    rpc, c, contract, signer, directory, index, candidate, head, _ = market
    e = Executor(rpc, c, contract, signer, directory)
    request, batch = rpc.request, rpc.batch
    finality_calls, header_batches = [], []
    finality_available = False
    def finality(method, params):
        if method == "eth_getBlockByNumber" and params[0] == "finalized":
            finality_calls.append(params)
            if not finality_available: raise Reject("finalized tag unavailable")
            return request("eth_getBlockByNumber", ["latest", False])
        return request(method, params)
    def tracked_batch(calls):
        if all(method == "eth_getBlockByNumber" for method, _ in calls): header_batches.append(calls)
        return batch(calls)
    monkeypatch.setattr(rpc, "request", finality)
    monkeypatch.setattr(rpc, "batch", tracked_batch)
    try:
        e.execute(candidate, index.routes[candidate["route_key"]], index.states, head, time.monotonic_ns()+2_000_000_000)
        rpc.request("evm_mine", []); e.reconcile()
        e.verify_settlements(); e.verify_settlements()
        assert len(finality_calls) == len(header_batches) == 1
        assert e.db.execute("SELECT finalized FROM intents").fetchone()[0] == 0
        e.close(); e = None
        e = Executor(rpc, c, contract, signer, directory)
        e.verify_settlements()
        assert len(header_batches) == 2  # restart does not reuse an old head check
        assert e.db.execute("SELECT finalized FROM intents").fetchone()[0] == 0
        finality_available = True
        rpc.request("evm_mine", []); e.verify_settlements()
        assert e.db.execute("SELECT finalized FROM intents").fetchone()[0] == 1
        assert json.loads(e.db.execute("SELECT outcome FROM intents").fetchone()[0])["finalized"]
        calls = len(header_batches)
        rpc.request("evm_mine", []); e.verify_settlements()
        assert len(header_batches) == calls  # finalized history is retired
    finally:
        if e: e.close()


def test_old_settled_ledger_migrates_and_reorg_rollback_is_atomic(market, monkeypatch):
    rpc, c, contract, signer, directory, index, candidate, head, _ = market
    snapshot = rpc.request("evm_snapshot", [])
    e = Executor(rpc, c, contract, signer, directory)
    try:
        e.execute(candidate, index.routes[candidate["route_key"]], index.states, head, time.monotonic_ns()+2_000_000_000)
        rpc.request("evm_mine", []); e.reconcile()
        with e.db:
            e.db.execute("DROP INDEX pending_finality")
            e.db.execute("ALTER TABLE intents DROP COLUMN finalized")
        e.close(); e = None
        rpc.request("evm_revert", [snapshot])
        e = Executor(rpc, c, contract, signer, directory)
        assert e.db.execute("SELECT finalized FROM intents").fetchone()[0] == 0
        reopen = e.ledger.reopen
        def fail_after_reopen(*args, **kwargs):
            reopen(*args, **kwargs)
            raise OSError("induced failure before clearing outcome")
        monkeypatch.setattr(e.ledger, "reopen", fail_after_reopen)
        with pytest.raises(OSError, match="induced failure"):
            e.verify_settlements()
        assert e.db.execute("SELECT state,actual IS NOT NULL FROM attempts").fetchone() == ("success", 1)
        assert e.db.execute("SELECT outcome IS NOT NULL FROM intents").fetchone()[0] == 1
        monkeypatch.setattr(e.ledger, "reopen", reopen)
        e.verify_settlements()
        assert e.db.execute("SELECT state,actual,outcome FROM attempts JOIN intents ON attempts.id=intents.attempt").fetchone() == ("submitted", None, None)
    finally:
        if e: e.close()


def test_outcome_and_fee_settlement_commit_together(market, monkeypatch):
    rpc, c, contract, signer, directory, index, candidate, head, _ = market
    e = Executor(rpc, c, contract, signer, directory)
    try:
        e.execute(candidate, index.routes[candidate["route_key"]], index.states, head, time.monotonic_ns()+2_000_000_000)
        rpc.request("evm_mine", [])
        settle = e.ledger.settle
        def fail_after_settle(*args, **kwargs):
            settle(*args, **kwargs)
            raise OSError("induced settlement write failure")
        monkeypatch.setattr(e.ledger, "settle", fail_after_settle)
        with pytest.raises(OSError, match="induced settlement"):
            e.reconcile()
        assert e.db.execute("SELECT state,actual,outcome FROM attempts JOIN intents ON attempts.id=intents.attempt").fetchone() == ("submitted", None, None)
        monkeypatch.setattr(e.ledger, "settle", settle)
        e.reconcile()
        assert e.db.execute("SELECT state FROM attempts").fetchone()[0] == "success"
    finally: e.close()


def test_cached_orphan_receipt_and_transaction_do_not_prevent_same_nonce_recovery(market, monkeypatch):
    import itertools
    from types import SimpleNamespace
    import searcher.execution as execution_module
    rpc, c, contract, signer, directory, index, candidate, head, _ = market
    snapshot = rpc.request("evm_snapshot", [])
    e = Executor(rpc, c, contract, signer, directory)
    try:
        # Establish the already-sent transaction required by this recovery
        # case. Its setup is not a localhost final-validation speed benchmark.
        # Replace only execution's time binding, never the shared time module:
        # RPC deadlines and asyncio keep using the real monotonic clock.
        original_time, real_monotonic_ns = execution_module.time, time.monotonic_ns
        seed_started = real_monotonic_ns()
        seed_clock = SimpleNamespace(time=time.time,
            monotonic_ns=itertools.count(seed_started, 1_000_000).__next__)
        assert c['policy']['final_validation_max_age_ms'] == 500
        with monkeypatch.context() as seed:
            seed.setattr(execution_module, 'time', seed_clock)
            assert time.monotonic_ns is real_monotonic_ns
            result = e.execute(candidate, index.routes[candidate["route_key"]], index.states, head, seed_started+2_000_000_000)
        assert execution_module.time is original_time and time.monotonic_ns is real_monotonic_ns
        rpc.request("evm_mine", []); e.reconcile()
        old_receipt = rpc.request("eth_getTransactionReceipt", [result["tx_hash"]])
        old_transaction = rpc.request("eth_getTransactionByHash", [result["tx_hash"]])
        rpc.request("evm_revert", [snapshot]); rpc.request("evm_mine", [])
        rpc.request("evm_setAutomine", [False])
        batch, request = rpc.batch, rpc.request
        def cached_batch(calls):
            values = batch(calls)
            for i, (method, params) in enumerate(calls):
                if method == "eth_getTransactionReceipt" and params[0] == result["tx_hash"]:
                    values[i] = old_receipt
            return values
        def cached_request(method, params):
            if method == "eth_getTransactionByHash" and params[0] == result["tx_hash"]:
                return old_transaction
            return request(method, params)
        monkeypatch.setattr(rpc, "batch", cached_batch)
        monkeypatch.setattr(rpc, "request", cached_request)
        e.reconcile()
        assert e.db.execute("SELECT state,actual,outcome FROM attempts JOIN intents ON attempts.id=intents.attempt").fetchone() == ("submitted", None, None)
        pending = request("eth_getTransactionByHash", [result["tx_hash"]])
        assert pending and pending["blockNumber"] is None
        assert number(pending["nonce"]) == number(old_transaction["nonce"])
    finally: e.close()


@pytest.mark.parametrize('observed_priority', [20_000_000, 200_000_000])
def test_observed_bid_is_bound_to_signed_fee_and_full_profit_floor(market, observed_priority):
    import json
    import time
    from searcher.execution import Executor,required_net_profit
    rpc,config,contract,signer,directory,index,candidate,head,_=market
    config['policy']['profit_aware_bidding']=True
    # This checks signed fee binding, not host fsync latency under a full test run.
    # Dedicated expiry tests retain the production 500 ms clock.
    config['policy']['final_validation_max_age_ms']=2000
    executor=Executor(rpc,config,contract,signer,directory/'bid-audit')
    try:
        executor.context['observed_priority_fee']=observed_priority
        result=executor.execute(candidate,index.routes[candidate['route_key']],index.states,head,time.monotonic_ns()+5_000_000_000)
        intent=json.loads(executor.db.execute('SELECT payload FROM intents').fetchone()[0])
        tx=intent['transaction']
        assert tx['maxPriorityFeePerGas']>candidate['cost_budget']['priority_fee']
        assert tx['maxPriorityFeePerGas']==intent['fee_bid']['selected_priority_fee_wei']
        assert tx['maxFeePerGas']>=observed_priority
        assert tx['gas']*tx['maxFeePerGas']+executor.context['extra_fee']<=intent['reserved_fee_wei']
        assert intent['reserved_fee_wei']<=int(config['policy']['max_total_fee_wei'])
        token=config['settlement_tokens'][0]
        assert intent['minimum_profit']>=intent['fee_in_settlement']+required_net_profit(config,token,executor.context,intent['fee_in_settlement'])
        rpc.request('evm_mine',[])
        executor.reconcile()
        state,outcome=executor.db.execute('SELECT state,outcome FROM attempts JOIN intents ON attempts.id=intents.attempt').fetchone()
        assert state=='success' and json.loads(outcome)['net_profit_raw']>0
    finally:
        executor.close()
