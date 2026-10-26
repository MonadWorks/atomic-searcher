"""Signed execution, durable fee reservations and canonical receipt accounting.

Only the explicit trade entry point constructs this class. Research commands
retain the read-only RPC allowlist. Tests submit exclusively to a local chain.
"""
import fcntl
import json
import os
import time
import threading
import re
from pathlib import Path

from eth_abi import decode
from eth_account import Account
from eth_account.typed_transactions import TypedTransaction
from hexbytes import HexBytes
from eth_utils import keccak, to_checksum_address

from .core import ROOT, RPC, RPCError, USDC, WETH, Reject, address, check, data, number, unhex, safe_error
from .context import simulation_context
from .engine import PLAN_TYPE, make_hops
from .events import topic
from .ledger import RiskLedger
from .statefeed import LENS
from .compatibility import blocked_tokens
from .costs import quote_profit_floor

ORACLE = "0x420000000000000000000000000000000000000f"
CYCLE_EVENT = topic("CycleExecuted(uint64,address,uint256,uint256)")
# Setup/recovery also needs old transaction receipts. PublicNode's anonymous
# endpoint currently requires a personal token for those archive requests.
DEFAULT_EXECUTION_RPC = "https://mainnet.base.org"
CONTEXT_PROBE = "0x000000000000000000000000000000000000c1c4"
# Return NUMBER, TIMESTAMP, BLOCKHASH(calldata[0:32]), CHAINID. Read-only code
# override at an otherwise unused address; the deployed executor is unchanged.
CONTEXT_PROBE_CODE = "0x4360005242602052600035406040524660605260806000f3"
TERMINAL_REVERTS = {data(name+'()')[:10]: name for name in
    ('Unauthorized', 'InvalidPlan', 'InvalidPool', 'BadCallback', 'InsufficientProfit')}
# TransferFailed and Error(string) may wrap an inner out-of-gas; retain the
# bounded escalation for those instead of declaring them semantic failures.


class ExecutionAnchorChanged(Reject):
    """Preparation stopped before the whole-call probe or any signing.

    Only this typed, pre-submission failure permits the pipeline to spend its
    existing one-refresh budget on a new complete state read and reprice.
    """
    def __init__(self, candidate_anchor, execution_head):
        super().__init__('anchor expired during preparation')
        self.anchor_mismatch = {
            'candidate': {k:candidate_anchor.get(k) for k in ('number', 'hash')},
            'execution': {k:execution_head.get(k) for k in ('number', 'hash')},
            'preflight_reached': False,
        }


def execution_failure_details(exc):
    """Small, non-secret diagnostics; preserve the original cause of a timeout."""
    original = getattr(exc, 'original_error', exc)
    raw = getattr(original, 'revert_data', None)
    if raw is None:
        # Compatibility for legacy transports and archived failure strings.
        match = re.search(r"['\"]data['\"]\s*:\s*['\"](0x[0-9a-fA-F]*)['\"]", str(original))
        raw = match.group(1) if match else None
    selector = raw[:10].lower() if raw and len(raw) >= 10 else None
    result = {'cause': safe_error(original)}
    if selector:
        name = TERMINAL_REVERTS.get(selector, 'TransferFailed' if selector == data('TransferFailed()')[:10] else 'unclassified')
        # Aerodrome's custom K() has the same scheduling consequence as the
        # string-form pool invariant error. It does not establish a token tax;
        # directed transfer sampling determines that separately.
        if selector == data('K()')[:10]: name = 'PoolInvariant'
        if selector == '0x08c379a0' and raw and len(raw) <= 8192:
            try:
                reason = decode(['string'], unhex(raw)[4:])[0]
                if reason == 'UniswapV2: K': name = 'UniswapV2Invariant'
                elif reason == 'UniswapV2: TRANSFER_FAILED': name = 'V2TransferFailed'
            except Exception:
                pass
        result.update(revert_selector=selector, revert_name=name)
    if hasattr(exc, 'gas_attempts'): result['gas_attempts'] = exc.gas_attempts
    if original is not exc: result['validation_deadline_reached'] = True
    previous = getattr(exc, 'previous_gas_error', getattr(original, 'previous_gas_error', None))
    if previous is not None:
        result['previous_gas_failure'] = execution_failure_details(previous)
    if hasattr(exc, 'validation_outcome'): result['validation_outcome'] = exc.validation_outcome
    if hasattr(exc, 'transfer_retry_after_seconds'):
        result['transfer_retry_after_seconds'] = exc.transfer_retry_after_seconds
    probe = getattr(exc, 'execution_probe', getattr(original, 'execution_probe', None))
    if probe is not None: result['execution_probe'] = probe
    if hasattr(exc, 'context_timing'): result['context_timing'] = exc.context_timing
    if hasattr(exc, 'execution_gas_progress'): result['execution_gas_progress'] = exc.execution_gas_progress
    if isinstance(exc, ExecutionAnchorChanged): result['anchor_mismatch'] = exc.anchor_mismatch
    if hasattr(exc, 'anchor_refresh'): result['anchor_refresh'] = exc.anchor_refresh
    return result


def capture_execution_probe(operation, call, context, stage):
    """Preserve the exact unsigned request and timing on failure, with no RPCs.

    This records what we asked pending state to execute. It does not assert the
    provider's pending state root, which ordinary eth_call does not return.
    No signer, endpoint, credential or signed transaction is recorded.
    """
    started_ns, started_utc = time.monotonic_ns(), time.time()
    try:
        return operation()
    except Exception as exc:
        fields = ('from','to','data','value','gas','maxFeePerGas','maxPriorityFeePerGas')
        exc.execution_probe = {'stage':stage, 'started_utc':started_utc,
            'elapsed_ms':(time.monotonic_ns()-started_ns)/1e6, 'state_tag':'pending',
            'unsigned_call':{k:call[k] for k in fields if k in call},
            'block_context':json.loads(json.dumps(context)), 'state_root_proven':False}
        raise


class GasValidationExpired(Reject):
    def __init__(self, original, attempts):
        self.original_error, self.gas_attempts = original, list(attempts)
        super().__init__('gas validation deadline reached after: '+safe_error(original))


def estimate_context_supported(rpc, anchor):
    """Probe estimateGas independently: some endpoints ignore its fourth arg.

    The artificial future clock is used only by a temporary assertion contract,
    never for pool reads or execution. It cannot pass by matching today's clock.
    """
    height, stamp = number(anchor['number']), number(anchor['timestamp'])
    context = {'number': hex(height+64), 'time': hex(stamp+128),
               'blockHash': {str(height): anchor['hash']}}
    code = bytearray()
    for op, expected in ((b'\x43', height+64), (b'\x42', stamp+128),
                         (b'\x7f'+height.to_bytes(32, 'big')+b'\x40', int(anchor['hash'], 16))):
        code += op+b'\x7f'+expected.to_bytes(32, 'big')+b'\x14'
        target = len(code)+7
        code += b'\x61'+target.to_bytes(2, 'big')+b'\x57\x5f\x5f\xfd\x5b'
    code += b'\x00'
    params = [{'to': CONTEXT_PROBE, 'gas': hex(100_000)}, 'pending',
              {CONTEXT_PROBE: {'code': '0x'+code.hex()}}, context]
    try:
        # Control check makes a broken assertion probe fail closed as well.
        check(rpc.request('eth_call', params) == '0x', 'invalid estimate context probe')
        return 21_000 <= number(rpc.request('eth_estimateGas', params)) <= 100_000
    except Reject:
        return False


def verified_call_gas_budget(rpc, call, context, initial, maximum, deadline_ns, before_first_call=None,
                             on_failed_call=None, on_success=None):
    """Find a successful gas ceiling using the real sender, code and storage.

    eth_call honors the verified block context. A successful ceiling is a
    budget, not a measurement of gas consumed. The caller reserves its full fee
    and repeats validation after repricing. No balances or code are overridden.
    """
    check(0 < initial and 0 < maximum, 'invalid gas validation limits')
    gas = min(initial, maximum)
    attempts, last_error = [], None
    while True:
        if time.monotonic_ns() > deadline_ns:
            if last_error is not None: raise GasValidationExpired(last_error, attempts) from last_error
            raise Reject('observation expired during gas validation')
        try:
            if not attempts and before_first_call is not None: before_first_call()
            attempts.append(gas)
            probe_call = {**call, 'gas':hex(gas)}
            raw = capture_execution_probe(
                lambda:rpc.request('eth_call', [probe_call, 'pending', {}, context]),
                probe_call, context, 'initial_call')
            check(len(unhex(raw)) == 32, 'invalid execution gas validation result')
            if on_success is not None: on_success()
            if time.monotonic_ns() > deadline_ns:
                exc = Reject('observation expired during gas validation')
                exc.validation_outcome = 'call_succeeded_after_deadline'
                raise exc
            return gas
        except Exception as exc:
            # Nested calls can report a transfer/callback revert when out of
            # gas. Test a larger ceiling, bounded by the existing deadline/cap.
            reason = str(exc).lower()
            execution_failure = reason.startswith('rpc eth_call:') and any(
                text in reason for text in ('execution reverted', 'out of gas', 'outofgas', 'intrinsic gas too low'))
            exc.gas_attempts = list(attempts)
            if last_error is not None and last_error is not exc: exc.previous_gas_error = last_error
            details = execution_failure_details(exc)
            completed_in_time = time.monotonic_ns() <= deadline_ns
            if (completed_in_time and execution_failure and isinstance(exc, RPCError)
                    and exc.method == 'eth_call' and details.get('revert_name') in
                    ('TransferFailed', 'V2TransferFailed') and on_failed_call is not None):
                on_failed_call(gas, details['revert_name'])
            # A final ceiling or terminal revert can also arrive too late.
            # Do not let those exits bypass the original observation deadline
            # and become fresh structural-failure evidence for quarantine.
            if execution_failure and not completed_in_time:
                raise GasValidationExpired(exc, attempts) from exc
            terminal = (details.get('revert_selector') in TERMINAL_REVERTS or
                        details.get('revert_name') in ('UniswapV2Invariant', 'PoolInvariant'))
            if terminal or not execution_failure or gas >= maximum:
                raise
            last_error = exc
            gas = min(maximum, max(gas+1, gas*2))


class BroadcastRPC(RPC):
    METHODS = RPC.METHODS | {"eth_sendRawTransaction"}

    def __init__(self, url=None, chain_id=8453, timeout=15, cancel_event=None):
        endpoint = url or os.environ.get('BASE_EXECUTION_RPC_URL') or os.environ.get('BASE_RPC_URL') or DEFAULT_EXECUTION_RPC
        super().__init__(endpoint, chain_id, timeout, cancel_event)
        self.critical_reads = True


def verify_execution_context(rpc):
    """Reject a pending simulator that loses the real anchor hash on overrides."""
    for attempt in range(4):
        head = rpc.header()
        height, timestamp = number(head['number']), number(head['timestamp'])
        raw = rpc.request('eth_call', [{'to': CONTEXT_PROBE, 'data': '0x'+height.to_bytes(32, 'big').hex(),
                                       'gas': hex(100_000)}, 'pending', {CONTEXT_PROBE: {'code': CONTEXT_PROBE_CODE}},
                                      simulation_context(head)])
        block, stamp, anchor_hash, chain = decode(['uint256', 'uint256', 'bytes32', 'uint256'], unhex(raw))
        check(chain == rpc.chain_id and block == height+1 and stamp == timestamp+2,
              '执行节点不支持所需的区块上下文参数。')
        if anchor_hash == unhex(head['hash']) and rpc.header(height)['hash'] == head['hash']:
            return {'anchor': height, 'anchor_hash': head['hash'], 'pending_anchor_verified': True}
        if attempt < 3: time.sleep(.2)
    check(False, '执行节点的 pending 区块校验不兼容或尚未同步；请稍后重试或设置 BASE_EXECUTION_RPC_URL。')


def required_net_profit(config, settlement, context, fee_in_settlement=0):
    # The floor is NET of this attempt's entire fee reservation. Requiring one
    # fee budget of net profit leaves room for a comparable failed attempt;
    # this policy is a margin, not an assumed or measured win probability.
    floor = quote_profit_floor(config, settlement, fee_in_settlement)
    usdc_floor = int(config['policy'].get('min_net_profit_usdc_raw', 0))
    if settlement['address'] == USDC:
        return max(floor, usdc_floor)
    if settlement['address'] == WETH and usdc_floor:
        check(context.get('weth_profit_usdc_raw') == usdc_floor and context.get('weth_profit_floor', 0) > 0,
              'WETH 净利润的 USDC 报价不可用。')
        return max(floor, context['weth_profit_floor'])
    return floor


def runtime_matches(actual, artifact):
    """Match compiler output except immutable slots; getters verify their values."""
    wanted, got = bytearray(unhex(artifact["deployedBytecode"]["object"])), bytearray(unhex(actual))
    if len(wanted) != len(got): return False
    for refs in artifact["deployedBytecode"].get("immutableReferences", {}).values():
        # A getter alone is insufficient: every reference to the same immutable
        # must contain the identical value, including owner checks and payouts.
        values = {bytes(got[r["start"]:r["start"]+r["length"]]) for r in refs}
        if len(values) != 1: return False
        for ref in refs:
            start, size = ref["start"], ref["length"]
            wanted[start:start+size] = got[start:start+size] = bytes(size)
    return wanted == got


class FeeOracle:
    def __init__(self, rpc, config):
        self.rpc, self.config = rpc, config

    def maximum_fee(self, calldata, gas, max_fee):
        # An oversized unsigned envelope includes all RLP fields and access list.
        size = len(unhex(calldata))+512
        calls = [("eth_call", [{"to": ORACLE, "data": data(sig, ["uint256"], [value])}, "pending"])
                 for sig, value in (("getL1FeeUpperBound(uint256)", size), ("getOperatorFee(uint256)", gas))]
        l1, operator = [decode(["uint256"], unhex(v))[0] for v in self.rpc.batch(calls)]
        return gas*max_fee + 2*l1 + 2*operator

    def settlement_cost(self, token, maximum_fee):
        if token == WETH: return maximum_fee
        check(token == USDC, "unsupported fee settlement")
        quoter = next(f["quoter"] for f in self.config["factories"] if f["kind"] == "v3")
        # Price the actual ETH fee amount in USDC through an executable quote.
        # The configured ceiling is retained as an additional conservative floor.
        raw = self.rpc.call(quoter, data("quoteExactOutputSingle((address,address,uint256,uint24,uint160))",
                    ["(address,address,uint256,uint24,uint160)"], [(USDC, WETH, maximum_fee, 500, 0)]), "pending")
        quoted = decode(["uint256", "uint160", "uint32", "uint256"], raw)[0]
        configured = (maximum_fee*self.config["policy"]["eth_usdc_ceiling"]+10**12-1)//10**12
        return max(quoted, configured)


class Executor:
    def __init__(self, rpc, config, contract, signer, directory, v4_contract=None, structural_contract=None, protocol_contracts=None):
        self.rpc, self.config, self.signer = rpc, config, signer
        self.contract, self.owner = address(contract), address(signer.address)
        self.v4_contract = address(v4_contract) if v4_contract else None
        self.structural_contract = address(structural_contract) if structural_contract else None
        self.protocol_contracts = {k:address(v) for k,v in (protocol_contracts or {}).items()}
        from .gas_progress import GasValidationProgress
        self.gas_progress = GasValidationProgress(config['chain_id'], self.owner)
        self.clock_lens = json.loads((ROOT/'out/StateLens.sol/StateLens.json').read_text())['deployedBytecode']['object']
        check(self.owner == config["owner"], "signer does not match configured owner")
        directory = Path(directory); directory.mkdir(parents=True, exist_ok=True)
        directory.chmod(0o700)
        from .quarantine_runtime import quarantine_path
        self.transfer_quarantine_path = quarantine_path(directory, self.contract)
        self.lock = open(directory/"executor.lock", "a+")
        try: fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.lock.close(); raise RuntimeError("another executor is using this account ledger")
        p = config["policy"]
        self.ledger = RiskLedger(directory/"transactions.sqlite", int(p["max_daily_fee_wei"]), p["max_consecutive_failures"])
        self.db = self.ledger.db
        self.db.execute("CREATE TABLE IF NOT EXISTS intents (attempt TEXT PRIMARY KEY, tx_hash TEXT NOT NULL, payload TEXT NOT NULL, outcome TEXT)")
        if "finalized" not in {row[1] for row in self.db.execute("PRAGMA table_info(intents)")}:
            self.db.execute("ALTER TABLE intents ADD COLUMN finalized INTEGER NOT NULL DEFAULT 0")
        self.db.execute("CREATE INDEX IF NOT EXISTS pending_finality ON intents(finalized) WHERE finalized=0")
        self.fees = FeeOracle(rpc, config)
        self.context = None
        self.context_error = None
        self.context_stop = threading.Event()
        self.context_thread = None
        self.context_changed = threading.Condition()
        self.context_kick_sequence = 0
        self.context_refresh_sequence = 0
        self.context_published_sequence = 0
        self.checked_settlement_head = None
        try:
            self.preflight()
            # An exclusive process lock proves no other sender can be between
            # reserve and submitted. Reserved-only attempts never reached RPC.
            for attempt, in self.db.execute("SELECT id FROM attempts WHERE state='reserved'").fetchall():
                self.ledger.cancel_unsent(attempt)
            self.refresh_context()
        except Exception:
            self.close(); raise

    def preflight(self):
        self.rpc.verify_chain()
        verify_execution_context(self.rpc)
        self.gas_estimate_context_verified = estimate_context_supported(self.rpc, self.rpc.header())
        artifact = json.loads((ROOT/"out/AtomicCycle.sol/AtomicCycle.json").read_text())
        code = self.rpc.request("eth_getCode", [self.contract, "latest"])
        check(runtime_matches(code, artifact), "deployed executor differs from verified build")
        calls = [("eth_call", [{"to": self.contract, "data": data(sig)}, "latest"])
                 for sig in ("owner()", "chainId()", "paused()")]
        owner, chain, paused = self.rpc.batch(calls)
        check(decode(["address"], unhex(owner))[0].lower() == self.owner, "contract owner mismatch")
        check(decode(["uint256"], unhex(chain))[0] == self.config["chain_id"], "contract chain mismatch")
        check(not decode(["bool"], unhex(paused))[0], "executor is paused")
        if self.v4_contract:
            from .v4 import verify_contract
            verify_contract(self.rpc, self.config, self.v4_contract)

        if self.structural_contract:
            from .structural import verify_contract
            verify_contract(self.rpc, self.config, self.structural_contract)
        for name,contract in self.protocol_contracts.items():
            from .protocol_execution import verify_contract as verify_protocol
            verify_protocol(self.rpc,self.config,contract,name=name)

    def _context_condition(self):
        # Minimal __new__ test doubles also use the real context decoder.
        return self.__dict__.setdefault('context_changed', threading.Condition())

    def refresh_context(self, rpc=None):
        rpc = rpc or self.rpc
        started = time.monotonic_ns()
        condition = self._context_condition()
        with condition:
            generation = getattr(self, 'context_refresh_sequence', 0) + 1
            self.context_refresh_sequence = generation
        p = self.config["policy"]
        calls = [("eth_getTransactionCount", [self.owner, "latest"]), ("eth_getTransactionCount", [self.owner, "pending"]),
                 ("eth_getBlockByNumber", ["latest", False]), ("eth_getBalance", [self.owner, "pending"]),
                 ("eth_call", [{"to": ORACLE, "data": data("getL1FeeUpperBound(uint256)", ["uint256"], [2048])}, "pending"]),
                 ("eth_call", [{"to": ORACLE, "data": data("getOperatorFee(uint256)", ["uint256"], [p["max_gas"]])}, "pending"]),
                 ('eth_getBlockByNumber', ['pending', False])]
        native_cap = int(p["max_total_fee_wei"])
        usdc_index, weth_floor_index = None, None
        if any(t["address"] == USDC for t in self.config["settlement_tokens"]):
            quoter = next(f["quoter"] for f in self.config["factories"] if f["kind"] == "v3")
            usdc_index = len(calls)
            calls.append(("eth_call", [{"to": quoter, "data": data("quoteExactOutputSingle((address,address,uint256,uint24,uint160))",
                ["(address,address,uint256,uint24,uint160)"], [(USDC, WETH, native_cap, 500, 0)])}, "pending"]))
        usdc_floor = int(p.get('min_net_profit_usdc_raw', 0))
        if usdc_floor and any(t['address'] == WETH for t in self.config['settlement_tokens']):
            quoter = next(f['quoter'] for f in self.config['factories'] if f['kind'] == 'v3')
            weth_floor_index = len(calls)
            calls.append(('eth_call', [{'to': quoter, 'data': data('quoteExactOutputSingle((address,address,uint256,uint24,uint160))',
                ['(address,address,uint256,uint24,uint160)'], [(WETH, USDC, usdc_floor, 500, 0)])}, 'pending']))
        reward_index = None
        if p.get('profit_aware_bidding'):
            reward_index = len(calls)
            calls.append(('eth_feeHistory', ['0x4', 'latest', [p.get('priority_fee_percentile', 90)]]))
        # These specific Oracle getters and QuoterV2 amount quotes do not depend
        # on caller identity. QuoterV2 swaps revert their temporary pool writes.
        approved = [(ORACLE, data('getL1FeeUpperBound(uint256)')[:10]),
                    (ORACLE, data('getOperatorFee(uint256)')[:10])]
        approved += [(f['quoter'].lower(), data('quoteExactOutputSingle((address,address,uint256,uint24,uint160))')[:10])
                     for f in self.config['factories'] if f['kind'] == 'v3' and f.get('quoter')]
        prior_measurements = getattr(rpc, 'measurements', ())
        prior_measurement = prior_measurements[-1] if prior_measurements else None
        values = rpc.batch_contract_reads(calls, approved) if p.get('aggregate_execution_context', False) else rpc.batch(calls)
        l1, operator = [decode(["uint256"], unhex(v))[0] for v in values[4:6]]
        usdc = decode(["uint256", "uint160", "uint32", "uint256"], unhex(values[usdc_index]))[0] if usdc_index is not None else 0
        weth_floor = decode(['uint256', 'uint160', 'uint32', 'uint256'], unhex(values[weth_floor_index]))[0] if weth_floor_index is not None else 0
        received_ns = time.monotonic_ns()
        measurements = getattr(rpc, 'measurements', ())
        measurement = measurements[-1] if measurements and measurements[-1] is not prior_measurement else {}
        wire_started = measurement.get('network_started_ns')
        wire_finished = measurement.get('network_finished_ns')
        # Queueing happens before any of these account/fee queries are sent.
        # Freshness starts at the earliest actual send of the entire batch;
        # absent or unbound timing retains the original conservative bound.
        timed = (measurement.get('method') == 'batch' and measurement.get('outcome') == 'ok'
                 and measurement.get('timing_complete') is True
                 and isinstance(wire_started, int) and isinstance(wire_finished, int)
                 and started <= wire_started <= wire_finished <= received_ns)
        context = {"requested_ns": wire_started if timed else started,
                        "refresh_started_ns": started, "received_ns": received_ns,
                        "freshness_origin": 'network_send' if timed else 'refresh_start',
                        "nonce": number(values[0]), "pending_nonce": number(values[1]),
                        "head": values[2], "balance": number(values[3]), "extra_fee": 2*l1+2*operator,
                        'pending_head': values[6],
                        "usdc_at_native_cap": usdc, "native_cap": native_cap,
                        'weth_profit_floor': weth_floor, 'weth_profit_usdc_raw': usdc_floor}
        if reward_index is not None:
            rewards = values[reward_index].get('reward', [])
            check(rewards and all(isinstance(row, list) and len(row) == 1 for row in rewards), 'fee history rewards unavailable')
            context['observed_priority_fee'] = max(number(row[0]) for row in rewards)
        with condition:
            stop = getattr(self, 'context_stop', None)
            if ((stop is None or not stop.is_set()) and
                    generation > getattr(self, 'context_published_sequence', 0)):
                # Publish the decoded nonce, fees, balance and both heads as one
                # generation. A late older request cannot replace a newer one.
                context['refresh_generation'] = generation
                self.context = context
                self.context_published_sequence = generation
                self.context_error = None
            condition.notify_all()

    def start_context_worker(self):
        if self.context_thread: return
        condition = self._context_condition()
        def update():
            rpc = RPC(self.rpc.url, self.rpc.chain_id, timeout=3, cancel_event=self.context_stop)
            rpc.critical_reads = True
            try:
                while not self.context_stop.is_set():
                    with condition:
                        kick = getattr(self, 'context_kick_sequence', 0)
                    try: self.refresh_context(rpc)
                    except Exception as exc:
                        with condition:
                            self.context_error = type(exc).__name__
                            condition.notify_all()
                    with condition:
                        interval_ns = 250_000_000 if getattr(self,'structural_contract',None) else 500_000_000
                        next_refresh = time.monotonic_ns() + interval_ns
                        while (not self.context_stop.is_set() and
                               kick == getattr(self, 'context_kick_sequence', 0)):
                            remaining = (next_refresh-time.monotonic_ns())/1e9
                            if remaining <= 0: break
                            # The bound stop event can be set by another worker
                            # without notifying this condition.
                            condition.wait(min(.05, remaining))
            finally:
                rpc.close()
                with condition: condition.notify_all()
        self.context_thread = threading.Thread(target=update, name="execution-context", daemon=True)
        self.context_thread.start()

    def _context_for_anchor(self, candidate, anchor, observation_deadline_ns):
        """Wait for the existing background reader, never refresh pools or send.

        Only a strictly lagging context can catch up this way. A newer head or
        same-height hash mismatch keeps the original typed recovery path.
        """
        condition = self._context_condition()
        candidate.pop('execution_context_sync', None)
        with condition:
            context = self.context
            worker = getattr(self, 'context_thread', None)
            if (worker is None or not worker.is_alive() or context is None or
                    number(context['head']['number']) >= number(anchor['number'])):
                return context
            initial_context = context
            started = time.monotonic_ns()
            deadline = min(observation_deadline_ns, started + 500_000_000)
            details = {'candidate_block':number(anchor['number']),
                       'initial_context_block':number(context['head']['number']),
                       'max_wait_ms':max(0, (deadline-started)/1e6),
                       'refresh_requested':False, 'wait_ms':0.0, 'outcome':'waiting'}
            candidate['execution_context_sync'] = details
            try:
                self.rpc.check_cancelled()
                check(not self.context_stop.is_set(), 'execution context stopped')
                check(started <= observation_deadline_ns, 'observation expired before context synchronization')
                self.context_kick_sequence = getattr(self, 'context_kick_sequence', 0) + 1
                details['refresh_requested'] = True
                condition.notify_all()
                while True:
                    self.rpc.check_cancelled()
                    check(not self.context_stop.is_set(), 'execution context stopped')
                    now = time.monotonic_ns()
                    details['wait_ms'] = max(0, (now-started)/1e6)
                    check(now <= observation_deadline_ns, 'observation expired during context synchronization')
                    if now >= deadline:
                        details['outcome'] = 'timeout'
                        # A publication after our bounded wait cannot turn this
                        # attempt into an unbounded preparation extension.
                        return initial_context
                    context = self.context
                    if context is not None:
                        current_height = number(context['head']['number'])
                        details['final_context_block'] = current_height
                        if current_height >= number(anchor['number']):
                            details['outcome'] = ('caught_up' if context['head']['hash'] == anchor['hash']
                                                  else 'anchor_changed')
                            return context
                    if not worker.is_alive():
                        details['outcome'] = 'worker_stopped'
                        return context
                    condition.wait(min(.05, (deadline-now)/1e9))
            except Exception:
                details['wait_ms'] = max(0, (time.monotonic_ns()-started)/1e6)
                details['outcome'] = 'stopped' if self.context_stop.is_set() else 'expired_or_cancelled'
                raise

    def bind_stop_event(self, stop):
        check(self.context_thread is None, "bind stop event before starting executor workers")
        self.context_stop = stop
        self.rpc.cancel_event = stop

    def execute(self, candidate, route, states, anchor, observation_deadline_ns, *, submit=True):
        try:
            return self._execute_once(candidate, route, states, anchor, observation_deadline_ns, submit=submit)
        except ExecutionAnchorChanged:
            # These routes have a complete funded quote entry point. A changed
            # head before any signing is a reason to remeasure this exact route,
            # not to lose it behind broad discovery or reuse its old profit.
            if getattr(route, 'execution_kind', None) not in ('structural-v1', 'protocol-v1'):
                raise
        self.rpc.check_cancelled()
        context = self.context
        check(context is not None and time.monotonic_ns()-context['requested_ns'] <= 1_500_000_000,
              'execution context stale during anchor refresh')
        fresh_anchor = context['head']
        check(fresh_anchor['hash'] != anchor['hash'], 'anchor refresh made no progress')
        target = (self.protocol_contracts.get(route.contract_name)
                  if route.execution_kind == 'protocol-v1' else self.structural_contract)
        check(target is not None, 'strategy executor deployment required')
        refresh = {'old_block': number(anchor['number']), 'new_block': number(fresh_anchor['number']),
                   'old_hash': anchor['hash'], 'new_hash': fresh_anchor['hash'],
                   'initial_gross_surplus': candidate['gross_surplus'], 'attempts': 1,
                   'remeasured': False}
        candidate['anchor_refresh'] = refresh
        try:
            from .structural_runner import complete_quote
            fresh, started = complete_quote(self.rpc, self.config, target, route, int(candidate['amount_in']),
                fresh_anchor, simulation_context(fresh_anchor, context['pending_head']), context['nonce'],
                int(candidate['cost_budget']['gas_limit']))
            refresh.update(remeasured=True, gross_surplus=fresh['gross_surplus'],
                           minimum_gross_surplus=fresh['minimum_gross_surplus'])
            candidate.update(fresh)
            deadline = started + int(self.config['policy']['snapshot_max_age_ms']*1e6)
            # Call the non-retrying path: a second head change ends this attempt.
            result = self._execute_once(candidate, route, states, fresh_anchor, deadline, submit=submit)
            result['anchor_refresh'] = refresh
            return result
        except Exception as exc:
            from .structural_screening import UnprofitableQuote
            if isinstance(exc, UnprofitableQuote):
                refresh.update(remeasured=True, gross_surplus=exc.gross, minimum_gross_surplus=exc.required)
                candidate.update(gross_surplus=exc.gross, model_fee_raw=exc.fee, minimum_gross_surplus=exc.required)
            exc.anchor_refresh = refresh
            raise

    def _execute_once(self, candidate, route, states, anchor, observation_deadline_ns, *, submit=True):
        policy = self.config["policy"]
        self.rpc.check_cancelled()
        check(time.monotonic_ns() <= observation_deadline_ns, "observation expired")
        is_v4 = getattr(route, 'execution_kind', None) == 'v4-pair-v1'
        is_protocol = getattr(route, 'execution_kind', None) == 'protocol-v1'
        is_structural = is_protocol or getattr(route, 'execution_kind', None) == 'structural-v1'
        incompatible = blocked_tokens(self.config, (route.tokens[0],) if is_v4 else route.tokens)
        check(not incompatible, 'unsupported token transfer: '+','.join(incompatible))
        # Also protect direct callers: a restart or a changed canonical head
        # must be reconciled before releasing the account for another send.
        self.verify_settlements()
        check(not self.db.execute("SELECT 1 FROM attempts WHERE state IN ('reserved','submitted')").fetchone(), "previous transaction unresolved")
        check(candidate["route_key"] == route.key and tuple(candidate["pools"]) == route.pools
              and tuple(candidate["tokens"]) == route.tokens, "candidate route binding mismatch")
        token = route.tokens[0]
        settlement = next(t for t in self.config["settlement_tokens"] if t["address"] == token)
        amount = int(candidate["amount_in"])
        check(0 < amount <= int(settlement["max_input_raw"]), "input cap exceeded")
        if getattr(route,'strategy',None)==4 and getattr(route,'execution_kind',None)=='structural-v1':
            funding=next(t for t in self.config['settlement_tokens'] if t['address']==route.input_token)
            check(amount<=int(funding['max_input_raw']),'funding asset input cap exceeded')
        gas = int(candidate["cost_budget"]["gas_limit"])
        max_fee = int(candidate["cost_budget"]["max_fee_per_gas"])
        priority = int(candidate["cost_budget"]["priority_fee"])
        check(0 < gas <= policy["max_gas"] and 0 <= priority <= max_fee < 2**256, "invalid transaction fee envelope")
        context = self._context_for_anchor(candidate, anchor, observation_deadline_ns)
        context_age = (time.monotonic_ns()-context['requested_ns'])/1e6 if context is not None else None
        if context_age is None or context_age > 1500:
            error = Reject('execution context stale')
            error.context_timing = {'age_ms':context_age, 'max_age_ms':1500,
                'freshness_origin':context.get('freshness_origin', 'refresh_start') if context else None,
                'refresh_error':self.context_error}
            raise error
        nonce, head = context["nonce"], context["head"]
        check(nonce == context["pending_nonce"], "account already has a pending transaction")
        if head['hash'] != anchor['hash']:
            # A funded DEX quote may finish across a normal two-second block
            # boundary. Build the final transaction on the current complete
            # account context; its actual deployed-executor call below still
            # rechecks repayment, profit, canonical hash, clock and nonce.
            # Do not extend the original deadline or permit same-height reorgs,
            # backward contexts, arbitrary route types, or repeated rollover.
            rollover = (candidate.get('allow_execution_anchor_rollover') is True
                        and candidate.get('execution_kind') == 'mixed-v1'
                        and not (is_v4 or is_structural)
                        and not candidate.get('execution_anchor_rollover')
                        and 0 < number(head['number'])-number(anchor['number']) <= 2)
            if not rollover: raise ExecutionAnchorChanged(anchor, head)
            check(time.monotonic_ns() <= observation_deadline_ns, 'observation expired before anchor rollover')
            candidate['execution_anchor_rollover'] = {
                'old_block':number(anchor['number']), 'new_block':number(head['number']),
                'mode':'current_context_complete_call', 'original_deadline_preserved':True,
                'final_validation_passed':False}
            anchor = head
        target = self.protocol_contracts.get(route.contract_name) if is_protocol else self.structural_contract if is_structural else self.v4_contract if is_v4 else self.contract
        check(target is not None, 'strategy executor deployment required')
        check(context.get('pending_head') is not None, 'execution pending clock unavailable')
        block_context = simulation_context(anchor, context['pending_head'])
        height, timestamp = number(anchor["number"]), number(anchor["timestamp"])
        plan = [None if (is_v4 or is_structural) else make_hops(route, states), amount, int(candidate["minimum_gross_surplus"]), max_fee,
                timestamp+6, height, unhex(anchor["hash"]), height+2, nonce]
        def encode_current():
            if is_protocol:
                from .protocol_execution import encode_plan
                return encode_plan(candidate,route,plan)
            if is_structural:
                from .structural import encode_plan
                return encode_plan(candidate, route, plan)
            if is_v4:
                from .v4 import encode_plan
                return encode_plan(candidate, route, plan)
            return data(f'execute({PLAN_TYPE})', [PLAN_TYPE], [tuple(plan)])
        calldata = encode_current()
        extra_fee=context["extra_fee"]
        if is_structural and len(unhex(calldata))+512>2048:
            check(len(unhex(calldata))+512<=8192,"structural calldata bound")
            extra_fee=max(extra_fee,self.fees.maximum_fee(calldata,policy["max_gas"],max_fee)-policy["max_gas"]*max_fee)
        else:check(len(unhex(calldata))+512<=2048,"transaction exceeds cached L1 fee envelope")
        maximum = max(gas*max_fee+extra_fee, int(candidate["cost_budget"]["total_fee_wei"]))
        check(maximum <= int(policy["max_total_fee_wei"]), "attempt fee cap exceeded")
        check(context["balance"] >= maximum, "insufficient ETH fee balance")
        def settlement_cost(native_fee):
            if token == WETH: return native_fee
            check(token == USDC and context["usdc_at_native_cap"] > 0, "USDC fee conversion unavailable")
            return max((native_fee*context["usdc_at_native_cap"]+context["native_cap"]-1)//context["native_cap"],
                       (native_fee*policy["eth_usdc_ceiling"]+10**12-1)//10**12)
        cost = settlement_cost(maximum)
        plan[2] = max(plan[2], cost+required_net_profit(self.config, settlement, context, cost))
        check(candidate['gross_surplus'] >= plan[2], '候选净利润低于当前 USDC 金额门槛。')
        calldata = encode_current()
        transaction = {"chainId": self.config["chain_id"], "type": 2, "nonce": nonce,
                       "to": to_checksum_address(target), "value": 0, "data": calldata,
                       "gas": gas, "maxFeePerGas": max_fee, "maxPriorityFeePerGas": priority}
        call = {k: hex(v) if isinstance(v, int) else v for k, v in transaction.items() if k not in ("chainId", "type", "nonce")}
        call["from"] = self.owner
        # The hop-count model is only an initial budget. Registration and
        # dynamic-fee contracts can use more gas, sometimes surfaced as a
        # nested token-transfer revert. Validate a gas budget for the deployed
        # executor in the same block context, then reprice before final checking.
        estimate_call = {**call, 'gas': hex(policy['max_gas'])}
        check(time.monotonic_ns() <= observation_deadline_ns, 'observation expired before execution probe')
        transfers = None if (is_v4 or is_structural) else getattr(self, 'transfer_feedback', None)
        route_feedback = None if (is_v4 or is_structural) else getattr(self, 'execution_feedback', None)
        probe_claimed = False
        gas_progress_key = self.gas_progress.key(target, route.key, amount)
        validation_maximum = policy['max_gas']*100//115
        validation_initial, gas_progress = self.gas_progress.start(gas_progress_key, gas, validation_maximum)
        candidate['execution_gas_progress'] = gas_progress
        def claim_probe():
            nonlocal probe_claimed
            self.rpc.check_cancelled()
            check(time.monotonic_ns() <= observation_deadline_ns, 'observation expired before execution probe')
            if transfers is not None:
                check(transfers.claim_probe(route, amount=amount, states=states,
                    settlements=tuple(t['address'] for t in self.config['settlement_tokens'])),
                    'sampled transfer retry or prewarm waiting')
                probe_claimed = True
        try:
            if is_structural and candidate.get('execution_gas_estimate'):
                # The discovery whole call used this deployed contract's measure()
                # entry. The final call below still executes the signed selector
                # with the resulting gas limit, sender, price and profit floor.
                estimated = int(candidate['execution_gas_estimate'])
                check(0 < estimated <= policy['max_gas'], 'invalid structural gas measurement')
                gas_budget_source = 'complete_structural_measure_call'
            elif (not (is_v4 or is_structural) and candidate.get('execution_kind') == 'mixed-v1'
                  and candidate.get('execution_gas_source') == 'whole_cycle_probe'):
                # The funded whole-cycle probe already ran first registration
                # and swaps, adding intrinsic/cold-access overhead. Treat this
                # as a gas ceiling hint, never as execution authorization. The
                # final exact deployed call below must succeed at the repriced
                # gas limit and profit floor before any signing or reservation.
                estimated = candidate.get('execution_gas_estimate')
                check(type(estimated) is int and 0 < estimated <= policy['max_gas'],
                      'invalid whole-cycle gas budget')
                claim_probe()
                gas_budget_source = 'whole_cycle_probe_upper_bound'
            elif self.gas_estimate_context_verified:
                claim_probe()
                estimated = number(capture_execution_probe(
                    lambda:self.rpc.request('eth_estimateGas', [estimate_call, 'pending', {}, block_context]),
                    estimate_call, block_context, 'gas_estimate'))
                check(estimated > 0, 'invalid execution gas estimate')
                self.gas_progress.clear(gas_progress_key)
                gas_budget_source = 'context_verified_estimateGas'
            else:
                estimated = verified_call_gas_budget(self.rpc, call, block_context, validation_initial,
                    validation_maximum, observation_deadline_ns, before_first_call=claim_probe,
                    on_failed_call=lambda failed_gas, name:self.gas_progress.record(gas_progress_key, failed_gas, name),
                    on_success=lambda:self.gas_progress.clear(gas_progress_key))
                gas_budget_source = 'successful_context_eth_call_ceiling'
                if transfers is not None: transfers.passed(route)
                if route_feedback is not None: route_feedback.passed(route.key)
        except Exception as exc:
            exc.execution_gas_progress = gas_progress
            if probe_claimed and transfers is not None:
                delay = transfers.rejected(route, execution_failure_details(exc), amount=amount)
                if delay: exc.transfer_retry_after_seconds = delay
            raise
        self.rpc.check_cancelled()
        check(estimated > 0, 'invalid execution gas estimate')
        gas = max(gas, (estimated*115+99)//100)
        check(gas <= policy['max_gas'], 'estimated execution exceeds gas cap')
        bid = {'selected_priority_fee_wei':priority, 'scope':'configured minimum'}
        if policy.get('profit_aware_bidding'):
            from .costs import competitive_bid
            # Large liquidation payloads can exceed the cached L1 allowance.
            # Allocate the tip after their measured extra fee, not before it.
            priority, bid = competitive_bid(policy, token, {**context, 'extra_fee':extra_fee}, candidate['gross_surplus'], gas,
                number(head['baseFeePerGas']), priority, required_net_profit(self.config, settlement, context, 0))
            max_fee = max(max_fee, 2*number(head['baseFeePerGas'])+priority)
            plan[3] = max_fee
        maximum = max(gas*max_fee+extra_fee, int(candidate['cost_budget']['total_fee_wei']))
        check(maximum <= int(policy['max_total_fee_wei']), 'attempt fee cap exceeded after gas estimate')
        check(context['balance'] >= maximum, 'insufficient ETH fee balance after gas estimate')
        cost = settlement_cost(maximum)
        plan[2] = max(plan[2], cost+required_net_profit(self.config, settlement, context, cost))
        check(candidate['gross_surplus'] >= plan[2], '估算完整交易费用后，候选净利润低于门槛。')
        calldata = encode_current()
        transaction.update(gas=gas, data=calldata, maxFeePerGas=max_fee, maxPriorityFeePerGas=priority)
        call.update(gas=hex(gas), data=calldata, maxFeePerGas=hex(max_fee), maxPriorityFeePerGas=hex(priority))
        # Explicit block context avoids pending clients' historical EVM context.
        # Include the real canonical hash: number/time alone cannot repair a
        # pending simulator's lagging BLOCKHASH lookup. Onchain checks stay intact.
        check(time.monotonic_ns() <= observation_deadline_ns, "observation expired before final validation")
        validation_started = time.monotonic_ns()
        # A single transport round trip gives both independent checks the same
        # bounded validation window. JSON-RPC batch order is not execution order.
        if is_protocol:
            from .protocol_execution import clock_call
            clock_request = clock_call(route, block_context)
        elif is_structural:
            from .structural import clock_call
            clock_request = clock_call(route, block_context)
        elif is_v4:
            from .v4 import clock_call
            clock_request = clock_call(block_context)
        else:
            clock_request = ('eth_call', [{'to': LENS, 'gas': hex(1_000_000),
                'data': (data('checkMixedClock(address[],int24[])', ['address[]','int24[]'],
                    [list(route.pools), [states[a].spacing for a in route.pools]])
                    if any(states[a].kind == 'v2' for a in route.pools) else
                    data('checkClock(address[])', ['address[]'], [list(route.pools)]))},
                'pending', {LENS: {'code': self.clock_lens}}, block_context])
        raw, canonical, pending_after, clock_raw, fresh_nonce = capture_execution_probe(lambda:self.rpc.batch([
            ('eth_call', [call, 'pending', {}, block_context]),
            ('eth_getBlockByNumber', [hex(height), False]),
            ('eth_getBlockByNumber', ['pending', False]),
            clock_request,
            ('eth_getTransactionCount', [self.owner, 'pending']),
        ]), call, block_context, 'final_call_batch')
        check(number(fresh_nonce) == nonce, 'account nonce changed before signing')
        self.rpc.check_cancelled()
        profit = decode(["uint256"], unhex(raw))[0]
        check(profit >= plan[2], "current execution does not cover fees and profit")
        if transfers is not None: transfers.passed(route)
        if route_feedback is not None: route_feedback.passed(route.key)
        check(canonical is not None and number(canonical['number']) == height and canonical['hash'] == anchor['hash'],
              'execution anchor not canonical before signing')
        check(pending_after is not None and simulation_context(anchor, pending_after) == block_context,
              'pending execution clock changed before signing')
        clock_valid, clock_block, clock_time = decode(['bool', 'uint256', 'uint256'], unhex(clock_raw))
        check(clock_valid and clock_block == number(block_context['number']) and clock_time == number(block_context['time']),
              'pool oracle state is newer than the execution clock')
        # This call revalidates the complete executable transaction at current
        # pending state. Its own request start is the new observation boundary.
        submission_deadline = validation_started + int(policy.get("final_validation_max_age_ms", 500)*1e6)
        check(time.monotonic_ns() <= submission_deadline, "final validation expired before signing")
        if gas_budget_source == 'whole_cycle_probe_upper_bound': self.gas_progress.clear(gas_progress_key)
        if candidate.get('execution_anchor_rollover'):
            candidate['execution_anchor_rollover']['final_validation_passed'] = True
        if not submit:
            return {'status': 'execution_preflight_passed', 'execution_kind': 'protocol-v1' if is_protocol else 'structural-v1' if is_structural else 'v4-pair-v1' if is_v4 else 'mixed-v1',
                    'simulated_gross_profit': profit, 'fee_in_settlement': cost, 'reserved_fee_wei': maximum,
                    'signed': False, 'broadcast': False, 'execution_gas_progress':gas_progress}
        signed = self.signer.sign_transaction(transaction)
        raw_tx = bytes(signed.raw_transaction)
        check(Account.recover_transaction(raw_tx).lower() == self.owner, "signature owner mismatch")
        check(TypedTransaction.from_bytes(HexBytes(raw_tx)).hash() == TypedTransaction.from_dict(transaction).hash(),
              "signer changed transaction fields")
        tx_hash = "0x"+keccak(raw_tx).hex()
        intent = {"transaction": transaction, "plan_nonce": nonce, "settlement": token,
                  "funding_token": route.input_token if is_structural and not is_protocol and route.strategy==4 else None,
                  "minimum_profit": plan[2], "input": amount, "reserved_fee_wei": maximum,
                  "fee_in_settlement": cost, "maximum_execution_block": height+2,
                  "simulated_gross_profit": profit, "prepared_at": time.time(), "contract": target,
                  "execution_kind": "protocol-v1" if is_protocol else "structural-v1" if is_structural else "v4-pair-v1" if is_v4 else "mixed-v1",
                  "route_key": route.key,
                  "amount_semantics": "protocol_debt_or_units" if is_protocol else "maximum_debt" if is_structural and route.strategy in (1,3) else "flash_loan_principal" if is_structural and route.strategy==4 else "exact_settlement_output" if is_v4 else "exact_settlement_input",
                  "estimated_execution_gas": estimated, "gas_budget_source": gas_budget_source, "fee_bid":bid,
                  'execution_block_context': block_context,
                  "raw_transaction": "0x"+raw_tx.hex()}
        self.rpc.check_cancelled()
        self.ledger.reserve(tx_hash, maximum)
        with self.db:
            self.db.execute("INSERT INTO intents(attempt,tx_hash,payload,outcome) VALUES(?,?,?,NULL)", (tx_hash, tx_hash, json.dumps(intent)))
        if self.context_stop.is_set() or time.monotonic_ns() > submission_deadline:
            self.ledger.cancel_unsent(tx_hash)
            raise RuntimeError("stopped or observation expired before submission")
        # Persist submitted BEFORE the request. A timeout may mean accepted;
        # never retry with a different nonce or release that fee reservation.
        self.ledger.submitted(tx_hash, tx_hash)
        returned = self.rpc.request("eth_sendRawTransaction", ["0x"+raw_tx.hex()])
        check(returned.lower() == tx_hash, "submission returned a different transaction hash")
        return {"tx_hash": tx_hash, "status": "submitted", "reserved_fee_wei": maximum,
                'simulated_gross_profit':profit,'fee_in_settlement':cost,
                'execution_gas_progress':gas_progress}

    def verify_settlements(self):
        """Recheck provisional receipts once per observed head, in header batches.

        Finalized receipts leave the watch set. Older databases migrate with
        finalized=0, so restarting never silently trusts their old outcomes.
        A missing finalized tag only prevents retirement from the watch set.
        """
        rows = self.db.execute("SELECT a.id,i.outcome FROM attempts a LEFT JOIN intents i ON a.id=i.attempt "
                               "WHERE a.kind='transaction' AND a.state IN ('success','failed') AND COALESCE(i.finalized,0)=0").fetchall()
        if not rows:
            return
        context = self.context
        head = (context["head"] if context and self.context_thread and
                (time.monotonic_ns()-context["requested_ns"])/1e6 <= 1500 else self.rpc.header())
        head_key = (number(head["number"]), head["hash"])
        if self.checked_settlement_head == head_key:
            return
        outcomes = {}
        for attempt, raw in rows:
            check(raw is not None, "settled transaction lacks receipt outcome; account reconciliation required")
            outcomes[attempt] = json.loads(raw)
        try:
            finalized = self.rpc.request("eth_getBlockByNumber", ["finalized", False])
        except Exception:
            finalized = None
        if finalized and number(finalized["number"]) > head_key[0]:
            finalized = None
        heights = {o["block"] for o in outcomes.values() if o["block"] <= head_key[0]}
        heights.add(head_key[0])
        if finalized:
            heights.add(number(finalized["number"]))
        heights = sorted(heights)
        headers = {}
        for start in range(0, len(heights), 128):
            chunk = heights[start:start+128]
            values = self.rpc.batch([("eth_getBlockByNumber", [hex(h), False]) for h in chunk])
            for height, value in zip(chunk, values):
                check(value is not None and number(value["number"]) == height, "canonical receipt header unavailable")
                headers[height] = value["hash"]
        check(headers[head_key[0]] == head_key[1], "head reorganized during settlement verification")
        if finalized:
            check(headers[number(finalized["number"])] == finalized["hash"], "finalized header is not canonical")
        with self.db:
            for attempt, outcome in outcomes.items():
                if headers.get(outcome["block"]) != outcome["block_hash"]:
                    self.ledger.reopen(attempt, commit=False)
                    self.db.execute("UPDATE intents SET outcome=NULL,finalized=0 WHERE attempt=?", (attempt,))
                elif finalized and outcome["block"] <= number(finalized["number"]):
                    outcome["finalized"] = True
                    self.db.execute("UPDATE intents SET finalized=1,outcome=? WHERE attempt=?", (json.dumps(outcome), attempt))
        self.checked_settlement_head = head_key

    def reconcile(self):
        self.verify_settlements()
        rows = self.db.execute("SELECT a.id,a.tx_hash,i.payload FROM attempts a LEFT JOIN intents i ON a.id=i.attempt WHERE a.kind='transaction' AND a.state='submitted'").fetchall()
        rows.sort(key=lambda row: json.loads(row[2])["plan_nonce"])
        for attempt, tx_hash, payload in rows:
            intent = json.loads(payload)
            cancellation = intent.get("cancellation")
            cancellations = intent.get("cancellation_history", []) + ([cancellation] if cancellation else [])
            hashes = list(dict.fromkeys([tx_hash] + [c["hash"] for c in cancellations]))
            receipts = self.rpc.batch([("eth_getTransactionReceipt", [h]) for h in hashes])
            if not any(receipts):
                self.recover_submission(attempt, intent, tx_hash)
                continue
            # One sealed successor makes preconfirmations insufficient for accounting.
            head = self.rpc.header()
            eligible = []
            awaiting_visibility = False
            for candidate in receipts:
                if not candidate: continue
                check(candidate["transactionHash"].lower() in hashes, "receipt transaction mismatch")
                if candidate.get("blockNumber") is None or candidate.get("blockHash") is None:
                    awaiting_visibility = True
                    continue
                block = number(candidate["blockNumber"])
                if number(head["number"]) < block:
                    awaiting_visibility = True
                    continue
                canonical = self.rpc.request("eth_getBlockByNumber", [hex(block), False])
                if canonical is None:
                    awaiting_visibility = True
                    continue
                check(number(canonical["number"]) == block, "receipt block height mismatch")
                if canonical["hash"] == candidate["blockHash"]:
                    if number(head["number"]) > block: eligible.append(candidate)
                    else: awaiting_visibility = True
            if not eligible:
                # A provider's cached orphan receipt must not suppress recovery
                # forever. Missing block visibility is not evidence of an orphan.
                if not awaiting_visibility: self.recover_submission(attempt, intent, tx_hash)
                continue
            check(len(eligible) == 1, "conflicting canonical receipts for the same nonce")
            receipt = eligible[0]
            cancelled = receipt["transactionHash"].lower() != tx_hash
            block = number(receipt["blockNumber"])
            check(receipt["transactionHash"].lower() in hashes, "receipt transaction mismatch")
            check(intent["contract"] == intent["transaction"]["to"].lower(), "stored intent contract mismatch")
            check(receipt.get("from", "").lower() == self.owner and receipt.get("to", "").lower() == (self.owner if cancelled else intent["contract"]),
                  "receipt sender or target mismatch")
            check("l1Fee" in receipt, "receipt lacks L1 fee accounting")
            gas_used = number(receipt["gasUsed"])
            operator = (number(receipt["operatorFee"]) if "operatorFee" in receipt else
                        decode(["uint256"], self.rpc.call(ORACLE, data("getOperatorFee(uint256)", ["uint256"], [gas_used]), block))[0])
            fee = gas_used*number(receipt["effectiveGasPrice"])+number(receipt["l1Fee"])+operator
            success = number(receipt["status"]) == 1 and not cancelled
            gross = 0
            if success:
                events = [log for log in receipt.get("logs", []) if log["address"].lower() == intent["contract"]
                          and log.get("topics", [None])[0] == CYCLE_EVENT]
                check(len(events) == 1, "missing or duplicated execution profit event")
                event = events[0]
                check(number(event["topics"][1]) == intent["plan_nonce"]
                      and "0x"+event["topics"][2][-40:] == intent["settlement"], "execution event binding mismatch")
                input_amount, gross = decode(["uint256", "uint256"], unhex(event["data"]))
                check(input_amount == intent["input"] and gross >= intent["minimum_profit"], "execution outcome mismatch")
            outcome = {"success": success, "gross_profit_raw": gross, "settlement": intent["settlement"],
                       "funding_token": intent.get('funding_token'),
                       "actual_network_fee_wei": fee, "block": block, "block_hash": receipt["blockHash"],
                       "cancelled": cancelled, "included_tx_hash": receipt["transactionHash"], "finalized": False,
                       "execution_kind": intent.get("execution_kind", "mixed-v1"),
                       "amount_semantics": intent.get("amount_semantics", "exact_settlement_input")}
            if intent["settlement"] == WETH: outcome["net_profit_raw"] = gross-fee
            else: outcome["net_after_reserved_fee_raw"] = gross-intent["fee_in_settlement"]
            check(self.rpc.header(block)["hash"] == receipt["blockHash"], "receipt reorganized during accounting")
            with self.db:
                self.db.execute("UPDATE intents SET outcome=?,finalized=0 WHERE attempt=?", (json.dumps(outcome), attempt))
                self.ledger.settle(attempt, success, fee, True, commit=False)
            self.checked_settlement_head = None
            self.context = None
            if not self.context_thread: self.refresh_context()

    def recover_submission(self, attempt, intent, tx_hash):
        head = self.rpc.header()
        nonce = number(self.rpc.request("eth_getTransactionCount", [self.owner, "latest"]))
        if nonce > intent["plan_nonce"]:
            # The receipt may be propagating. Retain the reservation and expose
            # the conflict instead of submitting over an unknown account action.
            with self.db:
                intent["recovery_status"] = "nonce consumed; awaiting matching canonical receipt or account reconciliation"
                self.db.execute("UPDATE intents SET payload=? WHERE attempt=?", (json.dumps(intent), attempt))
            return
        cancellation = intent.get("cancellation")
        current_hash = cancellation["hash"] if cancellation else tx_hash
        known = self.rpc.request("eth_getTransactionByHash", [current_hash])
        height = number(head["number"])
        if known and known.get("blockNumber") is not None:
            if number(known["blockNumber"]) > height:
                return  # Preconfirmation can lead both the receipt and sealed head.
            canonical = self.rpc.request("eth_getBlockByNumber", [known["blockNumber"], False])
            if canonical is None:
                return  # Retain the intent until inclusion can be checked.
            check(number(canonical["number"]) == number(known["blockNumber"]), "transaction block height mismatch")
            if canonical["hash"] == known.get("blockHash"):
                return  # Inclusion is known; await its receipt, not a replacement.
            known = None
        reprice = cancellation and (intent.get("reprice_requested") or
                  (height >= cancellation.get("created_block", height)+3 and not (known and known.get("blockNumber"))))
        # A new inclusion is after the sealed head; equality is already too late.
        if (not cancellation and height >= intent["maximum_execution_block"]) or reprice:
            def hold(reason):
                intent["recovery_status"] = reason
                with self.db:
                    self.db.execute("UPDATE intents SET payload=? WHERE attempt=?", (json.dumps(intent), attempt))
            history = intent.setdefault("cancellation_history", [])
            if len(history) >= 5:
                hold("cancellation repricing limit reached; account reconciliation required")
                return
            original = (TypedTransaction.from_bytes(HexBytes(cancellation["raw"])).as_dict()
                        if cancellation else intent["transaction"])
            priority = max(self.config["policy"]["min_priority_fee_wei"], (original["maxPriorityFeePerGas"]*9+7)//8)
            max_fee = max((original["maxFeePerGas"]*9+7)//8, 2*number(head["baseFeePerGas"])+priority)
            # A replacement may need a higher unit price than the trade.
            # Its smaller gas limit must fit the ORIGINAL fee reservation.
            fee = self.fees.maximum_fee("0x", 21000, max_fee)
            if fee > intent["reserved_fee_wei"]:
                hold("cancellation exceeds existing fee reservation; account reconciliation required")
                return
            tx = {"chainId": self.config["chain_id"], "type": 2, "nonce": intent["plan_nonce"],
                  "to": to_checksum_address(self.owner), "value": 0, "data": "0x", "gas": 21000,
                  "maxFeePerGas": max_fee, "maxPriorityFeePerGas": priority}
            raw = bytes(self.signer.sign_transaction(tx).raw_transaction)
            check(Account.recover_transaction(raw).lower() == self.owner and
                  TypedTransaction.from_bytes(HexBytes(raw)).hash() == TypedTransaction.from_dict(tx).hash(),
                  "cancellation signer mismatch")
            if cancellation: history.append(cancellation)
            cancellation = {"hash": "0x"+keccak(raw).hex(), "raw": "0x"+raw.hex(), "created_block": height}
            intent["cancellation"] = cancellation
            intent["reprice_requested"] = False
            intent["recovery_status"] = "cancellation prepared within original fee reservation"
            with self.db:
                self.db.execute("UPDATE intents SET payload=? WHERE attempt=?", (json.dumps(intent), attempt))
            known = None
        if known is None:
            raw = cancellation["raw"] if cancellation else intent["raw_transaction"]
            expected = cancellation["hash"] if cancellation else tx_hash
            try:
                returned = self.rpc.request("eth_sendRawTransaction", [raw])
            except Exception as exc:
                if cancellation and ("underpriced" in str(exc).lower() or "fee cap less than block base fee" in str(exc).lower()):
                    intent["reprice_requested"] = True
                    intent["recovery_status"] = "cancellation underpriced; bounded repricing scheduled"
                    with self.db:
                        self.db.execute("UPDATE intents SET payload=? WHERE attempt=?", (json.dumps(intent), attempt))
                raise
            check(returned.lower() == expected, "recovery submission hash mismatch")

    def close(self):
        self.context_stop.set()
        with self._context_condition(): self.context_changed.notify_all()
        if self.context_thread: self.context_thread.join()
        self.ledger.close()
        fcntl.flock(self.lock, fcntl.LOCK_UN)
        self.lock.close()
