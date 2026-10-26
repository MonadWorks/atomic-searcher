from __future__ import annotations

from .protocols import kind_id

import json
import time
from collections import Counter, deque, OrderedDict
from dataclasses import asdict

from eth_abi import decode

from .core import ROOT, RPC, Reject, Incomplete, atomic_json, check, data, number, safe_error, unhex
from .context import simulation_context
from .costs import cost_budget, quote_profit_floor
from .compatibility import blocked_tokens, eligible_pools
from .events import StateBook, V3_SWAP, SLIP_SWAP, V2_SYNC, POOL_ACTIVITY_TOPICS
from .pools import discover, discover_pairs, validate_pools, snapshot, Pool
from .discovery_expansion import factory_registry_id, retryable_identity_errors, run_expansion
from .routes import RouteIndex, marginal_profitable, optimize, gross_upper_bound, route_capacity, sizing_fingerprint

PROBE = "0x000000000000000000000000000000000000c1c2"
HOP_TYPE = "(address,bool,uint24,uint160)"
PLAN_TYPE = f"({HOP_TYPE}[],uint256,uint256,uint256,uint64,uint64,bytes32,uint64,uint64)"


def make_hops(route, states):
    return [(key, token == states[key].token0, states[key].fee,
             0 if states[key].kind == "v2" else (4295128740 if token == states[key].token0 else 1461446703485210103287273052203988822378723970341))
            for key, token in zip(route.pools, route.tokens)]


def probe_cycle(rpc, route, states, amount, floor, tag="pending", anchor=None, block_context=None):
    runtime = json.loads((ROOT/"out/CycleProbe.sol/CycleProbe.json").read_text())["deployedBytecode"]["object"]
    calldata = data(f"probe({HOP_TYPE}[],uint8[],uint256,uint256)",
                    [HOP_TYPE+"[]", "uint8[]", "uint256", "uint256"],
                    [make_hops(route, states), [kind_id(states[k].kind) for k in route.pools], amount, floor])
    started = time.monotonic_ns()
    params = [{"to": PROBE, "data": calldata, "gas": hex(15_000_000)},
              hex(tag) if isinstance(tag, int) else tag, {PROBE: {"code": runtime}}]
    if tag == 'pending' and anchor is not None: params.append(block_context or simulation_context(anchor))
    raw = rpc.request('eth_call', params)
    profit, gas, block, timestamp = decode(["uint256"]*4, unhex(raw))
    if tag=='pending' and anchor is not None:
        expected=block_context or simulation_context(anchor)
        check(block==number(expected['number']) and timestamp==number(expected['time']),
              'whole cycle clock mismatch')
    executable = data(f"execute({PLAN_TYPE})", [PLAN_TYPE], [(make_hops(route, states), amount, floor,
                      (1 << 256)-1, timestamp+2, max(0, block-1), b"\xff"*32, block, 1)])
    # Intrinsic calldata upper bound plus a cold-access allowance: temporary
    # construction warmed some factory/token storage in this eth_call.
    first_transaction_gas = gas+21_000+16*len(unhex(executable))+30_000
    return {"gross_surplus": profit, "execution_gas": gas, "first_transaction_gas_budget":first_transaction_gas,
            "includes_pool_registration":True, "evm_context_block": block,
            "evm_context_timestamp": timestamp, "request_ms": (time.monotonic_ns()-started)/1e6,
            "state_tag": tag, "executed_on_chain": False,
            "scope": "whole-cycle eth_call; pending EVM block context can lag preconfirmed state; no placement proof"}


def expansion_plan(config, previous, known, activity):
    """Revisit old tokens when the authenticated factory registry expands."""
    registry = factory_registry_id(config)
    expanded = set(previous.get('expanded_tokens',[])) if previous.get('factory_registry_id') == registry else set()
    counts = Counter()
    for pool in known.values():
        for token in (pool.token0,pool.token1):
            if token not in expanded: counts[token] += 1+activity.get(pool.address,0)
    tokens = sorted(set(config['seed_tokens']) | {t for t,_ in counts.most_common(config['policy']['max_new_tokens_per_refresh'])})
    return tokens, expanded, registry


def pair_expansion_plan(config, previous, known, registry):
    """Persist a bounded FIFO for quiet pools whose tokens span discovery batches."""
    edges = {tuple(sorted((p.token0, p.token1))) for p in known.values()}
    same_registry = previous.get('factory_registry_id') == registry
    completed = {tuple(p) for p in previous.get('expanded_factory_pairs', [])} if same_registry else set()
    completed &= edges
    pending = [tuple(p) for p in previous.get('pending_factory_pairs', [])] if same_registry else []
    pending = list(dict.fromkeys(p for p in pending if p in edges and p not in completed))
    pending.extend(sorted(edges - completed - set(pending)))
    limit = max(1, config['policy']['max_new_tokens_per_refresh'])
    return pending[:limit], completed, pending[limit:]


def prepare(rpc, config, recent_blocks=6, previous=None, *, history_rpc=None):
    """Refresh discovery without forgetting previously authenticated pools.

    Recent sealed blocks have priority. Older missed logs stay in a durable
    queue and one bounded page is retried per refresh.
    A failed read never publishes an empty replacement universe.
    """
    from .feed import validate_frame
    from .statefeed import StateFeed
    start = time.monotonic_ns()
    phase_started = start
    phase_ms = {}
    history_rpc = history_rpc if history_rpc is not None else rpc
    measurement_sources = [('recent', rpc)]
    if history_rpc is not rpc: measurement_sources.append(('history', history_rpc))
    measurement_boundaries = {}
    for role, connection in measurement_sources:
        measurements = getattr(connection, 'measurements', ())
        measurement_boundaries[role] = measurements[-1] if measurements else None
    def phase(name):
        nonlocal phase_started
        now = time.monotonic_ns()
        phase_ms[name] = (now-phase_started)/1e6
        phase_started = now
    previous = previous or {}
    rpc.verify_chain()
    header = rpc.header()
    height = number(header["number"])
    policy = config["policy"]
    registry = factory_registry_id(config)
    allowed = {f["address"]:f["kind"] for f in config["factories"]}
    known = {p["address"]:Pool.from_json(p) for p in previous.get("known_pools", previous.get("pools", []))
             if allowed.get(p["factory"]) == p["kind"]}
    # Fresh logs may authenticate an isolated pool without making a usable
    # settlement cycle. They must not suppress the original full bootstrap.
    previous_usable = (previous.get('factory_registry_id') == registry and
                       any(p.get('valid', True) and p.get('address') in known
                           for p in previous.get('pools', []))
                       if policy.get('cycle_pages') else bool(known))
    last_seen = dict(previous.get("last_seen", {}))
    from .discovery_history import (plan_history, subtract_range, merge_ranges,
                                    pool_hint_queue, select_pool_hints, finish_pool_hints)
    fresh_range, gap, backlog, historical_range = plan_history(
        previous, height, recent_blocks, policy.get('discovery_catchup_blocks', 32))
    lower = fresh_range[0]
    activity = Counter()
    fresh_observed = {}
    expected_parent = None
    phase('initial_headers_and_queue')
    if policy.get('discovery_mode') == 'logs':
        from .universe import swap_activity
        chunk_size = getattr(rpc, 'discovery_log_chunk_size', 10)
        check(type(chunk_size) is int and 1 <= chunk_size <= 32, 'invalid discovery log chunk size')
        activity, observed = swap_activity(rpc, lower, height, header['hash'], chunk_size)
        fresh_observed = observed
        for key, block in observed.items():
            last_seen[key] = max(block, last_seen.get(key, 0))
        expected_parent = header['hash']
    for offset in ([] if policy.get('discovery_mode') == 'logs' else range(lower, height+1, 4)):
        heights = list(range(offset, min(height+1, offset+4)))
        calls = [call for b in heights for call in (("eth_getBlockByNumber", [hex(b), False]), ("eth_getBlockReceipts", [hex(b)]))]
        values = rpc.batch(calls)
        for i, b in enumerate(heights):
            h, receipts = values[i*2:i*2+2]
            validate_frame(h, h, receipts)
            if expected_parent is not None: check(h["parentHash"] == expected_parent, "discovery range reorganized")
            expected_parent = h["hash"]
            for receipt in receipts:
                for log in receipt.get("logs", []):
                    if log.get("topics") and log["topics"][0] in POOL_ACTIVITY_TOPICS:
                        key = log["address"].lower(); activity[key] += 1
                        last_seen[key] = max(b, last_seen.get(key, 0))
                        fresh_observed[key] = max(b, fresh_observed.get(key, 0))
    phase('recent_activity')
    recovered = None
    history_error = None
    historical_activity = Counter()
    history_observed = {}
    if historical_range is not None:
        from .universe import swap_activity
        try:
            first, last = historical_range
            if history_rpc is not rpc: history_rpc.verify_chain()
            old_header = history_rpc.header(last)
            check(number(old_header['number']) == last, 'discovery history height mismatch')
            historical_activity, observed = swap_activity(history_rpc, first, last, old_header['hash'])
            check(history_rpc.header(last)['hash'] == old_header['hash'], 'discovery history anchor reorganized')
            history_observed = observed
            for key, block in history_observed.items():
                last_seen[key] = max(block, last_seen.get(key, 0))
            recovered = historical_range
        except Exception as exc:
            # Keep live discovery useful during an archive failure/rate limit.
            # No checkpoint is changed until the final live snapshot validates.
            historical_activity = Counter()
            history_observed = {}
            history_error = safe_error(exc)
    phase('historical_activity')
    pending_discovery, overflow, deferred_ranges = pool_hint_queue(previous, known, fresh_observed, history_observed)
    if overflow[0]:
        backlog = merge_ranges([*backlog, fresh_range])
    if recovered is not None:
        if overflow[1]:
            recovered = None
        else:
            backlog = subtract_range(backlog, recovered)
    backlog = merge_ranges([*backlog, *deferred_ranges])
    unknown = select_pool_hints(pending_discovery, activity, height, policy['max_pools'],
                                previous.get('pending_discovery_pools', {}))
    recent, rejected = validate_pools(rpc, config, unknown, height) if unknown else ([], [])
    finish_pool_hints(pending_discovery, unknown, [p.address for p in recent], rejected, height)
    known.update((p.address, p) for p in recent)
    phase('recent_pool_authentication')
    errors = []
    expansion_previous = previous
    bootstrap = not previous_usable
    bootstrap_tokens = []
    if bootstrap:
        # With no usable previous universe, a partial page cannot reach the
        # final snapshot/checkpoint. Preserve the original complete selected
        # batch here: a fresh non-seed token may supply the last cycle edge.
        bootstrap_tokens, _, _ = expansion_plan(config, previous, known, activity)
        neighbors, errors = discover(rpc, config, height, bootstrap_tokens)
        known.update((p.address, p) for p in neighbors)
        if not retryable_identity_errors(errors):
            expanded = (set(previous.get('expanded_tokens', []))
                        if previous.get('factory_registry_id') == registry else set())
            expansion_previous = dict(previous, factory_registry_id=registry,
                                      expanded_tokens=sorted(expanded | set(bootstrap_tokens)))
    phase('initial_bootstrap')
    pairs, completed_pairs, pending_pairs = pair_expansion_plan(config, previous, known, registry)
    if pairs:
        neighbors, pair_errors = discover_pairs(rpc, config, height, pairs)
        known.update((p.address, p) for p in neighbors)
        errors.extend(pair_errors)
        if pair_errors:
            pending_pairs.extend(pairs)
        else:
            completed_pairs.update(pairs)
    phase('known_pair_expansion')
    # Known edges seek actual cross-venue peers first. Speculative combinations
    # then receive one bounded factory-read chunk and keep their own checkpoint.
    expansion = run_expansion(rpc, config, expansion_previous, known, height,
                              activity, validator=validate_pools)
    known.update((p.address, p) for p in expansion['neighbors'])
    errors.extend(expansion['errors'])
    phase('speculative_token_expansion')
    check(known, "no authenticated pools; retain previous universe")
    if policy.get('cycle_pages'):
        from .universe import CyclePages
        pages = CyclePages(list(known.values()), [t['address'] for t in config['settlement_tokens']],
                           policy['max_hops'], last_seen,
                           excluded_tokens=config.get('unsupported_transfer_tokens', {}))
        pools = pages.take(policy['max_pools'])
        check(pools, 'no complete settlement cycles in authenticated universe')
    else:
        pools = sorted(eligible_pools(config, known.values()), key=lambda p: (-last_seen.get(p.address, 0), -activity[p.address], p.address))[:policy["max_pools"]]
    phase('cycle_selection')
    frame = StateFeed(rpc, config, {"pools":[p.json() for p in pools]}).poll(height)
    states = frame.snapshot_pools
    check(any(p["valid"] for p in states), "atomic refresh contains no usable pool state; retain previous universe")
    phase('pool_state_snapshot')
    check(rpc.header(height)["hash"] == header["hash"] == expected_parent, "discovery anchor reorganized")
    phase('canonical_anchor')
    reads, source_timing = [], {}
    complete = True
    for role, connection in measurement_sources:
        measurements = getattr(connection, 'measurements', ())
        entries = list(measurements)
        prior_measurement = measurement_boundaries[role]
        boundary = next((i for i, value in enumerate(entries) if value is prior_measurement), None)
        capacity = getattr(measurements, 'maxlen', None)
        source_complete = (boundary is not None if prior_measurement is not None else
                           capacity is None or len(entries) < capacity)
        if boundary is not None: entries = entries[boundary+1:]
        reads.extend(entries)
        needed = role == 'recent' or historical_range is not None
        complete = complete and (not needed or bool(entries) and source_complete)
        detail = {'operations':len(entries), 'complete':bool(entries) and source_complete,
                  'available':bool(entries), 'needed':needed}
        for field in ('attempts', 'elapsed_ms', 'public_limiter_wait_ms', 'minimum_interval_wait_ms',
                      'network_total_ms', 'retry_wait_ms'):
            values = [value.get(field) for value in entries]
            detail['http_attempts' if field == 'attempts' else field] = (sum(values) if entries and
                all(isinstance(v, (int, float)) and not isinstance(v, bool) and
                    0 <= v < float('inf') for v in values) else None)
        source_timing[role] = detail
    available = bool(reads)
    read_timing = {'available':available, 'complete':available and complete,
                   'operations':len(reads), 'sources':source_timing}
    attempts = [value.get('attempts') for value in reads]
    read_timing['http_attempts'] = (sum(attempts) if available and
        all(isinstance(value, int) and not isinstance(value, bool) and value >= 0
            for value in attempts) else None)
    for field in ('elapsed_ms', 'public_limiter_wait_ms', 'minimum_interval_wait_ms',
                  'network_total_ms', 'retry_wait_ms'):
        values = [value.get(field) for value in reads]
        read_timing[field] = (sum(values) if available and
            all(isinstance(v, (int, float)) and not isinstance(v, bool) and
                0 <= v < float('inf') for v in values) else None)
    private_attempts = [attempt for read in reads for attempt in read.get('attempt_timings', [])
                        if attempt.get('discovery_rpc_role') == 'private_recent']
    if private_attempts:
        read_timing['discovery_budget_wait_ms'] = sum(a.get('discovery_budget_wait_ms', 0) for a in private_attempts)
        read_timing['private_units_debited'] = sum(a.get('discovery_units_debited', 0) for a in private_attempts)
    if reads and 'provider_budget' in reads[-1]:
        read_timing['provider_budget'] = reads[-1]['provider_budget']
    known.update((p["address"], Pool.from_json(p)) for p in states)
    pending_discovery = {key: value for key, value in pending_discovery.items() if key not in known}
    return {"header": header, "pools": states, "known_pools": [p.json() for p in known.values()],
            "last_seen": last_seen, "cycle_groups": pages.groups if policy.get("cycle_pages") else None, "discovery_cursor": height, "offline_history_gap": gap,
            "discovery_backlog": backlog, "discovery_fresh_range": fresh_range,
            "discovery_recovered_range": recovered, "discovery_history_error": history_error,
            "pending_discovery_pools": pending_discovery,
            "expanded_tokens": expansion['expanded_tokens'], "factory_registry_id":registry,
            "pending_expansion_tokens": expansion['pending_expansion_tokens'],
            "token_expansion": expansion['token_expansion'],
            "discovery_expansion_error": expansion['error'],
            "discovery_phase_ms": phase_ms,
            "discovery_read_timing": read_timing,
            "discovery_expansion_progress": {"factory_getters_attempted": expansion['getters_attempted'],
                                             "factory_getters_completed": expansion['getters_completed'],
                                             "seed_bootstrap": bootstrap,
                                             "bootstrap_token_count": len(bootstrap_tokens)},
            "expanded_factory_pairs": sorted(completed_pairs), "pending_factory_pairs": pending_pairs,
            "errors": errors, "discovery_rejections": rejected,
            "observed_at": time.time(), "load_ms": (time.monotonic_ns()-start)/1e6,
            "scope": "pool universe selected from swap and liquidity events already in sealed blocks and authenticated factories; no future winner labels"}


def evaluate(index, config, header, changed=None, *, sizing_deadline_ns=None, initial_amounts=None, required_amounts=None):
    start = time.monotonic_ns()
    deadline = start+int(config['policy']['max_quote_ms']*.8*1_000_000)
    if sizing_deadline_ns is not None: deadline = min(deadline, sizing_deadline_ns)
    if not hasattr(index, 'sizing_progress'): index.sizing_progress = OrderedDict()
    # Keep unfinished routes across observations. Updating a busy pool must not
    # erase work deferred for a quieter pool. Quotes always use newest states.
    if not hasattr(index, "work_queue"):
        index.work_queue = deque(index.routes)
        index.queued_routes = set(index.routes)
    affected = index.affected(changed) if changed is not None else index.routes.values()
    for route in affected:
        if route.key not in index.queued_routes:
            index.work_queue.append(route.key)
            index.queued_routes.add(route.key)
    transfers = getattr(index, 'transfer_feedback', None)
    feedback = getattr(index, 'execution_feedback', None)
    if transfers is not None or feedback is not None:
        targets = getattr(index, 'current_page_target_keys', ())
        index.work_queue = deque(sorted(index.work_queue, key=lambda key:(
            key not in targets, max(transfers.priority(index.routes[key]) if transfers is not None else 0,
                                    feedback.priority(key) if feedback is not None else 0))))
    settlements = {t["address"]: t for t in config["settlement_tokens"]}
    candidates, rejects, depth = [], Counter(), {}
    cooldown_deferred = []
    route_errors = []
    def request_depth(route, cost, cached_upper):
        group = tuple(sorted(route.pools))
        # A comparable benefit/cost ratio across settlement tokens. This is a
        # scheduling hint from currently observed state, never a profit claim.
        score = min(10**9, max(0, cached_upper or 0)*10000//max(1, cost))
        item = depth.setdefault(group, {'pools':group, 'score':score, 'route_keys':[]})
        item['score'] = max(item['score'], score)
        if route.key not in item['route_keys']: item['route_keys'].append(route.key)
    index.last_evaluated_keys = []
    index.interrupted_keys = set()
    index.unstarted_keys = set()
    index.quote_resolved_keys = set()
    prewarm_quoted_amounts = {}
    quoted_routes = 0
    considered = 0
    search_time_limited = False
    while index.work_queue:
        if considered and time.monotonic_ns() >= deadline:
            search_time_limited = True
            break
        if considered and (time.monotonic_ns()-start)/1e6 > config["policy"]["max_quote_ms"]*.8:
            break
        key = index.work_queue.popleft()
        index.queued_routes.remove(key)
        route = index.routes[key]
        considered += 1
        index.last_evaluated_keys.append(key)
        try:
            transfers = getattr(index, 'transfer_feedback', None)
            if transfers is not None and transfers.deferred(route):
                index.sizing_progress.pop(key, None)
                rejects['sampled_transfer_backoff'] += 1
                cooldown_deferred.append(key)
                continue
            feedback = getattr(index, 'execution_feedback', None)
            if feedback is not None and feedback.blocked(key):
                index.sizing_progress.pop(key, None)
                rejects['execution_failure_cooldown'] += 1
                cooldown_deferred.append(key)
                continue
            if blocked_tokens(config, route.tokens):
                index.sizing_progress.pop(key, None)
                rejects['unsupported_token_transfer'] += 1
                continue
            if key in (required_amounts or {}):
                # This amount comes from the live account-scoped diagnostic
                # registry. It is scheduling input only: quote current state,
                # preserve capital/fee/net bounds, and still use full execute.
                amount = required_amounts[key]
                settlement = settlements[route.tokens[0]]
                check(type(amount) is int and 0 < amount <= int(settlement['max_input_raw']),
                      'prewarm amount outside settlement cap')
                try:
                    output, crossings = route.quote(index.states, amount)
                except Incomplete:
                    rejects['prewarm_incomplete_state'] += 1
                    request_depth(route, 1, None)
                    index.interrupted_keys.add(key)
                    cooldown_deferred.append(key)
                    continue
                quoted_routes += 1
                budget = cost_budget(config['policy'],route.tokens[0],number(header['baseFeePerGas']),
                                     len(route.pools),crossings)
                if time.monotonic_ns() >= deadline:
                    rejects['prewarm_quote_deadline'] += 1
                    index.interrupted_keys.add(key)
                    cooldown_deferred.append(key)
                    continue
                floor = quote_profit_floor(config,settlement,budget.settlement_cost_raw)
                net = output-amount-budget.settlement_cost_raw
                index.quote_resolved_keys.add(key)
                prewarm_quoted_amounts[key] = amount
                index.sizing_progress.pop(key,None)
                if net < floor:
                    rejects['below_configured_net_floor'] += 1
                else:
                    candidates.append(dict(route_key=key,tokens=route.tokens,pools=route.pools,
                        complete=True,amount_in=amount,amount_out=output,gross_surplus=output-amount,
                        net_after_modelled_cost=net,tick_crossings=crossings,quote_count=1,
                        sizing_time_limited=False,global_optimum_proven=False,
                        prewarm_exact_amount=True,cost_budget=asdict(budget),
                        minimum_gross_surplus=budget.settlement_cost_raw+floor,
                        transfer_revalidation_priority=transfers.priority(route) if transfers is not None else 0,
                        execution_revalidation_priority=feedback.priority(key) if feedback is not None else 0,
                        status='quoted_only',placement_proven=False))
                continue
            if not marginal_profitable(route, index.states):
                index.sizing_progress.pop(key, None)
                continue
            settlement = settlements[route.tokens[0]]
            try:
                cheapest = cost_budget(config["policy"], route.tokens[0], number(header["baseFeePerGas"]), len(route.pools), 0)
            except Reject:
                index.sizing_progress.pop(key, None)
                rejects["cost_cap"] += 1
                continue
            upper = gross_upper_bound(route, index.states)
            if upper is None:
                rejects["profit_bound_unknown"] += 1
                cached_upper = gross_upper_bound(route, index.states, cached_only=True)
                request_depth(route, cheapest.settlement_cost_raw+quote_profit_floor(config, settlement, cheapest.settlement_cost_raw), cached_upper)
                if cached_upper is not None and cached_upper < cheapest.settlement_cost_raw+quote_profit_floor(config, settlement, cheapest.settlement_cost_raw):
                    index.sizing_progress.pop(key, None)
                    # Only skip sizing amounts this snapshot can fully quote.
                    # The route remains unknown and queued for a deeper atomic read.
                    rejects["cached_range_below_budget_deeper_range_unknown"] += 1
                    continue
            if upper is not None and upper < cheapest.settlement_cost_raw+quote_profit_floor(config, settlement, cheapest.settlement_cost_raw):
                index.sizing_progress.pop(key, None)
                rejects["upper_bound_below_configured_budget_and_floor"] += 1
                continue
            def fee(crossings):
                try:
                    return cost_budget(config["policy"], route.tokens[0], number(header["baseFeePerGas"]), len(route.pools), crossings).settlement_cost_raw
                except Reject as exc:
                    raise Incomplete(str(exc)) from exc
            quoted_routes += 1
            maximum = int(settlement['max_input_raw'])
            capacity_limited = False
            if config['policy'].get('sizing_mode') == 'liquidity':
                maximum, capacity_limited = route_capacity(route, index.states, maximum)
                if maximum <= 0:
                    index.sizing_progress.pop(key, None)
                    rejects['unknown_executable_size'] += 1
                    request_depth(route, cheapest.settlement_cost_raw+quote_profit_floor(config, settlement, cheapest.settlement_cost_raw), upper)
                    continue
            fingerprint = (sizing_fingerprint(route, index.states, maximum),
                           number(header['baseFeePerGas']), tuple(sorted(config['policy'].items())))
            previous = index.sizing_progress.get(key)
            seeds = tuple((initial_amounts or {}).get(key, ()))
            if previous is None or previous['fingerprint'] != fingerprint:
                if previous is not None and previous.get('seed'): seeds += (previous['seed'],)
                previous = {'fingerprint':fingerprint, 'work':{}, 'seed':None}
                if key in index.sizing_progress or len(index.sizing_progress) < 32:
                    index.sizing_progress[key] = previous
            retained = key in index.sizing_progress
            if retained: index.sizing_progress.move_to_end(key)
            # A single difficult route gets one quarter of a normal page's
            # budget, leaving room for alternatives from this same fresh state.
            slice_end = min(deadline, time.monotonic_ns()+max(1, int(config['policy']['max_quote_ms']*.2*1_000_000)))
            result = optimize(route, index.states, maximum, fee, deadline_ns=slice_end,
                              initial_amounts=seeds, continuation=previous['work'])
            if result.get('complete'): previous['seed'] = result['amount_in']
            if result.get('sizing_time_limited'):
                search_time_limited = True
                rejects['sizing_budget_exhausted'] += 1
                if not retained: rejects['sizing_waiting_for_slot'] += 1
                index.interrupted_keys.add(key)
                if not previous['work'].get('quotes'): index.unstarted_keys.add(key)
                cooldown_deferred.append(key)
            else:
                index.sizing_progress.pop(key, None)
            result['liquidity_range_limited'] = capacity_limited
            if upper is None or result.get('state_coverage_limited'):
                request_depth(route, cheapest.settlement_cost_raw+quote_profit_floor(config, settlement, cheapest.settlement_cost_raw),
                              max(result.get('gross_surplus',0), gross_upper_bound(route,index.states,cached_only=True) or 0))
            if not result["complete"] and result.get('sizing_time_limited'):
                continue
            if not result["complete"]:
                rejects["incomplete_state_or_cost_cap"] += 1
            elif result["net_after_modelled_cost"] < quote_profit_floor(config, settlement, fee(result["tick_crossings"])):
                if not result.get('sizing_time_limited'): rejects["below_configured_net_floor"] += 1
            else:
                budget = cost_budget(config["policy"], route.tokens[0], number(header["baseFeePerGas"]), len(route.pools), result["tick_crossings"])
                candidates.append({"route_key": route.key, "tokens": route.tokens, "pools": route.pools,
                                   **result, "cost_budget": asdict(budget), "minimum_gross_surplus": budget.settlement_cost_raw+quote_profit_floor(config, settlement, budget.settlement_cost_raw),
                                   "transfer_revalidation_priority": transfers.priority(route) if transfers is not None else 0,
                                   "execution_revalidation_priority": feedback.priority(key) if feedback is not None else 0,
                                   "status": "quoted_only", "placement_proven": False})
                # A fully quoted profitable size can proceed immediately even
                # while further size optimization remains possible.
                index.interrupted_keys.discard(key)
                index.sizing_progress.pop(key, None)
        except Reject as exc:
            # Bad/incomplete state in one route must not discard other routes'
            # valid decisions or prevent the page scheduler from advancing.
            rejects['route_quote_rejected'] += 1
            if len(route_errors) < 8:
                route_errors.append({'route_key':route.key, 'reason':safe_error(exc)})
    candidates.sort(key=lambda c: (c["tokens"][0], max(c.get('transfer_revalidation_priority',0),
        c.get('execution_revalidation_priority',0)), -c["net_after_modelled_cost"]))
    # A transient submission failure or occupied nonce must not erase an
    # otherwise persistent opportunity. Re-price it on the next observation.
    for candidate in candidates:
        key = candidate["route_key"]
        if key not in index.queued_routes:
            index.work_queue.append(key)
            index.queued_routes.add(key)
    for key in cooldown_deferred:
        if key not in index.queued_routes:
            index.work_queue.append(key)
            index.queued_routes.add(key)
    # Preserve exploration requests as well as the strongest observed signals.
    first = list(depth)[:16]
    ranked = sorted((k for k in depth if k not in first), key=lambda k:-depth[k]['score'])
    requests = [depth[k] for k in (first+ranked)[:128]]
    return {"block": number(header["number"]), "block_hash": header["hash"], "observed_at": time.time(),
            "computation_ms": (time.monotonic_ns()-start)/1e6, "routes_considered": considered,
            "routes_deferred": len(index.work_queue),
            "routes_sized": quoted_routes, "rejected": dict(rejects), "candidates": candidates,
            "search_time_limited": search_time_limited,
            "route_errors": route_errors,
            "quote_resolved_route_keys":sorted(index.quote_resolved_keys),
            "prewarm_quoted_amounts":prewarm_quoted_amounts,
            "deep_read_routes":[r['pools'] for r in requests], "deep_read_requests":requests,
            "scope": "conditional gross surplus less conservative cost model; not realized income or win-rate estimate"}


def advance(rpc, book, header, config):
    previous = number(book.header["number"])
    check(rpc.header(previous)["hash"] == book.header["hash"], "prior snapshot reorganized")
    height = number(header["number"])
    logs = []
    addresses = list(book.pools)
    for offset in range(0, len(addresses), 80):
        part = rpc.request("eth_getLogs", [{"address": addresses[offset:offset+80], "fromBlock":hex(previous+1), "toBlock":hex(height)}])
        check(isinstance(part,list), "missing log range")
        logs.extend(part)
    check(rpc.header(height)["hash"] == header["hash"], "log range reorganized")
    changed = book.apply_observed_range(logs, header)
    dynamic = [p for p in book.pools.values() if book.invalid_reasons.get(p.address, "").startswith("dynamic fee")]
    values = rpc.views([(p.address, data("fee()")) for p in dynamic], height)
    for p, value in zip(dynamic, values):
        if value is not None and len(value) == 32:
            fee = decode(["uint24"], value)[0]
            if fee < 1_000_000:
                if fee != p.fee:
                    changed.add(p.address)
                p.fee, p.valid = fee, True
                book.invalid_reasons.pop(p.address, None)
    invalid = [p for p in book.pools.values() if not p.valid]
    if invalid:
        refreshed, _ = snapshot(rpc, invalid, header, config["policy"]["bitmap_radius"])
        for p in refreshed:
            if p.valid:
                book.invalid_reasons.pop(p.address, None)
                changed.add(p.address)
    check(rpc.header(height)["hash"] == header["hash"], "block changed during event application")
    return changed


def shadow(rpc, config, snapshot_path, seconds, output_dir):
    """Continuous automatic discovery/evaluation/probing; transport cannot send.

    This free-HTTP path evaluates complete observed blocks. Intra-Flashblock
    first-seen frames are recorded independently, never synthesized from them.
    """
    rpc.verify_chain()
    saved = json.loads(snapshot_path.read_text())
    book = StateBook([Pool.from_json(p) for p in saved["pools"]], saved["header"])
    index = RouteIndex(list(book.pools.values()), [t["address"] for t in config["settlement_tokens"]], config["policy"]["max_hops"])
    finish = time.monotonic()+seconds
    checkpoint = output_dir/"checkpoint.json"
    cycles = 0
    while time.monotonic() < finish:
        try:
            h = rpc.header()
            needs_reload = any(not p.valid for p in book.pools.values())
            if h["hash"] == book.header["hash"] and not needs_reload:
                time.sleep(config["policy"]["poll_interval_seconds"])
                continue
            if needs_reload or number(h["number"]) <= number(book.header["number"]) or number(h["number"])-number(book.header["number"]) > 128:
                # Do not replay skipped txs one at a time and invent chances.
                pools, errors = snapshot(rpc, list(book.pools.values()), h, config["policy"]["bitmap_radius"])
                book = StateBook(pools, h)
                index = RouteIndex(pools, [t["address"] for t in config["settlement_tokens"]], config["policy"]["max_hops"])
                changed = None
            else:
                changed = advance(rpc, book, h, config)
            report = evaluate(index, config, h, changed)
            probed = Counter()
            for candidate in report["candidates"]:
                settlement = candidate["tokens"][0]
                if probed[settlement] >= 2:
                    continue
                probed[settlement] += 1
                try:
                    candidate["probe"] = probe_cycle(rpc, index.routes[candidate["route_key"]], index.states,
                                                     candidate["amount_in"], candidate["minimum_gross_surplus"])
                    candidate["status"] = "pending_call_passed_placement_unproven"
                except Exception as exc:
                    candidate["status"], candidate["probe_error"] = "pending_call_rejected", safe_error(exc)
            atomic_json(output_dir/f"scan-{report['block']}.json", report)
            atomic_json(checkpoint, {"header": book.header, "pools": [p.json() for p in book.pools.values()], "observed_at": time.time()})
            atomic_json(output_dir/"health.json", {"state": "observing", "last_completed_at": time.time(), "mode": "shadow", "funded_execution_enabled": False, "last_block": number(h["number"]), "processing_ms": report["computation_ms"]})
            cycles += 1
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as exc:
            book.invalidate_all("runtime error; pinned snapshot reload required")
            atomic_json(output_dir/"health.json", {"state": "recovering", "error": safe_error(exc), "at": time.time(), "funded_execution_enabled": False})
            time.sleep(min(5, config["policy"]["poll_interval_seconds"]))
        time.sleep(min(config["policy"]["poll_interval_seconds"], max(0, finish-time.monotonic())))
    result = {"state": "stopped", "completed_cycles": cycles, "stopped_at": time.time(), "funded_execution_enabled": False}
    atomic_json(output_dir/"health.json", result)
    return result
