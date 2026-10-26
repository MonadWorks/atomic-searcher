"""Reprice a discovered route from one new atomic read before its deadline."""
import copy
import math
import re
import time
from dataclasses import asdict
from contextlib import contextmanager

from .core import check, number, Incomplete, Reject, RPCUnavailable
from .costs import cost_budget, quote_profit_floor
from .engine import evaluate
from .pools import Pool
from .routes import RouteIndex
from .statefeed import StateFeed, observation_start_ns


_LOCAL_FAILURE_REASONS = frozenset((
    'RPC cancelled', 'RPC total request deadline exceeded',
    'RPC throttled; Retry-After exceeds bounded retry window',
    'snapshot clock unavailable', 'snapshot anchor unavailable', 'snapshot pending clock unavailable',
    'snapshot anchor changed during read', 'snapshot execution clock mismatch',
    'incomplete atomic snapshot', 'snapshot protocol layout mismatch', 'snapshot pool order mismatch',
    'invalid execution anchor hash', 'pending execution clock outside anchor window',
    'pending parent differs from anchor', 'refresh includes unsupported token transfer',
    'refreshed route no longer valid', 'refresh candidate route mismatch', 'invalid refresh amount',
    'truncated packed snapshot', 'unsupported packed snapshot version', 'packed snapshot pool limit',
    'invalid packed snapshot status', 'invalid packed V2 reserves or fee', 'invalid packed pool state',
    'invalid packed bitmap count', 'duplicate packed bitmap word', 'invalid packed tick count',
    'invalid packed tick indices', 'trailing packed snapshot data'))


def _read_transport_evidence(transport):
    # Retain useful timings without trusting arbitrary transport extensions,
    # provider error text, URLs, calldata, headers or signed payload fields.
    numeric = {'elapsed_ms', 'retry_wait_ms', 'attempts', 'minimum_interval_wait_ms',
               'public_limiter_wait_ms', 'network_headers_ms', 'network_body_ms', 'network_total_ms',
               'post_total_ms', 'response_headers_ms', 'response_bytes', 'http_status',
               'network_started_ns', 'network_finished_ns', 'batch_started_ns', 'batch_wall_ms', 'clock_attempt'}
    enums = {'method':{'batch'}, 'outcome':{'ok','error'}, 'role':{'clock_bootstrap','atomic_snapshot'}}
    def row(value, attempts=False):
        if not isinstance(value,dict): return {}
        result = {k:v for k,v in value.items() if k in numeric and
                  (v is None or type(v) is int and 0 <= v < 1 << 63 or
                   type(v) is float and math.isfinite(v) and v >= 0)}
        for key in ('timing_complete','succeeded'):
            if type(value.get(key)) is bool: result[key] = value[key]
        for key,allowed in enums.items():
            if isinstance(value.get(key),str) and value[key] in allowed: result[key] = value[key]
        digest = value.get('batch_request_digest')
        if isinstance(digest,str) and re.fullmatch('[0-9a-f]{64}',digest): result['batch_request_digest'] = digest
        methods = value.get('methods')
        if (isinstance(methods,list) and 0 < len(methods) <= 3 and
                all(m in ('eth_call','eth_getBlockByNumber') for m in methods)):
            result['methods'] = list(methods)
        items = value.get('attempt_timings')
        if attempts and isinstance(items,list): result['attempt_timings'] = [row(item) for item in items[:8]]
        return result
    batches = transport.get('rpc_batches') if isinstance(transport,dict) else None
    return {'rpc_batches':[row(value,True) for value in batches[:4]]} if isinstance(batches,list) else {}


class CandidateRefreshFailure(Reject):
    """Failure before full execution validation, with read-stage evidence."""
    def __init__(self, progress, error):
        reason = str(error) if type(error) in (Reject,RPCUnavailable) and str(error) in _LOCAL_FAILURE_REASONS else type(error).__name__
        self.evidence = copy.deepcopy(progress)
        self.evidence.update(failed_ns=time.monotonic_ns(), error=reason,
                             error_type=type(error).__name__, preflight_reached=False)
        if type(getattr(error,'code',None)) is int: self.evidence['rpc_error_code'] = error.code
        super().__init__('candidate refresh '+progress['stage']+': '+reason)


@contextmanager
def prioritized_candidate_read(rpc, clock_hints=None):
    # The main pipeline RPC has a single caller. Elevation covers only this
    # bounded full-state read; subsequent ordinary work restores its old class.
    missing = object()
    priority = getattr(rpc, 'critical_reads', missing)
    previous_hints = getattr(rpc, 'clock_hints', missing)
    try:
        if priority is not missing: rpc.critical_reads = True
        if rpc is not None and clock_hints is not None: rpc.clock_hints = clock_hints
        yield
    finally:
        if priority is not missing: rpc.critical_reads = priority
        if rpc is not None and clock_hints is not None:
            if previous_hints is missing: del rpc.clock_hints
            else: rpc.clock_hints = previous_hints


def refresh_candidate(rpc, config, route, states, context=None, previous_candidate=None, *, clock_hints=None):
    progress = {'stage':'setup', 'started_ns':time.monotonic_ns(), 'transport':{}}
    try:
        return _refresh_candidate(rpc, config, route, states, context, previous_candidate, clock_hints, progress)
    except Exception as exc:
        raise CandidateRefreshFailure(progress, exc) from exc


def _refresh_candidate(rpc, config, route, states, context, previous_candidate, clock_hints, progress):
    # Discovery may be slow; its old values never authorize a transaction.
    # Read the entire route together, then recompute size, fees and net floor.
    local = copy.deepcopy(config)
    local['policy']['cycle_pages'] = False
    saved = {'pools':[states[key].json() for key in route.pools]}
    feed = StateFeed(rpc, local, saved)
    check(len(feed.pools) == len(route.pools), 'refresh includes unsupported token transfer')
    if (context and 0 <= (time.monotonic_ns()-context['requested_ns'])/1e6 <= 1500
            and context.get('head') is not None and context.get('pending_head') is not None):
        feed.anchor, feed.pending = context['head'], context['pending_head']
        # Keep this clock's original age/version when comparing peer hints;
        # attaching a cache must not erase a fresher supplied context clock.
        feed.clock_requested_ns = feed.clock_received_ns = context['requested_ns']
    # Retain deeper coverage when discovery needed it; all prices/ticks are
    # fetched anew in the same EVM call rather than mixing cached observations.
    deep = [key for key in route.pools if len(states[key].ticks) > 2*local['policy'].get('snapshot_ticks_per_side',4)]
    progress['stage'] = 'atomic_state_read'
    progress['read_priority'] = 'critical' if hasattr(rpc, 'critical_reads') else 'legacy_transport'
    progress['read_started_ns'] = time.monotonic_ns()
    try:
        with prioritized_candidate_read(rpc, clock_hints):
            frame = feed.poll(deep_pools=deep)
    finally:
        progress['transport'] = _read_transport_evidence(getattr(feed, 'last_transport', {}))
        progress['read_finished_ns'] = time.monotonic_ns()
    progress['stage'] = 'repricing'
    pools = [Pool.from_json(p) for p in frame.snapshot_pools]
    identity = frame.identity
    for pool in pools: pool.block_hash, pool.block = identity, number(frame.latest['number'])
    index = RouteIndex(pools,[route.tokens[0]],len(route.pools),groups=[route.pools])
    # Only this original direction is being refreshed. Other directions retain
    # the ordinary scheduler's fairness and their own first-seen evidence.
    check(route.key in index.routes, 'refreshed route no longer valid')
    index.routes = {route.key:index.routes[route.key]}
    deadline = observation_start_ns(frame)+int(config['policy']['snapshot_max_age_ms']*1e6)
    start = time.monotonic_ns()
    report = {'block':number(frame.latest['number']), 'block_hash':frame.latest['hash'],
              'observed_at':time.time(), 'computation_ms':0, 'routes_considered':1,
              'routes_sized':0, 'candidates':[], 'rejected':{}, 'search_time_limited':False,
              'scope':'Fresh exact-input quote; full executor validation still required.'}
    amount = None
    settlement = next(t for t in local['settlement_tokens'] if t['address']==route.tokens[0])
    if previous_candidate is not None:
        check(previous_candidate['route_key']==route.key, 'refresh candidate route mismatch')
        amount = previous_candidate['amount_in']
        check(isinstance(amount, int) and 0 < amount < 1 << 255, 'invalid refresh amount')
    # Keep the already-discovered input size, never its old output or profit.
    # Every leg and the gas/fee floor are recomputed on the NEW atomic snapshot.
    if (amount is not None and amount <= int(settlement['max_input_raw']) and start < deadline):
        try:
            output, crossings = route.quote(index.states, amount)
            budget = cost_budget(local['policy'], route.tokens[0], number(frame.latest['baseFeePerGas']),
                                 len(route.pools), crossings)
            floor = quote_profit_floor(local, settlement, budget.settlement_cost_raw)
            net = output-amount-budget.settlement_cost_raw
            report['routes_sized'] = 1
            if net >= floor:
                report['candidates'] = [{
                    'route_key':route.key, 'tokens':route.tokens, 'pools':route.pools,
                    'complete':True, 'amount_in':amount, 'amount_out':output,
                    'gross_surplus':output-amount, 'net_after_modelled_cost':net,
                    'tick_crossings':crossings, 'quote_count':1, 'coverage_limited':False,
                    'state_coverage_limited':False, 'errors':[], 'global_optimum_proven':False,
                    'sizing_mode':'previous_amount_repriced', 'sizing_time_limited':False,
                    'cost_budget':asdict(budget), 'minimum_gross_surplus':budget.settlement_cost_raw+floor,
                    'transfer_revalidation_priority':0, 'status':'quoted_only', 'placement_proven':False}]
        except (Incomplete, Reject):
            # A size that no longer fits its known liquidity may still have a
            # smaller executable alternative. The bounded search quotes anew.
            pass
    if not report['candidates']:
        # Reserve the latter half of the observation window for execution
        # validation. Search timeout means unknown, not proof of no profit.
        reserve_ns = int(config['policy']['snapshot_max_age_ms']*1e6/2)
        search_deadline = min(deadline-reserve_ns,
                              time.monotonic_ns()+int(config['policy']['max_quote_ms']*1e6))
        if time.monotonic_ns() < search_deadline:
            report = evaluate(index, local, frame.latest, sizing_deadline_ns=search_deadline,
                              initial_amounts={route.key:(amount,)} if amount else None)
        else:
            report['search_time_limited'] = True
    report['refresh_computation_ms'] = (time.monotonic_ns()-start)/1e6
    if report['candidates'] and report['candidates'][0].get('sizing_mode')=='previous_amount_repriced':
        report['computation_ms'] = report['refresh_computation_ms']
    report['decision_age_ms'] = (time.monotonic_ns()-frame.requested_ns)/1e6
    report['observation_age_ms'] = (time.monotonic_ns()-observation_start_ns(frame))/1e6
    candidate = next((c for c in report['candidates'] if c['route_key']==route.key),None)
    evidence = {'requested_ns':frame.requested_ns,'received_ns':frame.received_ns,
                'observation_started_ns':getattr(frame, 'observation_started_ns', None),
                'received_utc':frame.received_utc,'frame':frame.payload(),'decision':report,
                'read_priority':progress['read_priority']}
    return candidate, index.states, frame.latest, deadline, evidence
