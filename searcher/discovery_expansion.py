"""Durable, bounded speculative token-pair discovery; never a price cache."""
from collections import Counter
from itertools import combinations
import copy
import hashlib
import json

from .core import ZERO, check, data, safe_error
from .pools import decode_address, validate_pools


GETTER_BUDGET = 120
TERMINAL_IDENTITY_REJECTIONS = frozenset({
    'factory outside supported registry', 'invalid fee/spacing',
    'invalid token order', 'factory reverse lookup mismatch',
})


def factory_registry_id(config):
    return hashlib.sha256(json.dumps([
        (f['address'], f['kind'], f.get('keys', [])) for f in config['factories']
    ], sort_keys=True).encode()).hexdigest()


def retryable_identity_errors(errors):
    # Only explicit, definitive identity failures finish a lookup. Missing or
    # malformed metadata stays retryable, even when its diagnostic wording varies.
    return any(row.get('reason') not in TERMINAL_IDENTITY_REJECTIONS for row in errors)


def factory_slots(config):
    return [(f['address'], f['kind'], key)
            for f in config['factories']
            for key in ([None] if f['kind'] == 'v2' else f.get('keys', []))]


def expansion_state(config, previous, known, activity=None):
    """Keep the current batch intact and append newly observed tokens to a FIFO."""
    registry = factory_registry_id(config)
    same_registry = previous.get('factory_registry_id') == registry
    seeds = set(config['seed_tokens'])
    universe = seeds | {t for p in known.values() for t in (p.token0, p.token1)}
    expanded = set(previous.get('expanded_tokens', [])) & universe if same_registry else set()
    active = copy.deepcopy(previous.get('token_expansion')) if same_registry else None
    slots = factory_slots(config)
    if active is not None:
        tokens = active.get('tokens', [])
        pairs = active.get('remaining_pairs', [])
        offset = active.get('factory_offset', 0)
        ordered_pairs = [list(pair) for pair in combinations(sorted(set(tokens)), 2)]
        # A corrupt/incompatible checkpoint restarts uncompleted tokens; it
        # cannot fabricate completion or silently discard an unfinished token.
        valid = (active.get('registry') == registry and bool(tokens) and
                 len(tokens) == len(set(tokens)) and set(tokens) <= universe and
                 bool(pairs) and pairs == ordered_pairs[-len(pairs):] and
                 type(offset) is int and 0 <= offset < max(1, len(slots)))
        if not valid:
            active = None
    busy = set(active['tokens']) if active else set()
    pending = previous.get('pending_expansion_tokens', []) if same_registry else []
    pending = list(dict.fromkeys(t for t in pending
                                if t in universe - seeds - expanded - busy))
    counts = Counter()
    for pool in known.values():
        for token in (pool.token0, pool.token1):
            counts[token] += 1 + (activity or {}).get(pool.address, 0)
    additions = universe - seeds - expanded - busy - set(pending)
    pending.extend(sorted(additions, key=lambda t: (-counts[t], t)))
    if active is None and (pending or not seeds <= expanded):
        cap = config['policy']['max_new_tokens_per_refresh']
        check(type(cap) is int and cap >= 0, 'invalid token expansion batch size')
        chosen, pending = pending[:cap], pending[cap:]
        tokens = sorted(seeds | set(chosen))
        pairs = [list(p) for p in combinations(tokens, 2)] if chosen or not seeds <= expanded else []
        if not pairs or not slots:
            expanded.update(tokens)
        else:
            active = dict(registry=registry, tokens=tokens,
                          remaining_pairs=pairs, factory_offset=0)
    return dict(factory_registry_id=registry, expanded_tokens=sorted(expanded),
                pending_expansion_tokens=pending, token_expansion=active)


def next_chunk(config, state):
    """Select at most one Multicall's factory getters, including a partial pair."""
    active = state['token_expansion']
    if active is None:
        return [], None
    slots = factory_slots(config)
    remaining = copy.deepcopy(active['remaining_pairs'])
    offset = active['factory_offset']
    calls = []
    while remaining and len(calls) < GETTER_BUDGET:
        token0, token1 = remaining[0]
        factory, kind, key = slots[offset]
        if kind == 'v2':
            calldata = data('getPair(address,address)', ['address', 'address'], [token0, token1])
        else:
            typ = 'uint24' if kind == 'v3' else 'int24'
            calldata = data(f'getPool(address,address,{typ})',
                            ['address', 'address', typ], [token0, token1, key])
        calls.append((factory, calldata))
        offset += 1
        if offset == len(slots):
            remaining.pop(0)
            offset = 0
    after = dict(active, remaining_pairs=remaining, factory_offset=offset) if remaining else None
    return calls, after


def run_expansion(rpc, config, previous, known, block, activity=None, *, validator=validate_pools):
    """Checkpoint a chunk only after complete lookups and usable identity replies.

    This optional lane can fail without discarding recently authenticated pools.
    Its returned state is committed only alongside the caller's final snapshot.
    """
    state = expansion_state(config, previous, known, activity)
    calls, after = next_chunk(config, state)
    neighbors, errors, failure = [], [], None
    advanced = 0
    if calls:
        try:
            replies = rpc.views(calls, block)
            check(len(replies) == len(calls), 'incomplete token expansion lookup batch')
            check(all(v is not None and len(v) == 32 for v in replies),
                  'token expansion factory lookup unavailable')
            found = sorted({decode_address(v) for v in replies} - {ZERO})
            neighbors, errors = validator(rpc, config, found, block) if found else ([], [])
            if not retryable_identity_errors(errors):
                advanced = len(calls)
                if after is None:
                    state['expanded_tokens'] = sorted(set(state['expanded_tokens']) |
                                                       set(state['token_expansion']['tokens']))
                state['token_expansion'] = after
        except Exception as exc:
            failure = safe_error(exc)
    state.update(neighbors=neighbors, errors=errors, error=failure,
                 getters_attempted=len(calls), getters_completed=advanced)
    return state
