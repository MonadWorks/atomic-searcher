from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from math import gcd, isqrt
import time

from .core import Incomplete, check


@dataclass(frozen=True)
class Route:
    tokens: tuple[str, ...]
    pools: tuple[str, ...]

    @property
    def key(self):
        return "/".join((self.tokens[0], *self.pools))

    def quote(self, states, amount):
        output, crossings = amount, 0
        hashes = {states[p].block_hash for p in self.pools}
        check(len(hashes) == 1, "mixed block states")
        for pool, token in zip(self.pools, self.tokens):
            output, crossed = states[pool].quote(token, output)
            crossings += crossed
            if not output:
                return 0, crossings
        return output, crossings


def iter_cycles(pools, settlement_tokens, max_hops):
    adjacent = defaultdict(list)
    for pool in pools:
        adjacent[pool.token0].append(pool)
        adjacent[pool.token1].append(pool)
    v2_adjacent = {token: [p for p in neighbors if p.kind == 'v2'] for token, neighbors in adjacent.items()}
    for start in settlement_tokens:
        v2_return_tokens = {p.other(start) for p in v2_adjacent.get(start, ())}
        def visit(token, tokens, path, used_v2=False):
            neighbors = v2_adjacent.get(token, ()) if len(path) == 3 and not used_v2 else adjacent[token]
            for pool in neighbors:
                if pool.address in path: continue
                nxt = pool.other(token)
                route_pools, route_tokens = path+(pool.address,), tokens+(nxt,)
                if nxt == start and len(route_pools) >= 2:
                    yield route_tokens, route_pools
                elif nxt not in tokens and len(route_pools) < max_hops:
                    has_v2 = used_v2 or pool.kind == 'v2'
                    if len(route_pools) == 3 and not has_v2 and nxt not in v2_return_tokens: continue
                    yield from visit(nxt, route_tokens, route_pools, has_v2)
        yield from visit(start, (start,), ())


class RouteIndex:
    def __init__(self, pools, settlement_tokens, max_hops=3, groups=None):
        if groups is not None:
            self.states = {p.address:p for p in pools}
            self.routes, self.by_pool = {}, defaultdict(set)
            for group in groups:
                check(set(group).issubset(self.states), 'route page lacks a pool')
                part = RouteIndex([self.states[a] for a in group], settlement_tokens, max_hops)
                for key, route in part.routes.items():
                    self.routes[key] = route
                    for a in route.pools: self.by_pool[a].add(key)
            return
        self.states = {p.address: p for p in pools}
        self.routes, self.by_pool = {}, defaultdict(set)
        for tokens, path in iter_cycles(pools, settlement_tokens, max_hops):
            route = Route(tokens, path)
            key = route.key
            self.routes[key] = route
            for a in path: self.by_pool[a].add(key)

    def affected(self, changed):
        keys = set().union(*(self.by_pool.get(p, set()) for p in changed)) if changed else set()
        return [self.routes[k] for k in sorted(keys)]


def marginal(pool, token):
    if not pool.valid: return None
    if pool.kind == "v2":
        if not (pool.reserve0 > 0 and pool.reserve1 > 0 and pool.fee == 3000): return None
        return (pool.reserve1, pool.reserve0) if token == pool.token0 else (pool.reserve0, pool.reserve1)
    if not pool.sqrt_price: return None
    return (pool.sqrt_price**2, 1 << 192) if token == pool.token0 else (1 << 192, pool.sqrt_price**2)


def marginal_profitable(route, states):
    """Exact marginal upper bound for distinct, constant-fee CL pools.

    Price impact makes each leg worse with increasing exact input. This rejects
    routes whose infinitesimal output cannot exceed input, before costly sizing.
    """
    numerator, denominator = 1, 1
    for key, token in zip(route.pools, route.tokens):
        p = states[key]
        rate = marginal(p, token)
        if rate is None: return False
        n, d = rate
        numerator *= n * (1_000_000-p.fee)
        denominator *= d * 1_000_000
    return numerator > denominator


def gross_upper_bound(route, states, *, cached_only=False):
    """Optimistic continuous-liquidity profit bound, or None if data is missing.

    Before any leg exhausts the entire initial marginal edge, all reachable
    liquidity changes must be known. Replacing these intervals' liquidity by
    their directional maximum reduces price impact. Compose those optimistic
    curves, ignore rounding losses, and maximize over all positive inputs.
    An unknown tick/bitmap before that frontier cannot justify rejecting a route.

    cached_only=True instead bounds just amounts fully quotable from the known
    prefix. It may skip local sizing, but must never suppress an unknown result
    from the default mode or exclude the route from deeper-state discovery.
    """
    numerator, denominator = 1, 1
    for key, token in zip(route.pools, route.tokens):
        p = states[key]
        rate = marginal(p, token)
        if rate is None: return None
        n, d = rate
        numerator *= n*(1_000_000-p.fee); denominator *= d*1_000_000
    if numerator <= denominator: return 0
    common = gcd(numerator, denominator); numerator //= common; denominator //= common
    a, b, c = 1, 1, 0
    for key, token in zip(route.pools, route.tokens):
        p = states[key]
        if not p.valid: return None
        z = token == p.token0
        if p.kind == "v2":
            rin, rout = (p.reserve0, p.reserve1) if z else (p.reserve1, p.reserve0)
            aa, bb, cc = 997*rout, 1000*rin, 997
            a, b, c = aa*a, bb*b, bb*c+cc*a
            common = gcd(gcd(a, b), c); a, b, c = a//common, b//common, c//common
            continue
        version = (p.sqrt_price, p.tick, p.liquidity, p.fee, p.spacing,
                   tuple(p.ticks.items()), tuple(p.bitmap.items()))
        cache = getattr(p, "_directional_upper", None)
        if cache is None or getattr(p, "_directional_upper_version", None) != version:
            cache = p._directional_upper = {}
            p._directional_upper_version = version
        if z not in cache:
            from .math import MIN_TICK, MAX_TICK, next_tick, sqrt_at_tick
            liquidity = p.liquidity
            levels = []
            cursor, price_square = p.tick, p.sqrt_price**2
            while True:
                try:
                    tick, initialized = next_tick(cursor, p.spacing, p.bitmap, z)
                except Incomplete:
                    # This is the last proven boundary, not an assertion that
                    # the missing word contains no initialized liquidity.
                    levels.append((price_square, None))
                    break
                price_square = sqrt_at_tick(tick)**2
                if initialized:
                    if tick not in p.ticks:
                        levels.append((price_square, None))
                        break
                    liquidity += (-1 if z else 1)*p.ticks[tick][1]
                    if not 0 <= liquidity < 1 << 128:
                        levels.append((price_square, None))
                        break
                levels.append((price_square, liquidity))
                if tick == (MIN_TICK if z else MAX_TICK): break
                cursor = tick-1 if z else tick
            cache[z] = levels
        maximum = p.liquidity
        for price_square, liquidity in cache[z]:
            # Once this leg alone consumes the entire initial marginal edge,
            # the cycle's continuous profit is decreasing. Later liquidity
            # cannot increase its maximum; all other legs also worsen.
            if (price_square*numerator <= p.sqrt_price**2*denominator if z else
                    price_square*denominator >= p.sqrt_price**2*numerator): break
            if liquidity is None:
                if cached_only: break
                return None
            maximum = max(maximum, liquidity)
        if maximum <= 0: return 0
        q, s, f = 1 << 96, p.sqrt_price, 1_000_000-p.fee
        aa, bb, cc = (f*maximum*(s*s if z else q*q),
                      1_000_000*maximum*(q*q if z else s*s), f*s*q)
        a, b, c = aa*a, bb*b, bb*c+cc*a
        common = gcd(gcd(a, b), c); a, b, c = a//common, b//common, c//common
    if a <= b: return 0
    if not c: return None
    # floor(sqrt(a*b)) yields an upper bound, then round the profit upward.
    return (a+b-2*isqrt(a*b)+c-1)//c


def input_frontiers(pool, token):
    """Exact-input boundaries justified by known ticks, not a wallet dollar cap."""
    from .math import next_tick, sqrt_at_tick, amount0_delta, amount1_delta, ceildiv, MIN_SQRT, MAX_SQRT
    zero = token == pool.token0
    if pool.kind == "v2":
        rin, rout = (pool.reserve0, pool.reserve1) if zero else (pool.reserve1, pool.reserve0)
        # Low-decimal tokens have wide integer-output plateaus. Their edges
        # are real sizing discontinuities, just like initialized CL ticks.
        cap = (1 << 112)-1-rin
        steps = [((k*rin*1000+997*(rout-k)-1)//(997*(rout-k))) for k in range(1, rout)] if rout <= 128 else []
        return cap, [x for x in steps if x <= cap], False
    price, tick, liquidity, total = pool.sqrt_price, pool.tick, pool.liquidity, 0
    boundaries = []
    for _ in range(4096):
        try: nxt, initialized = next_tick(tick, pool.spacing, pool.bitmap, zero)
        except Incomplete: return total, boundaries, True
        tick_price = sqrt_at_tick(nxt)
        target = max(MIN_SQRT+1, tick_price) if zero else min(MAX_SQRT-1, tick_price)
        initialized = initialized and target == tick_price
        delta = (amount0_delta if zero else amount1_delta)(price, target, liquidity, True)
        step = delta+ceildiv(delta*pool.fee, 1_000_000-pool.fee)
        if initialized and nxt not in pool.ticks:
            return total+max(0, step-1), boundaries, True
        total += step
        if initialized:
            boundaries.append(total)
            liquidity += (-1 if zero else 1)*pool.ticks[nxt][1]
            check(0 <= liquidity < 1 << 128, 'invalid frontier liquidity')
        if target in (MIN_SQRT+1, MAX_SQRT-1): return total, boundaries, False
        tick, price = nxt-1 if zero else nxt, target
    raise Incomplete('frontier iteration limit')


def route_capacity(route, states, arithmetic_limit=(1 << 255)-1):
    """Largest size inside ALL cached legs; unknown liquidity stays explicit."""
    cap, _, limited = input_frontiers(states[route.pools[0]], route.tokens[0])
    if cap >= arithmetic_limit: limited = False
    cap = min(cap, arithmetic_limit)
    if cap <= 0: return 0, limited
    # Full-input feasibility is monotone up to the first unknown tick. Do not
    # infer that prices beyond this finite cached prefix have no opportunity.
    def complete(amount):
        try: route.quote(states, amount); return True
        except Incomplete: return False
    if not complete(cap):
        lo, hi = 0, cap
        while lo < hi:
            mid = (lo+hi+1)//2
            if complete(mid): lo = mid
            else: hi = mid-1
        cap, limited = lo, True
    return cap, limited


def stationary_seed(route, states, amount):
    """Continuous stationary point inside the current fixed-liquidity segment.

    Each leg is anchored at an exact quote and represented as a fractional
    linear map. This supplies candidate sizes; only integer quotes determine
    retained profit. Never use this approximation to reject a route.
    """
    from .math import quote_cl
    a,b,c,d = 1,0,0,1
    current = amount
    for key, token in zip(route.pools, route.tokens):
        p = states[key]
        if current <= 0: return None
        if p.kind == "v2":
            rin, rout = (p.reserve0, p.reserve1) if token == p.token0 else (p.reserve1, p.reserve0)
            output, _ = p.quote(token, current)
            la, lc, ld = 997*rout, 997, 1000*rin
            a, b, c, d = la*a, la*b, lc*a+ld*c, lc*b+ld*d
            common = gcd(gcd(abs(a),abs(b)),gcd(abs(c),abs(d)))
            a,b,c,d = a//common,b//common,c//common,d//common
            current = output
            continue
        output, _, price, liquidity = quote_cl(p, token, current, include_state=True)
        if liquidity <= 0: return None
        q,f = 1 << 96, 1_000_000-p.fee
        z = token == p.token0
        aa = f*liquidity*(price*price if z else q*q)
        bb = 1_000_000*liquidity*(q*q if z else price*price)
        cc = f*price*q
        # output + aa*(x-current)/(bb+cc*(x-current))
        la,lb,lc,ld = output*cc+aa, output*(bb-cc*current)-aa*current, cc, bb-cc*current
        a,b,c,d = la*a+lb*c, la*b+lb*d, lc*a+ld*c, lc*b+ld*d
        common = gcd(gcd(abs(a),abs(b)),gcd(abs(c),abs(d)))
        a,b,c,d = a//common,b//common,c//common,d//common
        current = output
    determinant = a*d-b*c
    if determinant <= 0 or c == 0: return None
    if c*amount+d < 0: a,b,c,d = -a,-b,-c,-d
    return (isqrt(determinant)-d)//c


class _SizingDeadline(Exception):
    pass


class _PrefixSearch:
    """Share exact prefix quotes and boundary inversions within ONE snapshot."""
    def __init__(self, route, states, maximum, checkpoint=lambda: None):
        self.route, self.states, self.maximum = route, states, maximum
        self.checkpoint = checkpoint
        self.quotes, self.roots = {}, {}

    def quote(self, end, amount):
        if end == 0: return amount
        key = (end, amount)
        if key not in self.quotes:
            self.checkpoint()
            value = self.quote(end-1, amount)
            self.quotes[key] = (self.states[self.route.pools[end-1]].quote(
                self.route.tokens[end-1], value)[0] if value else 0)
        return self.quotes[key]

    def root(self, end, target):
        if end == 0: return target if target <= self.maximum else None
        key = (end, target)
        if key not in self.roots:
            self.checkpoint()
            try: top = self.quote(end, self.maximum)
            except Incomplete: top = None
            if top is not None and top < target:
                self.roots[key] = None
            else:
                lo, hi = 1, self.maximum
                while lo < hi:
                    mid = (lo+hi)//2
                    try: output = self.quote(end, mid)
                    except Incomplete: output = None
                    if output is None or output >= target: hi = mid
                    else: lo = mid+1
                self.roots[key] = lo
        return self.roots[key]


def v2_rounding_points(route, states, amount, maximum, *, prefix_search=None):
    """Pull integer output edges near a continuous seed back to settlement input."""
    current = amount
    points = set()
    search = prefix_search or _PrefixSearch(route, states, maximum)
    for i, (key, token) in enumerate(zip(route.pools, route.tokens)):
        search.checkpoint()
        # A rounded-zero prefix cannot fund the next leg. Keep any upstream
        # rounding boundaries already found; those may restore nonzero output.
        if current == 0: break
        pool = states[key]
        output, _ = pool.quote(token, current)
        if pool.kind == 'v2':
            rin, rout = (pool.reserve0, pool.reserve1) if token == pool.token0 else (pool.reserve1, pool.reserve0)
            for k in range(max(1, output-2), min(rout, output+3)):
                target = (k*rin*1000+997*(rout-k)-1)//(997*(rout-k))
                root = search.root(i, target)
                if root is None: continue
                points.update(x for x in (root-1, root, root+1) if 0 < x <= maximum)
        current = output
    return points


def sizing_fingerprint(route, states, maximum):
    """Only exact pricing facts permit reuse of interrupted integer quotes."""
    fields = ('kind', 'token0', 'token1', 'valid', 'spacing', 'fee', 'sqrt_price',
              'tick', 'liquidity', 'reserve0', 'reserve1')
    return (route.key, maximum, tuple((a, *(getattr(states[a], k) for k in fields),
                tuple(sorted(states[a].bitmap.items())), tuple(sorted(states[a].ticks.items())))
                for a in route.pools))


def optimize(route, states, maximum, fee_cost=lambda crossings: 0, *, deadline_ns=None, initial_amounts=(), continuation=None):
    """Compare tick-cost segments and their boundaries across the whole range.

    Rounded quotes remain authoritative. The result is an evaluated candidate,
    not a claim of a globally exact integer optimum.
    """
    check(maximum > 0, 'invalid size ceiling')
    # The caller invalidates this work on ANY pricing or fee change. Exact
    # quote/prefix caches let a later slice resume without starting over.
    work = {} if continuation is None else continuation
    cache = work.setdefault('quotes', {})
    errors, state_errors = work.setdefault('errors', []), work.setdefault('state_errors', [])
    def checkpoint():
        if deadline_ns is not None and time.monotonic_ns() >= deadline_ns:
            raise _SizingDeadline()
    def evaluate(amount):
        amount = max(1, min(maximum, amount))
        if amount not in cache:
            checkpoint()
            try:
                output, ticks = route.quote(states, amount)
            except Incomplete as exc:
                cache[amount] = None; errors.append(str(exc)); state_errors.append(str(exc))
            else:
                try: cache[amount] = (output-amount-fee_cost(ticks), output, ticks)
                except Incomplete as exc:
                    cache[amount] = None; errors.append(str(exc))
        return cache[amount]
    boundaries = work.setdefault('boundaries', {1, maximum})
    if 'prefix' not in work:
        work['prefix'] = _PrefixSearch(route, {a:states[a] for a in route.pools} if isinstance(route, Route) else states,
                                     maximum, checkpoint)
    search = work['prefix']
    search.checkpoint = checkpoint
    completed_segments = work.setdefault('completed_segments', set())
    segment_seeds = work.setdefault('segment_seeds', {})
    def explore():
        for amount in initial_amounts:
            if 0 < amount <= maximum: evaluate(amount)
        if 'grid' not in work:
            work['grid'] = {1, maximum}
            value = maximum
            while value > 1:
                value //= 2; work['grid'].add(max(1, value))
        grid = work['grid']
        # Keep exact feasible quotes even if later boundary refinement runs
        # out of time. An interrupted search never proves an empty market.
        for amount in sorted(grid): evaluate(amount)
        if isinstance(route, Route) and not work.get('frontiers_complete'):
            # Downstream tick boundaries are expressed in settlement input by
            # inverting the monotone exact integer quote of the preceding legs.
            for i, (key, token) in enumerate(zip(route.pools, route.tokens)):
                checkpoint()
                _, ticks, _ = input_frontiers(states[key], token)
                for boundary in ticks:
                    root = search.root(i, boundary)
                    if root is not None and 0 < root <= maximum:
                        boundaries.add(root)
                        grid.update(max(1, min(maximum, root+d)) for d in (-2,-1,0,1,2))
            work['frontiers_complete'] = True
        for amount in sorted(grid): evaluate(amount)
        # Within a fixed crossing-cost segment the continuous CL profit is
        # concave. Endpoint values are NOT upper bounds on its interior: refine
        # every segment, including one whose endpoints rank below twelve others.
        points = sorted(boundaries if isinstance(route, Route) else grid)
        for low, high in zip(points, points[1:]):
            segment = (low, high)
            if segment in completed_segments: continue
            checkpoint()
            if evaluate(low) is None and evaluate(high) is None:
                completed_segments.add(segment)
                continue
            if isinstance(route, Route):
                if segment not in segment_seeds:
                    try: segment_seeds[segment] = stationary_seed(route, states, (low+high)//2)
                    except Incomplete: segment_seeds[segment] = None
                seed = segment_seeds[segment]
                if seed is not None:
                    seed = max(low, min(high, seed))
                    try:
                        for edge in v2_rounding_points(route, states, seed, maximum, prefix_search=search): evaluate(edge)
                    except Incomplete: pass
                    # Rounding at preceding steps shifts the continuous seed by
                    # a few units. Check both sides and fee boundaries exactly.
                    for offset in (-32,-8,-2,-1,0,1,2,8,32):
                        evaluate(max(low, min(high, seed+offset)))
                    completed_segments.add(segment)
                    continue
            for _ in range(64):
                if high-low <= 8: break
                a,b=low+(high-low)//3,high-(high-low)//3
                av,bv=evaluate(a),evaluate(b)
                if bv is None or (av is not None and av[0] >= bv[0]): high=b-1
                else: low=a+1
            for amount in range(low, min(high,low+8)+1): evaluate(amount)
            completed_segments.add(segment)
    time_limited = False
    try: explore()
    except _SizingDeadline: time_limited = True
    feasible = [x for x,v in cache.items() if v is not None]
    if not feasible: return {'complete':False,'reason':'no fully quoted size','errors':sorted(set(errors)),
                            'sizing_time_limited':time_limited}
    best=max(feasible,key=lambda x:cache[x][0]);profit,output,crossings=cache[best]
    return {'complete':True,'amount_in':best,'amount_out':output,'gross_surplus':output-best,
            'net_after_modelled_cost':profit,'tick_crossings':crossings,'quote_count':len(cache),
            'coverage_limited':bool(errors),'state_coverage_limited':bool(state_errors),
            'errors':sorted(set(errors)),'global_optimum_proven':False,
            'sizing_search_maximum':maximum,'tick_boundaries_evaluated':len(boundaries),
            'sizing_time_limited':time_limited}
