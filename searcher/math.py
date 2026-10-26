"""Integer concentrated-liquidity arithmetic matching Uniswap V3 swap steps.

Tick constants and rounding rules follow Uniswap v3-core TickMath/SwapMath
(GPL-2.0-or-later). No floating-point prices enter an execution quote.
"""
from functools import lru_cache

from .core import Incomplete, Reject, check

Q96 = 1 << 96
MAX_UINT256 = (1 << 256) - 1
MIN_TICK, MAX_TICK = -887272, 887272
MIN_SQRT, MAX_SQRT = 4295128739, 1461446703485210103287273052203988822378723970342
FACTORS = [
    0xfffcb933bd6fad37aa2d162d1a594001, 0xfff97272373d413259a46990580e213a,
    0xfff2e50f5f656932ef12357cf3c7fdcc, 0xffe5caca7e10e4e61c3624eaa0941cd0,
    0xffcb9843d60f6159c9db58835c926644, 0xff973b41fa98c081472e6896dfb254c0,
    0xff2ea16466c96a3843ec78b326b52861, 0xfe5dee046a99a2a811c461f1969c3053,
    0xfcbe86c7900a88aedcffc83b479aa3a4, 0xf987a7253ac413176f2b074cf7815e54,
    0xf3392b0822b70005940c7a398e4b70f3, 0xe7159475a2c29b7443b29c7fa6e889d9,
    0xd097f3bdfd2022b8845ad8f792aa5825, 0xa9f746462d870fdf8a65dc1f90e061e5,
    0x70d869a156d2a1b890bb3df62baf32f7, 0x31be135f97d08fd981231505542fcfa6,
    0x9aa508b5b7a84e1c677de54f3e99bc9, 0x5d6af8dedb81196699c329225ee604,
    0x2216e584f5fa1ea926041bedfe98, 0x48a170391f7dc42444e8fa2,
]


def ceildiv(a, b):
    check(a >= 0 and b > 0, "invalid unsigned division")
    return (a + b - 1) // b


@lru_cache(maxsize=32768)
def sqrt_at_tick(tick):
    check(MIN_TICK <= tick <= MAX_TICK, "tick out of range")
    ratio = 1 << 128
    for bit, factor in enumerate(FACTORS):
        if abs(tick) & (1 << bit):
            ratio = (ratio * factor) >> 128
    if tick > 0:
        ratio = MAX_UINT256 // ratio
    return ceildiv(ratio, 1 << 32)


def amount0_delta(a, b, liquidity, up):
    a, b = min(a, b), max(a, b)
    n = (liquidity << 96) * (b - a)
    return ceildiv(ceildiv(n, b), a) if up else n // b // a


def amount1_delta(a, b, liquidity, up):
    n = liquidity * abs(a-b)
    return ceildiv(n, Q96) if up else n // Q96


def next_sqrt(sqrt, liquidity, amount, zero_for_one):
    check(liquidity > 0 and sqrt > 0, "zero liquidity step")
    if zero_for_one:
        n = liquidity << 96
        product = amount * sqrt
        if product <= MAX_UINT256 and n + product <= MAX_UINT256:
            return ceildiv(n * sqrt, n + product)
        return ceildiv(n, n // sqrt + amount)
    return sqrt + amount * Q96 // liquidity


def swap_step(sqrt, target, liquidity, remaining, fee):
    check(0 <= fee < 1_000_000 and remaining >= 0 and liquidity >= 0, "invalid swap state")
    z = sqrt >= target
    less_fee = remaining * (1_000_000 - fee) // 1_000_000
    delta_in = amount0_delta if z else amount1_delta
    delta_out = amount1_delta if z else amount0_delta
    required = delta_in(sqrt, target, liquidity, True)
    reached = less_fee >= required
    new = target if reached else next_sqrt(sqrt, liquidity, less_fee, z)
    used = required if reached else delta_in(sqrt, new, liquidity, True)
    output = delta_out(sqrt, new, liquidity, False)
    paid_fee = ceildiv(used * fee, 1_000_000-fee) if reached else remaining-used
    return new, used, output, paid_fee


def next_tick(tick, spacing, bitmap, zero_for_one):
    compressed = tick // spacing  # Python floors negatives, as required by V3.
    if not zero_for_one:
        compressed += 1
    word, bit = compressed >> 8, compressed & 255
    if word not in bitmap:
        raise Incomplete(f"missing bitmap word {word}")
    bits = bitmap[word]
    if zero_for_one:
        masked = bits & ((1 << (bit+1))-1)
        nxt = compressed - (bit-(masked.bit_length()-1)) if masked else compressed-bit
    else:
        masked = bits & (MAX_UINT256 ^ ((1 << bit)-1))
        lsb = (masked & -masked).bit_length()-1 if masked else 0
        nxt = compressed + (lsb-bit) if masked else compressed+(255-bit)
    return max(MIN_TICK, min(MAX_TICK, nxt*spacing)), bool(masked)


def quote_cl(pool, token_in, amount, *, include_state=False):
    check(0 < amount < 1 << 255, "amount out of range")
    check(token_in in (pool.token0, pool.token1), "wrong input asset")
    z = token_in == pool.token0
    sqrt, tick, liquidity = pool.sqrt_price, pool.tick, pool.liquidity
    remaining, output, crossings = amount, 0, 0
    limit = MIN_SQRT+1 if z else MAX_SQRT-1
    for _ in range(4096):
        if remaining == 0:
            return (output, crossings, sqrt, liquidity) if include_state else (output, crossings)
        nxt, initialized = next_tick(tick, pool.spacing, pool.bitmap, z)
        target = sqrt_at_tick(nxt)
        bounded = max(limit, target) if z else min(limit, target)
        new, used, out, fee = swap_step(sqrt, bounded, liquidity, remaining, pool.fee)
        remaining -= used + fee
        output += out
        if new == target:
            if initialized:
                if nxt not in pool.ticks:
                    raise Incomplete(f"missing initialized tick {nxt}")
                liquidity += (-1 if z else 1) * pool.ticks[nxt][1]
                check(0 <= liquidity < 1 << 128, "invalid tick liquidity")
                crossings += 1
            tick = nxt-1 if z else nxt
        elif remaining:
            raise Incomplete("price limit reached before full input consumption")
        sqrt = new
    raise Incomplete("quote crossing budget exceeded")


def quote_v2(reserve_in, reserve_out, amount, fee):
    check(reserve_in > 0 and reserve_out > 0 and amount > 0 and 0 <= fee < 1_000_000, "invalid V2 state")
    with_fee = amount * (1_000_000-fee)
    return with_fee * reserve_out // (reserve_in*1_000_000+with_fee)
