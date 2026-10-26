from __future__ import annotations

from dataclasses import asdict, dataclass, field
from itertools import combinations

from eth_abi import decode

from .core import Incomplete, Reject, ZERO, address, check, data, number
from .math import MAX_SQRT, MIN_SQRT, quote_cl


@dataclass
class Pool:
    address: str
    factory: str
    kind: str
    token0: str
    token1: str
    spacing: int
    fee: int
    sqrt_price: int = 0
    tick: int = 0
    liquidity: int = 0
    bitmap: dict[int, int] = field(default_factory=dict)
    ticks: dict[int, tuple[int, int]] = field(default_factory=dict)
    block: int = 0
    block_hash: str = ""
    valid: bool = True
    reserve0: int = 0
    reserve1: int = 0

    def other(self, token):
        check(token in (self.token0, self.token1), "token not in pool")
        return self.token1 if token == self.token0 else self.token0

    def quote(self, token, amount):
        if not self.valid:
            raise Incomplete("invalidated pool state")
        self.other(token)
        if self.kind == "v2":
            rin, rout = (self.reserve0, self.reserve1) if token == self.token0 else (self.reserve1, self.reserve0)
            if not (0 < rin < 1 << 112 and 0 < rout < 1 << 112 and self.fee == 3000):
                raise Incomplete("invalid V2 reserves or fee")
            if not 0 <= amount <= (1 << 112)-1-rin:
                raise Incomplete("V2 input exceeds uint112 reserve capacity")
            adjusted = amount*997
            return adjusted*rout//(rin*1000+adjusted), 0
        return quote_cl(self, token, amount)

    def json(self):
        return asdict(self)

    @classmethod
    def from_json(cls, value):
        value = dict(value)
        value["bitmap"] = {int(k): int(v) for k, v in value.get("bitmap", {}).items()}
        value["ticks"] = {int(k): tuple(v) for k, v in value.get("ticks", {}).items()}
        return cls(**value)


def decode_address(raw):
    return decode(["address"], raw)[0].lower()


def discover(rpc, config, block, tokens=None, extra_pools=()):
    """Find pools from authenticated factories, never trust event signatures alone."""
    tokens = sorted(set(address(x) for x in (tokens or config["seed_tokens"])))
    return discover_pairs(rpc, config, block, combinations(tokens, 2), extra_pools)


def discover_pairs(rpc, config, block, pairs, extra_pools=()):
    """Authenticate explicit edges without requiring a full token cross product."""
    pairs = sorted({tuple(sorted((address(a), address(b)))) for a, b in pairs if a != b})
    calls = []
    for factory in config["factories"]:
        key_type = "uint24" if factory["kind"] == "v3" else "int24"
        for a, b in pairs:
            if factory["kind"] == "v2":
                calls.append((factory["address"], data("getPair(address,address)", ["address", "address"], [a, b])))
                continue
            for key in factory["keys"]:
                calls.append((factory["address"], data(f"getPool(address,address,{key_type})",
                              ["address", "address", key_type], [a, b, key])))
    replies = rpc.views(calls, block)
    check(len(replies) == len(calls), "incomplete factory lookup batch")
    check(all(v is not None and len(v) == 32 for v in replies), "factory lookup unavailable; retain previous universe")
    found = {decode_address(v) for v in replies if v and len(v) == 32 and decode_address(v) != ZERO}
    found.update(address(x) for x in extra_pools)
    return validate_pools(rpc, config, sorted(found), block)


def validate_pools(rpc, config, addresses, block):
    allowed = {f["address"]: f for f in config["factories"]}
    fields = ("factory()", "token0()", "token1()", "tickSpacing()", "fee()")
    replies = rpc.views([(a, data(s)) for a in addresses for s in fields], block)
    preliminary, errors = [], []
    for i, a in enumerate(addresses):
        try:
            row = replies[i*5:i*5+5]
            check(all(v is not None and len(v) >= 32 for v in row[:3]), "metadata unavailable")
            factory, t0, t1 = map(decode_address, row[:3])
            check(factory in allowed, "factory outside supported registry")
            if allowed[factory]["kind"] == "v2":
                spacing, fee = 0, 3000
            else:
                check(all(v is not None and len(v) >= 32 for v in row[3:]), "CL metadata unavailable")
                spacing, fee = decode(["int24"], row[3])[0], decode(["uint24"], row[4])[0]
                check(0 < spacing < 16384 and 0 <= fee < 1_000_000, "invalid fee/spacing")
            check(int(t0, 16) < int(t1, 16) and t0 != ZERO, "invalid token order")
            preliminary.append(Pool(a.lower(), factory, allowed[factory]["kind"], t0, t1, spacing, fee))
        except Exception as exc:
            errors.append({"pool": a, "reason": str(exc)[:160]})
    checks = []
    for p in preliminary:
        if p.kind == "v2":
            checks.append((p.factory, data("getPair(address,address)", ["address", "address"], [p.token0, p.token1])))
            continue
        typ, key = ("uint24", p.fee) if p.kind == "v3" else ("int24", p.spacing)
        checks.append((p.factory, data(f"getPool(address,address,{typ})", ["address", "address", typ],
                                     [p.token0, p.token1, key])))
    replies = rpc.views(checks, block)
    valid = []
    for p, reply in zip(preliminary, replies):
        if reply is None or len(reply) < 32:
            errors.append({"pool": p.address, "reason": "factory reverse lookup unavailable"})
        elif decode_address(reply) == p.address:
            valid.append(p)
        else:
            errors.append({"pool": p.address, "reason": "factory reverse lookup mismatch"})
    return valid, errors


def snapshot(rpc, pools, header, radius=2, tick_limit=64):
    """Read price, fee, bitmap and initialized liquidity from the SAME block."""
    block = number(header["number"])
    v2_pools, pools = [p for p in pools if p.kind == "v2"], [p for p in pools if p.kind != "v2"]
    v2_values = rpc.views([(p.address, data("getReserves()")) for p in v2_pools], block) if v2_pools else []
    calls = [(p.address, data(s)) for p in pools for s in ("slot0()", "liquidity()", "fee()")]
    replies = rpc.views(calls, block)
    valid, errors = [], []
    for i, pool in enumerate(pools):
        pool.valid = False
        try:
            slot, liq, fee = replies[i*3:i*3+3]
            check(slot is not None and liq is not None and fee is not None, "pool state missing")
            sqrt, tick = decode(["uint160", "int24"], slot[:64])
            liquidity, f = decode(["uint128"], liq)[0], decode(["uint24"], fee)[0]
            check(MIN_SQRT <= sqrt < MAX_SQRT and 0 <= f < 1_000_000, "invalid pool state")
            pool.sqrt_price, pool.tick, pool.liquidity, pool.fee = sqrt, tick, liquidity, f
            pool.block, pool.block_hash = block, header["hash"]
            pool.bitmap, pool.ticks = {}, {}
            valid.append(pool)
        except Exception as exc:
            errors.append({"pool": pool.address, "reason": str(exc)[:160]})
    words = [(p, w) for p in valid for w in range((p.tick//p.spacing >> 8)-radius, (p.tick//p.spacing >> 8)+radius+1)]
    replies = rpc.views([(p.address, data("tickBitmap(int16)", ["int16"], [w])) for p, w in words], block)
    initialized = []
    for (p, w), reply in zip(words, replies):
        if reply is None:
            errors.append({"pool": p.address, "reason": f"missing bitmap {w}"})
            continue
        bitmap = decode(["uint256"], reply)[0]
        p.bitmap[w] = bitmap
        for bit in range(256):
            if bitmap >> bit & 1:
                initialized.append((p, ((w << 8)+bit)*p.spacing))
    if tick_limit is not None:
        # Keep both directions available. Bitmap bits for uncached ticks remain
        # set, so crossing one raises Incomplete instead of inventing liquidity.
        selected = []
        for p in valid:
            current = [item for item in initialized if item[0] is p]
            below = sorted((item for item in current if item[1] <= p.tick), key=lambda x: -x[1])
            above = sorted((item for item in current if item[1] > p.tick), key=lambda x: x[1])
            selected.extend(below[:tick_limit//2]+above[:tick_limit//2])
        initialized = selected
    replies = rpc.views([(p.address, data("ticks(int24)", ["int24"], [tick])) for p, tick in initialized], block)
    for (p, tick), reply in zip(initialized, replies):
        if reply is None or len(reply) < 64:
            errors.append({"pool": p.address, "reason": f"missing initialized tick {tick}"})
            continue
        p.ticks[tick] = decode(["uint128", "int128"], reply[:64])
    for pool, raw in zip(v2_pools, v2_values):
        pool.valid = False
        try:
            check(raw is not None and len(raw) == 96, "V2 reserves unavailable")
            pool.reserve0, pool.reserve1, _ = decode(["uint112", "uint112", "uint32"], raw)
            check(pool.reserve0 > 0 and pool.reserve1 > 0, "empty V2 pool")
            pool.block, pool.block_hash = block, header["hash"]
            valid.append(pool)
        except Exception as exc:
            errors.append({"pool": pool.address, "reason": str(exc)[:160]})
    failed = {e["pool"] for e in errors}
    for p in valid:
        p.valid = p.address not in failed
    check(rpc.header(block)["hash"] == header["hash"], "snapshot reorg; discard all reads")
    return valid, errors
