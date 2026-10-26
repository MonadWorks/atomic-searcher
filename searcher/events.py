"""Conservative incremental state updates with explicit completeness failures."""
from eth_abi import decode
from eth_utils import keccak

from .core import Incomplete, check, number, unhex


def topic(signature):
    return "0x" + keccak(text=signature).hex()


V2_SWAP = topic("Swap(address,uint256,uint256,uint256,uint256,address)")
V2_SYNC = topic("Sync(uint112,uint112)")
AERO_SYNC = topic("Sync(uint256,uint256)")
V3_SWAP = topic("Swap(address,address,int256,int256,uint160,uint128,int24)")
SLIP_SWAP = "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83"
MINT = topic("Mint(address,address,int24,int24,uint128,uint256,uint256)")
BURN = topic("Burn(address,int24,int24,uint128,uint256,uint256)")
INITIALIZE = topic("Initialize(uint160,int24)")
# New or removed liquidity can create an atomic cross-pool opportunity before
# the first swap. Hints trigger authenticated state reads, never authorization.
POOL_ACTIVITY_TOPICS = (V3_SWAP, SLIP_SWAP, V2_SYNC, AERO_SYNC, MINT, BURN, INITIALIZE)


class StateBook:
    def __init__(self, pools, header):
        self.pools = {p.address: p for p in pools}
        self.header = header
        self.seen = set()
        self.last_position = (-1, -1)
        self.invalid_reasons = {}

    def begin(self, header):
        n, previous = number(header["number"]), number(self.header["number"])
        if n != previous+1 or header["parentHash"] != self.header["hash"]:
            self.invalidate_all("gap or reorg; reload pinned snapshot")
            raise Incomplete("non-contiguous chain state")
        self.header = header
        self.last_position = (-1, -1)
        self.seen.clear()
        for pool in self.pools.values():
            pool.block, pool.block_hash = n, header["hash"]
            if pool.kind == "slipstream":
                # Dynamic fee modules may depend on time without a Swap event.
                # A block boundary alone makes yesterday's fee unproven.
                if pool.valid:
                    self.invalid_reasons[pool.address] = "dynamic fee needs a pinned current-context read"
                pool.valid = False

    def invalidate_all(self, reason):
        for pool in self.pools.values():
            pool.valid = False
            self.invalid_reasons[pool.address] = reason

    def apply_observed_range(self, logs, header):
        """Caller must pin and recheck both canonical endpoints around getLogs.
        Missed blocks are applied as ONE observation, never as historical trades
        we could supposedly have submitted between already observed events.
        """
        previous, target = number(self.header["number"]), number(header["number"])
        check(target > previous, "range must advance")
        groups = {}
        for entry in logs:
            height = number(entry["blockNumber"])
            check(previous < height <= target and not entry.get("removed"), "invalid observed log range")
            groups.setdefault(height, []).append(entry)
        for height, entries in groups.items():
            hashes = {entry["blockHash"] for entry in entries}
            check(len(hashes) == 1, "mixed hashes at one height")
            if height == target:
                check(next(iter(hashes)) == header["hash"], "target log hash mismatch")
        changed = set()
        for height in sorted(groups):
            self.header = {"number": hex(height), "hash": groups[height][0]["blockHash"]}
            self.last_position, self.seen = (-1,-1), set()
            changed |= self.apply_visible_batch(groups[height])
        self.header = header
        self.last_position, self.seen = (-1,-1), set()
        for p in self.pools.values():
            p.block, p.block_hash = target, header["hash"]
            if p.kind == "slipstream":
                if p.valid:
                    self.invalid_reasons[p.address] = "dynamic fee needs a pinned current-context read"
                p.valid = False
        return changed

    def apply_visible_batch(self, logs):
        """Evaluate opportunities only AFTER a whole observed batch, never between
        transactions learned simultaneously from the same published payload.
        """
        changed = set()
        for log in sorted(logs, key=lambda x: (number(x["transactionIndex"]), number(x["logIndex"]))):
            key = (log["blockHash"], log["transactionHash"], number(log["logIndex"]))
            if log.get("removed"):
                self.invalidate_all("removed preconfirmation/log")
                raise Incomplete("removed log")
            if key in self.seen:
                continue
            if log["blockHash"] != self.header["hash"]:
                self.invalidate_all("log/snapshot hash mismatch")
                raise Incomplete("log/snapshot hash mismatch")
            position = (number(log["transactionIndex"]), number(log["logIndex"]))
            if position <= self.last_position:
                self.invalidate_all("out-of-order event")
                raise Incomplete("out-of-order event")
            self.last_position = position
            self.seen.add(key)
            p = self.pools.get(log["address"].lower())
            if p is None or not log["topics"]:
                continue
            t = log["topics"][0]
            try:
                if t == V2_SYNC and p.kind == "v2":
                    p.reserve0, p.reserve1 = decode(["uint112", "uint112"], unhex(log["data"]))
                    if not p.reserve0 or not p.reserve1: raise Incomplete("empty V2 reserves")
                    changed.add(p.address)
                elif t in (V3_SWAP, SLIP_SWAP) and p.kind != "v2":
                    _, _, p.sqrt_price, p.liquidity, p.tick = decode(
                        ["int256", "int256", "uint160", "uint128", "int24"], unhex(log["data"])[:160])
                    changed.add(p.address)
                    if p.kind == "slipstream":
                        if p.valid:
                            self.invalid_reasons[p.address] = "dynamic fee changed or not yet refreshed"
                        p.valid = False
                elif t in (MINT, BURN):
                    low = decode(["int24"], unhex(log["topics"][2]))[0]
                    high = decode(["int24"], unhex(log["topics"][3]))[0]
                    raw = unhex(log["data"])
                    amount = decode(["uint128"], raw[32:64] if t == MINT else raw[:32])[0]
                    delta = amount if t == MINT else -amount
                    for tick, sign in ((low, 1), (high, -1)):
                        word, bit = (tick//p.spacing) >> 8, (tick//p.spacing) & 255
                        if word not in p.bitmap or (p.bitmap[word] >> bit & 1 and tick not in p.ticks):
                            raise Incomplete("liquidity update outside cached ticks")
                        gross, net = p.ticks.get(tick, (0, 0))
                        gross, net = gross+delta, net+sign*delta
                        check(0 <= gross < 1 << 128 and -(1 << 127) <= net < 1 << 127, "invalid liquidity delta")
                        if gross == 0:
                            check(net == 0, "nonzero net at cleared tick")
                            p.ticks.pop(tick, None)
                            p.bitmap[word] &= ~(1 << bit)
                        else:
                            p.ticks[tick] = (gross, net)
                            p.bitmap[word] |= 1 << bit
                    if low <= p.tick < high:
                        p.liquidity += delta
                        check(0 <= p.liquidity < 1 << 128, "invalid active liquidity")
                    changed.add(p.address)
                elif t == INITIALIZE:
                    raise Incomplete("pool initialization requires a full snapshot")
            except Exception as exc:
                p.valid = False
                self.invalid_reasons[p.address] = str(exc)
                changed.add(p.address)
        return changed
