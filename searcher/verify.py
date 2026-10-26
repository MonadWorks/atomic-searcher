"""Differential checks against real pool bytecode, with eth_call only."""
import json
import time
from pathlib import Path

from eth_abi import decode
from eth_utils import keccak

from .core import ROOT, MULTICALL, Reject, Incomplete, check, data, number, unhex
from .pools import Pool

HELPER = "0x000000000000000000000000000000000000c1c1"


def native_quotes(rpc, requests, block):
    artifact = json.loads((ROOT / "out/CLQuoter.sol/CLQuoter.json").read_text())
    runtime = artifact["deployedBytecode"]["object"]
    result = []
    for offset in range(0, len(requests), 12):
        chunk = requests[offset:offset+12]
        if any(p.kind == 'v2' for p, _, _ in chunk):
            for pool, token, amount in chunk:
                if pool.kind != 'v2':
                    result.extend(native_quotes(rpc, [(pool, token, amount)], block))
                    continue
                try:
                    raw = rpc.call('0x4752ba5dbc23f44d87826276bf6fd6b1c372ad24',
                        data('getAmountsOut(uint256,address[])', ['uint256','address[]'],
                             [amount,[token,pool.other(token)]]), block)
                    values = decode(['uint256[]'],raw)[0]
                    check(len(values) == 2 and values[0] == amount, 'invalid V2 router quote')
                    result.append(tuple(values))
                except Reject: result.append(None)
            continue
        calls = [(HELPER, True, unhex(data("quote(address,bool,uint256)",
                 ["address", "bool", "uint256"], [p.address, token == p.token0, amount])))
                 for p, token, amount in chunk]
        payload = data("aggregate3((address,bool,bytes)[])", ["(address,bool,bytes)[]"], [calls])
        raw = rpc.request("eth_call", [{"to": MULTICALL, "data": payload, "gas": hex(15_000_000)},
                          hex(block), {HELPER: {"code": runtime}}])
        for (ok, value), (pool, token, amount) in zip(decode(["(bool,bytes)[]"], unhex(raw))[0], chunk):
            answer = decode(["uint256", "uint256"], value) if ok and len(value) == 64 else None
            if answer is None:
                # A sparse spacing-1 pool can exhaust the batch gas while
                # walking empty bitmap words to its price limit. Diagnose it
                # once in isolation; this read-only allowance is never a send cap.
                try:
                    single = rpc.request("eth_call", [{"to":HELPER, "data":data("quote(address,bool,uint256)",
                        ["address", "bool", "uint256"], [pool.address, token == pool.token0, amount]), "gas":hex(45_000_000)},
                        hex(block), {HELPER:{"code":runtime}}])
                    answer = decode(["uint256", "uint256"], unhex(single))
                except Reject: pass
            result.append(answer)
    return result


def verify_snapshot(rpc, snapshot_path, pool_limit=16):
    snap = json.loads(Path(snapshot_path).read_text())
    block = number(snap["header"]["number"])
    rpc.verify_chain()
    check(rpc.header(block)["hash"] == snap["header"]["hash"], "snapshot hash no longer canonical")
    pools = [Pool.from_json(x) for x in snap["pools"] if x["valid"] and (x["liquidity"] > 0 or (x["kind"] == "v2" and x.get("reserve0",0) > 0 and x.get("reserve1",0) > 0))]
    # Deterministic diversity, not selection based on successful quotes.
    pools.sort(key=lambda p: (p.kind, p.factory, p.spacing, p.address))
    if len(pools) > pool_limit:
        pools = [pools[i*(len(pools)-1)//(pool_limit-1)] for i in range(pool_limit)]
    requests, rows = [], []
    for pool in pools:
        for token in (pool.token0, pool.token1):
            # Liquidity-scaled inputs span several price-impact levels without
            # requiring token decimals from possibly adversarial contracts.
            if pool.kind == 'v2': virtual = pool.reserve0 if token == pool.token0 else pool.reserve1
            else: virtual = (pool.liquidity << 96)//pool.sqrt_price if token == pool.token0 else pool.liquidity*pool.sqrt_price//(1 << 96)
            for amount in sorted({max(2, virtual//d) for d in (1_000_000, 10_000, 1_000, 100)}):
                row = {"pool": pool.address, "kind": pool.kind, "token_in": token, "amount_in": amount}
                try:
                    row["python_out"], row["tick_crossings"] = pool.quote(token, amount)
                except Incomplete as exc:
                    row["incomplete"] = str(exc)
                requests.append((pool, token, amount))
                rows.append(row)
    native = native_quotes(rpc, requests, block)
    for row, answer in zip(rows, native):
        if answer is None:
            row["status"] = "native_quote_reverted"
        else:
            row["native_used"], row["native_out"] = answer
            if "incomplete" in row:
                row["status"] = "explicitly_incomplete"
            elif answer[0] != row["amount_in"]:
                row["status"] = "native_partial_fill"
            else:
                row["status"] = "match" if answer[1] == row["python_out"] else "MISMATCH"
    return {"block": block, "block_hash": snap["header"]["hash"], "checked_at": time.time(),
            "method": "CL: native pool swap callback under eth_call override; V2: official router getAmountsOut; no broadcast",
            "helper_runtime_hash": "0x"+keccak(unhex(json.loads((ROOT/"out/CLQuoter.sol/CLQuoter.json").read_text())["deployedBytecode"]["object"])).hex(),
            "all_complete_quotes_equal": any(r["status"] == "match" for r in rows) and all(r["status"] in ("match", "explicitly_incomplete") for r in rows),
            "results": rows}
