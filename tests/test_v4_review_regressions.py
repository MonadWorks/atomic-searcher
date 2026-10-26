"""Independent V4 coverage regressions; synthetic state and loopback reads only."""
import json
import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest
from eth_abi import decode, encode
from eth_utils import keccak

from searcher.core import ROOT, RPC, USDC, data, load_config, unhex
from searcher.math import sqrt_at_tick
from searcher.v4 import Frame, LENS, Reader, Route, evaluate
from searcher.v4_market import CapacityLimit, Key, MANAGER, Snapshot


ASSET = "0xff00000000000000000000000000000000000001"


def mapping_slot(base, item):
    return int.from_bytes(keccak(encode(["int256", "uint256"], [item, base])), "big")


def small_profitable_market():
    """Two legal constant-liquidity positions with a profitable sub-10-USDC size."""
    keys = (Key(USDC, ASSET, 0, 1), Key(USDC, ASSET, 0, 2))
    storage = {}
    liquidity = 50_000_000
    for key, tick in zip(keys, (0, 3000)):
        storage[hex(key.slot)] = hex(sqrt_at_tick(tick) | (tick << 160))
        storage[hex(key.slot + 3)] = hex(liquidity)
        lower, upper = -10000, 10000
        for word in range((lower // key.spacing) >> 8, ((upper // key.spacing) >> 8) + 1):
            bitmap = 0
            for boundary in (lower, upper):
                compressed = boundary // key.spacing
                if compressed >> 8 == word:
                    bitmap |= 1 << (compressed & 255)
            storage[hex(mapping_slot(key.slot + 5, word))] = hex(bitmap)
        for boundary, net in ((lower, liquidity), (upper, -liquidity)):
            storage[hex(mapping_slot(key.slot + 4, boundary))] = hex(
                liquidity | ((net % (1 << 128)) << 128))
    return keys, Snapshot(storage)


def test_output_search_finds_profitable_size_below_old_ten_usdc_grid():
    keys, snapshot = small_profitable_market()
    route = Route(*keys)
    known = snapshot.cycle(*keys, USDC, 4_000_000)
    assert known["cost"] == 3_481_584
    assert known["gross_surplus"] == 518_416
    assert snapshot.cycle(*keys, USDC, 10_000_000)["gross_surplus"] < 0

    config = load_config()
    # This tests size coverage, independently of a machine-speed scheduling budget.
    config["policy"]["max_quote_ms"] = 10_000
    now = time.monotonic_ns()
    frame = Frame(anchor={"number": "0x1", "hash": "0x" + "11" * 32,
                          "baseFeePerGas": "0x0"}, context={}, snapshot=snapshot,
                  keys=keys, requested_ns=now, received_ns=now, received_utc=time.time())
    report = evaluate(frame, config, [route])
    assert report["candidates"], "A complete profitable small size must not be lost to a fixed grid"
    candidate = report["candidates"][0]
    assert 0 < candidate["settlement_output_raw"] < 10_000_000
    actual = snapshot.cycle(*keys, USDC, candidate["settlement_output_raw"])
    assert actual["gross_surplus"] == candidate["gross_surplus"]
    assert actual["gross_surplus"] >= candidate["minimum_gross_surplus"]


def test_missing_slot_admission_renews_full_group_without_unbounded_growth():
    keys = (Key(USDC, ASSET, 0, 1), Key(USDC, ASSET, 0, 2))
    reader = Reader(None, {})
    reader.request_missing(keys, set(range(512)))
    group = tuple(sorted(key.id for key in keys))
    assert len(reader.extra[group]) == 512
    # The same pools moved to new depth. Historical requests cannot permanently
    # occupy every slot and prevent a currently required word from being read.
    current = set(range(10000, 10008))
    reader.request_missing(tuple(reversed(keys)), current)
    assert current <= set(reader.extra[group])
    assert len(reader.extra[group]) <= 512
    for generation in range(4):
        newest = {20000 + generation}
        reader.request_missing(keys, newest)
        assert newest <= set(reader.extra[group])
        assert len(reader.extra[group]) <= 512


def test_intermediate_int128_frontier_keeps_complete_profitable_smaller_sizes():
    keys = (Key(USDC, ASSET, 0, 1000), Key(USDC, ASSET, 0, 2000))
    liquidity = 10**37  # Valid uint128 liquidity, with a large intermediate-asset price.
    storage = {}
    for key, tick in zip(keys, (200000, 202000)):
        storage[hex(key.slot)] = hex(sqrt_at_tick(tick) | (tick << 160))
        storage[hex(key.slot + 3)] = hex(liquidity)
        for word in range(-4, 4):
            bitmap = 0
            for boundary in (-880000, 880000):
                compressed = boundary // key.spacing
                if compressed >> 8 == word:
                    bitmap |= 1 << (compressed & 255)
            storage[hex(mapping_slot(key.slot + 5, word))] = hex(bitmap)
        for boundary, net in ((-880000, liquidity), (880000, -liquidity)):
            storage[hex(mapping_slot(key.slot + 4, boundary))] = hex(
                liquidity | ((net % (1 << 128)) << 128))
    snapshot = Snapshot(storage)
    small = snapshot.cycle(*keys, USDC, 1_000_000)
    assert small["gross_surplus"] == 181260
    with pytest.raises(CapacityLimit):
        snapshot.cycle(*keys, USDC, 10**30)

    config = load_config()
    config["policy"]["max_quote_ms"] = 10_000
    now = time.monotonic_ns()
    frame = Frame(anchor={"number": "0x1", "hash": "0x" + "11" * 32,
                          "baseFeePerGas": "0x0"}, context={}, snapshot=snapshot,
                  keys=keys, requested_ns=now, received_ns=now, received_utc=time.time())
    report = evaluate(frame, config, [Route(*keys)])
    assert report["candidates"], "A later protocol-capacity frontier must retain earlier valid candidates"
    candidate = report["candidates"][0]
    actual = snapshot.cycle(*keys, USDC, candidate["settlement_output_raw"])
    assert 0 < actual["intermediate_debt"] < (1 << 127)
    assert actual["gross_surplus"] == candidate["gross_surplus"]
    assert actual["gross_surplus"] >= max(small["gross_surplus"], candidate["minimum_gross_surplus"])


@pytest.fixture
def local_read_rpc():
    binary = shutil.which("anvil")
    if binary is None:
        fallback = Path.home() / ".foundry/bin/anvil"
        binary = str(fallback) if fallback.is_file() else None
    if binary is None:
        pytest.skip("Anvil is required for the real lens read regression")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    process = subprocess.Popen([binary, "--host", "127.0.0.1", "--port", str(port),
                                "--chain-id", "8453", "--silent"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    rpc = RPC(f"http://127.0.0.1:{port}", 8453, timeout=3)
    try:
        for _ in range(100):
            try:
                rpc.verify_chain()
                break
            except Exception:
                time.sleep(.02)
        else:
            raise RuntimeError("temporary loopback node did not start")
        yield rpc
    finally:
        rpc.close()
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


@pytest.mark.parametrize("current_tick", [0, 200])
def test_atomic_lens_preserves_each_pools_near_depth_despite_first_pools_far_ticks(local_read_rpc, current_tick):
    keys = (Key(USDC, ASSET, 0, 1), Key(USDC, ASSET, 500, 1))
    storage = {}
    expected_near = {}
    for key in keys:
        storage[key.slot] = sqrt_at_tick(current_tick) | (current_tick << 160) | (key.fee << 208)
        storage[key.slot + 3] = 1_000_000
        for word in range(-16, 17):
            storage[mapping_slot(key.slot + 5, word)] = 0
        for tick, net in ((current_tick - 1, 10), (current_tick + 1, -10)):
            word, bit = divmod(tick, 256)
            storage[mapping_slot(key.slot + 5, word)] |= 1 << bit
            slot = mapping_slot(key.slot + 4, tick)
            storage[slot] = 10 | ((net % (1 << 128)) << 128)
            expected_near[slot] = storage[slot]
    # The first pool has 128 distant initialized boundaries. A global ascending
    # tick scan spends the entire budget here before visiting either near pair.
    storage[mapping_slot(keys[0].slot + 5, -16)] = (1 << 128) - 1
    for bit in range(128):
        storage[mapping_slot(keys[0].slot + 4, -4096 + bit)] = 2
    if current_tick == 200:
        # A word-level nearest-first policy still fails if it consumes bits
        # 0..63 before looking at the same word's near-price ticks 199/201.
        storage[mapping_slot(keys[0].slot + 5, 0)] |= (1 << 128) - 1
        for tick in range(128):
            storage[mapping_slot(keys[0].slot + 4, tick)] = 2

    runtime = json.loads((ROOT / "out/V4StateLens.sol/V4StateLens.json").read_text())["deployedBytecode"]["object"]
    # extsload(bytes32): load the requested storage word and return it. This is
    # a temporary state override, not a deployment or a submitted transaction.
    manager_reader = "0x6004355460005260206000f3"
    overrides = {LENS: {"code": runtime}, MANAGER: {"code": manager_reader,
        "stateDiff": {"0x" + slot.to_bytes(32, "big").hex(): "0x" + value.to_bytes(32, "big").hex()
                      for slot, value in storage.items()}}}
    payload = data("read(address,bytes32[],int24[],uint16,uint16,bytes32[])",
                   ["address", "bytes32[]", "int24[]", "uint16", "uint16", "bytes32[]"],
                   [MANAGER, [unhex(key.id) for key in keys], [key.spacing for key in keys], 16, 128, []])
    raw = local_read_rpc.request("eth_call", [{"to": LENS, "data": payload, "gas": hex(8_000_000)},
                                             "latest", overrides])
    _, _, slots, values = decode(["uint256", "uint256", "bytes32[]", "bytes32[]"], unhex(raw))
    observed = {int.from_bytes(slot, "big"): int.from_bytes(value, "big") for slot, value in zip(slots, values)}
    assert len(slots) == len(values) <= 1288
    for slot, value in expected_near.items():
        assert observed.get(slot) == value, "Far depth from one pool must not starve another pool's near ticks"
