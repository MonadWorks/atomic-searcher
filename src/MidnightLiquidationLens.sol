
// SPDX-License-Identifier: GPL-2.0-or-later
pragma solidity ^0.8.19;

struct CollateralParams { address token; uint256 lltv; uint256 liquidationCursor; address oracle; }
struct Market {
  uint256 chainId;
  address midnight;
  address loanToken;
  CollateralParams[] collateralParams;
  uint256 maturity;
  uint256 rcfThreshold;
  address enterGate;
  address liquidatorGate;
}

interface IMidnight {
  function toMarket(bytes32 id) external view returns (Market memory);
  function liquidationLocked(bytes32 id, address user) external view returns (bool);
  function debt(bytes32 id, address user) external view returns (uint128);
  function collateralBitmap(bytes32 id, address user) external view returns (uint128);
  function collateral(bytes32 id, address user, uint256 index) external view returns (uint128);
}
interface IOracle { function price() external view returns (uint256); }
interface ILiquidatorGate { function canLiquidate(address account) external view returns (bool); }

contract MidnightLiquidationLens {
  uint256 internal constant WAD = 1e18;
  uint256 internal constant ORACLE_PRICE_SCALE = 1e36;

  IMidnight public constant MIDNIGHT=IMidnight(0xAdedD8ab6dE832766Fedf0FaC4992E5C4D3EA18A);

  // One (id, borrower) pair. caller is the Executor (the liquidate msg.sender), whose
  // canLiquidate is checked -- not the EOA.
  struct Input { bytes32 id; address borrower; address caller; }

  // One activated collateral slot, with everything sizing needs for it. index is the MARKET-level
  // index into market.collateralParams (what liquidate() takes), NOT a position in this array.
  struct CollateralSlot {
    uint8 index; uint128 amt;
    uint256 price; uint256 maxLif; uint256 lltv;
  }

  struct LensOut {
    bool valid; bool hasDebt; bool healthy; bool locked; bool gateAllows;
    uint64 blockTimestamp;
    uint128 debt; uint128 maxDebt; uint128 badDebt; uint128 activatedBitmap;
    // Every activated slot, in DESCENDING market index (the msb walk order) -- not ranked. Slot
    // choice is the off-chain planner's job: it ranks by expected surplus, which the chain cannot
    // know because it depends on whether a slot needs a swap at all.
    CollateralSlot[] collaterals;
    Market market;
  }



  function _mulDivDown(uint256 x, uint256 y, uint256 d) internal pure returns (uint256) { return (x * y) / d; }
  function _mulDivUp(uint256 x, uint256 y, uint256 d) internal pure returns (uint256) { return (x * y + (d - 1)) / d; }
  // Derives maxLif from (lltv, liquidationCursor), mirroring ConstantsLib.maxLif @ 336b924a exactly
  // (mulDivDown on both divisions): maxLif = WAD / (WAD - cursor*(WAD-lltv)/WAD). The on-chain
  // Market now stores liquidationCursor instead of the pre-derived maxLif (no backticks here: they
  // would terminate the sol tagged-template).
  function _maxLif(uint256 lltv, uint256 liquidationCursor) internal pure returns (uint256) {
    return _mulDivDown(WAD, WAD, WAD - _mulDivDown(liquidationCursor, WAD - lltv, WAD));
  }
  function _zeroFloorSub(uint256 x, uint256 y) internal pure returns (uint256) { return x > y ? x - y : 0; }
  function _msb(uint128 bitmap) internal pure returns (uint256 res) { while (bitmap >> (res + 1) != 0) { res++; } }
  function _clearBit(uint128 bitmap, uint256 bit) internal pure returns (uint128) { return uint128(bitmap & ~(uint128(1) << bit)); }
  // Set-bit count, so _collectSlots can size its array without carrying a second loop's locals
  // alongside its own (this contract is already at the stack-depth limit -- see vitest.config.ts).
  function _countActivated(uint128 bitmap) internal pure returns (uint256 res) {
    while (bitmap != 0) { res += bitmap & 1; bitmap >>= 1; }
  }

  // Must not revert for an input it simply cannot price: a declined element is indistinguishable
  // from one the provider refused, so an unpriceable input is data (valid=false) and a revert is
  // left to mean something is wrong.
  //
  // Running out of gas is NOT such an input. EIP-150 keeps 1/64 of the forwarded gas in this frame,
  // so a child that died of gas still lets the catch run and would be recorded as a SERVED
  // valid=false row — which the envelope never retries and declined:'throw' cannot see, silently
  // dropping the position from the liquidation set. Returndata cannot tell the two apart, but gas
  // can: a child that consumed essentially its whole allowance ran out, so re-revert and let the
  // element be declined (loud) rather than answered wrongly (silent).
  function computeOne(Input calldata e) external view returns (LensOut memory o) {
    uint256 forwarded = gasleft();
    try this.computeOneInner(e) returns (LensOut memory r) {
      o = r;
    } catch {
      if (gasleft() < forwarded / 32) revert();
      // o keeps its zeroed (valid=false) default.
    }
  }

  function computeOneInner(Input calldata e) external view returns (LensOut memory o) {
    // Reverts MarketNotCreated for an unknown id — caught by computeOne, which leaves a zeroed
    // (valid=false) row. A successful read is the canonical Market for this id.
    Market memory market = MIDNIGHT.toMarket(e.id);

    o.blockTimestamp = uint64(block.timestamp);
    o.valid = true;
    o.market = market;

    uint256 debt = uint256(MIDNIGHT.debt(e.id, e.borrower));
    o.debt = uint128(debt);
    o.hasDebt = debt > 0;
    o.locked = MIDNIGHT.liquidationLocked(e.id, e.borrower);

    address gate = market.liquidatorGate;
    if (gate == address(0)) {
      o.gateAllows = true;
    } else {
      try ILiquidatorGate(gate).canLiquidate(e.caller) returns (bool ok) { o.gateAllows = ok; }
      catch { o.gateAllows = false; }
    }

    o.activatedBitmap = MIDNIGHT.collateralBitmap(e.id, e.borrower);
    _accumulate(o, market, e.id, e.borrower, debt);
    _collectSlots(o, market, e.id, e.borrower);
  }

  // Two passes (not one combined loop): a single loop carrying maxDebt/badDebt AND building the slot
  // array exceeds the EVM stack ("stack too deep"). Each slot is re-read in _collectSlots — acceptable
  // for a read-only lens. _accumulate mirrors the liquidate() loop exactly (maxDebt down-down; badDebt
  // ceil-ceil, seeded with debt and floored at 0 per slot).
  function _accumulate(LensOut memory o, Market memory market, bytes32 id, address borrower, uint256 debt) private view {
    uint256 maxDebt;
    uint256 badDebt = debt;
    uint128 work = o.activatedBitmap;
    while (work != 0) {
      uint256 idx = _msb(work);
      CollateralParams memory cp = market.collateralParams[idx];
      uint256 amt = uint256(MIDNIGHT.collateral(id, borrower, idx));
      uint256 price = IOracle(cp.oracle).price();
      maxDebt += _mulDivDown(_mulDivDown(amt, price, ORACLE_PRICE_SCALE), cp.lltv, WAD);
      badDebt = _zeroFloorSub(badDebt, _mulDivUp(_mulDivUp(amt, price, ORACLE_PRICE_SCALE), WAD, _maxLif(cp.lltv, cp.liquidationCursor)));
      work = _clearBit(work, idx);
    }
    o.maxDebt = uint128(maxDebt);
    o.badDebt = uint128(badDebt);
    o.healthy = maxDebt >= debt; // matches isHealthy's final return
  }

  // Emits every activated slot, in msb (descending index) order. No ranking and no argmax: which
  // slot is worth liquidating depends on whether it needs a swap, which is off-chain knowledge.
  function _collectSlots(LensOut memory o, Market memory market, bytes32 id, address borrower) private view {
    o.collaterals = new CollateralSlot[](_countActivated(o.activatedBitmap));
    uint128 work = o.activatedBitmap;
    uint256 n;
    while (work != 0) {
      uint256 idx = _msb(work);
      CollateralParams memory cp = market.collateralParams[idx];
      o.collaterals[n] = CollateralSlot({
        index: uint8(idx),
        amt: MIDNIGHT.collateral(id, borrower, idx),
        price: IOracle(cp.oracle).price(),
        maxLif: _maxLif(cp.lltv, cp.liquidationCursor),
        lltv: cp.lltv
      });
      n++;
      work = _clearBit(work, idx);
    }
  }
}