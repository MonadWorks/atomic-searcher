// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;
import "../src/StateLens.sol";

contract LensPoolMock {
    int24 public currentTick;
    int24 public spacing;
    uint128 public liquidity = 1e18;
    uint24 public fee = 500;
    mapping(int16 => uint256) public tickBitmap;
    mapping(int24 => uint128) private gross;
    uint32 public observedAt;
    constructor(int24 tick_, int24 spacing_) { currentTick = tick_; spacing = spacing_; }
    function slot0() external view returns (uint160, int24, uint16) { return (uint160(1 << 96), currentTick, 0); }
    function observations(uint256) external view returns (uint32, int56, uint160, bool) { return (observedAt, 0, 0, true); }
    function setObservedAt(uint32 value) external { observedAt = value; }
    function add(int24 tick_) external {
        require(tick_ % spacing == 0);
        int24 c = tick_ / spacing;
        tickBitmap[int16(c >> 8)] |= uint256(1) << uint8(uint24(c));
        gross[tick_] = 100;
    }
    function ticks(int24 tick_) external view returns (uint128, int128) { return (gross[tick_], int128(-10)); }
}

contract StateLensTest {
    function testFuzzPackedEqualsIndependentTypedEncoding(uint8 depthSeed, bool futureClock) public {
        StateLens lens = new StateLens();
        LensPoolMock pool = new LensPoolMock(-1, 10);
        for (int24 i = -65; i <= 65; ++i) pool.add(i*10);
        if (futureClock) pool.setObservedAt(uint32(block.timestamp)+1);
        address[] memory pools = new address[](3);
        pools[0] = address(pool); pools[1] = address(0xbad); pools[2] = address(pool);
        int24[] memory spacings = new int24[](3);
        spacings[0] = 10; spacings[1] = 1; spacings[2] = 10;
        uint8[] memory depths = new uint8[](3);
        depths[0] = depthSeed%64+1; depths[1] = 4; depths[2] = 64;
        (StateLens.State[] memory states, uint256 height, uint256 stamp) = lens.readWithDepths(pools, spacings, 2, depths);
        bytes memory expected = abi.encodePacked(uint8(1), height, stamp, uint16(3));
        for (uint256 i; i < states.length; ++i) {
            StateLens.State memory s = states[i];
            expected = bytes.concat(expected, abi.encodePacked(s.pool, s.ok));
            if (!s.ok) continue;
            expected = bytes.concat(expected, abi.encodePacked(s.price, s.tick, s.liquidity, s.fee, uint8(s.words.length)));
            for (uint256 j; j < s.words.length; ++j)
                expected = bytes.concat(expected, abi.encodePacked(s.words[j], s.bitmap[j]));
            expected = bytes.concat(expected, abi.encodePacked(uint8(s.ticks.length)));
            for (uint256 j; j < s.ticks.length; ++j)
                expected = bytes.concat(expected, abi.encodePacked(s.ticks[j].index, s.ticks[j].gross, s.ticks[j].net));
        }
        bytes memory packed = lens.readPacked(pools, spacings, 2, depths);
        require(packed.length == expected.length && keccak256(packed) == keccak256(expected));
        require(packed.length < abi.encode(states, height, stamp).length);
    }
    function testFutureOracleUpdateInvalidatesQuoteAndFinalClockGuard() public {
        StateLens lens = new StateLens();
        LensPoolMock pool = new LensPoolMock(0, 1);
        address[] memory pools = new address[](1); pools[0] = address(pool);
        int24[] memory spacings = new int24[](1); spacings[0] = 1;
        pool.setObservedAt(uint32(block.timestamp)+1);
        (StateLens.State[] memory states,,) = lens.read(pools, spacings, 1, 1);
        require(!states[0].ok);
        (bool valid,,) = lens.checkClock(pools); require(!valid);
        pool.setObservedAt(uint32(block.timestamp));
        (valid,,) = lens.checkClock(pools); require(valid);
        (states,,) = lens.read(pools, spacings, 1, 1); require(states[0].ok);
    }
    function testTargetedDeepReadKeepsAllPoolsInOneObservation() public {
        StateLens lens = new StateLens();
        LensPoolMock pool = new LensPoolMock(0, 1);
        for (int24 i = -12; i <= 12; ++i) pool.add(i);
        address[] memory pools = new address[](2); pools[0] = address(pool); pools[1] = address(pool);
        int24[] memory spacings = new int24[](2); spacings[0] = 1; spacings[1] = 1;
        uint8[] memory depths = new uint8[](2); depths[0] = 4; depths[1] = 64;
        (StateLens.State[] memory states, uint256 number, uint256 timestamp) = lens.readWithDepths(pools, spacings, 2, depths);
        require(states[0].ok && states[1].ok && states[0].ticks.length == 8 && states[1].ticks.length == 25);
        require(states[0].price == states[1].price && number == block.number && timestamp == block.timestamp);
        depths[1] = 65;
        try lens.readWithDepths(pools, spacings, 2, depths) { revert("excess depth accepted"); } catch {}
    }
    function testNegativeFloorAndNearestTicksRetainMissingBitmapBits() public {
        StateLens lens = new StateLens();
        LensPoolMock pool = new LensPoolMock(-1, 10);
        pool.add(-2580); pool.add(-20); pool.add(-10); pool.add(0); pool.add(10);
        StateLens.State memory s = lens.readPool(address(pool), 10, 2, 1);
        require(s.ok && s.tick == -1 && s.words[2] == -1);
        require(s.ticks.length == 2 && s.ticks[0].index == -10 && s.ticks[1].index == 0);
        require(s.ticks[0].gross == 100 && s.ticks[0].net == -10);
        require(s.bitmap[2] & (uint256(1) << 254) != 0); // -20 remains initialized but uncached.
        require(s.bitmap[3] & 2 != 0); // +10 also remains initialized.
    }
    function testExactBoundaryAndCrossWordSelection() public {
        StateLens lens = new StateLens();
        LensPoolMock pool = new LensPoolMock(2550, 10);
        pool.add(2540); pool.add(2550); pool.add(2560); pool.add(2570);
        StateLens.State memory s = lens.readPool(address(pool), 10, 1, 2);
        require(s.ticks.length == 4 && s.ticks[0].index == 2550 && s.ticks[1].index == 2540);
        require(s.ticks[2].index == 2560 && s.ticks[3].index == 2570);
    }
    function testUnavailablePoolDoesNotInventStateOrLoseFollowingPool() public {
        StateLens lens = new StateLens();
        LensPoolMock pool = new LensPoolMock(0, 1);
        address[] memory pools = new address[](2); pools[0] = address(0xbad); pools[1] = address(pool);
        int24[] memory spacings = new int24[](2); spacings[0] = 1; spacings[1] = 1;
        (StateLens.State[] memory states, uint256 number, uint256 timestamp) = lens.read(pools, spacings, 2, 4);
        require(!states[0].ok && states[0].pool == address(0xbad) && states[1].ok);
        require(number == block.number && timestamp == block.timestamp);
    }
}
