// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;

interface IStatePool {
    function liquidity() external view returns (uint128);
    function fee() external view returns (uint24);
    function tickBitmap(int16) external view returns (uint256);
}

/// @notice One eth_call reads every returned pool from a single EVM state.
/// @dev Used through a state override; never requires a paid deployment.
contract StateLens {
    struct Tick { int24 index; uint128 gross; int128 net; }
    struct State {
        address pool; bool ok; uint160 price; int24 tick; uint128 liquidity; uint24 fee;
        int16[] words; uint256[] bitmap; Tick[] ticks;
    }
    function poolClockValid(address pool, bytes memory slot) private view returns (bool) {
        if (slot.length < 96) return false;
        (,,uint16 index) = abi.decode(slot, (uint160,int24,uint16));
        (bool ok, bytes memory observation) = pool.staticcall(abi.encodeWithSignature("observations(uint256)", uint256(index)));
        if (!ok || observation.length < 32) return false;
        uint32 stamp = abi.decode(observation, (uint32));
        return stamp <= uint32(block.timestamp);
    }
    /// @dev A pending overlay must not be evaluated before its latest oracle
    ///      update. Such a rewound clock can make dynamic-fee modules fall back
    ///      to their minimum fee and invent an executable-looking price gap.
    function checkClock(address[] calldata pools) external view returns (bool valid, uint256 contextBlock, uint256 contextTime) {
        require(pools.length > 0 && pools.length <= 4);
        for (uint256 i; i < pools.length; ++i) {
            (bool ok, bytes memory slot) = pools[i].staticcall(abi.encodeWithSignature("slot0()"));
            if (!ok || !poolClockValid(pools[i], slot)) return (false, block.number, block.timestamp);
        }
        return (true, block.number, block.timestamp);
    }
    function read(address[] calldata pools, int24[] calldata spacings, uint8 radius, uint8 perSide)
        external view returns (State[] memory states, uint256 contextBlock, uint256 contextTime)
    {
        require(pools.length == spacings.length && pools.length <= 400 && radius <= 2 && perSide <= 64);
        states = new State[](pools.length);
        for (uint256 i; i < pools.length; ++i) {
            try this.readPool(pools[i], spacings[i], radius, perSide) returns (State memory s) { states[i] = s; }
            catch { states[i].pool = pools[i]; }
        }
        return (states, block.number, block.timestamp);
    }
    function readPool(address pool, int24 spacing, uint8 radius, uint8 perSide) external view returns (State memory s) {
        require(spacing > 0);
        s.pool = pool;
        (bool ok, bytes memory raw) = pool.staticcall(abi.encodeWithSignature("slot0()"));
        require(ok && poolClockValid(pool, raw));
        (s.price, s.tick) = abi.decode(raw, (uint160, int24));
        s.liquidity = IStatePool(pool).liquidity(); s.fee = IStatePool(pool).fee();
        require(s.price > 0 && s.fee < 1_000_000);
        int24 compressed = s.tick / spacing;
        if (s.tick < 0 && s.tick % spacing != 0) --compressed;
        int16 center = int16(compressed >> 8);
        uint256 count = uint256(radius)*2+1;
        s.words = new int16[](count); s.bitmap = new uint256[](count);
        for (uint256 i; i < count; ++i) {
            s.words[i] = center-int16(uint16(radius))+int16(uint16(i));
            s.bitmap[i] = IStatePool(pool).tickBitmap(s.words[i]);
        }
        Tick[] memory scratch = new Tick[](uint256(perSide)*2);
        uint256 used;
        for (uint256 direction; direction < 2; ++direction) {
            uint256 found;
            for (uint256 offset; offset <= radius && found < perSide; ++offset) {
                uint256 wordIndex = direction == 0 ? uint256(radius)-offset : uint256(radius)+offset;
                uint256 mask = s.bitmap[wordIndex];
                if (offset == 0) {
                    uint256 bit = uint8(uint24(compressed));
                    uint256 lower = bit == 255 ? type(uint256).max : (uint256(1) << (bit+1))-1;
                    mask &= direction == 0 ? lower : ~lower;
                }
                while (mask != 0 && found < perSide) {
                    uint8 bit = mostSignificant(direction == 0 ? mask : mask & (~mask+1));
                    int24 tick = (int24(s.words[wordIndex])*256+int24(uint24(bit)))*spacing;
                    (ok, raw) = pool.staticcall(abi.encodeWithSignature("ticks(int24)", tick));
                    require(ok && raw.length >= 64);
                    (uint128 gross, int128 net) = abi.decode(raw, (uint128, int128));
                    scratch[used++] = Tick(tick, gross, net); ++found;
                    mask &= ~(uint256(1) << bit);
                }
            }
        }
        s.ticks = new Tick[](used);
        for (uint256 i; i < used; ++i) s.ticks[i] = scratch[i];
        s.ok = true;
    }
    function readWithDepths(address[] calldata pools, int24[] calldata spacings, uint8 radius, uint8[] calldata depths)
        external view returns (State[] memory states, uint256 contextBlock, uint256 contextTime)
    {
        require(pools.length == spacings.length && pools.length == depths.length && pools.length <= 400 && radius <= 2);
        states = new State[](pools.length);
        for (uint256 i; i < pools.length; ++i) {
            require(depths[i] > 0 && depths[i] <= 64);
            try this.readPool(pools[i], spacings[i], radius, depths[i]) returns (State memory s) { states[i] = s; }
            catch { states[i].pool = pools[i]; }
        }
        return (states, block.number, block.timestamp);
    }
    /// @notice Versioned, fixed-width encoding of the SAME atomic pool read.
    /// @dev Reduces JSON hex response size. No price precision is discarded.
    function readPacked(address[] calldata pools, int24[] calldata spacings, uint8 radius, uint8[] calldata depths)
        external view returns (bytes memory packed)
    {
        require(pools.length == spacings.length && pools.length == depths.length && pools.length <= 400 && radius <= 2);
        State[] memory states = new State[](pools.length);
        uint256 size = 67; // version (1), block (32), timestamp (32), pool count (2)
        for (uint256 i; i < pools.length; ++i) {
            require(depths[i] > 0 && depths[i] <= 64);
            try this.readPool(pools[i], spacings[i], radius, depths[i]) returns (State memory s) {
                states[i] = s;
                size += 65 + s.words.length*34 + s.ticks.length*35;
            } catch { states[i].pool = pools[i]; size += 21; }
        }
        // A final unaligned mstore may touch up to 31 extra bytes; reserve them.
        packed = new bytes(size+32);
        uint256 pos = put(packed, 0, 1, 1);
        pos = put(packed, pos, block.number, 32);
        pos = put(packed, pos, block.timestamp, 32);
        pos = put(packed, pos, pools.length, 2);
        for (uint256 i; i < states.length; ++i) {
            State memory s = states[i];
            pos = put(packed, pos, uint160(s.pool), 20);
            pos = put(packed, pos, s.ok ? 1 : 0, 1);
            if (!s.ok) continue;
            pos = put(packed, pos, s.price, 20);
            pos = put(packed, pos, uint24(s.tick), 3);
            pos = put(packed, pos, s.liquidity, 16);
            pos = put(packed, pos, s.fee, 3);
            pos = put(packed, pos, s.words.length, 1);
            for (uint256 j; j < s.words.length; ++j) {
                pos = put(packed, pos, uint16(s.words[j]), 2);
                pos = put(packed, pos, s.bitmap[j], 32);
            }
            pos = put(packed, pos, s.ticks.length, 1);
            for (uint256 j; j < s.ticks.length; ++j) {
                pos = put(packed, pos, uint24(s.ticks[j].index), 3);
                pos = put(packed, pos, s.ticks[j].gross, 16);
                pos = put(packed, pos, uint128(s.ticks[j].net), 16);
            }
        }
        assert(pos == size);
        assembly ("memory-safe") { mstore(packed, size) }
    }
    function contextClock() external view returns (bool,uint256,uint256) {return (true,block.number,block.timestamp);}

    function checkMixedClock(address[] calldata pools, int24[] calldata spacings)
        external view returns (bool valid, uint256 contextBlock, uint256 contextTime)
    {
        require(pools.length == spacings.length && pools.length > 0 && pools.length <= 8);
        for (uint256 i; i < pools.length; ++i) {
            if (spacings[i] == -1) {
                try this.readAerodromeClock(pools[i]) {} catch { return (false, block.number, block.timestamp); }
            } else if (spacings[i] == 0) {
                try this.readV2(pools[i]) returns (uint112, uint112) {} catch { return (false, block.number, block.timestamp); }
            } else {
                (bool ok, bytes memory slot) = pools[i].staticcall(abi.encodeWithSignature("slot0()"));
                if (!ok || !poolClockValid(pools[i], slot)) return (false, block.number, block.timestamp);
            }
        }
        return (true, block.number, block.timestamp);
    }

    /// @dev Aerodrome uses full-width reserves and a full-width timestamp.
    function readAerodromeClock(address pool) external view {
        (bool ok, bytes memory raw) = pool.staticcall(abi.encodeWithSignature("getReserves()"));
        require(ok && raw.length == 96);
        (uint256 r0,uint256 r1,uint256 stamp) = abi.decode(raw,(uint256,uint256,uint256));
        require(r0 > 0 && r1 > 0 && stamp <= block.timestamp);
    }

    function readV2(address pool) external view returns (uint112 r0, uint112 r1) {
        (bool ok, bytes memory raw) = pool.staticcall(abi.encodeWithSignature("getReserves()"));
        require(ok && raw.length == 96);
        uint32 stamp;
        (r0,r1,stamp) = abi.decode(raw,(uint112,uint112,uint32));
        require(r0 > 0 && r1 > 0 && stamp <= uint32(block.timestamp));
    }

    /// @notice Version 2: status 0 invalid; 1 CL (v1 layout); 2 V2 (two uint112 reserves, uint24 fee).
    /// @dev spacing=0 is assigned ONLY after factory authentication off chain.
    function readMixedPacked(address[] calldata pools, int24[] calldata spacings, uint8 radius, uint8[] calldata depths)
        external view returns (bytes memory packed)
    {
        require(pools.length == spacings.length && pools.length == depths.length && pools.length <= 400 && radius <= 2);
        State[] memory states = new State[](pools.length);
        uint112[] memory r0 = new uint112[](pools.length);
        uint112[] memory r1 = new uint112[](pools.length);
        uint256 size = 67;
        for (uint256 i; i < pools.length; ++i) {
            require(depths[i] > 0 && depths[i] <= 64);
            if (spacings[i] == 0) {
                try this.readV2(pools[i]) returns (uint112 a, uint112 b) { r0[i] = a; r1[i] = b; size += 52; }
                catch { size += 21; }
            } else {
                try this.readPool(pools[i], spacings[i], radius, depths[i]) returns (State memory s) {
                    states[i] = s; size += 65+s.words.length*34+s.ticks.length*35;
                } catch { size += 21; }
            }
        }
        packed = new bytes(size+32);
        uint256 pos = put(packed,0,2,1);
        pos = put(packed,pos,block.number,32);
        pos = put(packed,pos,block.timestamp,32);
        pos = put(packed,pos,pools.length,2);
        for (uint256 i; i < pools.length; ++i) {
            State memory s = states[i];
            pos = put(packed,pos,uint160(pools[i]),20);
            if (r0[i] > 0) {
                pos = put(packed,pos,2,1);
                pos = put(packed,pos,r0[i],14); pos = put(packed,pos,r1[i],14); pos = put(packed,pos,3000,3);
                continue;
            }
            pos = put(packed,pos,s.ok ? 1 : 0,1);
            if (!s.ok) continue;
            pos = put(packed,pos,s.price,20); pos = put(packed,pos,uint24(s.tick),3);
            pos = put(packed,pos,s.liquidity,16); pos = put(packed,pos,s.fee,3);
            pos = put(packed,pos,s.words.length,1);
            for (uint256 j; j < s.words.length; ++j) {
                pos = put(packed,pos,uint16(s.words[j]),2); pos = put(packed,pos,s.bitmap[j],32);
            }
            pos = put(packed,pos,s.ticks.length,1);
            for (uint256 j; j < s.ticks.length; ++j) {
                pos = put(packed,pos,uint24(s.ticks[j].index),3);
                pos = put(packed,pos,s.ticks[j].gross,16); pos = put(packed,pos,uint128(s.ticks[j].net),16);
            }
        }
        assert(pos == size);
        assembly ("memory-safe") { mstore(packed,size) }
    }
    function put(bytes memory target, uint256 pos, uint256 value, uint256 width) private pure returns (uint256) {
        assembly ("memory-safe") {
            mstore(add(add(target, 32), pos), shl(mul(sub(32, width), 8), value))
        }
        return pos+width;
    }
    function mostSignificant(uint256 x) private pure returns (uint8 r) {
        if (x >= 1 << 128) { x >>= 128; r += 128; }
        if (x >= 1 << 64) { x >>= 64; r += 64; }
        if (x >= 1 << 32) { x >>= 32; r += 32; }
        if (x >= 1 << 16) { x >>= 16; r += 16; }
        if (x >= 1 << 8) { x >>= 8; r += 8; }
        if (x >= 1 << 4) { x >>= 4; r += 4; }
        if (x >= 1 << 2) { x >>= 2; r += 2; }
        if (x >= 1 << 1) r += 1;
    }
}
