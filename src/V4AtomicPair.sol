// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;

struct V4Key { address currency0; address currency1; uint24 fee; int24 tickSpacing; address hooks; }
struct V4Swap { bool zeroForOne; int256 amountSpecified; uint160 sqrtPriceLimitX96; }
interface IV4PairManager {
    function unlock(bytes calldata) external returns (bytes memory);
    function swap(V4Key calldata, V4Swap calldata, bytes calldata) external returns (int256);
    function take(address currency, address to, uint256 amount) external;
}
interface IV4PairToken { function balanceOf(address) external view returns (uint256); }

/// @notice Isolated research executor: two hookless ERC20 V4 pools, no input inventory.
/// @dev Manager deltas finance both swaps; only the net settlement credit is taken.
///      The off-chain caller must include network cost in minProfit. Not wired to trading.
contract V4AtomicPair {
    uint160 private constant MIN_SQRT = 4295128740;
    uint160 private constant MAX_SQRT = 1461446703485210103287273052203988822378723970341;
    struct Plan {
        V4Key sell;
        V4Key buy;
        address settlement;
        uint128 output;
        uint160 sellLimit;
        uint160 buyLimit;
        uint256 minProfit;
        uint256 maxGasPrice;
        uint64 deadline;
        uint64 anchorBlock;
        bytes32 anchorHash;
        uint64 lastBlock;
        uint64 nonce;
    }
    address public immutable owner;
    uint256 public immutable chainId;
    IV4PairManager public immutable manager;
    bool public paused = true;
    mapping(address => uint128) public maxOutput;
    mapping(uint64 => bool) public usedNonces;
    bool private transient running;
    bool private transient waiting;
    bytes32 private transient payloadHash;
    error Unauthorized();
    error InvalidPlan();
    error InvalidPool();
    error BadCallback();
    error InsufficientProfit();
    error PartialFill();
    event CycleExecuted(uint64 indexed nonce, address indexed settlement, uint256 output, uint256 profit);

    constructor(address owner_, address manager_, uint256 chainId_, address settlement_, uint128 cap) {
        if (owner_ == address(0) || manager_.code.length == 0 || settlement_.code.length == 0 ||
            chainId_ != block.chainid || cap == 0 || cap > uint128(type(int128).max)) revert InvalidPlan();
        owner = owner_; manager = IV4PairManager(manager_); chainId = chainId_; maxOutput[settlement_] = cap;
    }
    modifier onlyOwner() { if (msg.sender != owner) revert Unauthorized(); _; }
    function setPaused(bool value) external onlyOwner {
        if (running) revert BadCallback();
        paused = value;
    }
    function execute(Plan calldata p) external onlyOwner returns (uint256 profit) {
        if (running) revert BadCallback();
        if (paused || block.chainid != chainId || p.output == 0 || p.output > maxOutput[p.settlement] ||
            p.sellLimit < MIN_SQRT || p.sellLimit > MAX_SQRT || p.buyLimit < MIN_SQRT || p.buyLimit > MAX_SQRT ||
            p.minProfit == 0 || tx.gasprice > p.maxGasPrice || block.timestamp > p.deadline ||
            p.deadline > block.timestamp + 30 || block.number <= p.anchorBlock ||
            block.number > p.lastBlock || p.lastBlock > p.anchorBlock + 2 ||
            p.anchorHash == bytes32(0) || blockhash(p.anchorBlock) != p.anchorHash || usedNonces[p.nonce]) revert InvalidPlan();
        _key(p.sell); _key(p.buy);
        if (p.sell.currency0 != p.buy.currency0 || p.sell.currency1 != p.buy.currency1 ||
            (p.settlement != p.sell.currency0 && p.settlement != p.sell.currency1) ||
            keccak256(abi.encode(p.sell)) == keccak256(abi.encode(p.buy))) revert InvalidPool();
        running = true; waiting = true; usedNonces[p.nonce] = true;
        bytes memory encoded = abi.encode(p); payloadHash = keccak256(encoded);
        uint256 beforeBalance = IV4PairToken(p.settlement).balanceOf(owner);
        profit = abi.decode(manager.unlock(encoded), (uint256));
        if (waiting || profit < p.minProfit || IV4PairToken(p.settlement).balanceOf(owner) < beforeBalance + profit)
            revert InsufficientProfit();
        running = false;
        emit CycleExecuted(p.nonce, p.settlement, p.output, profit);
    }
    function unlockCallback(bytes calldata encoded) external returns (bytes memory) {
        if (msg.sender != address(manager) || !running || !waiting || keccak256(encoded) != payloadHash) revert BadCallback();
        waiting = false;
        Plan memory p = abi.decode(encoded, (Plan));
        bool settlementIs0 = p.settlement == p.sell.currency0;
        int256 first = manager.swap(p.sell, V4Swap(!settlementIs0, int256(uint256(p.output)), p.sellLimit), "");
        (int256 received, int256 owedAsset) = _split(first, settlementIs0);
        if (received != int256(uint256(p.output)) || owedAsset >= 0) revert PartialFill();
        int256 second = manager.swap(p.buy, V4Swap(settlementIs0, -owedAsset, p.buyLimit), "");
        (int256 paid, int256 recoveredAsset) = _split(second, settlementIs0);
        if (recoveredAsset != -owedAsset || paid >= 0) revert PartialFill();
        int256 net = received + paid;
        if (net <= 0 || uint256(net) < p.minProfit) revert InsufficientProfit();
        // Both intermediate-asset deltas cancel exactly; take clears the only remaining credit.
        manager.take(p.settlement, owner, uint256(net));
        return abi.encode(uint256(net));
    }
    function _split(int256 packed, bool settlementIs0) private pure returns (int256 settlement, int256 asset) {
        int256 a0 = int256(int128(packed >> 128));
        int256 a1 = int256(int128(uint128(uint256(packed))));
        return settlementIs0 ? (a0, a1) : (a1, a0);
    }
    function _key(V4Key calldata k) private pure {
        if (k.currency0 == address(0) || k.currency0 >= k.currency1 || k.hooks != address(0) ||
            k.fee >= 1_000_000 || k.tickSpacing <= 0 || k.tickSpacing > type(int16).max) revert InvalidPool();
    }
}
