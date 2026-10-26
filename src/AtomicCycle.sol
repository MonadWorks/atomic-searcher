// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;

interface IERC20Cycle {
    function balanceOf(address) external view returns (uint256);
    function transfer(address, uint256) external returns (bool);
}

interface ICLCycle {
    function factory() external view returns (address);
    function token0() external view returns (address);
    function token1() external view returns (address);
    function fee() external view returns (uint24);
    function tickSpacing() external view returns (int24);
    function swap(address, bool, int256, uint160, bytes calldata) external returns (int256, int256);
}

interface IV2Cycle {
    function getReserves() external view returns (uint112, uint112, uint32);
    function swap(uint256, uint256, address, bytes calldata) external;
}
interface IV2FactoryCycle {
    function getPair(address, address) external view returns (address);
}
interface IV3FactoryCycle {
    function getPool(address, address, uint24) external view returns (address);
}

interface ISlipFactoryCycle {
    function getPool(address, address, int24) external view returns (address);
}

/// @notice Owner-operated, pool-callback-financed two-to-four-hop V2/CL cycles.
/// @dev No external-call router or token allowances. Initial inventory is never
///      counted as profit. Network fees are bounded by the off-chain transaction
///      envelope and included in the required settlement-token profit floor.
contract AtomicCycle {
    uint160 private constant MIN_SQRT = 4295128740;
    uint160 private constant MAX_SQRT = 1461446703485210103287273052203988822378723970341;

    struct PoolInfo { address token0; address token1; address factory; uint8 kind; }
    struct Hop { address pool; bool zeroForOne; uint24 maxFee; uint160 priceLimit; }
    struct Plan {
        Hop[] hops;
        uint256 amountIn;
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
    bool public paused = true;
    mapping(address => uint8) public factoryKind;
    mapping(address => PoolInfo) public pools;
    mapping(address => uint256) public maxInput;
    mapping(uint64 => bool) public usedNonces;

    bool private transient running;
    bool private transient waiting;
    bool private transient rootCallback;
    address private transient activePool;
    address private transient activeInput;
    address private transient activeOutput;
    uint256 private transient inputLimit;
    uint256 private transient outputBefore;
    uint256 private transient expectedV2Output;
    uint256 private transient settlementBefore;
    bytes32 private transient expectedCallbackHash;

    event PoolRegistered(address indexed pool, address indexed factory, uint8 kind);
    event CycleExecuted(uint64 indexed nonce, address indexed settlement, uint256 input, uint256 profit);

    error Unauthorized();
    error InvalidPlan();
    error InvalidPool();
    error BadCallback();
    error InsufficientProfit();
    error TransferFailed();

    modifier onlyOwner() {
        if (msg.sender != owner) revert Unauthorized();
        _;
    }

    modifier idle() {
        if (running) revert BadCallback();
        _;
    }

    constructor(address owner_, uint256 chainId_, address[] memory factories, uint8[] memory kinds,
                address[] memory settlements, uint256[] memory caps) {
        if (owner_ == address(0) || chainId_ != block.chainid || factories.length != kinds.length ||
            settlements.length != caps.length || settlements.length == 0) revert InvalidPlan();
        owner = owner_;
        chainId = chainId_;
        for (uint256 i; i < factories.length; ++i) {
            if (factories[i].code.length == 0 || kinds[i] < 1 || kinds[i] > 3) revert InvalidPool();
            factoryKind[factories[i]] = kinds[i];
        }
        for (uint256 i; i < settlements.length; ++i) {
            if (settlements[i].code.length == 0 || caps[i] == 0 || caps[i] >= 1 << 255) revert InvalidPlan();
            maxInput[settlements[i]] = caps[i];
        }
    }

    function setPaused(bool value) external onlyOwner idle { paused = value; }

    function registerPools(address[] calldata addresses) external onlyOwner idle {
        for (uint256 i; i < addresses.length; ++i) {
            _registerPool(addresses[i]);
        }
    }

    function _registerPool(address pool) private {
            if (pool.code.length == 0) revert InvalidPool();
            address factory = ICLCycle(pool).factory();
            uint8 kind = factoryKind[factory];
            address token0 = ICLCycle(pool).token0();
            address token1 = ICLCycle(pool).token1();
            if (kind == 0 || token0 == address(0) || token0 >= token1) revert InvalidPool();
            address actual;
            if (kind == 1) {
                actual = IV3FactoryCycle(factory).getPool(token0, token1, ICLCycle(pool).fee());
            } else if (kind == 3) {
                actual = IV2FactoryCycle(factory).getPair(token0, token1);
            } else {
                actual = ISlipFactoryCycle(factory).getPool(token0, token1, ICLCycle(pool).tickSpacing());
            }
            if (actual != pool) revert InvalidPool();
            pools[pool] = PoolInfo(token0, token1, factory, kind);
            emit PoolRegistered(pool, factory, kind);
    }

    function execute(Plan calldata plan) external onlyOwner idle returns (uint256 profit) {
        if (paused || block.chainid != chainId || plan.hops.length < 2 || plan.hops.length > 4 ||
            plan.minProfit == 0 || plan.amountIn == 0 || plan.amountIn >= 1 << 255 ||
            tx.gasprice > plan.maxGasPrice || block.timestamp > plan.deadline ||
            plan.deadline > block.timestamp + 30 || block.number <= plan.anchorBlock ||
            block.number > plan.lastBlock || plan.lastBlock > plan.anchorBlock + 2 ||
            plan.anchorHash == bytes32(0) || blockhash(plan.anchorBlock) != plan.anchorHash || usedNonces[plan.nonce]) revert InvalidPlan();

        uint256 count = plan.hops.length;
        address[] memory tokens = new address[](count);
        uint256[] memory beforeBalances = new uint256[](count);
        address expectedInput;
        for (uint256 i; i < count; ++i) {
            Hop calldata hop = plan.hops[i];
            if (pools[hop.pool].kind == 0) _registerPool(hop.pool);
            PoolInfo memory info = pools[hop.pool];
            if (info.kind == 0 || (info.kind == 3 ? hop.priceLimit != 0 :
                (hop.priceLimit < MIN_SQRT || hop.priceLimit > MAX_SQRT))) revert InvalidPool();
            address tokenIn = hop.zeroForOne ? info.token0 : info.token1;
            address tokenOut = hop.zeroForOne ? info.token1 : info.token0;
            if (i > 0 && tokenIn != expectedInput) revert InvalidPlan();
            for (uint256 j; j < i; ++j) {
                if (plan.hops[j].pool == hop.pool || tokens[j] == tokenIn) revert InvalidPlan();
            }
            tokens[i] = tokenIn;
            beforeBalances[i] = IERC20Cycle(tokenIn).balanceOf(address(this));
            expectedInput = tokenOut;
        }
        if (expectedInput != tokens[0] || plan.amountIn > maxInput[tokens[0]]) revert InvalidPlan();
        running = true;
        usedNonces[plan.nonce] = true;
        settlementBefore = beforeBalances[0];
        _swap(plan.hops[0], plan.amountIn, abi.encode(plan), true);

        uint256 end = IERC20Cycle(tokens[0]).balanceOf(address(this));
        if (end < beforeBalances[0] + plan.minProfit) revert InsufficientProfit();
        profit = end - beforeBalances[0];
        for (uint256 i = 1; i < count; ++i) {
            if (IERC20Cycle(tokens[i]).balanceOf(address(this)) < beforeBalances[i]) revert InsufficientProfit();
        }
        uint256 ownerBefore = IERC20Cycle(tokens[0]).balanceOf(owner);
        _pay(tokens[0], owner, profit);
        if (IERC20Cycle(tokens[0]).balanceOf(owner) < ownerBefore + profit ||
            IERC20Cycle(tokens[0]).balanceOf(address(this)) < beforeBalances[0]) revert TransferFailed();
        running = false;
        emit CycleExecuted(plan.nonce, tokens[0], plan.amountIn, profit);
    }

    function _swap(Hop memory hop, uint256 amount, bytes memory callbackData, bool isRoot)
        private returns (uint256 output)
    {
        if (amount == 0 || amount >= 1 << 255 || waiting) revert InvalidPlan();
        PoolInfo memory info = pools[hop.pool];
        if (info.kind == 3 ? hop.maxFee < 3000 : ICLCycle(hop.pool).fee() > hop.maxFee) revert InvalidPlan();
        activePool = hop.pool;
        activeInput = hop.zeroForOne ? info.token0 : info.token1;
        activeOutput = hop.zeroForOne ? info.token1 : info.token0;
        inputLimit = amount;
        outputBefore = IERC20Cycle(activeOutput).balanceOf(address(this));
        expectedCallbackHash = keccak256(callbackData);
        rootCallback = isRoot;
        waiting = true;
        if (info.kind == 3) {
            (uint112 r0, uint112 r1,) = IV2Cycle(hop.pool).getReserves();
            (uint256 rin, uint256 rout) = hop.zeroForOne ? (uint256(r0),uint256(r1)) : (uint256(r1),uint256(r0));
            if (rin == 0 || rout == 0 || amount > type(uint112).max-rin) revert InvalidPlan();
            output = amount*997*rout/(rin*1000+amount*997);
            if (output == 0 || output >= rout) revert InvalidPlan();
            expectedV2Output = output;
            IV2Cycle(hop.pool).swap(hop.zeroForOne ? 0 : output, hop.zeroForOne ? output : 0, address(this), callbackData);
            if (waiting) revert BadCallback();
            return output;
        }
        (int256 a0, int256 a1) = ICLCycle(hop.pool).swap(address(this), hop.zeroForOne,
                                                       int256(amount), hop.priceLimit, callbackData);
        int256 owed = hop.zeroForOne ? a0 : a1;
        int256 got = hop.zeroForOne ? a1 : a0;
        if (waiting || owed <= 0 || uint256(owed) != amount || got >= 0 || got == type(int256).min) revert BadCallback();
        output = uint256(-got);
    }

    function uniswapV3SwapCallback(int256 amount0, int256 amount1, bytes calldata callbackData) external {
        if (!running || !waiting || msg.sender != activePool ||
            keccak256(callbackData) != expectedCallbackHash) revert BadCallback();
        PoolInfo memory info = pools[msg.sender];
        if (info.kind != 1 && info.kind != 2) revert BadCallback();
        bool zeroForOne = activeInput == info.token0;
        int256 debtDelta = zeroForOne ? amount0 : amount1;
        int256 outputDelta = zeroForOne ? amount1 : amount0;
        if (debtDelta <= 0 || outputDelta >= 0 || outputDelta == type(int256).min ||
            uint256(debtDelta) != inputLimit) revert BadCallback();
        uint256 received = uint256(-outputDelta);
        _settle(uint256(debtDelta), received, callbackData);
    }

    function uniswapV2Call(address sender, uint256 amount0, uint256 amount1, bytes calldata callbackData) external {
        if (!running || !waiting || sender != address(this) || msg.sender != activePool ||
            pools[msg.sender].kind != 3 || keccak256(callbackData) != expectedCallbackHash) revert BadCallback();
        bool zeroForOne = activeInput == pools[msg.sender].token0;
        uint256 received = zeroForOne ? amount1 : amount0;
        if ((zeroForOne ? amount0 : amount1) != 0 || received != expectedV2Output) revert BadCallback();
        _settle(inputLimit, received, callbackData);
    }

    function _settle(uint256 debt, uint256 received, bytes calldata callbackData) private {
        if (IERC20Cycle(activeOutput).balanceOf(address(this)) < outputBefore + received) revert BadCallback();
        address debtToken = activeInput;
        bool isRoot = rootCallback;
        waiting = false;
        rootCallback = false;
        if (isRoot) {
            Plan memory plan = abi.decode(callbackData, (Plan));
            for (uint256 i = 1; i < plan.hops.length; ++i) {
                received = _swap(plan.hops[i], received, abi.encode(plan.nonce, i), false);
            }
            if (IERC20Cycle(debtToken).balanceOf(address(this)) < settlementBefore + debt + plan.minProfit)
                revert InsufficientProfit();
        }
        _pay(debtToken, msg.sender, debt);
    }

    function _pay(address token, address to, uint256 amount) private {
        (bool ok, bytes memory result) = token.call(abi.encodeCall(IERC20Cycle.transfer, (to, amount)));
        if (!ok || (result.length != 0 && (result.length != 32 || !abi.decode(result, (bool))))) revert TransferFailed();
    }

    function recover(address token, uint256 amount) external onlyOwner idle { _pay(token, owner, amount); }
}
