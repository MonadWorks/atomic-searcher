// SPDX-License-Identifier: GPL-2.0-or-later
pragma solidity ^0.8.25;

// Kept self-contained because soltag compiles this file without an import callback. These structs
// mirror the deployed Midnight interface vendored in interfaces/IMidnight.sol.
struct CrossedBooksCollateralParams {
    address token;
    uint256 lltv;
    uint256 liquidationCursor;
    address oracle;
}

struct CrossedBooksMarket {
    uint256 chainId;
    address midnight;
    address loanToken;
    CrossedBooksCollateralParams[] collateralParams;
    uint256 maturity;
    uint256 rcfThreshold;
    address enterGate;
    address liquidatorGate;
}

struct CrossedBooksOffer {
    CrossedBooksMarket market;
    bool buy;
    address maker;
    uint256 start;
    uint256 expiry;
    uint256 tick;
    bytes32 group;
    address callback;
    bytes callbackData;
    address receiverIfMakerIsSeller;
    address ratifier;
    bool reduceOnly;
    uint128 maxUnits;
    uint128 maxAssets;
    uint256 continuousFeeCap;
}

struct CrossedBooksTake {
    CrossedBooksOffer offer;
    bytes ratifierData;
    uint256 units;
}

interface ICrossedBooksMidnight {
    function take(CrossedBooksOffer memory, bytes memory, uint256, address, address, address, bytes memory)
        external
        returns (uint256 buyerAssets, uint256 sellerAssets);
}

interface ICrossedBooksToken {
    function balanceOf(address) external view returns (uint256);
    function approve(address, uint256) external returns (bool);
    function transfer(address, uint256) external returns (bool);
}

/// @notice Atomically crosses multiple resting Midnight sell offers against multiple buy offers.
/// @dev Permissionless. Sell offers are taken recursively: each onBuy callback takes the next sell
/// offer. The deepest callback sells all received units into the buy offers, approves the aggregate
/// sell cost, and lets the nested callbacks unwind. Positive loan-token balance delta goes to caller.
contract CrossedBooksResolver {
    bytes32 public constant CALLBACK_SUCCESS = keccak256("morpho.midnight.callbackSuccess");
    bytes private constant SSTORE2_PREFIX = hex"600b380380600b5f395ff3";
    address public immutable MIDNIGHT;
    bool private resolving;
    bytes32 private activeMarketHash;
    bytes32 private activePlanHash;
    uint256 private activeSellIndex;
    uint256 private activeBuyerAssets;
    bool private activeCallbackPending;

    error InactiveCallback();
    error InsufficientProfit(uint256 profit, uint256 minimum);
    error InvalidMarket();
    error InvalidOfferCount();
    error InvalidSides();
    error InvalidUnits();
    error NotCrossed(uint256 buyProceeds, uint256 sellCost);
    error ReentrantCall();
    error TokenCallFailed();
    error UnauthorizedCallback();
    error UnbalancedUnits(uint256 sellUnits, uint256 buyUnits);
    error UnexpectedBuyer();
    error UnexpectedCallback();
    error UnexpectedUnits();
    error ZeroAddress();
    event Resolve(
        address indexed caller,
        bytes32 indexed marketHash,
        uint256 sellOfferCount,
        uint256 buyOfferCount,
        uint256 units,
        uint256 profit
    );

    constructor(address midnight) {
        if (midnight == address(0)) revert ZeroAddress();
        MIDNIGHT = midnight;
    }

    function resolve(
        CrossedBooksTake[] calldata sellOffers,
        CrossedBooksTake[] calldata buyOffers,
        uint256 minimumProfit
    ) external returns (uint256 profit) {
        if (resolving) revert ReentrantCall();
        (bytes32 marketHash, address loanToken, uint256 units) = _validatePlan(sellOffers, buyOffers);

        ICrossedBooksToken token = ICrossedBooksToken(loanToken);
        uint256 beforeBalance = token.balanceOf(address(this));
        resolving = true;
        activeMarketHash = marketHash;
        activePlanHash = keccak256(abi.encode(sellOffers, buyOffers));
        activeBuyerAssets = 0;
        _takeSell(sellOffers, buyOffers, 0);
        uint256 afterBalance = token.balanceOf(address(this));
        if (afterBalance < beforeBalance) revert NotCrossed(afterBalance, beforeBalance);
        profit = afterBalance - beforeBalance;
        if (profit == 0 || profit < minimumProfit) revert InsufficientProfit(profit, minimumProfit);

        activeMarketHash = bytes32(0);
        activePlanHash = bytes32(0);
        activeSellIndex = 0;
        activeBuyerAssets = 0;
        activeCallbackPending = false;
        _callOptional(address(token), abi.encodeCall(token.transfer, (msg.sender, profit)));
        resolving = false;
        emit Resolve(msg.sender, marketHash, sellOffers.length, buyOffers.length, units, profit);
    }

    function onBuy(
        bytes32 id,
        CrossedBooksMarket calldata market,
        uint256 buyerAssets,
        uint256 units,
        uint256,
        address buyer,
        bytes calldata data
    ) external returns (bytes32) {
        if (msg.sender != MIDNIGHT) revert UnauthorizedCallback();
        if (!resolving) revert InactiveCallback();
        if (buyer != address(this)) revert UnexpectedBuyer();
        if (id != _marketId(market) || keccak256(abi.encode(market)) != activeMarketHash) revert InvalidMarket();
        _onBuy(market.loanToken, buyerAssets, units, data);
        return CALLBACK_SUCCESS;
    }

    function _onBuy(address loanToken, uint256 buyerAssets, uint256 units, bytes calldata data) internal {
        (CrossedBooksTake[] memory sellOffers, CrossedBooksTake[] memory buyOffers, uint256 sellIndex) =
            abi.decode(data, (CrossedBooksTake[], CrossedBooksTake[], uint256));
        if (
            !activeCallbackPending || keccak256(abi.encode(sellOffers, buyOffers)) != activePlanHash
                || sellIndex != activeSellIndex
        ) revert UnexpectedCallback();
        if (units != sellOffers[sellIndex].units) revert UnexpectedUnits();

        activeCallbackPending = false;
        activeBuyerAssets += buyerAssets;
        if (sellIndex + 1 < sellOffers.length) {
            _takeSell(sellOffers, buyOffers, sellIndex + 1);
        } else {
            _sellCredits(buyOffers, loanToken, activeBuyerAssets);
        }
    }

    function _takeSell(CrossedBooksTake[] memory sellOffers, CrossedBooksTake[] memory buyOffers, uint256 sellIndex)
        internal
    {
        activeSellIndex = sellIndex;
        activeCallbackPending = true;
        CrossedBooksTake memory sell = sellOffers[sellIndex];
        ICrossedBooksMidnight(MIDNIGHT)
            .take(
                sell.offer,
                sell.ratifierData,
                sell.units,
                address(this),
                address(0),
                address(this),
                abi.encode(sellOffers, buyOffers, sellIndex)
            );
        if (activeCallbackPending) revert UnexpectedCallback();
    }

    function _sellCredits(CrossedBooksTake[] memory buyOffers, address loanToken, uint256 sellCost) internal {
        uint256 buyProceeds;
        for (uint256 i; i < buyOffers.length; ++i) {
            CrossedBooksTake memory buy = buyOffers[i];
            (, uint256 proceeds) = ICrossedBooksMidnight(MIDNIGHT)
                .take(buy.offer, buy.ratifierData, buy.units, address(this), address(this), address(0), bytes(""));
            buyProceeds += proceeds;
        }
        if (buyProceeds < sellCost) revert NotCrossed(buyProceeds, sellCost);

        ICrossedBooksToken token = ICrossedBooksToken(loanToken);
        _callOptional(address(token), abi.encodeCall(token.approve, (MIDNIGHT, 0)));
        _callOptional(address(token), abi.encodeCall(token.approve, (MIDNIGHT, sellCost)));
    }

    function _validatePlan(CrossedBooksTake[] calldata sellOffers, CrossedBooksTake[] calldata buyOffers)
        internal
        view
        returns (bytes32 marketHash, address loanToken, uint256 sellUnits)
    {
        if (sellOffers.length == 0 || buyOffers.length == 0) revert InvalidOfferCount();
        marketHash = keccak256(abi.encode(sellOffers[0].offer.market));
        loanToken = sellOffers[0].offer.market.loanToken;

        for (uint256 i; i < sellOffers.length; ++i) {
            CrossedBooksTake calldata sell = sellOffers[i];
            _validateTake(sell, false, marketHash);
            sellUnits += sell.units;
        }

        uint256 buyUnits;
        for (uint256 i; i < buyOffers.length; ++i) {
            CrossedBooksTake calldata buy = buyOffers[i];
            _validateTake(buy, true, marketHash);
            buyUnits += buy.units;
        }
        if (sellUnits != buyUnits) revert UnbalancedUnits(sellUnits, buyUnits);
    }

    function _validateTake(CrossedBooksTake calldata offer, bool buy, bytes32 marketHash) internal view {
        if (offer.units == 0) revert InvalidUnits();
        if (offer.offer.buy != buy) revert InvalidSides();
        if (offer.offer.market.midnight != MIDNIGHT || keccak256(abi.encode(offer.offer.market)) != marketHash) {
            revert InvalidMarket();
        }
    }

    /// @dev Mirrors Midnight's IdLib.toId for the deployed market-id callback argument.
    function _marketId(CrossedBooksMarket memory market) internal pure returns (bytes32) {
        return keccak256(
            abi.encodePacked(
                uint8(0xff),
                market.midnight,
                uint256(0),
                keccak256(abi.encodePacked(SSTORE2_PREFIX, abi.encode(market)))
            )
        );
    }

    function _callOptional(address target, bytes memory data) internal {
        (bool success, bytes memory returned) = target.call(data);
        if (!success || (returned.length != 0 && !abi.decode(returned, (bool)))) revert TokenCallFailed();
    }
}
