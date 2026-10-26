// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;
import "./AerodromePool.sol";
import "./AtomicCycle.sol";
import "./AuctionQuoteLens.sol";

/// @notice Read-only RPC helper: each native pool swap deliberately reverts in
///         its callback. Intended for eth_call with a temporary code override.
contract CLQuoter is AuctionQuoteLens {
    struct Hop {address pool;address tokenIn;address tokenOut;uint8 kind;}
    error QuoteResult(uint256 amountIn, uint256 amountOut);
    error QuoteFailed();

    /// @dev Advisory ranking only. Every swap reverts before payment. A bad,
    /// empty or unusually expensive path cannot revert the entire batch.
    function quotePaths(Hop[][] calldata paths,uint256[] calldata amounts,uint256 gasPerPath)
        external returns(uint256[] memory outputs)
    {
        require(paths.length==amounts.length && paths.length<=32 && gasPerPath<=1500000);
        outputs=new uint256[](paths.length);
        for(uint256 i;i<paths.length;i++) {
            if(gasleft()<gasPerPath+100000)break;
            try this.quotePath{gas:gasPerPath}(paths[i],amounts[i]) returns(uint256 out) {outputs[i]=out;} catch {}
        }
    }

    function quotePath(Hop[] calldata path,uint256 amount) external returns(uint256) {
        if(path.length==0||path.length>4||amount==0)revert QuoteFailed();
        for(uint256 i;i<path.length;i++) {
            Hop calldata h=path[i];
            if(h.tokenIn==h.tokenOut || (i>0&&path[i-1].tokenOut!=h.tokenIn))revert QuoteFailed();
            for(uint256 j;j<i;j++)if(path[j].pool==h.pool||path[j].tokenIn==h.tokenOut)revert QuoteFailed();
            bool direction=h.tokenIn<h.tokenOut;
            if(h.kind==3) {
                (uint112 r0,uint112 r1,)=IQuoteV2(h.pool).getReserves();
                (uint256 a,uint256 b)=direction?(uint256(r0),uint256(r1)):(uint256(r1),uint256(r0));
                amount=amount*997*b/(a*1000+amount*997);
            } else if(h.kind==4) {
                // The pool applies its current custom fee and stable/volatile invariant.
                amount=IAerodromePool(h.pool).getAmountOut(amount,h.tokenIn);
            } else if(h.kind==1||h.kind==2) {
                (uint256 used,uint256 output)=this.quote(h.pool,direction,amount);
                if(used!=amount)revert QuoteFailed();
                amount=output;
            } else revert QuoteFailed();
            if(amount==0)revert QuoteFailed();
        }
        return amount;
    }

    function quote(address pool, bool zeroForOne, uint256 amount) external returns (uint256 used, uint256 output) {
        if (amount == 0 || amount >= 1 << 255) revert QuoteFailed();
        try ICLCycle(pool).swap(address(this), zeroForOne, int256(amount),
            zeroForOne ? 4295128740 : 1461446703485210103287273052203988822378723970341,
            abi.encode(pool, zeroForOne)) returns (int256, int256) {
            revert QuoteFailed();
        } catch (bytes memory reason) {
            if (reason.length != 68 || bytes4(reason) != QuoteResult.selector) revert QuoteFailed();
            assembly ("memory-safe") {
                used := mload(add(reason, 36))
                output := mload(add(reason, 68))
            }
        }
    }

    function uniswapV3SwapCallback(int256 a0, int256 a1, bytes calldata payload) external view {
        (address pool, bool zeroForOne) = abi.decode(payload, (address, bool));
        if (msg.sender != pool) revert QuoteFailed();
        int256 paid = zeroForOne ? a0 : a1;
        int256 got = zeroForOne ? a1 : a0;
        if (paid <= 0 || got > 0 || got == type(int256).min) revert QuoteFailed();
        revert QuoteResult(uint256(paid), uint256(-got));
    }
}
interface IQuoteV2 {function getReserves() external view returns(uint112,uint112,uint32);}
