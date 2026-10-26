// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;

interface IAerodromePool {
    function stable() external view returns (bool);
    function getAmountOut(uint256 amountIn, address tokenIn) external view returns (uint256);
}

interface IAerodromeFactory {
    function getPool(address tokenA, address tokenB, bool stable) external view returns (address);
}
