// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;

// Local test double. Its bytecode hash is permitted ONLY by the pytest fixture.
contract ContextAggregatorMock {
    struct Call { address target; bool allowFailure; bytes callData; }
    struct Result { bool success; bytes returnData; }
    function aggregate3(Call[] calldata calls) external payable returns (Result[] memory results) {
        results = new Result[](calls.length);
        for (uint256 i; i < calls.length; ++i) {
            (results[i].success, results[i].returnData) = calls[i].target.call(calls[i].callData);
            require(calls[i].allowFailure || results[i].success);
        }
    }
}

contract ContextQuoteMock {
    struct Params { address tokenIn; address tokenOut; uint256 amount; uint24 fee; uint160 limit; }
    address private input;
    address private output;
    uint256 private amount;
    uint256 private quote;
    function configure(address a, address b, uint256 n, uint256 value) external {
        input=a; output=b; amount=n; quote=value;
    }
    function quoteExactOutputSingle(Params calldata p) external view returns (uint256, uint160, uint32, uint256) {
        require(p.tokenIn==input && p.tokenOut==output && p.amount==amount && p.fee==500 && p.limit==0);
        return (quote,0,0,0);
    }
}
