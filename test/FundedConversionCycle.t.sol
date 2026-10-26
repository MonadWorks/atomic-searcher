// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;
import {StructuralTokenMock} from "./StructuralExecutor.t.sol";
interface FundedCycleCallback {function uniswapV3SwapCallback(int256,int256,bytes calldata) external;}
interface FundedCycleBorrower {function onMorphoFlashLoan(uint256,bytes calldata) external;}

contract FundedCycleLenderMock {
    function drain(address token,uint256 amount) external {StructuralTokenMock(token).transfer(msg.sender,amount);}
    function flashLoan(address token,uint256 amount,bytes calldata cb) external {
        StructuralTokenMock(token).transfer(msg.sender,amount);
        FundedCycleBorrower(msg.sender).onMorphoFlashLoan(amount,cb);
        StructuralTokenMock(token).transferFrom(msg.sender,address(this),amount);
    }
}

contract FundedCycleCLMock {
    address public factory;address public token0;address public token1;
    uint24 public constant fee=500;int24 public constant tickSpacing=1;
    uint256 n;uint256 d;bool locked;
    function initialize(address f,address a,address b,uint256 n_,uint256 d_) external {
        factory=f;token0=a;token1=b;n=n_;d=d_;
    }
    function liquidity() external pure returns(uint128){return 1e12;}
    function slot0() external view returns(uint160,int24,uint16){
        uint256 x=(uint256(1)<<192)*n/d;uint256 z=x;uint256 y=(x+1)/2;
        while(y<z){z=y;y=(x/y+y)/2;}
        return(uint160(z),0,0);
    }
    function observations(uint256) external pure returns(uint32,int56,uint160,bool){return(0,0,0,true);}
    function swap(address receiver,bool z,int256 amount,uint160,bytes calldata cb) external returns(int256 a,int256 b){
        require(!locked&&amount>0);locked=true;uint256 input=uint256(amount);uint256 output=z?input*n/d:input*d/n;
        address tin=z?token0:token1;address tout=z?token1:token0;
        uint256 beforeBalance=StructuralTokenMock(tin).balanceOf(address(this));
        StructuralTokenMock(tout).transfer(receiver,output);
        a=z?int256(input):-int256(output);b=z?-int256(output):int256(input);
        FundedCycleCallback(msg.sender).uniswapV3SwapCallback(a,b,cb);
        require(StructuralTokenMock(tin).balanceOf(address(this))==beforeBalance+input);locked=false;
    }
}

// Two independent exact-output fee conversions for the Python settlement test.
contract FundedCycleQuoteMock {
    struct Params {address tokenIn;address tokenOut;uint256 amount;uint24 fee;uint160 limit;}
    mapping(bytes32=>uint256) values;
    function configure(address a,address b,uint256 amount,uint256 quote) external {
        values[keccak256(abi.encode(a,b,amount))]=quote;
    }
    function quoteExactOutputSingle(Params calldata p) external view returns(uint256,uint160,uint32,uint256) {
        uint256 q=values[keccak256(abi.encode(p.tokenIn,p.tokenOut,p.amount))];
        require(q>0&&p.fee==500&&p.limit==0);return(q,0,0,0);
    }
}
