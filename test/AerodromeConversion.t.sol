// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;
import {ProtocolBase} from "../src/ProtocolExecutor.sol";
import {CLQuoter} from "../src/CLQuoter.sol";
import {StateLens} from "../src/StateLens.sol";
import {VmStructural,StructuralTokenMock} from "./StructuralExecutor.t.sol";

contract AeroFactoryMock {
    mapping(bytes32=>address) pools;
    function add(address a,address b,bool s,address p) external {pools[keccak256(abi.encode(a,b,s))]=p;}
    function getPool(address a,address b,bool s) external view returns(address){return pools[keccak256(abi.encode(a,b,s))];}
}

// Deliberately different native quotes for the two variants. The tests verify
// delegation and settlement, not a second implementation of Aerodrome's math.
contract AeroPoolMock {
    address public factory;address public token0;address public token1;bool public stable;
    uint256 r0;uint256 r1;uint256 stamp;uint256 fee;
    function initialize(address f,address a,address b,bool s,uint256 fee_) external {
        factory=f;token0=a;token1=b;stable=s;fee=fee_;sync();
    }
    function sync() public {r0=StructuralTokenMock(token0).balanceOf(address(this));r1=StructuralTokenMock(token1).balanceOf(address(this));stamp=block.timestamp;}
    function setFee(uint256 f) external {fee=f;}
    function setClock(uint256 r,uint256 t) external {r0=r;r1=r;stamp=t;}
    function getReserves() external view returns(uint256,uint256,uint256){return(r0,r1,stamp);}
    function getAmountOut(uint256 amount,address token) public view returns(uint256){
        require(token==token0||token==token1);amount-=amount*fee/10000;
        (uint256 a,uint256 b)=token==token0?(r0,r1):(r1,r0);
        return stable?amount:amount*b/(a+amount);
    }
    function swap(uint256 a,uint256 b,address to,bytes calldata) external {
        uint256 in0=StructuralTokenMock(token0).balanceOf(address(this))-r0;
        uint256 in1=StructuralTokenMock(token1).balanceOf(address(this))-r1;
        require((a==0&&in0>0&&in1==0&&b==getAmountOut(in0,token0))||
                (b==0&&in1>0&&in0==0&&a==getAmountOut(in1,token1)));
        if(a>0)StructuralTokenMock(token0).transfer(to,a);
        if(b>0)StructuralTokenMock(token1).transfer(to,b);
        sync();
    }
}

contract AeroWalkHarness is ProtocolBase {
    constructor() ProtocolBase(msg.sender,8453,1e30,address(1)) {}
    function _dispatch(Plan calldata) internal override {}
    function walk(Hop[] memory h,uint256 amount) external returns(uint256){_path(h,h[0].tokenIn,h[h.length-1].tokenOut);return _walk(h,amount);}
}

contract AerodromeConversionTest {
    VmStructural constant vm=VmStructural(address(uint160(uint256(keccak256("hevm cheat code")))));
    address constant F=0x420DD381b31aEf6683db6B902084cB0FFECe40Da;
    StructuralTokenMock a;StructuralTokenMock b;AeroPoolMock p;AeroWalkHarness h;
    function setUp() public {
        vm.chainId(8453);vm.warp(1000);vm.etch(F,type(AeroFactoryMock).runtimeCode);
        a=new StructuralTokenMock();b=new StructuralTokenMock();
        if(address(a)>address(b))(a,b)=(b,a);
        p=new AeroPoolMock();a.mint(address(p),1e12);b.mint(address(p),1e12);
        p.initialize(F,address(a),address(b),false,75);AeroFactoryMock(F).add(address(a),address(b),false,address(p));
        h=new AeroWalkHarness();
    }
    function path(bool reverse) internal view returns(ProtocolBase.Hop[] memory out){
        out=new ProtocolBase.Hop[](1);out[0]=ProtocolBase.Hop(address(p),reverse?address(b):address(a),reverse?address(a):address(b),4);
    }
    function testNativeFeeQuoteAndBothSwapDirections() public {
        for(uint256 i;i<2;i++) {
            bool reverse=i==1;StructuralTokenMock input=reverse?b:a;StructuralTokenMock output=reverse?a:b;
            input.mint(address(h),1e6);uint256 expected=p.getAmountOut(1e6,address(input));uint256 before=output.balanceOf(address(h));
            require(h.walk(path(reverse),1e6)==expected&&output.balanceOf(address(h))-before==expected);
        }
    }
    function testStableVariantUsesNativeQuoteAndUpdatedFee() public {
        p.initialize(F,address(a),address(b),true,13);AeroFactoryMock(F).add(address(a),address(b),true,address(p));
        CLQuoter q=new CLQuoter();CLQuoter.Hop[] memory hops=new CLQuoter.Hop[](1);
        hops[0]=CLQuoter.Hop(address(p),address(a),address(b),4);
        require(q.quotePath(hops,1e6)==998700);
        p.setFee(123);require(q.quotePath(hops,1e6)==987700);
        a.mint(address(h),1e6);require(h.walk(path(false),1e6)==987700);
    }
    function testUnregisteredVariantRejectedBeforePayment() public {
        p.initialize(F,address(a),address(b),true,13);a.mint(address(h),1e6);
        vm.expectRevert();h.walk(path(false),1e6);require(a.balanceOf(address(h))==1e6);
    }
    function testFullWidthReservesAndFutureClock() public {
        StateLens lens=new StateLens();address[] memory pools=new address[](1);pools[0]=address(p);
        int24[] memory spacings=new int24[](1);spacings[0]=-1;
        p.setClock(1<<128,block.timestamp);(bool valid,,)=lens.checkMixedClock(pools,spacings);require(valid);
        p.setClock(1<<128,block.timestamp+1);(valid,,)=lens.checkMixedClock(pools,spacings);require(!valid);
        p.setClock(0,block.timestamp);(valid,,)=lens.checkMixedClock(pools,spacings);require(!valid);
    }
}
