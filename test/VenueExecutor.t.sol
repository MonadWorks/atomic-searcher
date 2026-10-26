// SPDX-License-Identifier: GPL-2.0-or-later
pragma solidity ^0.8.33;
import {VenueExecutor,VEManager} from "../src/VenueExecutor.sol";
import {ProtocolBase} from "../src/ProtocolExecutor.sol";
import {V4Key,V4Swap} from "../src/V4AtomicPair.sol";
import {StructuralTokenMock,VmStructural} from "./StructuralExecutor.t.sol";

contract VenueBlueMock {
    function flashLoan(address t,uint256 a,bytes calldata cb) external {
        StructuralTokenMock(t).transfer(msg.sender,a);VenueExecutor(msg.sender).onMorphoFlashLoan(a,cb);
        StructuralTokenMock(t).transferFrom(msg.sender,address(this),a);
    }
}
contract VenueForteMock {
    address public xToken;address public yToken;uint256 public n;uint256 public d;
    uint256 public haircut;bool public paused;
    function initialize(address x,address y,uint256 n_,uint256 d_)external{xToken=x;yToken=y;n=n_;d=d_;}
    function setFault(uint256 h,bool p)external{haircut=h;paused=p;}
    function swap(address t,uint256 a,uint256 minimum,address receiver,uint256 expires) external returns(uint256 output,uint256,uint256){
        require(!paused&&block.timestamp<=expires&&(t==xToken||t==yToken));
        StructuralTokenMock(t).transferFrom(msg.sender,address(this),a);
        output=t==xToken?a*n/d:a*d/n;require(output>=minimum);
        StructuralTokenMock(t==xToken?yToken:xToken).transfer(receiver,output-haircut);return(output,0,0);
    }
}
contract VenueManagerMock {
    bool busy;address caller;address synced;uint256 beforeBalance;
    mapping(address=>int256) public delta;
    mapping(bytes32=>uint256) public ratios;
    address token0;address token1;
    function setRate(V4Key calldata k,uint256 rate)external{ratios[keccak256(abi.encode(k))]=rate;}
    function unlock(bytes calldata cb)external returns(bytes memory result){
        require(!busy);busy=true;caller=msg.sender;result=VenueExecutor(msg.sender).unlockCallback(cb);
        require(delta[token0]==0&&delta[token1]==0);busy=false;caller=address(0);
    }
    function swap(V4Key calldata k,V4Swap calldata s,bytes calldata)external returns(int256 packed){
        require(busy&&msg.sender==caller&&s.amountSpecified<0);uint256 rate=ratios[keccak256(abi.encode(k))];require(rate>0);
        uint256 a=uint256(-s.amountSpecified);uint256 out=s.zeroForOne?a*rate/1e6:a*1e6/rate;
        token0=k.currency0;token1=k.currency1;int128 d0=s.zeroForOne?-int128(int256(a)):int128(int256(out));
        int128 d1=s.zeroForOne?int128(int256(out)):-int128(int256(a));
        delta[token0]+=d0;delta[token1]+=d1;return(int256(d0)<<128)|int256(uint256(uint128(d1)));
    }
    function sync(address t)external{require(busy&&msg.sender==caller);synced=t;beforeBalance=StructuralTokenMock(t).balanceOf(address(this));}
    function settle()external payable returns(uint256 paid){require(busy&&msg.sender==caller);paid=StructuralTokenMock(synced).balanceOf(address(this))-beforeBalance;delta[synced]+=int256(paid);}
    function take(address t,address receiver,uint256 a)external{require(busy&&msg.sender==caller&&delta[t]>=int256(a));delta[t]-=int256(a);StructuralTokenMock(t).transfer(receiver,a);}
}
contract VenueExecutorTest {
    VmStructural constant vm=VmStructural(address(uint160(uint256(keccak256("hevm cheat code")))));
    address constant U=0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913;
    address constant B=0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb;
    address constant M=0x498581fF718922c3f8e6A244956aF099B2652b2b;
    address constant X=address(0x111);address constant Y=address(0x222);
    VenueExecutor bot;VenueForteMock a;VenueForteMock b;
    function setUp()public{
        vm.chainId(8453);vm.roll(100);vm.warp(1000);vm.setBlockhash(99,bytes32(uint256(7)));
        for(uint256 i;i<3;i++){address t=i==0?U:i==1?X:Y;vm.etch(t,type(StructuralTokenMock).runtimeCode);}
        vm.etch(B,type(VenueBlueMock).runtimeCode);vm.etch(M,type(VenueManagerMock).runtimeCode);
        a=new VenueForteMock();a.initialize(U,X,1,1);b=new VenueForteMock();b.initialize(Y,U,11,10);
        for(uint256 i;i<3;i++){address t=i==0?U:i==1?X:Y;StructuralTokenMock(t).mint(B,1e12);StructuralTokenMock(t).mint(M,1e12);StructuralTokenMock(t).mint(address(a),1e12);StructuralTokenMock(t).mint(address(b),1e12);}
        VenueManagerMock(M).setRate(V4Key(X,Y,100,1,address(0)),1e6);
        bot=new VenueExecutor(address(this),8453,1e12,address(0x333));bot.setPaused(false);
    }
    function plan()internal view returns(ProtocolBase.Plan memory p){
        VenueExecutor.Leg[] memory h=new VenueExecutor.Leg[](3);
        h[0]=VenueExecutor.Leg(address(a),U,X,5,abi.encode(address(a).codehash));
        h[1]=VenueExecutor.Leg(M,X,Y,6,abi.encode(V4Key(X,Y,100,1,address(0))));
        h[2]=VenueExecutor.Leg(address(b),Y,U,5,abi.encode(address(b).codehash));
        p=ProtocolBase.Plan(6,abi.encode(h),10e6,1e6,type(uint256).max,1005,99,bytes32(uint256(7)),101,1);
    }
    function testMixedFlashCyclePaysOnlyFreshProfit()public{
        StructuralTokenMock(U).mint(address(bot),123);StructuralTokenMock(X).mint(address(bot),456);
        require(bot.execute(plan())==1e6);require(StructuralTokenMock(U).balanceOf(address(this))==1e6);
        require(StructuralTokenMock(U).balanceOf(address(bot))==123&&StructuralTokenMock(X).balanceOf(address(bot))==456);
        require(StructuralTokenMock(U).allowance(address(bot),B)==0&&StructuralTokenMock(U).allowance(address(bot),address(a))==0);
    }
    function testQuoteBatchRollsBackPoolAndNonceState()public{
        ProtocolBase.Plan[] memory ps=new ProtocolBase.Plan[](2);ps[0]=plan();ps[1]=plan();
        (uint256[2][] memory q,bytes4[] memory errors)=bot.quotePlans(ps,1200000);
        require(q[0][0]==1e6&&q[1][0]==1e6&&q[0][1]>0&&errors[0]==0);
        require(!bot.usedNonces(1)&&StructuralTokenMock(U).balanceOf(address(this))==0);
        require(bot.execute(plan())==1e6);
    }
    function testCompetitorPriceChangeRejectsWithoutEatingInventory()public{
        b.initialize(Y,U,99,100);StructuralTokenMock(U).mint(address(bot),100e6);
        vm.expectRevert();bot.execute(plan());require(StructuralTokenMock(U).balanceOf(address(bot))==100e6&&!bot.usedNonces(1));
    }
    function testTransferShortfallRejectsAndRestoresLoan()public{a.setFault(1,false);vm.expectRevert();bot.execute(plan());require(!bot.usedNonces(1));}
    function testPoolPausedRejects()public{a.setFault(0,true);vm.expectRevert();bot.execute(plan());}
    function testChangedCodeHashRejects()public{
        ProtocolBase.Plan memory p=plan();vm.etch(address(a),hex"5f5ffd");vm.expectRevert();bot.execute(p);
    }
    function testForgedCallbacksReject()public{vm.expectRevert();bot.onMorphoFlashLoan(1,"");vm.expectRevert();bot.unlockCallback("");}
    function testBatchFailureDoesNotPoisonNextQuote()public{
        ProtocolBase.Plan[] memory ps=new ProtocolBase.Plan[](2);ps[0]=plan();ps[0].amountIn=0;ps[1]=plan();
        (uint256[2][] memory q,bytes4[] memory errors)=bot.quotePlans(ps,1200000);require(errors[0]!=0&&q[0][0]==0&&q[1][0]==1e6);
    }
}
