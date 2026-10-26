// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;
import "../src/V4AtomicPair.sol";
interface VmPair {
    function chainId(uint256) external;
    function roll(uint256) external;
    function warp(uint256) external;
    function setBlockhash(uint256,bytes32) external;
    function prank(address) external;
    function expectRevert() external;
    function expectRevert(bytes4) external;
    function txGasPrice(uint256) external;
}
contract PairToken {
    mapping(address=>uint256) public balanceOf;
    bool public shortTransfer;
    function mint(address a,uint256 n) external {balanceOf[a]+=n;}
    function setShort(bool x) external {shortTransfer=x;}
    function transfer(address a,uint256 n) external returns(bool) {balanceOf[msg.sender]-=n;balanceOf[a]+=shortTransfer?n-1:n;return true;}
}
interface PairCallback {function unlockCallback(bytes calldata) external returns(bytes memory);}
contract PairManager {
    bool private open;
    address private locker;
    mapping(address=>int256) private deltas;
    address private settlement;
    address private asset;
    uint8 public fault;
    bool public edge=true;
    function setFault(uint8 f) external {fault=f;}
    function consume() external {edge=false;}
    function pack(int256 a,int256 b) private pure returns(int256) {return (a<<128)|int256(uint256(uint128(int128(b))));}
    function unlock(bytes calldata data) external returns(bytes memory r) {
        require(!open);open=true;locker=msg.sender;
        if(fault==1)return abi.encode(uint256(100));
        r=PairCallback(locker).unlockCallback(fault==2?abi.encode(uint256(42)):data);
        if(fault==3)PairCallback(locker).unlockCallback(data);
        require(deltas[settlement]==0 && deltas[asset]==0,"unsettled deltas");open=false;
    }
    function swap(V4Key calldata k,V4Swap calldata p,bytes calldata) external returns(int256 result) {
        require(open && msg.sender==locker && p.amountSpecified>0);
        address input=p.zeroForOne?k.currency0:k.currency1;
        address output=p.zeroForOne?k.currency1:k.currency0;
        // Distinct pool keys, favourable first output / second cost, with exact output semantics.
        int256 received=p.amountSpecified;
        int256 paid=k.fee==50000?received:(edge?received/2:received*2);
        if(k.fee==50000){settlement=output;asset=input;}
        if(fault==4 && k.fee==50000)received-=1;
        if(fault==5 && k.fee==40000)received-=1;
        deltas[input]-=paid;deltas[output]+=received;
        result=p.zeroForOne?pack(-paid,received):pack(received,-paid);
    }
    function take(address t,address to,uint256 n) external {
        require(open && msg.sender==locker && deltas[t]>=int256(n));
        deltas[t]-=int256(n);require(PairToken(t).transfer(to,n));
    }
}
contract V4AtomicPairTest {
    VmPair constant vm=VmPair(address(uint160(uint256(keccak256("hevm cheat code")))));
    bytes32 constant ANCHOR=bytes32(uint256(456));
    PairToken token0;PairToken token1;PairManager manager;V4AtomicPair bot;
    function setUp() public {
        vm.chainId(8453);vm.roll(100);vm.warp(1000);vm.setBlockhash(99,ANCHOR);
        PairToken a=new PairToken();PairToken b=new PairToken();
        (token0,token1)=address(a)<address(b)?(a,b):(b,a);
        manager=new PairManager();bot=new V4AtomicPair(address(this),address(manager),8453,address(token0),250000e6);bot.setPaused(false);
        token0.mint(address(manager),1e18);token1.mint(address(manager),1e18);
    }
    function plan() internal view returns(V4AtomicPair.Plan memory p) {
        p.sell=V4Key(address(token0),address(token1),50000,500,address(0));
        p.buy=V4Key(address(token0),address(token1),40000,400,address(0));
        p.settlement=address(token0);p.output=1000;p.minProfit=500;p.maxGasPrice=100;
        p.sellLimit=1461446703485210103287273052203988822378723970341;p.buyLimit=4295128740;
        p.deadline=1005;p.anchorBlock=99;p.anchorHash=ANCHOR;p.lastBlock=101;p.nonce=1;
    }
    function rejected(V4AtomicPair.Plan memory p) internal {vm.expectRevert();bot.execute(p);require(!bot.usedNonces(p.nonce));}
    function testZeroInputPrincipalPaysOnlyNetSurplus() public {require(bot.execute(plan())==500);require(token0.balanceOf(address(this))==500 && token0.balanceOf(address(bot))==0);}
    function testFuzzZeroInventory(uint64 n) public {V4AtomicPair.Plan memory p=plan();p.output=uint128(uint256(n)%(250000e6-1)+2);p.minProfit=1;uint256 x=bot.execute(p);require(x==uint256(p.output)-(uint256(p.output)/2));}
    function testExactCapAndNextUnit() public {V4AtomicPair.Plan memory p=plan();p.output=250000e6;p.minProfit=1;require(bot.execute(p)>0);p.nonce=2;p.output+=1;rejected(p);}
    function testSettlementCurrency1() public {
        bot=new V4AtomicPair(address(this),address(manager),8453,address(token1),250000e6);bot.setPaused(false);
        V4AtomicPair.Plan memory p=plan();p.settlement=address(token1);require(bot.execute(p)==500 && token1.balanceOf(address(this))==500);
    }
    function testInventoryUntouched() public {token0.mint(address(bot),777);bot.execute(plan());require(token0.balanceOf(address(bot))==777);}
    function testCompetitorLeavesLossRevertsAll() public {manager.consume();rejected(plan());require(token0.balanceOf(address(this))==0);}
    function testFirstPartialFill() public {manager.setFault(4);rejected(plan());}
    function testSecondPartialFill() public {manager.setFault(5);rejected(plan());}
    function testMissingCallback() public {manager.setFault(1);rejected(plan());}
    function testChangedPayload() public {manager.setFault(2);rejected(plan());}
    function testRepeatedCallback() public {manager.setFault(3);rejected(plan());}
    function testExternalCallback() public {vm.expectRevert(V4AtomicPair.BadCallback.selector);bot.unlockCallback(abi.encode(plan()));}
    function testUnauthorizedExecute() public {V4AtomicPair.Plan memory p=plan();vm.prank(address(7));vm.expectRevert(V4AtomicPair.Unauthorized.selector);bot.execute(p);}
    function testShortTransferReverts() public {token0.setShort(true);rejected(plan());}
    function testHooksExcluded() public {V4AtomicPair.Plan memory p=plan();p.buy.hooks=address(7);rejected(p);}
    function testDynamicFeeExcluded() public {V4AtomicPair.Plan memory p=plan();p.buy.fee=0x800000;rejected(p);}
    function testSamePoolExcluded() public {V4AtomicPair.Plan memory p=plan();p.buy=p.sell;rejected(p);}
    function testDifferentPairExcluded() public {V4AtomicPair.Plan memory p=plan();p.buy.currency1=address(7);rejected(p);}
    function testNativeCurrencyExcluded() public {V4AtomicPair.Plan memory p=plan();p.buy.currency0=address(0);rejected(p);}
    function testWrongAnchor() public {V4AtomicPair.Plan memory p=plan();p.anchorHash=bytes32(uint256(999));rejected(p);}
    function testExpired() public {V4AtomicPair.Plan memory p=plan();p.deadline=999;rejected(p);}
    function testTooWideWindow() public {V4AtomicPair.Plan memory p=plan();p.lastBlock=102;rejected(p);}
    function testFeeCap() public {vm.txGasPrice(101);rejected(plan());}
    function testOutputCap() public {V4AtomicPair.Plan memory p=plan();p.output=250001e6;rejected(p);}
    function testProfitFloor() public {V4AtomicPair.Plan memory p=plan();p.minProfit=501;rejected(p);}
    function testPaused() public {bot.setPaused(true);rejected(plan());}
    function testWrongChain() public {vm.chainId(1);rejected(plan());}
    function testNonceCannotReplay() public {V4AtomicPair.Plan memory p=plan();bot.execute(p);vm.expectRevert();bot.execute(p);}
}
