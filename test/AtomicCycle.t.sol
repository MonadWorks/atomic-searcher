// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;
import "../src/AtomicCycle.sol";
import "../src/CycleProbe.sol";

interface VmCycleTest {
    function roll(uint256) external;
    function warp(uint256) external;
    function chainId(uint256) external;
    function setBlockhash(uint256, bytes32) external;
    function expectRevert(bytes4) external;
    function prank(address) external;
    function txGasPrice(uint256) external;
}

contract TokenCycleMock {
    mapping(address => uint256) public balanceOf;
    bool public returnFalse;
    uint256 public tax;
    function mint(address who, uint256 n) external { balanceOf[who] += n; }
    function setBad(bool value, uint256 tax_) external { returnFalse=value; tax=tax_; }
    function transfer(address to, uint256 n) external returns (bool) {
        if (returnFalse) return false;
        balanceOf[msg.sender] -= n;
        balanceOf[to] += n-(n*tax/10000);
        return true;
    }
}

contract FactoryCycleMock {
    mapping(bytes32 => address) private entries;
    function add(address a, address b, uint24 f, address pool) external { entries[keccak256(abi.encode(a,b,f))] = pool; }
    function getPool(address a, address b, uint24 f) external view returns (address) { return entries[keccak256(abi.encode(a,b,f))]; }
}

contract PoolCycleMock {
    address public factory;
    address public token0;
    address public token1;
    uint24 public fee = 3000;
    int24 public tickSpacing = 60;
    uint256 private numerator;
    uint256 private denominator;
    uint8 public badMode;
    bool private locked;
    uint32 public observedAt;
    constructor(address f, address a, address b, uint256 n, uint256 d) {
        factory=f;token0=a;token1=b;numerator=n;denominator=d;
    }
    function setFee(uint24 f) external {fee=f;}
    function setBad(uint8 m) external {badMode=m;}
    function setObservedAt(uint32 t) external { observedAt=t; }
    function slot0() external pure returns (uint160, int24, uint16) { return (uint160(1 << 96), 0, 0); }
    function observations(uint256) external view returns (uint32, int56, uint160, bool) { return (observedAt, 0, 0, true); }
    function swap(address recipient, bool z, int256 amount, uint160, bytes calldata callbackData)
        external returns (int256 a0, int256 a1)
    {
        require(!locked,"locked");locked=true;
        uint256 n=uint256(amount);
        uint256 out=z?n*numerator/denominator:n*denominator/numerator;
        address input=z?token0:token1;
        uint256 beforeBalance=TokenCycleMock(input).balanceOf(address(this));
        require(TokenCycleMock(z?token1:token0).transfer(recipient,out));
        a0=z?amount:-int256(out);a1=z?-int256(out):amount;
        if (badMode==1) {
            AtomicCycle(msg.sender).uniswapV3SwapCallback(a0+1,a1,callbackData);
        } else if (badMode==2) {
            AtomicCycle(msg.sender).uniswapV3SwapCallback(a0,a1,abi.encode(uint256(666)));
        } else if (badMode!=3) {
            AtomicCycle(msg.sender).uniswapV3SwapCallback(a0,a1,callbackData);
        }
        require(TokenCycleMock(input).balanceOf(address(this)) >= beforeBalance+n,"unpaid");
        locked=false;
    }
}

contract AtomicCycleTest {
    VmCycleTest private constant vm=VmCycleTest(address(uint160(uint256(keccak256("hevm cheat code")))));
    TokenCycleMock private a;
    TokenCycleMock private b;
    FactoryCycleMock private factory;
    PoolCycleMock private first;
    PoolCycleMock private second;
    AtomicCycle private bot;
    bytes32 private constant ANCHOR=bytes32(uint256(0x1234));

    function setUp() public {
        vm.chainId(8453);vm.roll(100);vm.warp(1000);vm.setBlockhash(99,ANCHOR);
        TokenCycleMock x=new TokenCycleMock();TokenCycleMock y=new TokenCycleMock();
        (a,b)=address(x)<address(y)?(x,y):(y,x);
        factory=new FactoryCycleMock();
        first=new PoolCycleMock(address(factory),address(a),address(b),2,1);
        second=new PoolCycleMock(address(factory),address(a),address(b),5,3);
        second.setFee(500);
        factory.add(address(a),address(b),3000,address(first));
        factory.add(address(a),address(b),500,address(second));
        a.mint(address(first),1e24);b.mint(address(first),1e24);
        a.mint(address(second),1e24);b.mint(address(second),1e24);
        address[] memory fs=new address[](1);fs[0]=address(factory);
        uint8[] memory ks=new uint8[](1);ks[0]=1;
        address[] memory ts=new address[](1);ts[0]=address(a);
        uint256[] memory caps=new uint256[](1);caps[0]=1e22;
        bot=new AtomicCycle(address(this),8453,fs,ks,ts,caps);
        address[] memory ps=new address[](2);ps[0]=address(first);ps[1]=address(second);
        bot.registerPools(ps);bot.setPaused(false);
    }

    function plan() private view returns (AtomicCycle.Plan memory p) {
        p.hops=new AtomicCycle.Hop[](2);
        p.hops[0]=AtomicCycle.Hop(address(first),true,3000,4295128740);
        p.hops[1]=AtomicCycle.Hop(address(second),false,500,1461446703485210103287273052203988822378723970341);
        p.amountIn=1000;p.minProfit=100;p.maxGasPrice=1e9;p.deadline=1005;
        p.anchorBlock=99;p.anchorHash=ANCHOR;p.lastBlock=101;p.nonce=1;
    }

    function testZeroCapitalCycle() public {
        require(a.balanceOf(address(bot))==0 && b.balanceOf(address(bot))==0);
        uint256 profit=bot.execute(plan());
        require(profit==200 && a.balanceOf(address(this))==200);
        require(a.balanceOf(address(bot))==0 && b.balanceOf(address(bot))==0);
    }
    function testWholeCycleProbeUsesRealExecutor() public {
        CycleProbe probe=new CycleProbe();AtomicCycle.Plan memory p=plan();
        uint8[] memory kinds=new uint8[](2);kinds[0]=1;kinds[1]=1;
        (uint256 profit,uint256 gasUsed,uint256 number,uint256 timestamp)=probe.probe(p.hops,kinds,p.amountIn,p.minProfit);
        require(profit==200 && gasUsed>0 && number==100 && timestamp==1000);
        require(a.balanceOf(address(probe))==200);
    }
    function testThreeHopZeroInventoryCycle() public {
        TokenCycleMock c=new TokenCycleMock();
        (address low,address high)=address(b)<address(c)?(address(b),address(c)):(address(c),address(b));
        PoolCycleMock middle=new PoolCycleMock(address(factory),low,high,1,1);
        factory.add(low,high,3000,address(middle));
        (low,high)=address(c)<address(a)?(address(c),address(a)):(address(a),address(c));
        PoolCycleMock end=new PoolCycleMock(address(factory),low,high,address(c)<address(a)?3:5,address(c)<address(a)?5:3);
        factory.add(low,high,3000,address(end));
        b.mint(address(middle),1e24);c.mint(address(middle),1e24);
        a.mint(address(end),1e24);c.mint(address(end),1e24);
        address[] memory ps=new address[](2);ps[0]=address(middle);ps[1]=address(end);bot.registerPools(ps);
        AtomicCycle.Plan memory p=plan();p.hops=new AtomicCycle.Hop[](3);
        p.hops[0]=AtomicCycle.Hop(address(first),true,3000,4295128740);
        bool z=address(b)<address(c);
        p.hops[1]=AtomicCycle.Hop(address(middle),z,3000,z?4295128740:1461446703485210103287273052203988822378723970341);
        z=address(c)<address(a);
        p.hops[2]=AtomicCycle.Hop(address(end),z,3000,z?4295128740:1461446703485210103287273052203988822378723970341);
        require(bot.execute(p)==200 && a.balanceOf(address(this))==200);
        require(a.balanceOf(address(bot))==0 && b.balanceOf(address(bot))==0 && c.balanceOf(address(bot))==0);
    }
    function testMissingCallbackCannotSucceed() public {
        first.setBad(3);(bool ok,)=address(bot).call(abi.encodeCall(AtomicCycle.execute,(plan())));
        require(!ok && !bot.usedNonces(1));
    }
    function testFalseTransferCannotSucceed() public {
        a.setBad(true,0);(bool ok,)=address(bot).call(abi.encodeCall(AtomicCycle.execute,(plan())));
        require(!ok && a.balanceOf(address(bot))==0 && !bot.usedNonces(1));
    }
    function testZeroAnchorRejected() public {
        vm.setBlockhash(99,bytes32(0));AtomicCycle.Plan memory p=plan();p.anchorHash=bytes32(0);
        vm.expectRevert(AtomicCycle.InvalidPlan.selector);bot.execute(p);
    }
    function testInventoryNotCountedAsProfit() public {
        a.mint(address(bot),700);b.mint(address(bot),900);
        bot.execute(plan());
        require(a.balanceOf(address(bot))==700 && b.balanceOf(address(bot))==900);
        AtomicCycle.Plan memory p=plan();p.nonce=2;p.minProfit=201;
        vm.expectRevert(AtomicCycle.InsufficientProfit.selector);bot.execute(p);
        require(!bot.usedNonces(2));
    }
    function testExpired() public {AtomicCycle.Plan memory p=plan();vm.warp(1006);vm.expectRevert(AtomicCycle.InvalidPlan.selector);bot.execute(p);}
    function testReorg() public {vm.setBlockhash(99,bytes32(uint256(456)));vm.expectRevert(AtomicCycle.InvalidPlan.selector);bot.execute(plan());}
    function testTooLateBlock() public {vm.roll(102);vm.expectRevert(AtomicCycle.InvalidPlan.selector);bot.execute(plan());}
    function testGasCeiling() public {vm.txGasPrice(1e9+1);vm.expectRevert(AtomicCycle.InvalidPlan.selector);bot.execute(plan());}
    function testOnlyOwner() public {AtomicCycle.Plan memory p=plan();vm.prank(address(0xbad));vm.expectRevert(AtomicCycle.Unauthorized.selector);bot.execute(p);}
    function testPaused() public {bot.setPaused(true);vm.expectRevert(AtomicCycle.InvalidPlan.selector);bot.execute(plan());}
    function testNoReplay() public {bot.execute(plan());vm.expectRevert(AtomicCycle.InvalidPlan.selector);bot.execute(plan());}
    function testOutsideCallbackRejected() public {vm.prank(address(first));vm.expectRevert(AtomicCycle.BadCallback.selector);bot.uniswapV3SwapCallback(1000,-2000,abi.encode(plan()));}
    function testCallbackDebtMismatch() public {first.setBad(1);vm.expectRevert(AtomicCycle.BadCallback.selector);bot.execute(plan());}
    function testCallbackDataMismatch() public {first.setBad(2);vm.expectRevert(AtomicCycle.BadCallback.selector);bot.execute(plan());}
    function testTaxedOutputRejected() public {b.setBad(false,100);vm.expectRevert(AtomicCycle.BadCallback.selector);bot.execute(plan());}
    function testRepeatedBorrowPoolRejected() public {AtomicCycle.Plan memory p=plan();p.hops[1].pool=address(first);vm.expectRevert(AtomicCycle.InvalidPlan.selector);bot.execute(p);}
    function testFeeChanged() public {second.setFee(600);vm.expectRevert(AtomicCycle.InvalidPlan.selector);bot.execute(plan());}
    function testImpersonatingPoolRejected() public {
        PoolCycleMock fake=new PoolCycleMock(address(factory),address(a),address(b),2,1);
        address[] memory ps=new address[](1);ps[0]=address(fake);
        vm.expectRevert(AtomicCycle.InvalidPool.selector);bot.registerPools(ps);
    }
    function testFuzzPreservesStartingInventory(uint96 initial) public {
        a.mint(address(bot),initial);bot.execute(plan());require(a.balanceOf(address(bot))==initial);
    }
}
