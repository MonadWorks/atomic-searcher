// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;
import "./AtomicCycle.t.sol";
import "./StateLens.t.sol";

contract V2FactoryMock {
    mapping(bytes32=>address) private pairs;
    function add(address a,address b,address pair) external { pairs[keccak256(abi.encode(a,b))]=pair; }
    function getPair(address a,address b) external view returns(address) { return pairs[keccak256(abi.encode(a,b))]; }
}
contract V2PairMock {
    address public factory; address public token0; address public token1;
    uint112 private r0; uint112 private r1; uint32 private stamp;
    uint8 public bad; bool private locked;
    constructor(address f,address a,address b) { factory=f;token0=a;token1=b; }
    function setBad(uint8 b) external {bad=b;}
    function sync() public {r0=uint112(TokenCycleMock(token0).balanceOf(address(this)));r1=uint112(TokenCycleMock(token1).balanceOf(address(this)));stamp=uint32(block.timestamp);}
    function getReserves() external view returns(uint112,uint112,uint32) {return(r0,r1,stamp);}
    function swap(uint256 a,uint256 b,address to,bytes calldata payload) external {
        require(!locked,"locked");locked=true;
        require((a>0 || b>0) && a<r0 && b<r1,"liquidity");
        if(a>0)require(TokenCycleMock(token0).transfer(to,a));
        if(b>0)require(TokenCycleMock(token1).transfer(to,b));
        if(bad!=3) AtomicCycle(to).uniswapV2Call(bad==1?address(0xbad):msg.sender,a,b,bad==2?abi.encode(uint256(88)):payload);
        uint256 x=TokenCycleMock(token0).balanceOf(address(this));uint256 y=TokenCycleMock(token1).balanceOf(address(this));
        uint256 in0=x>uint256(r0)-a?x-(uint256(r0)-a):0;
        uint256 in1=y>uint256(r1)-b?y-(uint256(r1)-b):0;
        require((x*1000-in0*3)*(y*1000-in1*3)>=uint256(r0)*r1*1000000,"K");
        require(x<=type(uint112).max && y<=type(uint112).max,"overflow");
        sync();locked=false;
    }
}
contract MixedCycleTest {
    VmCycleTest private constant vm=VmCycleTest(address(uint160(uint256(keccak256("hevm cheat code")))));
    bytes32 private constant ANCHOR=bytes32(uint256(0x888));
    TokenCycleMock[4] private t;
    V2FactoryMock private vf;
    FactoryCycleMock private cf;
    V2PairMock private pair;
    AtomicCycle private bot;
    function setUp() public {
        vm.chainId(8453);vm.roll(100);vm.warp(1000);vm.setBlockhash(99,ANCHOR);
        for(uint256 i;i<4;i++)t[i]=new TokenCycleMock();
        vf=new V2FactoryMock();cf=new FactoryCycleMock();
        address[] memory fs=new address[](2);fs[0]=address(vf);fs[1]=address(cf);
        uint8[] memory ks=new uint8[](2);ks[0]=3;ks[1]=1;
        address[] memory ts=new address[](1);ts[0]=address(t[0]);
        uint256[] memory caps=new uint256[](1);caps[0]=1e23;
        bot=new AtomicCycle(address(this),8453,fs,ks,ts,caps);bot.setPaused(false);
    }
    function hop(uint256 i,uint256 j,bool v2,uint256 numerator) private returns(AtomicCycle.Hop memory h) {
        bool z=address(t[i])<address(t[j]);
        (address a,address b)=z?(address(t[i]),address(t[j])):(address(t[j]),address(t[i]));
        address pool;
        if(v2){
            V2PairMock p=new V2PairMock(address(vf),a,b);pair=p;pool=address(p);vf.add(a,b,pool);
            t[i].mint(pool,1e21);t[j].mint(pool,1e21*numerator);p.sync();
        }else{
            PoolCycleMock p=new PoolCycleMock(address(cf),a,b,z?numerator:1,z?1:numerator);pool=address(p);cf.add(a,b,3000,pool);
            t[i].mint(pool,1e24);t[j].mint(pool,1e24);
        }
        h=AtomicCycle.Hop(pool,z,3000,v2?0:(z?4295128740:1461446703485210103287273052203988822378723970341));
    }
    function plan(uint256 count,bool rootV2) private returns(AtomicCycle.Plan memory p) {
        p.hops=new AtomicCycle.Hop[](count);
        for(uint256 i;i<count;i++)p.hops[i]=hop(i,(i+1)%count,i==0?rootV2:!rootV2,i==0?2:1);
        p.amountIn=1e18;p.minProfit=1e16;p.maxGasPrice=1e9;p.deadline=1005;
        p.anchorBlock=99;p.anchorHash=ANCHOR;p.lastBlock=101;p.nonce=1;
    }
    function run(bool rootV2,uint256 count) private {
        AtomicCycle.Plan memory p=plan(count,rootV2);
        for(uint256 i;i<count;i++)t[i].mint(address(bot),777);
        uint256 profit=bot.execute(p);
        require(profit>1e16 && t[0].balanceOf(address(this))==profit);
        for(uint256 i;i<count;i++)require(t[i].balanceOf(address(bot))==777,"inventory spent");
    }
    function testFourHopV2RootAndNestedCL() public {run(true,4);}
    function testFourHopCLRootAndNestedV2() public {run(false,4);}
    function testTwoHopV2Root() public {run(true,2);}
    function testTwoHopCLRoot() public {run(false,2);}
    function testFuzzFourHopPrincipalIsSelfFinanced(uint64 amount) public {
        AtomicCycle.Plan memory p=plan(4,true);p.amountIn=uint256(amount)%1e19+1e15;p.minProfit=1;
        uint256 profit=bot.execute(p);require(profit>0 && t[0].balanceOf(address(bot))==0);
    }
    function testCompetitorConsumesEdgeRevertsAllBalancesAndNonce() public {
        AtomicCycle.Plan memory p=plan(2,true);
        t[0].mint(address(pair),5e21);pair.sync();
        uint256 before=t[0].balanceOf(address(pair));
        vm.expectRevert(AtomicCycle.InsufficientProfit.selector);bot.execute(p);
        require(!bot.usedNonces(1) && t[0].balanceOf(address(pair))==before);
    }
    function testV2WrongCallbackSender() public {AtomicCycle.Plan memory p=plan(2,true);pair.setBad(1);vm.expectRevert(AtomicCycle.BadCallback.selector);bot.execute(p);}
    function testV2ChangedCallbackPayload() public {AtomicCycle.Plan memory p=plan(2,true);pair.setBad(2);vm.expectRevert(AtomicCycle.BadCallback.selector);bot.execute(p);}
    function testV2WrongFactoryPairIsRejected() public {AtomicCycle.Plan memory p=plan(2,true);vf.add(pair.token0(),pair.token1(),address(0xbad));vm.expectRevert(AtomicCycle.InvalidPool.selector);bot.execute(p);}
    function testV2FeeCeilingEnforced() public {AtomicCycle.Plan memory p=plan(2,true);p.hops[0].maxFee=2999;vm.expectRevert(AtomicCycle.InvalidPlan.selector);bot.execute(p);}
    function testV2UnexpectedPriceLimitRejected() public {AtomicCycle.Plan memory p=plan(2,true);p.hops[0].priceLimit=4295128740;vm.expectRevert(AtomicCycle.InvalidPool.selector);bot.execute(p);}
    function testV2ExternalCallbackRejected() public {vm.expectRevert(AtomicCycle.BadCallback.selector);bot.uniswapV2Call(address(bot),1,0,"");}
    function testV2CannotCallCLCallback() public {vm.expectRevert(AtomicCycle.BadCallback.selector);bot.uniswapV3SwapCallback(1,-1,"");}
    function testPackedMixedSnapshotAndClock() public {
        plan(2,true); StateLens lens=new StateLens();LensPoolMock cl=new LensPoolMock(0,1);
        address[] memory pools=new address[](3);pools[0]=address(pair);pools[1]=address(cl);pools[2]=address(0xbad);
        int24[] memory spacings=new int24[](3);spacings[1]=1;
        uint8[] memory depths=new uint8[](3);depths[0]=1;depths[1]=1;depths[2]=1;
        bytes memory raw=lens.readMixedPacked(pools,spacings,0,depths);
        (uint112 a,uint112 b,)=pair.getReserves();
        bytes memory expected=abi.encodePacked(uint8(2),block.number,block.timestamp,uint16(3),address(pair),uint8(2),a,b,uint24(3000),address(cl),uint8(1),uint160(1<<96),int24(0),uint128(1e18),uint24(500),uint8(1),int16(0),uint256(0),uint8(0),address(0xbad),uint8(0));
        require(keccak256(raw)==keccak256(expected),"mixed encoding");
        pools=new address[](2);pools[0]=address(pair);pools[1]=address(cl);
        spacings=new int24[](2);spacings[1]=1;
        (bool ok,,)=lens.checkMixedClock(pools,spacings);require(ok);
        cl.setObservedAt(uint32(block.timestamp)+1);(ok,,)=lens.checkMixedClock(pools,spacings);require(!ok);
    }
}
