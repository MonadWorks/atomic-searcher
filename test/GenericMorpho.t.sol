// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;
import {StructuralExecutor,BlueMarket} from "../src/StructuralExecutor.sol";
import {VmStructural,StructuralTokenMock,StructuralFactoryMock,StructuralPoolMock} from "./StructuralExecutor.t.sol";
contract GenericBlueMock {
    mapping(bytes32=>BlueMarket) params;
    mapping(bytes32=>mapping(address=>uint256)) public debt;
    function add(BlueMarket calldata m,address borrower,uint256 amount) external returns(bytes32 id){id=keccak256(abi.encode(m));params[id]=m;debt[id][borrower]=amount;}
    function idToMarketParams(bytes32 id) external view returns(BlueMarket memory){return params[id];}
    function liquidate(BlueMarket calldata m,address who,uint256,uint256 shares,bytes calldata cb) external returns(uint256 seized,uint256 repaid){
        bytes32 id=keccak256(abi.encode(m));uint256 owed=debt[id][who];require(owed>0&&shares<=owed);repaid=shares;debt[id][who]-=repaid;seized=repaid*110/100;
        StructuralTokenMock(m.collateralToken).transfer(msg.sender,seized);
        StructuralExecutor(payable(msg.sender)).onMorphoLiquidate(repaid,cb);
        StructuralTokenMock(m.loanToken).transferFrom(msg.sender,address(this),repaid);
    }
}
contract GenericMorphoTest {
    VmStructural constant vm=VmStructural(address(uint160(uint256(keccak256("hevm cheat code")))));
    address constant MORPHO=0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb;
    address constant USDC=0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913;
    address constant F=0x33128a8fC17869897dcE68Ed026d694621f6FDfD;
    address constant LOW=address(100);address constant HIGH=address(type(uint160).max-100);
    bytes32 constant HASH=bytes32(uint256(55));StructuralExecutor bot;
    function setUp() public {
        vm.chainId(8453);vm.roll(101);vm.warp(1000);vm.setBlockhash(100,HASH);
        vm.etch(MORPHO,type(GenericBlueMock).runtimeCode);vm.etch(F,type(StructuralFactoryMock).runtimeCode);
        for(uint256 i;i<3;i++){address t=i==0?USDC:i==1?LOW:HIGH;vm.etch(t,type(StructuralTokenMock).runtimeCode);StructuralTokenMock(t).mint(MORPHO,1e18);}
        bot=new StructuralExecutor(address(this),8453,1e18);bot.setPaused(false);
        _pool(address(11),LOW,USDC);_pool(address(12),USDC,HIGH);_pool(address(13),LOW,HIGH);
    }
    function _pool(address p,address a,address b) internal {
        vm.etch(p,type(StructuralPoolMock).runtimeCode);StructuralPoolMock(p).initialize(F,a,b,1,1);StructuralFactoryMock(F).add(a,b,p);
        StructuralTokenMock(a).mint(p,1e18);StructuralTokenMock(b).mint(p,1e18);
    }
    function plan(address collateral,address loan,address pool,address settlement,uint256 debt) internal returns(StructuralExecutor.Plan memory p,bytes32 id) {
        BlueMarket memory m=BlueMarket(loan,collateral,address(500),address(600),625e15);
        id=GenericBlueMock(MORPHO).add(m,address(1),debt);
        StructuralExecutor.Request[] memory req=new StructuralExecutor.Request[](1);req[0]=StructuralExecutor.Request(address(1),debt,debt);
        StructuralExecutor.Hop[] memory unwind=new StructuralExecutor.Hop[](1);unwind[0]=StructuralExecutor.Hop(pool,collateral,loan,1);
        StructuralExecutor.Hop[] memory settle=new StructuralExecutor.Hop[](loan==USDC?0:1);
        if(loan!=USDC)settle[0]=StructuralExecutor.Hop(settlement,loan,USDC,1);
        p=StructuralExecutor.Plan(3,abi.encode(id,req,unwind,settle),debt,1,1e10,1006,100,HASH,102,7);
    }
    function testUnseenMarketBothTokenDirections() public {
        (StructuralExecutor.Plan memory p,)=plan(HIGH,USDC,address(12),address(0),1e6);require(bot.execute(p)==1e5);
        (p,)=plan(LOW,USDC,address(11),address(0),2e6);p.nonce=8;require(bot.execute(p)==2e5);
    }
    function testNonUSDCRepaymentConvertsOnlyNewSurplus() public {
        StructuralTokenMock(USDC).mint(address(bot),777);StructuralTokenMock(LOW).mint(address(bot),888);StructuralTokenMock(HIGH).mint(address(bot),999);
        (StructuralExecutor.Plan memory p,)=plan(HIGH,LOW,address(13),address(11),1e6);
        require(bot.execute(p)==1e5);require(StructuralTokenMock(USDC).balanceOf(address(bot))==777);
        require(StructuralTokenMock(LOW).balanceOf(address(bot))==888&&StructuralTokenMock(HIGH).balanceOf(address(bot))==999);
    }
    function testWrongMarketOrUntrustedPoolCannotSpendInventory() public {
        StructuralTokenMock(USDC).mint(address(bot),1e10);
        (StructuralExecutor.Plan memory p,)=plan(HIGH,USDC,address(11),address(0),1e6);
        vm.expectRevert();bot.execute(p);require(StructuralTokenMock(USDC).balanceOf(address(bot))==1e10);
    }
    function testProfitFloorRollbackRestoresLoan() public {
        (StructuralExecutor.Plan memory p,bytes32 id)=plan(HIGH,USDC,address(12),address(0),1e6);p.minProfit=100001;
        vm.expectRevert();bot.execute(p);require(GenericBlueMock(MORPHO).debt(id,address(1))==1e6);
    }
    function testSameAssetCollateralKeepsOnlyNewUSDCProfit() public {
        BlueMarket memory m=BlueMarket(USDC,USDC,address(501),address(600),625e15);
        bytes32 id=GenericBlueMock(MORPHO).add(m,address(1),1e6);
        StructuralExecutor.Request[] memory req=new StructuralExecutor.Request[](1);req[0]=StructuralExecutor.Request(address(1),1e6,1e6);
        StructuralExecutor.Hop[] memory empty=new StructuralExecutor.Hop[](0);
        StructuralExecutor.Plan memory p=StructuralExecutor.Plan(3,abi.encode(id,req,empty,empty),1e6,1,1e10,1006,100,HASH,102,7);
        StructuralTokenMock(USDC).mint(address(bot),777);require(bot.execute(p)==1e5);
        require(StructuralTokenMock(USDC).balanceOf(address(bot))==777);
    }
    function testFuzzNewMarketsAndSizes(uint80 value,bool reverse) public {
        uint256 amount=uint256(value)%1e12+100;
        (StructuralExecutor.Plan memory p,)=plan(reverse?HIGH:LOW,USDC,reverse?address(12):address(11),address(0),amount);
        require(bot.execute(p)==amount*110/100-amount);
    }
}
