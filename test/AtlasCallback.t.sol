// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;
import {GenericMorphoTest,GenericBlueMock} from "./GenericMorpho.t.sol";
import {StructuralExecutor} from "../src/StructuralExecutor.sol";
import {StructuralTokenMock} from "./StructuralExecutor.t.sol";
interface VmAtlas {function deal(address,uint256) external;function txGasPrice(uint256) external;}
contract AuctionWETHMock is StructuralTokenMock {
    function withdraw(uint256 amount) external {balanceOf[msg.sender]-=amount;(bool ok,)=msg.sender.call{value:amount}("");require(ok);}
}
contract AtlasFeeMock {
    uint256 public liability;uint256 public borrowed;uint256 public approved;
    function configure(uint256 gas_,uint256 debt_) external {liability=gas_;borrowed=debt_;}
    function shortfall() external view returns(uint256,uint256){return(liability,borrowed);}
    function reconcile(uint256 maximum) external payable returns(uint256){approved=maximum;return maximum>=liability?0:liability-maximum;}
}
contract AtlasCallbackTest is GenericMorphoTest {
    VmAtlas constant va=VmAtlas(address(uint160(uint256(keccak256("hevm cheat code")))));
    address constant ATLAS=0x583dcFef0D240DC80753F0F0B26513feE27D9B77;
    address constant WETH=0x4200000000000000000000000000000000000006;
    address constant ENV=address(0xabc);
    receive() external payable {}
    function setupAuction() internal returns(StructuralExecutor.AuctionPlan memory a,bytes32 id){
        vm.etch(WETH,type(AuctionWETHMock).runtimeCode);va.deal(WETH,1 ether);
        vm.etch(ATLAS,type(AtlasFeeMock).runtimeCode);AtlasFeeMock(ATLAS).configure(1e12,0);
        _pool(address(14),WETH,USDC);
        StructuralExecutor.Plan memory p;(p,id)=plan(HIGH,USDC,address(12),address(0),1e15);
        StructuralExecutor.Hop[] memory conversion=new StructuralExecutor.Hop[](1);
        conversion[0]=StructuralExecutor.Hop(address(14),USDC,WETH,1);
        a=StructuralExecutor.AuctionPlan(bytes32(uint256(1)),p.arguments,p.amountIn,conversion,1e12,1e12,8e13,1e10,102);
    }
    function testAtlasCallbackPaysBidAndPreservesExistingInventory() public {
        (StructuralExecutor.AuctionPlan memory a,)=setupAuction();
        va.deal(address(bot),777);StructuralTokenMock(WETH).mint(address(bot),888);StructuralTokenMock(USDC).mint(address(bot),999);
        uint256 beforeOwner=address(this).balance;vm.prank(ATLAS);bot.atlasSolverCall(address(this),ENV,address(0),8e13,abi.encode(a),"");
        require(ENV.balance==8e13 && address(this).balance==beforeOwner+2e13);
        require(address(bot).balance==777 && StructuralTokenMock(WETH).balanceOf(address(bot))==888 && StructuralTokenMock(USDC).balanceOf(address(bot))==999);
        require(AtlasFeeMock(ATLAS).approved()==1e12);
    }
    function testAtlasOverbidRollsBackEntireLiquidation() public {
        (StructuralExecutor.AuctionPlan memory a,bytes32 id)=setupAuction();a.maxBid=1e14;
        vm.prank(ATLAS);vm.expectRevert();bot.atlasSolverCall(address(this),ENV,address(0),1e14,abi.encode(a),"");
        require(GenericBlueMock(MORPHO).debt(id,address(1))==1e15 && ENV.balance==0);
    }
    function testAtlasUnexpectedLiabilityRollsBackBidAndLoan() public {
        (StructuralExecutor.AuctionPlan memory a,bytes32 id)=setupAuction();AtlasFeeMock(ATLAS).configure(1e12+1,0);
        vm.prank(ATLAS);vm.expectRevert();bot.atlasSolverCall(address(this),ENV,address(0),8e13,abi.encode(a),"");
        require(GenericBlueMock(MORPHO).debt(id,address(1))==1e15 && ENV.balance==0);
    }
    function testAtlasRequiresActualCallerOwnerAndDeadline() public {
        (StructuralExecutor.AuctionPlan memory a,)=setupAuction();
        vm.expectRevert();bot.atlasSolverCall(address(this),ENV,address(0),8e13,abi.encode(a),"");
        vm.prank(ATLAS);vm.expectRevert();bot.atlasSolverCall(address(9),ENV,address(0),8e13,abi.encode(a),"");
        a.deadline=100;vm.prank(ATLAS);vm.expectRevert();bot.atlasSolverCall(address(this),ENV,address(0),8e13,abi.encode(a),"");
    }
    function testBatchQuotesResetDebtLiquidityAndSkipFailedPath() public {
        (StructuralExecutor.AuctionPlan memory a,bytes32 id)=setupAuction();
        StructuralExecutor.AuctionPlan[] memory plans=new StructuralExecutor.AuctionPlan[](3);
        plans[0]=abi.decode(abi.encode(a),(StructuralExecutor.AuctionPlan));plans[1]=a;plans[2]=a;
        plans[0].deadline=100;
        uint256[2][] memory quotes=bot.measureAuctions(plans,1_800_000);
        require(quotes[0][0]==0 && quotes[1][0]==1e14 && quotes[2][0]==1e14);
        require(quotes[1][1]>0 && GenericBlueMock(MORPHO).debt(id,address(1))==1e15);
        require(StructuralTokenMock(WETH).balanceOf(address(bot))==0 && ENV.balance==0);
        vm.expectRevert();bot.quoteAuction(a);
    }
}
