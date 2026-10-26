// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;
import {AtlasCallbackTest} from "./AtlasCallback.t.sol";
import {StructuralExecutor} from "../src/StructuralExecutor.sol";
import {GenericBlueMock} from "./GenericMorpho.t.sol";
import {AuctionQuoteLens} from "../src/AuctionQuoteLens.sol";

contract FakeMeasuredExecutor {
    function measureAuction(StructuralExecutor.AuctionPlan calldata) external pure returns(uint256,uint256) {
        revert AuctionQuoteLens.MeasuredAuction(true,abi.encode(1 ether,100000));
    }
}
contract OversizedRevertExecutor {
    function measureAuction(StructuralExecutor.AuctionPlan calldata plan) external pure returns(uint256,uint256) {
        if(plan.amount==1)assembly {revert(0,131072)}
        return (100000000,100000);
    }
}

contract AuctionQuoteDetailsTest is AtlasCallbackTest,AuctionQuoteLens {
    function testDetailsPreserveFailureAndResetRealExecutorStateBetweenPlans() public {
        (StructuralExecutor.AuctionPlan memory a,bytes32 id)=setupAuction();
        StructuralExecutor.AuctionPlan[] memory plans=new StructuralExecutor.AuctionPlan[](3);
        plans[0]=abi.decode(abi.encode(a),(StructuralExecutor.AuctionPlan));plans[1]=a;plans[2]=a;
        plans[0].deadline=100;
        UserOperation memory user;user.from=address(this);
        (uint256[4][] memory quotes,bytes[] memory failures)=this.quoteAuctionDetails(address(bot),address(0),user,plans,1800000,6000000);
        require(quotes[0][1]==0 && bytes4(failures[0])==StructuralExecutor.InvalidPlan.selector);
        require(quotes[1][0]==1e14 && quotes[2][0]==1e14 && quotes[1][1]>0);
        require(failures[1].length==0 && failures[2].length==0);
        require(GenericBlueMock(MORPHO).debt(id,address(1))==1e15);
    }
    function testRevertCannotForgeTheQuoteWrapperSuccessMarker() public {
        (StructuralExecutor.AuctionPlan memory a,)=setupAuction();
        StructuralExecutor.AuctionPlan[] memory plans=new StructuralExecutor.AuctionPlan[](1);plans[0]=a;
        UserOperation memory user;user.from=address(this);
        (uint256[4][] memory quotes,bytes[] memory failures)=this.quoteAuctionDetails(address(new FakeMeasuredExecutor()),address(0),user,plans,1800000,6000000);
        require(quotes[0][1]==0 && failures[0].length>0);
    }
    function testOversizedRevertCannotConsumeLaterPlansWork() public {
        (StructuralExecutor.AuctionPlan memory a,)=setupAuction();
        StructuralExecutor.AuctionPlan[] memory plans=new StructuralExecutor.AuctionPlan[](2);
        plans[0]=abi.decode(abi.encode(a),(StructuralExecutor.AuctionPlan));plans[0].amount=1;plans[1]=a;
        UserOperation memory user;user.from=address(this);
        (uint256[4][] memory quotes,bytes[] memory failures)=this.quoteAuctionDetails(address(new OversizedRevertExecutor()),address(0),user,plans,1800000,6000000);
        require(quotes[0][1]==0 && failures[0].length==256 && quotes[1][0]==100000000);
    }
}
