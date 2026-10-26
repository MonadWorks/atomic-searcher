// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;

import "../src/CycleProbe.sol";

/// Independent adversarial token: its real storage is never overridden.
contract TransferStatusToken {
    enum Mode { Normal, FalseReturn, EmptyRevert, CustomRevert, NoReturn,
                InvalidBool, ShortReturn, ReturnBomb, RevertBomb, MutateThenFalse }
    error TransferBlocked(address sender,address destination);
    mapping(address=>uint256) public balanceOf;
    mapping(address=>Mode) public modes;
    uint256 public tax;

    function mint(address who,uint256 amount) external { balanceOf[who]+=amount; }
    function configure(address sender,Mode mode,uint256 taxBps) external {
        modes[sender]=mode;tax=taxBps;
    }

    function transfer(address to,uint256 amount) external returns(bool) {
        Mode mode=modes[msg.sender];
        if (mode==Mode.FalseReturn) return false;
        if (mode==Mode.EmptyRevert) assembly { revert(0,0) }
        if (mode==Mode.CustomRevert) revert TransferBlocked(msg.sender,to);
        if (mode==Mode.RevertBomb) {
            // 512 KiB of revert data. Callee memory expansion is real; the
            // caller must not allocate another equally large return buffer.
            assembly { mstore(0,shl(224,0xdeadbeef)) revert(0,0x80000) }
        }
        balanceOf[msg.sender]-=amount;
        balanceOf[to]+=amount-amount*tax/10000;
        if (mode==Mode.NoReturn) assembly { return(0,0) }
        if (mode==Mode.InvalidBool) assembly { mstore(0,2) return(0,32) }
        if (mode==Mode.ShortReturn) assembly { mstore(0,1) return(1,31) }
        if (mode==Mode.ReturnBomb) assembly { mstore(0,1) return(0,0x80000) }
        if (mode==Mode.MutateThenFalse) return false;
        return true;
    }
}

contract TransferCallFeedbackTest {
    address constant DESTINATION=address(0xd00d);

    function setup(uint256 amount) private returns(TransferStatusToken token,CycleProbe source,CycleProbe executor) {
        token=new TransferStatusToken();source=new CycleProbe();executor=new CycleProbe();
        token.mint(address(source),amount);
    }

    function assertFirstFailure(TransferStatusToken.Mode mode,bytes4 expectedReason) private {
        (TransferStatusToken token,CycleProbe source,CycleProbe executor)=setup(100);
        token.configure(address(source),mode,0);
        (uint256 funded,uint256 debited,uint256 credited,uint8 stage,bytes4 reason)=source.sampleTransferStatus(
            address(token),address(executor),DESTINATION,100);
        require(stage==1 && reason==expectedReason,"first call identity");
        require(funded==0 && debited==0 && credited==0,"failed call is not a receipt measurement");
        require(token.balanceOf(address(source))==100 && token.balanceOf(address(executor))==0,
                "failed transfer changed balances");
        require(token.balanceOf(DESTINATION)==0,"destination changed on failed first call");
    }

    function assertSecondFailure(TransferStatusToken.Mode mode,bytes4 expectedReason) private {
        (TransferStatusToken token,CycleProbe source,CycleProbe executor)=setup(100);
        token.configure(address(executor),mode,0);
        (uint256 funded,uint256 debited,uint256 credited,uint8 stage,bytes4 reason)=source.sampleTransferStatus(
            address(token),address(executor),DESTINATION,100);
        require(stage==2 && reason==expectedReason,"second call identity");
        require(funded==100 && debited==0 && credited==0,"funding identity lost or false receipt evidence");
        require(token.balanceOf(address(source))==0 && token.balanceOf(address(executor))==100,
                "second-leg failure must retain first-leg funding in discarded call state");
        require(token.balanceOf(DESTINATION)==0,"destination changed on failed second call");
    }

    function testFirstFalseReturnIsFailureNotShortfall() public {
        assertFirstFailure(TransferStatusToken.Mode.FalseReturn,bytes4(0));
    }
    function testFirstEmptyRevertHasIdentifiedStageWithoutInventedSelector() public {
        assertFirstFailure(TransferStatusToken.Mode.EmptyRevert,bytes4(0));
    }
    function testFirstCustomErrorPreservesSelector() public {
        assertFirstFailure(TransferStatusToken.Mode.CustomRevert,TransferStatusToken.TransferBlocked.selector);
    }
    function testSecondFalseReturnIsFailureNotShortfall() public {
        assertSecondFailure(TransferStatusToken.Mode.FalseReturn,bytes4(0));
    }
    function testSecondEmptyRevertPreservesSuccessfulFunding() public {
        assertSecondFailure(TransferStatusToken.Mode.EmptyRevert,bytes4(0));
    }
    function testSecondCustomErrorPreservesSelectorAndFunding() public {
        assertSecondFailure(TransferStatusToken.Mode.CustomRevert,TransferStatusToken.TransferBlocked.selector);
    }

    function testNormalReceiptsMeasureDeltasRatherThanExistingInventory() public {
        (TransferStatusToken token,CycleProbe source,CycleProbe executor)=setup(100);
        token.mint(address(executor),17);token.mint(DESTINATION,53);
        (uint256 funded,uint256 debited,uint256 credited,uint8 stage,bytes4 reason)=source.sampleTransferStatus(
            address(token),address(executor),DESTINATION,100);
        require(stage==0 && reason==bytes4(0),"normal call marked failed");
        require(funded==100 && debited==100 && credited==100,"inventory confused with receipt");
        require(token.balanceOf(address(executor))==17 && token.balanceOf(DESTINATION)==153,"actual balances");
    }

    function testNoReturnTokenAcceptedOnBothLegsLikeExecutor() public {
        (TransferStatusToken token,CycleProbe source,CycleProbe executor)=setup(100);
        token.configure(address(source),TransferStatusToken.Mode.NoReturn,0);
        token.configure(address(executor),TransferStatusToken.Mode.NoReturn,0);
        (uint256 funded,uint256 debited,uint256 credited,uint8 stage,bytes4 reason)=source.sampleTransferStatus(
            address(token),address(executor),DESTINATION,100);
        require(stage==0 && reason==bytes4(0),"legacy no-return token misclassified");
        require(funded==100 && debited==100 && credited==100,"non-return receipt mismatch");
    }

    function testFuzzActualLossAmountsOnlyOnSuccessfulCalls(uint128 amountRaw,uint16 taxRaw) public {
        uint256 amount=uint256(amountRaw)+1;uint256 tax=uint256(taxRaw)%10001;
        (TransferStatusToken token,CycleProbe source,CycleProbe executor)=setup(amount);
        token.configure(address(source),TransferStatusToken.Mode.Normal,tax);
        (uint256 funded,uint256 debited,uint256 credited,uint8 stage,bytes4 reason)=source.sampleTransferStatus(
            address(token),address(executor),DESTINATION,amount);
        uint256 first=amount-amount*tax/10000;
        uint256 second=first-first*tax/10000;
        require(stage==0 && reason==bytes4(0),"successful amount loss marked as token call failure");
        require(funded==first && debited==first && credited==second,"loss measurement mismatch");
        require(token.balanceOf(address(executor))==0 && token.balanceOf(DESTINATION)==second,"storage mismatch");
    }

    function testCompleteFundingLossIsSuccessfulZeroReceiptNotFailedCall() public {
        (TransferStatusToken token,CycleProbe source,CycleProbe executor)=setup(100);
        token.configure(address(source),TransferStatusToken.Mode.Normal,10000);
        token.configure(address(executor),TransferStatusToken.Mode.EmptyRevert,10000);
        (uint256 funded,uint256 debited,uint256 credited,uint8 stage,bytes4 reason)=source.sampleTransferStatus(
            address(token),address(executor),DESTINATION,100);
        require(stage==0 && reason==bytes4(0),"zero funding must not call unfunded second leg");
        require(funded==0 && debited==0 && credited==0 && token.balanceOf(address(source))==0,"100 percent loss");
    }

    function assertMalformedReturn(TransferStatusToken.Mode mode,bool second) private {
        (TransferStatusToken token,CycleProbe source,CycleProbe executor)=setup(100);
        token.configure(second?address(executor):address(source),mode,0);
        (uint256 funded,uint256 debited,uint256 credited,uint8 stage,bytes4 reason)=source.sampleTransferStatus(
            address(token),address(executor),DESTINATION,100);
        require(stage==(second?2:1) && reason==bytes4(0),"malformed or false return accepted");
        require(funded==(second?100:0) && debited==0 && credited==0,"failure amounts must not masquerade as successful receipt");
        // A false-return token can mutate state. Zero result fields in a
        // failedStage response mean unmeasured, NOT zero actual token credit.
        require(token.balanceOf(second?DESTINATION:address(executor))==100,"adversarial mutation not exercised");
    }

    function testFirstInvalidBoolReturnRejected() public { assertMalformedReturn(TransferStatusToken.Mode.InvalidBool,false); }
    function testSecondInvalidBoolReturnRejected() public { assertMalformedReturn(TransferStatusToken.Mode.InvalidBool,true); }
    function testFirstShortReturnRejected() public { assertMalformedReturn(TransferStatusToken.Mode.ShortReturn,false); }
    function testSecondShortReturnRejected() public { assertMalformedReturn(TransferStatusToken.Mode.ShortReturn,true); }
    function testFirstMutationThenFalseRetainsFailureIdentity() public { assertMalformedReturn(TransferStatusToken.Mode.MutateThenFalse,false); }
    function testSecondMutationThenFalseRetainsFailureIdentity() public { assertMalformedReturn(TransferStatusToken.Mode.MutateThenFalse,true); }

    function assertReturnBomb(TransferStatusToken.Mode mode,bool second) private {
        (TransferStatusToken token,CycleProbe source,CycleProbe executor)=setup(100);
        token.configure(second?address(executor):address(source),mode,0);
        (bool ok,bytes memory result)=address(source).call{gas:900000}(abi.encodeCall(
            source.sampleTransferStatus,(address(token),address(executor),DESTINATION,100)));
        require(ok,"large returndata exhausted caller instead of returning failure identity");
        (uint256 funded,uint256 debited,uint256 credited,uint8 stage,bytes4 reason)=abi.decode(
            result,(uint256,uint256,uint256,uint8,bytes4));
        require(stage==(second?2:1),"return bomb lost failed stage");
        require(reason==(mode==TransferStatusToken.Mode.RevertBomb?bytes4(0xdeadbeef):bytes4(0)),"return bomb selector");
        require(funded==(second?100:0) && debited==0 && credited==0,"return bomb became shortfall sample");
    }

    function testFirstLargeSuccessReturnRejectedWithBoundedCopy() public { assertReturnBomb(TransferStatusToken.Mode.ReturnBomb,false); }
    function testSecondLargeSuccessReturnRejectedWithBoundedCopy() public { assertReturnBomb(TransferStatusToken.Mode.ReturnBomb,true); }
    function testFirstLargeRevertRetainsSelectorWithBoundedCopy() public { assertReturnBomb(TransferStatusToken.Mode.RevertBomb,false); }
    function testSecondLargeRevertRetainsSelectorWithBoundedCopy() public { assertReturnBomb(TransferStatusToken.Mode.RevertBomb,true); }
}
