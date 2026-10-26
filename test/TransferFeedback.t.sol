// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;
import "../src/CycleProbe.sol";

contract DiagnosticToken {
    mapping(address=>uint256) public balanceOf;
    uint256 public tax;
    bool public fail;
    function mint(address who,uint256 amount) external { balanceOf[who]+=amount; }
    function configure(uint256 bps,bool failure) external { tax=bps;fail=failure; }
    function transfer(address to,uint256 amount) external returns(bool) {
        if (fail) return false;
        balanceOf[msg.sender]-=amount;
        balanceOf[to]+=amount-amount*tax/10000;
        return true;
    }
}

contract TransferFeedbackTest {
    function testFuzzMeasuresBothActualReceipts(uint64 amountRaw,uint16 taxRaw) public {
        uint256 amount=uint256(amountRaw)+1;
        uint256 tax=uint256(taxRaw)%10001;
        DiagnosticToken token=new DiagnosticToken();
        CycleProbe source=new CycleProbe();
        CycleProbe executor=new CycleProbe();
        address destination=address(0xd00d);
        token.mint(address(source),amount);
        token.configure(tax,false);
        (uint256 funded,uint256 debited,uint256 credited)=source.sampleTransfer(
            address(token),address(executor),destination,amount);
        require(funded==amount-amount*tax/10000,"funding measured incorrectly");
        require(debited==funded,"executor debit measured incorrectly");
        require(credited==funded-funded*tax/10000,"destination measured incorrectly");
        require(token.balanceOf(destination)==credited && token.balanceOf(address(executor))==0);
    }

    function testIRAReceiptPattern() public {
        DiagnosticToken token=new DiagnosticToken();
        CycleProbe source=new CycleProbe();CycleProbe executor=new CycleProbe();
        token.mint(address(source),100e18);token.configure(400,false);
        (uint256 funded,uint256 debited,uint256 credited)=source.sampleTransfer(
            address(token),address(executor),address(0xd00d),100e18);
        require(funded==96e18 && debited==96e18 && credited==9216e16);
    }

    function testRevertedTransferCannotReturnShortfallEvidence() public {
        DiagnosticToken token=new DiagnosticToken();
        CycleProbe source=new CycleProbe();CycleProbe executor=new CycleProbe();
        token.mint(address(source),100);token.configure(0,true);
        (bool ok,)=address(source).call(abi.encodeCall(source.sampleTransfer,
            (address(token),address(executor),address(0xd00d),100)));
        require(!ok && token.balanceOf(address(source))==100);
    }
}
