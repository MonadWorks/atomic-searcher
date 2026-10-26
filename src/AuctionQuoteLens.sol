// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;
import "./StructuralExecutor.sol";

/// @dev Temporary code at the executor owner for discarded eth_call only.
/// This combines exact swap measurements and Atlas's own fee calculation in
/// one state snapshot. It is never installed on chain or used for final signing.
contract AuctionQuoteLens {
    error MeasuredAuction(bool success,bytes result);
    struct UserOperation {
        address from;address to;uint256 value;uint256 gas;uint256 maxFeePerGas;
        uint256 nonce;uint256 deadline;address dapp;address control;uint32 callConfig;
        uint32 dappGasLimit;uint32 solverGasLimit;uint24 bundlerSurchargeRate;
        address sessionKey;bytes data;bytes signature;
    }
    struct SolverOperation {
        address from;address to;uint256 value;uint256 gas;uint256 maxFeePerGas;
        uint256 deadline;address solver;address control;bytes32 userOpHash;
        address bidToken;uint256 bidAmount;bytes data;bytes signature;
    }
    /// @dev Revert each complete real-executor call so later plans see the
    /// same state. This preserves the real failure bytes without installing
    /// replacement code at the deployed executor.
    function measureOne(address executor,StructuralExecutor.AuctionPlan calldata plan,uint256 gasLimit) external {
        require(msg.sender==address(this));
        (bool ok,bytes memory raw)=executor.call{gas:gasLimit}(abi.encodeCall(StructuralExecutor.measureAuction,(plan)));
        // Bound copying/decoding in the outer batch too, not only the final
        // evidence field. A callee's huge revert must not starve later plans.
        if(raw.length>256)assembly {mstore(raw,256)}
        revert MeasuredAuction(ok,raw);
    }
    function quoteAuctionDetails(address executor,address simulator,UserOperation calldata user,
        StructuralExecutor.AuctionPlan[] calldata plans,uint256 gasPerPlan,uint256 gasLimit)
        external returns(uint256[4][] memory result,bytes[] memory failures)
    {
        require(plans.length>0 && plans.length<=8 && gasPerPlan<=6000000 && user.from!=address(0));
        result=new uint256[4][](plans.length);failures=new bytes[](plans.length);
        for(uint256 i;i<plans.length;i++) {
            // Cover the wrapper and EIP-150's withheld 1/64 outside the
            // existing per-plan executor allowance, rather than shrinking it.
            try this.measureOne{gas:gasPerPlan+(gasPerPlan+62)/63+25000}(executor,plans[i],gasPerPlan) {} catch(bytes memory raw) {
                bool success;bool wrapped;bytes memory value;
                if(raw.length>=100 && bytes4(raw)==MeasuredAuction.selector) {
                    // Remove only our own wrapper's selector. A callee cannot
                    // impersonate a successful measurement with revert bytes.
                    bytes memory body=new bytes(raw.length-4);
                    for(uint256 j;j<body.length;j++)body[j]=raw[j+4];
                    (success,value)=abi.decode(body,(bool,bytes));
                    wrapped=true;
                }
                if(success && value.length==64) {
                    (uint256 gross,uint256 used)=abi.decode(value,(uint256,uint256));
                    uint256 gasAmount=(used+150000)*115/100;
                    result[i]=[gross,used,gasAmount,type(uint256).max];
                    if(gross<=plans[i].minNet || gasAmount>gasLimit)continue;
                    SolverOperation memory solver=SolverOperation(address(this),user.to,0,gasAmount,
                        user.maxFeePerGas,user.deadline,executor,user.control,plans[i].userOpHash,
                        address(0),0,abi.encode(plans[i]),new bytes(0));
                    (bool ok,bytes memory fee)=simulator.staticcall{gas:300000}(abi.encodeWithSignature(
                        "estimateMaxSolverWinGasCharge((address,address,uint256,uint256,uint256,uint256,uint256,address,address,uint32,uint32,uint32,uint24,address,bytes,bytes),(address,address,uint256,uint256,uint256,uint256,address,address,bytes32,address,uint256,bytes,bytes))",user,solver));
                    if(ok&&fee.length==32)result[i][3]=abi.decode(fee,(uint256));
                } else {
                    // Malicious external revert data cannot grow the record
                    // without bound; selectors and standard errors fit here.
                    bytes memory reason=wrapped?value:raw;
                    if(reason.length>256)assembly {mstore(reason,256)}
                    failures[i]=reason;
                }
            }
        }
    }
    function quoteAuctions(address executor,address simulator,UserOperation calldata user,
        StructuralExecutor.AuctionPlan[] calldata plans,uint256 gasPerPlan,uint256 gasLimit)
        external returns(uint256[4][] memory result)
    {
        require(plans.length<=8 && user.from!=address(0));
        uint256[2][] memory measured=StructuralExecutor(payable(executor)).measureAuctions(plans,gasPerPlan);
        result=new uint256[4][](measured.length);
        for(uint256 i;i<measured.length;i++) {
            uint256 gross=measured[i][0];uint256 used=measured[i][1];
            uint256 gasAmount=(used+150000)*115/100;
            result[i]=[gross,used,gasAmount,type(uint256).max];
            if(gross<=plans[i].minNet || gasAmount>gasLimit)continue;
            SolverOperation memory solver=SolverOperation(address(this),user.to,0,gasAmount,
                user.maxFeePerGas,user.deadline,executor,user.control,plans[i].userOpHash,
                address(0),0,abi.encode(plans[i]),new bytes(0));
            (bool ok,bytes memory raw)=simulator.staticcall{gas:300000}(abi.encodeWithSignature(
                "estimateMaxSolverWinGasCharge((address,address,uint256,uint256,uint256,uint256,uint256,address,address,uint32,uint32,uint32,uint24,address,bytes,bytes),(address,address,uint256,uint256,uint256,uint256,address,address,bytes32,address,uint256,bytes,bytes))",user,solver));
            if(ok&&raw.length==32)result[i][3]=abi.decode(raw,(uint256));
        }
    }
}
