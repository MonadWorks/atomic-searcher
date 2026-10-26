// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;
import {AtomicCycle,ICLCycle} from "./AtomicCycle.sol";

interface IProbeToken {
    function balanceOf(address) external view returns (uint256);
    function transfer(address,uint256) external returns (bool);
}

/// @notice Temporary eth_call-only harness for a full zero-inventory cycle.
/// @dev Creates a fresh executor in the discarded call state. The measured gas
///      covers first execute(), including automatic pool registration. Temporary
///      deployment is excluded; transaction intrinsic gas is added off-chain.
contract CycleProbe {
    /// Diagnostic only. A failed token call is a scheduling observation, not
    /// proof of permanent incompatibility. It never authorizes execution.
    function sampleTransferStatus(address token,address executor,address destination,uint256 amount)
        external returns (uint256 funded,uint256 debited,uint256 credited,uint8 failedStage,bytes4 failure)
    {
        uint256 beforeFunding=IProbeToken(token).balanceOf(executor);
        (bool ok,bytes4 reason)=_tryTransfer(token,executor,amount);
        if (!ok) return (0,0,0,1,reason);
        funded=IProbeToken(token).balanceOf(executor)-beforeFunding;
        if (funded!=0) (debited,credited,failedStage,failure)=CycleProbe(executor).measureTransferStatus(token,destination,funded);
    }

    function measureTransferStatus(address token,address destination,uint256 amount)
        external returns (uint256 debited,uint256 credited,uint8 failedStage,bytes4 failure)
    {
        uint256 beforeSelf=IProbeToken(token).balanceOf(address(this));
        uint256 beforeDestination=IProbeToken(token).balanceOf(destination);
        (bool ok,bytes4 reason)=_tryTransfer(token,destination,amount);
        if (!ok) return (0,0,2,reason);
        debited=beforeSelf-IProbeToken(token).balanceOf(address(this));
        credited=IProbeToken(token).balanceOf(destination)-beforeDestination;
    }

    function _tryTransfer(address token,address destination,uint256 amount) private returns(bool ok,bytes4 reason) {
        bytes memory payload=abi.encodeCall(IProbeToken.transfer,(destination,amount));
        // Copy only the first word. Malicious returndata cannot grow memory
        // without bound and erase the failed call's identity.
        uint256 size; uint256 first;
        assembly ("memory-safe") {
            let slot:=mload(0x40)
            mstore(slot,0)
            ok:=call(gas(),token,0,add(payload,32),mload(payload),slot,32)
            size:=returndatasize()
            first:=mload(slot)
        }
        if (!ok) return (false,size>=4?bytes4(bytes32(first)):bytes4(0));
        // The execution contract accepts either no return or exactly true.
        if (size!=0 && (size!=32 || first!=1)) return (false,bytes4(0));
    }

    /// Read-only diagnostic: this runtime is temporarily placed at the source
    /// pool and executor by eth_call. The token's code/storage stay unchanged.
    /// A successful sample is not permission to skip the real execution check.
    function sampleTransfer(address token,address executor,address destination,uint256 amount)
        external returns (uint256 funded,uint256 debited,uint256 credited)
    {
        uint256 beforeFunding=IProbeToken(token).balanceOf(executor);
        require(IProbeToken(token).transfer(executor,amount),"funding failed");
        funded=IProbeToken(token).balanceOf(executor)-beforeFunding;
        if (funded!=0) (debited,credited)=CycleProbe(executor).measureTransfer(token,destination,funded);
    }

    function measureTransfer(address token,address destination,uint256 amount)
        external returns (uint256 debited,uint256 credited)
    {
        uint256 beforeSelf=IProbeToken(token).balanceOf(address(this));
        uint256 beforeDestination=IProbeToken(token).balanceOf(destination);
        require(IProbeToken(token).transfer(destination,amount),"transfer failed");
        debited=beforeSelf-IProbeToken(token).balanceOf(address(this));
        credited=IProbeToken(token).balanceOf(destination)-beforeDestination;
    }

    function probe(AtomicCycle.Hop[] calldata hops,uint8[] calldata kinds,uint256 amount,uint256 floor)
        external returns (uint256 profit,uint256 executionGas,uint256 contextBlock,uint256 contextTimestamp)
    {
        require(hops.length==kinds.length && hops.length>=2 && hops.length<=4);
        address[] memory factories=new address[](hops.length);
        for (uint256 i;i<hops.length;i++) {factories[i]=ICLCycle(hops[i].pool).factory();}
        address[] memory ts=new address[](1);
        ts[0]=hops[0].zeroForOne?ICLCycle(hops[0].pool).token0():ICLCycle(hops[0].pool).token1();
        uint256[] memory caps=new uint256[](1);caps[0]=amount;
        AtomicCycle bot=new AtomicCycle(address(this),block.chainid,factories,kinds,ts,caps);
        bot.setPaused(false);
        AtomicCycle.Plan memory p;
        p.hops=hops;p.amountIn=amount;p.minProfit=floor;p.maxGasPrice=type(uint256).max;
        p.deadline=uint64(block.timestamp+2);p.anchorBlock=uint64(block.number-1);
        p.anchorHash=blockhash(block.number-1);p.lastBlock=uint64(block.number);p.nonce=1;
        uint256 beforeGas=gasleft();profit=bot.execute(p);executionGas=beforeGas-gasleft();
        contextBlock=block.number;contextTimestamp=block.timestamp;
    }
}
