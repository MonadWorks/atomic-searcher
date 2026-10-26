// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;
import "./CLQuoter.sol";

/// @notice Discarded native-swap estimates. The funded executor still validates
/// every transfer, callback, fee and the final balance in a complete eth_call.
contract CycleQuoteLens is CLQuoter {
    struct Screen {bool ok;uint160 sqrt;uint128 liquidity;uint256 r0;uint256 r1;uint256 out0;uint256 out1;uint256 fee;}
    /// @notice Cheap broad screening. These fields are priorities, not profit proofs.
    function screenPools(address[] calldata pools,uint8[] calldata kinds)
        external view returns(Screen[] memory rows,uint256 number,uint256 stamp)
    {
        require(pools.length==kinds.length&&pools.length<=96);
        rows=new Screen[](pools.length);
        for(uint256 i;i<pools.length;i++) {
            if(gasleft()<200000)break;
            try this.screenPool{gas:150000}(pools[i],kinds[i]) returns(Screen memory s){rows[i]=s;}catch{}
        }
        return(rows,block.number,block.timestamp);
    }
    function screenPool(address pool,uint8 kind) external view returns(Screen memory s) {
        if(kind==1||kind==2) {
            (bool ok,bytes memory raw)=pool.staticcall(abi.encodeWithSignature("slot0()"));
            require(ok&&raw.length>=96);
            uint16 index;(s.sqrt,,index)=abi.decode(raw,(uint160,int24,uint16));
            (ok,raw)=pool.staticcall(abi.encodeWithSignature("observations(uint256)",uint256(index)));
            require(ok&&raw.length>=32&&abi.decode(raw,(uint32))<=uint32(block.timestamp));
            (ok,raw)=pool.staticcall(abi.encodeWithSignature("liquidity()"));require(ok&&raw.length==32);s.liquidity=abi.decode(raw,(uint128));
            (ok,raw)=pool.staticcall(abi.encodeWithSignature("fee()"));require(ok&&raw.length==32);s.fee=abi.decode(raw,(uint24));
            require(s.sqrt>0&&s.liquidity>0&&s.fee<1000000);
        } else if(kind==3||kind==4) {
            (bool ok,bytes memory raw)=pool.staticcall(abi.encodeWithSignature("getReserves()"));require(ok&&raw.length==96);
            uint256 stamp;(s.r0,s.r1,stamp)=abi.decode(raw,(uint256,uint256,uint256));
            require(s.r0>0&&s.r1>0&&stamp<=(kind==3?uint32(block.timestamp):block.timestamp));
            if(kind==3){require(s.r0<1<<112&&s.r1<1<<112);s.fee=3000;}
            else {
                (ok,raw)=pool.staticcall(abi.encodeWithSignature("token0()"));require(ok&&raw.length==32);address t0=abi.decode(raw,(address));
                (ok,raw)=pool.staticcall(abi.encodeWithSignature("token1()"));require(ok&&raw.length==32);address t1=abi.decode(raw,(address));
                s.out0=IAerodromePool(pool).getAmountOut(s.r0/1000000>0?s.r0/1000000:1,t0);
                s.out1=IAerodromePool(pool).getAmountOut(s.r1/1000000>0?s.r1/1000000:1,t1);
            }
        } else revert QuoteFailed();
        s.ok=true;
    }
    /// @dev Financing capacity and each native quote share the same EVM state
    /// and one RPC call. Ordinary Aerodrome cycles use the external lender;
    /// legacy CL/V2 cycles retain first-pool callback financing.
    function quoteExecutableCycles(Hop[][] calldata paths,uint256[] calldata amounts,uint256 gasPerPath)
        external returns(uint256[] memory outputs,uint256[] memory inputs)
    {
        (outputs,inputs,,)=_quoteExecutable(paths,amounts,gasPerPath);
    }
    function quoteExecutableCyclesDetailed(Hop[][] calldata paths,uint256[] calldata amounts,uint256 gasPerPath)
        external returns(uint256[] memory outputs,uint256[] memory inputs,uint8[] memory failedHops,uint256[] memory failedAmounts)
    {
        return _quoteExecutable(paths,amounts,gasPerPath);
    }
    function _quoteExecutable(Hop[][] calldata paths,uint256[] calldata amounts,uint256 gasPerPath)
        private returns(uint256[] memory outputs,uint256[] memory inputs,uint8[] memory failedHops,uint256[] memory failedAmounts)
    {
        require(paths.length==amounts.length&&paths.length<=12&&gasPerPath<=1500000);
        outputs=new uint256[](paths.length);inputs=new uint256[](paths.length);
        failedHops=new uint8[](paths.length);failedAmounts=new uint256[](paths.length);
        for(uint256 i;i<paths.length;i++) {
            if(gasleft()<gasPerPath+150000)break;
            if(paths[i].length<2||paths[i].length>4)continue;
            uint256 amount=amounts[i];
            for(uint256 j;j<paths[i].length;j++) if(paths[i][j].kind==4) {
                uint256 available=IERC20Cycle(paths[i][0].tokenIn).balanceOf(0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb);
                if(amount>available)amount=available;break;
            }
            inputs[i]=amount;
            if(amount==0)continue;
            try this.quoteCycleDetailed{gas:gasPerPath}(paths[i],amount) returns(uint256 out,uint8 failed,uint256 failedInput){
                outputs[i]=out;failedHops[i]=failed;failedAmounts[i]=failedInput;
            }catch{failedHops[i]=255;}
        }
    }
    function quoteCycleDetailed(Hop[] calldata path,uint256 amount) external returns(uint256 output,uint8 failed,uint256 failedInput) {
        require(path.length>=2&&path.length<=4&&path[0].tokenIn==path[path.length-1].tokenOut);
        for(uint256 i;i<path.length;i++) {
            require(i==0||path[i-1].tokenOut==path[i].tokenIn);
            for(uint256 j;j<i;j++)require(path[j].pool!=path[i].pool&&(path[j].tokenIn!=path[i].tokenOut||(j==0&&i+1==path.length)));
            Hop[] memory leg=new Hop[](1);leg[0]=path[i];
            try this.quotePath(leg,amount) returns(uint256 out){amount=out;}
            catch{return(0,uint8(i+1),amount);}
        }
        return(amount,0,0);
    }
    function quoteCycles(Hop[][] calldata paths,uint256[] calldata amounts,uint256 gasPerPath)
        external returns(uint256[] memory outputs)
    {
        require(paths.length==amounts.length && paths.length<=12 && gasPerPath<=1500000);
        outputs=new uint256[](paths.length);
        for(uint256 i;i<paths.length;i++) {
            if(gasleft()<gasPerPath+100000)break;
            try this.quoteCycle{gas:gasPerPath}(paths[i],amounts[i]) returns(uint256 out) {outputs[i]=out;} catch {}
        }
    }
    function quoteCycle(Hop[] calldata path,uint256 amount) external returns(uint256) {
        require(path.length>=2 && path.length<=4 && path[0].tokenIn==path[path.length-1].tokenOut);
        for(uint256 i;i<path.length;i++) {
            require(i==0 || path[i-1].tokenOut==path[i].tokenIn);
            for(uint256 j;j<i;j++) {
                require(path[j].pool!=path[i].pool);
                require(path[j].tokenIn!=path[i].tokenOut || (j==0&&i+1==path.length));
            }
            Hop[] memory leg=new Hop[](1);leg[0]=path[i];
            amount=this.quotePath(leg,amount);
        }
        return amount;
    }
}
