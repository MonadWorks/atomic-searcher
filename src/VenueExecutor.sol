// SPDX-License-Identifier: GPL-2.0-or-later
pragma solidity ^0.8.33;
import {ProtocolBase,PEToken,PEBlue} from "./ProtocolExecutor.sol";
import {V4Key,V4Swap} from "./V4AtomicPair.sol";

interface VEForte {
    function xToken() external view returns(address);
    function yToken() external view returns(address);
    function swap(address,uint256,uint256,address,uint256) external returns(uint256,uint256,uint256);
}
interface VEManager {
    function unlock(bytes calldata) external returns(bytes memory);
    function swap(V4Key calldata,V4Swap calldata,bytes calldata) external returns(int256);
    function sync(address) external;
    function settle() external payable returns(uint256);
    function take(address,address,uint256) external;
}

/// @notice USDC-financed mixed venue cycles, using the shared account's profit policy.
/// @dev Forte runtime hashes bind authenticated off-chain discovery into the signed plan.
contract VenueExecutor is ProtocolBase {
    address public constant MANAGER=0x498581fF718922c3f8e6A244956aF099B2652b2b;
    struct Leg {address pool;address tokenIn;address tokenOut;uint8 kind;bytes auxiliary;}
    bool private transient v4Waiting;
    bytes32 private transient v4Hash;
    error VenueQuote(uint256 profit,uint256 gasUsed);
    constructor(address o,uint256 c,uint256 cap,address evc) ProtocolBase(o,c,cap,evc) {}

    function _dispatch(Plan calldata p) internal override {
        if(p.strategy!=6) revert InvalidPlan();
        Leg[] memory legs=abi.decode(p.arguments,(Leg[]));_validate(legs);
        loan=USDC;beforeLoan=PEToken(USDC).balanceOf(address(this));debtLimit=p.amountIn;
        callbackTarget=MORPHO;callbackHash=keccak256(p.arguments);
        PEBlue(MORPHO).flashLoan(USDC,p.amountIn,p.arguments);
        if(callbackTarget!=address(0)||waitingPool!=address(0)||v4Waiting||PEToken(USDC).allowance(address(this),MORPHO)!=0)revert BadCallback();
    }
    function _validate(Leg[] memory legs) private view {
        if(legs.length<2||legs.length>4||legs[0].tokenIn!=USDC||legs[legs.length-1].tokenOut!=USDC)revert InvalidPlan();
        for(uint256 i;i<legs.length;i++){
            Leg memory h=legs[i];
            if(h.tokenIn==address(0)||h.tokenOut==address(0)||h.tokenIn==h.tokenOut||(i>0&&legs[i-1].tokenOut!=h.tokenIn))revert InvalidPlan();
            for(uint256 j;j<i;j++){
                if(legs[j].tokenIn==h.tokenIn)revert InvalidPlan();
                if(legs[j].pool==h.pool&&(h.kind!=6||keccak256(legs[j].auxiliary)==keccak256(h.auxiliary)))revert InvalidPlan();
            }
            if(h.kind==5){
                if(h.auxiliary.length!=32||h.pool.code.length==0||h.pool.codehash!=abi.decode(h.auxiliary,(bytes32)))revert InvalidPool();
                address x=VEForte(h.pool).xToken();address y=VEForte(h.pool).yToken();
                if(!((x==h.tokenIn&&y==h.tokenOut)||(y==h.tokenIn&&x==h.tokenOut)))revert InvalidPool();
            }else if(h.kind==6){
                V4Key memory k=abi.decode(h.auxiliary,(V4Key));
                if(h.pool!=MANAGER||k.currency0==address(0)||k.currency0>=k.currency1||k.hooks!=address(0)||k.fee>=1000000||k.tickSpacing<=0||k.tickSpacing>32767||
                   !((k.currency0==h.tokenIn&&k.currency1==h.tokenOut)||(k.currency1==h.tokenIn&&k.currency0==h.tokenOut)))revert InvalidPool();
            }else{if(h.auxiliary.length!=0)revert InvalidPlan();_verify(Hop(h.pool,h.tokenIn,h.tokenOut,h.kind));}
        }
    }
    function onMorphoFlashLoan(uint256 amount,bytes calldata cb) external {
        if(!running||msg.sender!=MORPHO||callbackTarget!=MORPHO||amount!=debtLimit||keccak256(cb)!=callbackHash)revert BadCallback();
        callbackTarget=address(0);if(PEToken(USDC).balanceOf(address(this))!=beforeLoan+amount)revert BadCallback();
        Leg[] memory legs=abi.decode(cb,(Leg[]));
        for(uint256 i;i<legs.length;i++)amount=_leg(legs[i],amount);
        if(PEToken(USDC).balanceOf(address(this))<=beforeLoan+debtLimit)revert InsufficientProfit();
        _approve(USDC,MORPHO,debtLimit);
    }
    function _leg(Leg memory h,uint256 amount) private returns(uint256 output){
        uint256 beforeIn=PEToken(h.tokenIn).balanceOf(address(this));uint256 beforeOut=PEToken(h.tokenOut).balanceOf(address(this));
        if(amount==0||amount>=1<<127||amount>beforeIn)revert InvalidPlan();
        if(h.kind==5){
            _approve(h.tokenIn,h.pool,amount);
            (uint256 reported,,)=VEForte(h.pool).swap(h.tokenIn,amount,1,address(this),block.timestamp);
            if(PEToken(h.tokenIn).allowance(address(this),h.pool)!=0||PEToken(h.tokenOut).balanceOf(address(this))!=beforeOut+reported)revert BadCallback();
        }else if(h.kind==6){
            if(v4Waiting)revert BadCallback();bytes memory cb=abi.encode(h,amount);v4Hash=keccak256(cb);v4Waiting=true;
            VEManager(MANAGER).unlock(cb);if(v4Waiting)revert BadCallback();
        }else{
            Hop[] memory path=new Hop[](1);path[0]=Hop(h.pool,h.tokenIn,h.tokenOut,h.kind);_walk(path,amount);
        }
        if(PEToken(h.tokenIn).balanceOf(address(this))+amount!=beforeIn)revert BadCallback();
        output=PEToken(h.tokenOut).balanceOf(address(this))-beforeOut;if(output==0)revert InsufficientProfit();
    }
    function unlockCallback(bytes calldata cb) external returns(bytes memory){
        if(msg.sender!=MANAGER||!running||!v4Waiting||keccak256(cb)!=v4Hash)revert BadCallback();v4Waiting=false;
        (Leg memory h,uint256 amount)=abi.decode(cb,(Leg,uint256));V4Key memory k=abi.decode(h.auxiliary,(V4Key));bool z=h.tokenIn==k.currency0;
        int256 packed=VEManager(MANAGER).swap(k,V4Swap(z,-int256(amount),z?4295128740:1461446703485210103287273052203988822378723970341),"");
        int256 a=int256(int128(packed>>128));int256 b=int256(int128(uint128(uint256(packed))));
        int256 paid=z?a:b;int256 received=z?b:a;
        if(paid!=-int256(amount)||received<=0)revert BadCallback();
        VEManager(MANAGER).sync(h.tokenIn);_transfer(h.tokenIn,MANAGER,amount);
        if(VEManager(MANAGER).settle()!=amount)revert BadCallback();VEManager(MANAGER).take(h.tokenOut,address(this),uint256(received));
        return "";
    }
    /// @dev Each child reverts all state, including nonce/allowances and pool state.
    function quotePlans(Plan[] calldata plans,uint256 gasPerPlan) external ownerIdle returns(uint256[2][] memory quotes,bytes4[] memory errors){
        if(plans.length==0||plans.length>8||gasPerPlan<100000||gasPerPlan>3000000)revert InvalidPlan();
        quotes=new uint256[2][](plans.length);errors=new bytes4[](plans.length);
        for(uint256 i;i<plans.length;i++){
            try this.quotePlan{gas:gasPerPlan}(plans[i]){}catch(bytes memory reason){
                if(reason.length==68&&bytes4(reason)==VenueQuote.selector){uint256 p;uint256 g;assembly("memory-safe"){p:=mload(add(reason,36)) g:=mload(add(reason,68))}quotes[i]=[p,g];}
                else if(reason.length>=4)errors[i]=bytes4(reason);else errors[i]=bytes4(0xffffffff);
            }
        }
    }
    function quotePlan(Plan calldata p) external {
        if(msg.sender!=address(this)||running||paused||p.amountIn==0||p.amountIn>maxInput)revert Unauthorized();
        uint256 start=gasleft();running=true;uint256 baseline=PEToken(USDC).balanceOf(address(this));_dispatch(p);
        uint256 profit=PEToken(USDC).balanceOf(address(this))-baseline;if(profit==0)revert InsufficientProfit();
        uint256 recipient=PEToken(USDC).balanceOf(owner);_transfer(USDC,owner,profit);
        if(PEToken(USDC).balanceOf(owner)!=recipient+profit||PEToken(USDC).balanceOf(address(this))!=baseline)revert BadCallback();
        revert VenueQuote(profit,start-gasleft());
    }
}
