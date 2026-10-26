// SPDX-License-Identifier: GPL-2.0-or-later
pragma solidity ^0.8.33;
import "./AerodromePool.sol";
import {CrossedBooksResolver,CrossedBooksTake,CrossedBooksMarket} from "./CrossedBooksResolver.sol";

struct PEMarket {address loanToken;address collateralToken;address oracle;address irm;uint256 lltv;}
interface PEToken {
    function balanceOf(address) external view returns(uint256);
    function allowance(address,address) external view returns(uint256);
    function approve(address,uint256) external returns(bool);
    function transfer(address,uint256) external returns(bool);
}
interface PEBlue {function flashLoan(address,uint256,bytes calldata) external;}
interface PEPre {
    function MORPHO() external view returns(address);
    function ID() external view returns(bytes32);
    function marketParams() external view returns(PEMarket memory);
    function preLiquidate(address,uint256,uint256,bytes calldata) external returns(uint256,uint256);
}
interface PEComet {
    function baseToken() external view returns(address);
    function buyCollateral(address,uint256,uint256,address) external;
    function absorb(address,address[] calldata) external;
}
interface PEVault {
    function EVC() external view returns(address);
    function asset() external view returns(address);
    function checkLiquidation(address,address,address) external view returns(uint256,uint256);
    function liquidate(address,address,uint256,uint256) external;
    function redeem(uint256,address,address) external returns(uint256);
    function repay(uint256,address) external returns(uint256);
    function debtOf(address) external view returns(uint256);
    function disableController() external;
}
interface PEEVC {
    struct Item {address targetContract;address onBehalfOfAccount;uint256 value;bytes data;}
    function batch(Item[] calldata) external payable;
    function enableController(address,address) external;
    function enableCollateral(address,address) external;
    function disableCollateral(address,address) external;
}
interface PEMidnight {
    function toMarket(bytes32) external view returns(CrossedBooksMarket memory);
    function liquidate(CrossedBooksMarket calldata,uint256,uint256,uint256,address,bool,address,address,bytes calldata) external returns(uint256,uint256);
}
interface PEPool {
    function factory() external view returns(address);
    function token0() external view returns(address);
    function token1() external view returns(address);
    function fee() external view returns(uint24);
    function tickSpacing() external view returns(int24);
    function getReserves() external view returns(uint112,uint112,uint32);
    function swap(address,bool,int256,uint160,bytes calldata) external returns(int256,int256);
    function swap(uint256,uint256,address,bytes calldata) external;
}
interface PEFactory {function getPool(address,address,uint24) external view returns(address);}
interface PESlipFactory {function getPool(address,address,int24) external view returns(address);}
interface PEV2Factory {function getPair(address,address) external view returns(address);}

/// @notice Explicit protocol adapters, funded by callbacks or flash liquidity.
/// The owner's wallet never grants this contract a token allowance.
abstract contract ProtocolBase {
    address public immutable owner;
    uint256 public immutable chainId;
    uint256 public immutable maxInput;
    address public immutable EVC;
    address public constant MIDNIGHT=0xAdedD8ab6dE832766Fedf0FaC4992E5C4D3EA18A;
    address constant MORPHO=0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb;
    address constant USDC=0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913;
    address constant UNI=0x33128a8fC17869897dcE68Ed026d694621f6FDfD;
    address constant V2=0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6;
    bytes32 constant CALLBACK_SUCCESS=keccak256("morpho.midnight.callbackSuccess");
    bool public paused=true;
    mapping(uint64=>bool) public usedNonces;
    struct Hop {address pool;address tokenIn;address tokenOut;uint8 kind;}
    struct Plan {uint8 strategy;bytes arguments;uint256 amountIn;uint256 minProfit;uint256 maxGasPrice;uint64 deadline;uint64 anchorBlock;bytes32 anchorHash;uint64 lastBlock;uint64 nonce;}
    struct CompoundPlan {address comet;address collateral;address[] absorb;Hop[] unwind;Hop[] settlement;}
    struct EulerPlan {address vault;address borrower;address collateralVault;Hop[] unwind;Hop[] settlement;}
    struct PrePlan {address target;address borrower;uint256 shares;Hop[] unwind;Hop[] settlement;}
    struct MidnightPlan {bytes32 market;address borrower;uint256 collateralIndex;bool matured;Hop[] unwind;Hop[] settlement;}
    bool internal transient running;
    address internal transient callbackTarget;
    bytes32 internal transient callbackHash;
    uint256 internal transient debtLimit;
    uint256 internal transient beforeLoan;
    uint256 internal transient beforeCollateral;
    address internal transient loan;
    address internal transient collateral;
    address internal transient waitingPool;
    address internal transient swapToken;
    uint256 internal transient swapAmount;
    bool internal transient swapDirection;
    event CycleExecuted(uint64 indexed nonce,address indexed settlement,uint256 input,uint256 profit);
    error Unauthorized();error InvalidPlan();error BadCallback();error InvalidPool();error InsufficientProfit();
    modifier ownerIdle(){if(msg.sender!=owner||running)revert Unauthorized();_;}
    constructor(address owner_,uint256 chain_,uint256 cap_,address evc_) {
        if(owner_==address(0)||chain_!=8453||block.chainid!=chain_||cap_==0||cap_>=1<<255||evc_==address(0))revert InvalidPlan();
        owner=owner_;chainId=chain_;maxInput=cap_;EVC=evc_;
    }
    function setPaused(bool value) external ownerIdle {paused=value;}
    function rescue(address token) external ownerIdle {_transfer(token,owner,PEToken(token).balanceOf(address(this)));}
    function measure(Plan calldata p) external returns(uint256 profit,uint256 gasUsed){uint256 start=gasleft();profit=execute(p);gasUsed=start-gasleft();}
    function execute(Plan calldata p) public ownerIdle returns(uint256 profit) {
        if(paused||block.chainid!=chainId||p.amountIn==0||p.amountIn>maxInput||p.minProfit==0||tx.gasprice>p.maxGasPrice||
            block.timestamp>p.deadline||p.deadline>block.timestamp+30||block.number<=p.anchorBlock||block.number>p.lastBlock||
            p.lastBlock>p.anchorBlock+2||p.anchorHash==bytes32(0)||blockhash(p.anchorBlock)!=p.anchorHash||usedNonces[p.nonce])revert InvalidPlan();
        running=true;usedNonces[p.nonce]=true;uint256 baseline=PEToken(USDC).balanceOf(address(this));debtLimit=p.amountIn;
        _dispatch(p);
        if(callbackTarget!=address(0)||waitingPool!=address(0))revert BadCallback();
        uint256 afterBalance=PEToken(USDC).balanceOf(address(this));
        if(afterBalance<baseline+p.minProfit)revert InsufficientProfit();profit=afterBalance-baseline;
        uint256 recipientBefore=PEToken(USDC).balanceOf(owner);_transfer(USDC,owner,profit);
        if(PEToken(USDC).balanceOf(owner)!=recipientBefore+profit||PEToken(USDC).balanceOf(address(this))!=baseline)revert BadCallback();
        running=false;emit CycleExecuted(p.nonce,USDC,p.amountIn,profit);
    }
    function _dispatch(Plan calldata p) internal virtual;
    function _begin(address loan_,address collateral_,Hop[] memory unwind,Hop[] memory settlement) internal {
        _path(unwind,collateral_,loan_);_path(settlement,loan_,USDC);
        loan=loan_;collateral=collateral_;beforeLoan=PEToken(loan_).balanceOf(address(this));beforeCollateral=PEToken(collateral_).balanceOf(address(this));
    }
    function _settle(Hop[] memory path) internal {
        if(collateral!=loan&&PEToken(collateral).balanceOf(address(this))!=beforeCollateral)revert BadCallback();
        uint256 residual=PEToken(loan).balanceOf(address(this))-beforeLoan;if(residual==0)revert InsufficientProfit();
        if(loan!=USDC){_walk(path,residual);if(PEToken(loan).balanceOf(address(this))!=beforeLoan)revert BadCallback();}
    }






    function _unwindAndApprove(Hop[] memory hops,uint256 repaid,address target) internal {
        uint256 received=PEToken(collateral).balanceOf(address(this))-beforeCollateral;
        if(collateral!=loan)_walk(hops,received);
        if(PEToken(loan).balanceOf(address(this))<=beforeLoan+repaid)revert InsufficientProfit();_approve(loan,target,repaid);
    }



    function _path(Hop[] memory hops,address start,address end) internal view {
        if(start==end){if(hops.length!=0)revert InvalidPlan();return;}
        if(hops.length==0||hops.length>4)revert InvalidPlan();address current=start;
        for(uint256 i;i<hops.length;i++) {
            Hop memory h=hops[i];if(h.tokenIn!=current||h.tokenOut==current||h.tokenOut==address(0))revert InvalidPlan();
            for(uint256 j;j<i;j++)if(hops[j].pool==h.pool||hops[j].tokenIn==h.tokenOut)revert InvalidPlan();
            _verify(h);current=h.tokenOut;
        }
        if(current!=end)revert InvalidPlan();
    }
    function _verify(Hop memory h) internal view {
        address a=h.tokenIn<h.tokenOut?h.tokenIn:h.tokenOut;address b=h.tokenIn<h.tokenOut?h.tokenOut:h.tokenIn;
        PEPool p=PEPool(h.pool);address f=p.factory();if(p.token0()!=a||p.token1()!=b)revert InvalidPool();
        if(h.kind==1){if(f!=UNI||PEFactory(f).getPool(a,b,p.fee())!=h.pool)revert InvalidPool();}
        else if(h.kind==2){
            if(f!=0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A&&f!=0xaDe65c38CD4849aDBA595a4323a8C7DdfE89716a&&f!=0xf8f2eB4940CFE7d13603DDDD87f123820Fc061Ef)revert InvalidPool();
            if(PESlipFactory(f).getPool(a,b,p.tickSpacing())!=h.pool)revert InvalidPool();
        }else if(h.kind==3){if(f!=V2||PEV2Factory(f).getPair(a,b)!=h.pool)revert InvalidPool();}else if(h.kind==4){
            if(f!=0x420DD381b31aEf6683db6B902084cB0FFECe40Da||
               IAerodromeFactory(f).getPool(a,b,IAerodromePool(h.pool).stable())!=h.pool)revert InvalidPool();
        }else revert InvalidPool();
    }
    function _walk(Hop[] memory hops,uint256 amount) internal returns(uint256) {
        for(uint256 i;i<hops.length;i++) {
            Hop memory h=hops[i];uint256 oldOut=PEToken(h.tokenOut).balanceOf(address(this));uint256 oldIn=PEToken(h.tokenIn).balanceOf(address(this));
            if(amount==0||amount>=1<<255||amount>oldIn)revert InvalidPlan();bool direction=h.tokenIn<h.tokenOut;
            if(h.kind==3||h.kind==4){
                uint256 output;
                if(h.kind==4)output=IAerodromePool(h.pool).getAmountOut(amount,h.tokenIn);
                else {
                    (uint112 r0,uint112 r1,)=PEPool(h.pool).getReserves();(uint256 a,uint256 b)=direction?(uint256(r0),uint256(r1)):(uint256(r1),uint256(r0));
                    output=amount*997*b/(a*1000+amount*997);
                }
                if(output==0)revert InsufficientProfit();
                _transfer(h.tokenIn,h.pool,amount);PEPool(h.pool).swap(direction?0:output,direction?output:0,address(this),"");
            }else{
                if(waitingPool!=address(0))revert BadCallback();waitingPool=h.pool;swapToken=h.tokenIn;swapAmount=amount;swapDirection=direction;
                (int256 a,int256 b)=PEPool(h.pool).swap(address(this),direction,int256(amount),direction?4295128740:1461446703485210103287273052203988822378723970341,"");
                if(waitingPool!=address(0)||(direction?a:b)!=int256(amount))revert BadCallback();
            }
            if(PEToken(h.tokenIn).balanceOf(address(this))+amount!=oldIn)revert BadCallback();amount=PEToken(h.tokenOut).balanceOf(address(this))-oldOut;
        }return amount;
    }
    function uniswapV3SwapCallback(int256 a,int256 b,bytes calldata) external {
        if(!running||msg.sender!=waitingPool||waitingPool==address(0)||(swapDirection?a:b)!=int256(swapAmount))revert BadCallback();
        waitingPool=address(0);_transfer(swapToken,msg.sender,swapAmount);
    }
    function _transfer(address token,address to,uint256 amount) internal {
        (bool ok,bytes memory ret)=token.call(abi.encodeCall(PEToken.transfer,(to,amount)));require(ok&&(ret.length==0||abi.decode(ret,(bool))));
    }
    function _approve(address token,address spender,uint256 amount) internal {
        if(PEToken(token).allowance(address(this),spender)!=0)revert BadCallback();
        (bool ok,bytes memory ret)=token.call(abi.encodeCall(PEToken.approve,(spender,amount)));require(ok&&(ret.length==0||abi.decode(ret,(bool))));
    }
}

contract ProtocolExecutor is ProtocolBase {
    constructor(address o,uint256 c,uint256 cap,address evc) ProtocolBase(o,c,cap,evc) {}
    function _dispatch(Plan calldata p) internal override {
        if(p.strategy==1)_compound(abi.decode(p.arguments,(CompoundPlan)),p.amountIn);
        else if(p.strategy==2)_euler(abi.decode(p.arguments,(EulerPlan)),p.amountIn);
        else if(p.strategy==3)_pre(abi.decode(p.arguments,(PrePlan)));
        else revert InvalidPlan();
    }
    function _compound(CompoundPlan memory p,uint256 amount) internal {
        if(p.comet!=0xb125E6687d4313864e53df431d5425969c15Eb2F&&p.comet!=0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf&&p.comet!=0x46e6b214b524310239732D51387075E0e70970bf)revert InvalidPlan();
        if(p.absorb.length>8)revert InvalidPlan();
        _begin(PEComet(p.comet).baseToken(),p.collateral,p.unwind,p.settlement);
        if(loan==collateral)revert InvalidPlan();
        if(p.absorb.length>0)PEComet(p.comet).absorb(owner,p.absorb);
        bytes memory cb=abi.encode(p);callbackTarget=MORPHO;callbackHash=keccak256(cb);
        PEBlue(MORPHO).flashLoan(loan,amount,cb);
        if(callbackTarget!=address(0)||PEToken(loan).allowance(address(this),MORPHO)!=0)revert BadCallback();
        _settle(p.settlement);
    }
    function onMorphoFlashLoan(uint256 amount,bytes calldata cb) external {
        if(!running||msg.sender!=MORPHO||callbackTarget!=MORPHO||keccak256(cb)!=callbackHash||amount!=debtLimit)revert BadCallback();
        callbackTarget=address(0);CompoundPlan memory p=abi.decode(cb,(CompoundPlan));
        if(PEToken(loan).balanceOf(address(this))!=beforeLoan+amount)revert BadCallback();
        _approve(loan,p.comet,amount);PEComet(p.comet).buyCollateral(collateral,1,amount,address(this));
        if(PEToken(loan).allowance(address(this),p.comet)!=0)revert BadCallback();
        _walk(p.unwind,PEToken(collateral).balanceOf(address(this))-beforeCollateral);
        if(PEToken(loan).balanceOf(address(this))<=beforeLoan+amount)revert InsufficientProfit();
        _approve(loan,MORPHO,amount);
    }
    function _pre(PrePlan memory p) internal {
        if(PEPre(p.target).MORPHO()!=MORPHO||p.shares==0)revert InvalidPlan();
        PEMarket memory m=PEPre(p.target).marketParams();if(keccak256(abi.encode(m))!=PEPre(p.target).ID())revert InvalidPlan();
        _begin(m.loanToken,m.collateralToken,p.unwind,p.settlement);
        bytes memory cb=abi.encode(p.unwind);callbackTarget=p.target;callbackHash=keccak256(cb);
        (,uint256 repaid)=PEPre(p.target).preLiquidate(p.borrower,0,p.shares,cb);
        if(callbackTarget!=address(0)||repaid>debtLimit||PEToken(loan).allowance(address(this),p.target)!=0)revert BadCallback();
        _settle(p.settlement);
    }
    function onPreLiquidate(uint256 repaid,bytes calldata cb) external {
        if(!running||msg.sender!=callbackTarget||callbackTarget==address(0)||keccak256(cb)!=callbackHash||repaid==0||repaid>debtLimit)revert BadCallback();
        callbackTarget=address(0);_unwindAndApprove(abi.decode(cb,(Hop[])),repaid,msg.sender);
    }
    function _euler(EulerPlan memory p,uint256 repay) internal {
        if(PEVault(p.vault).EVC()!=EVC||PEVault(p.collateralVault).EVC()!=EVC||PEVault(p.vault).debtOf(address(this))!=0)revert InvalidPlan();
        _begin(PEVault(p.vault).asset(),PEVault(p.collateralVault).asset(),p.unwind,p.settlement);
        bytes memory cb=abi.encode(p,repay);callbackTarget=EVC;callbackHash=keccak256(cb);
        PEEVC.Item[] memory items=new PEEVC.Item[](5);
        items[0]=PEEVC.Item(EVC,address(0),0,abi.encodeCall(PEEVC.enableController,(address(this),p.vault)));
        items[1]=PEEVC.Item(EVC,address(0),0,abi.encodeCall(PEEVC.enableCollateral,(address(this),p.collateralVault)));
        items[2]=PEEVC.Item(address(this),address(this),0,abi.encodeCall(this.eulerStep,(cb)));
        items[3]=PEEVC.Item(p.vault,address(this),0,abi.encodeCall(PEVault.disableController,()));
        items[4]=PEEVC.Item(EVC,address(0),0,abi.encodeCall(PEEVC.disableCollateral,(address(this),p.collateralVault)));
        PEEVC(EVC).batch(items);
        if(callbackTarget!=address(0)||PEVault(p.vault).debtOf(address(this))!=0||PEToken(loan).allowance(address(this),p.vault)!=0)revert BadCallback();
        _settle(p.settlement);
    }
    function eulerStep(bytes calldata cb) external {
        if(!running||msg.sender!=EVC||callbackTarget!=EVC||keccak256(cb)!=callbackHash)revert BadCallback();callbackTarget=address(0);
        (EulerPlan memory p,uint256 repay)=abi.decode(cb,(EulerPlan,uint256));
        uint256 sharesBefore=PEToken(p.collateralVault).balanceOf(address(this));
        (uint256 maximum,)=PEVault(p.vault).checkLiquidation(address(this),p.borrower,p.collateralVault);
        if(repay==0||repay>maximum)revert InvalidPlan();
        PEVault(p.vault).liquidate(p.borrower,p.collateralVault,repay,1);
        uint256 seized=PEToken(p.collateralVault).balanceOf(address(this))-sharesBefore;
        PEVault(p.collateralVault).redeem(seized,address(this),address(this));
        if(PEToken(p.collateralVault).balanceOf(address(this))!=sharesBefore)revert BadCallback();
        if(collateral!=loan)_walk(p.unwind,PEToken(collateral).balanceOf(address(this))-beforeCollateral);
        uint256 debt=PEVault(p.vault).debtOf(address(this));
        if(debt==0||debt>debtLimit||PEToken(loan).balanceOf(address(this))<=beforeLoan+debt)revert InsufficientProfit();
        _approve(loan,p.vault,debt);PEVault(p.vault).repay(debt,address(this));
    }
}

contract BookExecutor is ProtocolBase {
    CrossedBooksResolver public immutable resolver;
    constructor(address o,uint256 c,uint256 cap,address evc) ProtocolBase(o,c,cap,evc) {resolver=new CrossedBooksResolver(MIDNIGHT);}
    function _dispatch(Plan calldata p) internal override {
        if(p.strategy==4)_midnight(abi.decode(p.arguments,(MidnightPlan)),p.amountIn);
        else if(p.strategy==5)_crossed(p.arguments,p.amountIn);
        else revert InvalidPlan();
    }
    function _marketId(CrossedBooksMarket memory m) internal pure returns(bytes32) {
        return keccak256(abi.encodePacked(uint8(0xff),m.midnight,uint256(0),keccak256(abi.encodePacked(hex"600b380380600b5f395ff3",abi.encode(m)))));
    }
    function _midnight(MidnightPlan memory p,uint256 units) internal {
        CrossedBooksMarket memory m=PEMidnight(MIDNIGHT).toMarket(p.market);
        if(m.chainId!=chainId||m.midnight!=MIDNIGHT||_marketId(m)!=p.market||p.collateralIndex>=m.collateralParams.length)revert InvalidPlan();
        _begin(m.loanToken,m.collateralParams[p.collateralIndex].token,p.unwind,p.settlement);
        bytes memory cb=abi.encode(p);callbackTarget=MIDNIGHT;callbackHash=keccak256(cb);
        (,uint256 repaid)=PEMidnight(MIDNIGHT).liquidate(m,p.collateralIndex,0,units,p.borrower,p.matured,address(this),address(this),cb);
        if(callbackTarget!=address(0)||repaid!=units||PEToken(loan).allowance(address(this),MIDNIGHT)!=0)revert BadCallback();
        _settle(p.settlement);
    }
    function onLiquidate(address sender,bytes32 id,CrossedBooksMarket calldata m,uint256 index,uint256,uint256 units,address borrower,address receiver,bytes calldata cb,uint256) external returns(bytes32) {
        if(!running||msg.sender!=MIDNIGHT||callbackTarget!=MIDNIGHT||sender!=address(this)||receiver!=address(this)||keccak256(cb)!=callbackHash||units!=debtLimit)revert BadCallback();
        MidnightPlan memory p=abi.decode(cb,(MidnightPlan));
        if(id!=p.market||_marketId(m)!=id||index!=p.collateralIndex||borrower!=p.borrower)revert BadCallback();
        callbackTarget=address(0);_unwindAndApprove(p.unwind,units,MIDNIGHT);return CALLBACK_SUCCESS;
    }
    function _crossed(bytes memory args,uint256 limit) internal {
        (CrossedBooksTake[] memory sells,CrossedBooksTake[] memory buys,Hop[] memory conversion)=abi.decode(args,(CrossedBooksTake[],CrossedBooksTake[],Hop[]));
        if(sells.length==0||buys.length==0||sells.length>8||buys.length>8)revert InvalidPlan();
        address token=sells[0].offer.market.loanToken;_path(conversion,token,USDC);
        uint256 total;for(uint256 i;i<sells.length;i++)total+=sells[i].units;if(total!=limit)revert InvalidPlan();
        uint256 beforeBalance=PEToken(token).balanceOf(address(this));resolver.resolve(sells,buys,1);
        uint256 delta=PEToken(token).balanceOf(address(this))-beforeBalance;if(delta==0)revert InsufficientProfit();
        if(token!=USDC){_walk(conversion,delta);if(PEToken(token).balanceOf(address(this))!=beforeBalance)revert BadCallback();}
    }
}
