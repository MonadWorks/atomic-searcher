// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;
import "./AerodromePool.sol";

struct BlueMarket { address loanToken; address collateralToken; address oracle; address irm; uint256 lltv; }
interface IBlue {
    function flashLoan(address,uint256,bytes calldata) external;
    function idToMarketParams(bytes32) external view returns (BlueMarket memory);
    function liquidate(BlueMarket calldata,address,uint256,uint256,bytes calldata) external returns(uint256,uint256);
}
interface IASAtlas {function shortfall() external view returns(uint256,uint256);function reconcile(uint256) external payable returns(uint256);}
interface IASWETH {function withdraw(uint256) external;}
interface ISToken {
    function balanceOf(address) external view returns(uint256);
    function transfer(address,uint256) external returns(bool);
    function approve(address,uint256) external returns(bool);
    function allowance(address,address) external view returns(uint256);
}
interface ISPool {
    function factory() external view returns(address);
    function token0() external view returns(address);
    function token1() external view returns(address);
    function fee() external view returns(uint24);
    function tickSpacing() external view returns(int24);
    function swap(address,bool,int256,uint160,bytes calldata) external returns(int256,int256);
}
interface IV2Pool { function factory() external view returns(address); function token0() external view returns(address); function token1() external view returns(address); function getReserves() external view returns(uint112,uint112,uint32); function swap(uint256,uint256,address,bytes calldata) external; }
interface IV2Factory {function getPair(address,address) external view returns(address);}
interface ISFactory { function getPool(address,address,uint24) external view returns(address); }
interface ISSlipFactory { function getPool(address,address,int24) external view returns(address); }
interface ISPSM {
    function usdc() external view returns(address);
    function susds() external view returns(address);
    function swapExactIn(address,address,uint256,uint256,address,uint256) external returns(uint256);
}

/// @notice Callback-financed liquidation and fixed-conversion cycles. No owner token allowance.
contract StructuralExecutor {
    address public immutable owner;
    uint256 public immutable chainId;
    uint256 public immutable maxInput;
    bool public paused = true;
    mapping(uint64=>bool) public usedNonces;
    address constant MORPHO=0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb;
    address constant USDC=0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913;
    address constant CBETH=0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22;
    address constant WETH=0x4200000000000000000000000000000000000006;
    address constant SUSDS=0x5875eEE11Cf8398102FdAd704C9E96607675467a;
    address constant PSM=0x1601843c5E9bC251A3272907010AFa41Fa18347E;
    address constant POOL1=0x47cA96Ea59C13F72745928887f84C9F52C3D7348;
    address constant POOL2=0xd0b53D9277642d899DF5C87A3966A349A798F224;
    address constant FACTORY=0x33128a8fC17869897dcE68Ed026d694621f6FDfD;
    address constant SLIP=0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A;
    bytes32 constant MARKET=0x0ca10126f6c94cbd9cf0a48cc9516ae5e3dec5aa68303e6d988ee37c5149bf0d;
    uint160 constant MIN=4295128740;
    uint160 constant MAX=1461446703485210103287273052203988822378723970341;

    address constant ATLAS=0x583dcFef0D240DC80753F0F0B26513feE27D9B77;
    struct AuctionPlan {bytes32 userOpHash;bytes arguments;uint256 amount;Hop[] conversion;uint256 minNet;uint256 maxLiability;uint256 maxBid;uint256 maxGasPrice;uint64 deadline;}
    struct Hop {address pool; address tokenIn; address tokenOut; uint8 kind;}
    struct Request { address borrower; uint256 shares; uint256 maximumDebt; }
    struct Plan {
        uint8 strategy; bytes arguments; uint256 amountIn; uint256 minProfit; uint256 maxGasPrice;
        uint64 deadline; uint64 anchorBlock; bytes32 anchorHash; uint64 lastBlock; uint64 nonce;
    }
    bool private transient running;
    bool private transient inBorrower;
    bool private transient waitingMorpho;
    bool private transient waitingFlash;
    address private transient waitingPool;
    uint8 private transient swapMode;
    uint256 private transient swapMaximum;
    uint256 private transient debtMaximum;
    address private transient genericLoan;
    address private transient genericCollateral;
    address private transient swapToken;
    bool private transient swapDirection;
    bytes32 private transient callbackHash;
    uint256 private transient loanBefore;
    uint256 private transient collateralBefore;
    uint256 private transient usdcBefore;
    uint256 private transient susdsBefore;
    event AuctionExecuted(bytes32 indexed userOpHash,uint256 gross,uint256 bid,uint256 payout,uint256 liabilityBudget);
    event CycleExecuted(uint64 indexed nonce,address indexed settlement,uint256 input,uint256 profit);
    event BorrowerCompleted(address indexed borrower,uint256 repaid,uint256 seized,uint256 profit);
    error Unauthorized(); error InvalidPlan(); error BadCallback(); error InvalidPool(); error InsufficientProfit();
    error AuctionQuote(uint256 gross,uint256 gasUsed);

    modifier ownerIdle() { if(msg.sender!=owner) revert Unauthorized(); if(running) revert BadCallback(); _; }
    constructor(address owner_,uint256 chain_,uint256 cap_) {
        if(owner_==address(0)||chain_!=block.chainid||chain_!=8453||cap_==0||cap_>=1<<255) revert InvalidPlan();
        owner=owner_;chainId=chain_;maxInput=cap_;
    }
    function setPaused(bool value) external ownerIdle {paused=value;}
    function rescue(address token) external ownerIdle { require(ISToken(token).transfer(owner,ISToken(token).balanceOf(address(this)))); }

    /// @dev Use through discarded eth_call. A submitted call executes the same plan.
    function measure(Plan calldata p) external returns(uint256 profit,uint256 gasUsed) {
        uint256 start=gasleft();profit=execute(p);gasUsed=start-gasleft();
    }

    function execute(Plan calldata p) public ownerIdle returns(uint256 profit) {
        if(paused||block.chainid!=chainId||p.amountIn==0||p.amountIn>maxInput||p.minProfit==0||
           tx.gasprice>p.maxGasPrice||block.timestamp>p.deadline||p.deadline>block.timestamp+30||
           block.number<=p.anchorBlock||block.number>p.lastBlock||p.lastBlock>p.anchorBlock+2||
           p.anchorHash==bytes32(0)||blockhash(p.anchorBlock)!=p.anchorHash||usedNonces[p.nonce]) revert InvalidPlan();
        running=true;usedNonces[p.nonce]=true;
        uint256 beforeBalance=ISToken(USDC).balanceOf(address(this));
        if(p.strategy==1) {
            Request[] memory requests=abi.decode(p.arguments,(Request[]));
            Hop[] memory hops=new Hop[](2);
            hops[0]=Hop(POOL1,CBETH,WETH,2);hops[1]=Hop(POOL2,WETH,USDC,1);
            _genericBatch(MARKET,requests,hops,new Hop[](0),p.amountIn);
        } else if(p.strategy==3) {
            (bytes32 id,Request[] memory requests,Hop[] memory unwind,Hop[] memory settlement)=abi.decode(p.arguments,(bytes32,Request[],Hop[],Hop[]));
            _genericBatch(id,requests,unwind,settlement,p.amountIn);
        } else if(p.strategy==2) {
            (address pool,bool reverse)=abi.decode(p.arguments,(address,bool));
            _psm(pool,p.amountIn,reverse);
        } else if(p.strategy==4) {
            (Hop[] memory cycle,Hop[] memory settlement)=abi.decode(p.arguments,(Hop[],Hop[]));
            _fundedCycle(cycle,settlement,p.amountIn);
        } else revert InvalidPlan();
        uint256 afterBalance=ISToken(USDC).balanceOf(address(this));
        if(afterBalance<beforeBalance+p.minProfit) revert InsufficientProfit();
        profit=afterBalance-beforeBalance;
        uint256 ownerBefore=ISToken(USDC).balanceOf(owner);
        require(ISToken(USDC).transfer(owner,profit));
        require(ISToken(USDC).balanceOf(owner)==ownerBefore+profit && ISToken(USDC).balanceOf(address(this))==beforeBalance);
        running=false;
        emit CycleExecuted(p.nonce,USDC,p.amountIn,profit);
    }

    receive() external payable {if(msg.sender!=WETH) revert Unauthorized();}

    // Native pool quotes are independent of financing. A Morpho flash loan
    // lets the same authenticated conversion legs form a complete DEX cycle,
    // including ordinary Aerodrome pools without a CL-style callback.
    function _fundedCycle(Hop[] memory cycle,Hop[] memory settlement,uint256 amount) private {
        if(cycle.length<2||cycle.length>4) revert InvalidPlan();
        address token=cycle[0].tokenIn;
        if(token!=USDC&&token!=WETH) revert InvalidPlan();
        for(uint256 i;i<cycle.length;i++) {
            Hop memory h=cycle[i];
            if(h.tokenIn==h.tokenOut||h.tokenOut==address(0)||
               (i>0&&h.tokenIn!=cycle[i-1].tokenOut)) revert InvalidPlan();
            for(uint256 j;j<i;j++) if(cycle[j].pool==h.pool||cycle[j].tokenIn==h.tokenIn) revert InvalidPlan();
            _verifyHop(h);
        }
        if(cycle[cycle.length-1].tokenOut!=token) revert InvalidPlan();
        _path(settlement,token,USDC);
        genericLoan=token;loanBefore=ISToken(token).balanceOf(address(this));debtMaximum=amount;
        bytes memory cb=abi.encode(cycle);callbackHash=keccak256(cb);waitingFlash=true;
        IBlue(MORPHO).flashLoan(token,amount,cb);
        if(waitingFlash||waitingPool!=address(0)||ISToken(token).allowance(address(this),MORPHO)!=0) revert BadCallback();
        uint256 residual=ISToken(token).balanceOf(address(this))-loanBefore;
        if(residual==0) revert InsufficientProfit();
        if(token!=USDC) {
            _walk(settlement,residual);
            if(ISToken(token).balanceOf(address(this))!=loanBefore) revert BadCallback();
        }
        genericLoan=address(0);callbackHash=bytes32(0);
    }
    function onMorphoFlashLoan(uint256 amount,bytes calldata cb) external {
        if(msg.sender!=MORPHO||!running||!waitingFlash||amount!=debtMaximum||keccak256(cb)!=callbackHash) revert BadCallback();
        waitingFlash=false;
        if(ISToken(genericLoan).balanceOf(address(this))!=loanBefore+amount) revert BadCallback();
        _walk(abi.decode(cb,(Hop[])),amount);
        if(ISToken(genericLoan).balanceOf(address(this))<=loanBefore+amount) revert InsufficientProfit();
        _approve(genericLoan,MORPHO,amount);
    }
    function _auctionTrade(AuctionPlan memory p) private returns(uint256 gross) {
        if(paused||block.chainid!=chainId||p.amount==0||p.amount>maxInput||p.minNet==0||p.maxLiability==0||block.number>p.deadline||tx.gasprice>p.maxGasPrice) revert InvalidPlan();
        uint256 beforeUSDC=ISToken(USDC).balanceOf(address(this));
        (bytes32 id,Request[] memory requests,Hop[] memory unwind,Hop[] memory settlement)=abi.decode(p.arguments,(bytes32,Request[],Hop[],Hop[]));
        _genericBatch(id,requests,unwind,settlement,p.amount);
        _path(p.conversion,USDC,WETH);
        uint256 fresh=ISToken(USDC).balanceOf(address(this))-beforeUSDC;
        gross=_walk(p.conversion,fresh);
        if(ISToken(USDC).balanceOf(address(this))!=beforeUSDC) revert BadCallback();
    }
    /// @dev Discarded eth_call quote, with the exact oracle report applied by the caller's simulation.
    function measureAuction(AuctionPlan calldata p) external ownerIdle returns(uint256 gross,uint256 gasUsed) {
        uint256 start=gasleft();running=true;gross=_auctionTrade(p);running=false;gasUsed=start-gasleft();
    }
    /// @dev Every child reverts, so alternate paths see identical liquidity and debt.
    function measureAuctions(AuctionPlan[] calldata plans,uint256 gasPerPlan) external ownerIdle returns(uint256[2][] memory quotes) {
        if(plans.length>8||gasPerPlan>6_000_000) revert InvalidPlan();
        quotes=new uint256[2][](plans.length);
        for(uint256 i;i<plans.length;i++) {
            try this.quoteAuction{gas:gasPerPlan}(plans[i]) {} catch(bytes memory reason) {
                if(reason.length==68 && bytes4(reason)==AuctionQuote.selector) {
                    uint256 gross;uint256 used;
                    assembly {gross:=mload(add(reason,36)) used:=mload(add(reason,68))}
                    quotes[i]=[gross,used];
                }
            }
        }
    }
    function quoteAuction(AuctionPlan calldata p) external {
        if(msg.sender!=address(this)||running) revert Unauthorized();
        uint256 start=gasleft();running=true;uint256 gross=_auctionTrade(p);
        revert AuctionQuote(gross,start-gasleft());
    }
    function atlasSolverCall(address from,address environment,address bidToken,uint256 bid,bytes calldata solverData,bytes calldata) external payable {
        if(msg.sender!=ATLAS||from!=owner||running||msg.value!=0||bidToken!=address(0)||environment==address(0)) revert Unauthorized();
        AuctionPlan memory p=abi.decode(solverData,(AuctionPlan));
        if(bid>p.maxBid||p.userOpHash==bytes32(0)) revert InvalidPlan();
        running=true;uint256 beforeNative=address(this).balance;
        uint256 gross=_auctionTrade(p);
        // Existing balances cannot finance a losing bid or disguise negative net economics.
        if(gross<bid+p.maxLiability+p.minNet) revert InsufficientProfit();
        IASWETH(WETH).withdraw(gross);
        (bool paid,)=environment.call{value:bid}("");require(paid);
        (uint256 gasLiability,uint256 borrowLiability)=IASAtlas(ATLAS).shortfall();
        if(borrowLiability!=0||gasLiability>p.maxLiability) revert InvalidPlan();
        if(IASAtlas(ATLAS).reconcile(p.maxLiability)!=0) revert InvalidPlan();
        uint256 payout=gross-bid;
        (bool sent,)=owner.call{value:payout}("");require(sent);
        if(address(this).balance!=beforeNative) revert BadCallback();
        emit AuctionExecuted(p.userOpHash,gross,bid,payout,p.maxLiability);running=false;
    }
    function liquidateOne(Request calldata) external pure {revert Unauthorized();}
    function onMorphoLiquidate(uint256 repaid,bytes calldata callbackData) external {
        if(msg.sender!=MORPHO||!running||!inBorrower||!waitingMorpho) revert BadCallback();
        waitingMorpho=false;require(repaid>0&&repaid<=debtMaximum);
        _genericCallback(repaid,callbackData);
    }
    function _genericBatch(bytes32 id,Request[] memory requests,Hop[] memory unwind,Hop[] memory settlement,uint256 budget) private {
        if(requests.length==0||requests.length>8) revert InvalidPlan();
        BlueMarket memory m=IBlue(MORPHO).idToMarketParams(id);
        if(keccak256(abi.encode(m))!=id||m.loanToken==address(0)||m.lltv==0) revert InvalidPlan();
        _path(unwind,m.collateralToken,m.loanToken);
        _path(settlement,m.loanToken,USDC);
        uint256 total;uint256 completed;
        for(uint256 i;i<requests.length;i++) {
            total+=requests[i].maximumDebt;
            if(total>budget) revert InvalidPlan();
            try this.liquidateMarket(m,requests[i],unwind,settlement) {completed++;} catch {}
        }
        if(total!=budget||completed==0) revert InsufficientProfit();
    }
    function liquidateMarket(BlueMarket calldata m,Request calldata request,Hop[] calldata unwind,Hop[] calldata settlement) external {
        if(msg.sender!=address(this)||!running||inBorrower) revert Unauthorized();
        if(request.shares==0||request.maximumDebt==0) revert InvalidPlan();
        inBorrower=true;waitingMorpho=true;debtMaximum=request.maximumDebt;
        genericLoan=m.loanToken;genericCollateral=m.collateralToken;
        loanBefore=ISToken(m.loanToken).balanceOf(address(this));
        collateralBefore=ISToken(m.collateralToken).balanceOf(address(this));
        uint256 beforeUSDC=ISToken(USDC).balanceOf(address(this));
        bytes memory cb=abi.encode(unwind);callbackHash=keccak256(cb);
        (uint256 seized,uint256 repaid)=IBlue(MORPHO).liquidate(m,request.borrower,0,request.shares,cb);
        if(waitingMorpho||waitingPool!=address(0)||repaid>request.maximumDebt) revert BadCallback();
        if((m.collateralToken!=m.loanToken&&ISToken(m.collateralToken).balanceOf(address(this))!=collateralBefore)||ISToken(m.loanToken).allowance(address(this),MORPHO)!=0) revert BadCallback();
        uint256 residual=ISToken(m.loanToken).balanceOf(address(this))-loanBefore;
        if(residual==0) revert InsufficientProfit();
        if(m.loanToken!=USDC) _walk(settlement,residual);
        if(m.loanToken!=USDC&&ISToken(m.loanToken).balanceOf(address(this))!=loanBefore) revert BadCallback();
        uint256 afterUSDC=ISToken(USDC).balanceOf(address(this));
        if(afterUSDC<=beforeUSDC) revert InsufficientProfit();
        emit BorrowerCompleted(request.borrower,repaid,seized,afterUSDC-beforeUSDC);
        genericLoan=address(0);genericCollateral=address(0);callbackHash=bytes32(0);inBorrower=false;
    }
    function _genericCallback(uint256 repaid,bytes calldata cb) private {
        if(keccak256(cb)!=callbackHash) revert BadCallback();
        Hop[] memory hops=abi.decode(cb,(Hop[]));
        _walk(hops,ISToken(genericCollateral).balanceOf(address(this))-collateralBefore);
        if(ISToken(genericLoan).balanceOf(address(this))<=loanBefore+repaid) revert InsufficientProfit();
        _approve(genericLoan,MORPHO,repaid);
    }
    function _path(Hop[] memory hops,address start,address end) private view {
        if(start==end) {if(hops.length!=0) revert InvalidPlan();return;}
        if(hops.length==0||hops.length>4) revert InvalidPlan();
        address current=start;
        for(uint256 i;i<hops.length;i++) {
            Hop memory h=hops[i];
            if(h.tokenIn!=current||h.tokenOut==current||h.tokenOut==address(0)) revert InvalidPlan();
            for(uint256 j;j<i;j++) if(hops[j].pool==h.pool||hops[j].tokenIn==h.tokenOut) revert InvalidPlan();
            _verifyHop(h);current=h.tokenOut;
        }
        if(current!=end) revert InvalidPlan();
    }
    function _verifyHop(Hop memory h) private view {
        address a=h.tokenIn<h.tokenOut?h.tokenIn:h.tokenOut;address b=h.tokenIn<h.tokenOut?h.tokenOut:h.tokenIn;
        ISPool p=ISPool(h.pool);address f=p.factory();
        if(p.token0()!=a||p.token1()!=b) revert InvalidPool();
        if(h.kind==1) {
            if(f!=FACTORY||ISFactory(f).getPool(a,b,p.fee())!=h.pool) revert InvalidPool();
        } else if(h.kind==2) {
            if(f!=SLIP&&f!=0xaDe65c38CD4849aDBA595a4323a8C7DdfE89716a&&f!=0xf8f2eB4940CFE7d13603DDDD87f123820Fc061Ef) revert InvalidPool();
            if(ISSlipFactory(f).getPool(a,b,p.tickSpacing())!=h.pool) revert InvalidPool();
        } else if(h.kind==3) {
            if(f!=0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6||IV2Factory(f).getPair(a,b)!=h.pool) revert InvalidPool();
        } else if(h.kind==4) {
            if(f!=0x420DD381b31aEf6683db6B902084cB0FFECe40Da||
               IAerodromeFactory(f).getPool(a,b,IAerodromePool(h.pool).stable())!=h.pool) revert InvalidPool();
        } else revert InvalidPool();
    }
    function _walk(Hop[] memory hops,uint256 amount) private returns(uint256) {
        for(uint256 i;i<hops.length;i++) {
            Hop memory h=hops[i];uint256 beforeOut=ISToken(h.tokenOut).balanceOf(address(this));
            uint256 beforeIn=ISToken(h.tokenIn).balanceOf(address(this));
            if(amount==0||amount>=1<<255||amount>beforeIn) revert InvalidPlan();
            bool direction=h.tokenIn<h.tokenOut;
            if(h.kind==3||h.kind==4) {
                uint256 out;
                if(h.kind==4)out=IAerodromePool(h.pool).getAmountOut(amount,h.tokenIn);
                else {
                    (uint112 r0,uint112 r1,)=IV2Pool(h.pool).getReserves();
                    (uint256 rIn,uint256 rOut)=direction?(uint256(r0),uint256(r1)):(uint256(r1),uint256(r0));
                    out=amount*997*rOut/(rIn*1000+amount*997);
                }
                if(out==0) revert InsufficientProfit();
                _transfer(h.tokenIn,h.pool,amount);
                IV2Pool(h.pool).swap(direction?0:out,direction?out:0,address(this),"");
            } else {
                if(waitingPool!=address(0)) revert BadCallback();
                waitingPool=h.pool;swapMaximum=amount;swapMode=4;swapToken=h.tokenIn;swapDirection=direction;
                (int256 a0,int256 a1)=ISPool(h.pool).swap(address(this),direction,int256(amount),direction?MIN:MAX,"");
                if(waitingPool!=address(0)||(direction?a0:a1)!=int256(amount)) revert BadCallback();
            }
            if(ISToken(h.tokenIn).balanceOf(address(this))+amount!=beforeIn) revert BadCallback();
            amount=ISToken(h.tokenOut).balanceOf(address(this))-beforeOut;
        }
        return amount;
    }
    function _transfer(address token,address to,uint256 amount) private {
        (bool ok,bytes memory ret)=token.call(abi.encodeWithSelector(ISToken.transfer.selector,to,amount));
        require(ok&&(ret.length==0||abi.decode(ret,(bool))));
    }
    function _approve(address token,address spender,uint256 amount) private {
        require(ISToken(token).allowance(address(this),spender)==0);
        (bool ok,bytes memory ret)=token.call(abi.encodeWithSelector(ISToken.approve.selector,spender,amount));
        require(ok&&(ret.length==0||abi.decode(ret,(bool))));
    }
    function _psm(address pool,uint256 amount,bool reverse) private {
        if(ISPool(pool).factory()!=FACTORY||ISPool(pool).token0()!=SUSDS||ISPool(pool).token1()!=USDC||
           ISFactory(FACTORY).getPool(SUSDS,USDC,ISPool(pool).fee())!=pool) revert InvalidPool();
        require(ISPSM(PSM).usdc()==USDC&&ISPSM(PSM).susds()==SUSDS);
        usdcBefore=ISToken(USDC).balanceOf(address(this));susdsBefore=ISToken(SUSDS).balanceOf(address(this));
        waitingPool=pool;swapMaximum=amount;swapMode=reverse?3:2;
        ISPool(pool).swap(address(this),reverse,reverse?-int256(amount):int256(amount),reverse?MIN:MAX,"");
        require(waitingPool==address(0));
        require(ISToken(SUSDS).balanceOf(address(this))==susdsBefore);
        require(ISToken(SUSDS).allowance(address(this),PSM)==0&&ISToken(USDC).allowance(address(this),PSM)==0);
    }
    function uniswapV3SwapCallback(int256 a0,int256 a1,bytes calldata) external {
        if(!running||waitingPool==address(0)||msg.sender!=waitingPool) revert BadCallback();
        address pool=waitingPool;waitingPool=address(0);
        if(swapMode==4) {
            int256 paid=swapDirection?a0:a1;int256 received=swapDirection?a1:a0;
            if(paid<=0||received>=0||uint256(paid)>swapMaximum) revert BadCallback();
            _transfer(swapToken,pool,uint256(paid));
        } else if(swapMode==2) {
            require(a0<0&&a0!=type(int256).min&&a1>0&&uint256(a1)<=swapMaximum);
            uint256 received=uint256(-a0);
            require(ISToken(SUSDS).balanceOf(address(this))==susdsBefore+received);
            require(ISToken(SUSDS).approve(PSM,received));
            ISPSM(PSM).swapExactIn(SUSDS,USDC,received,0,address(this),0);
            if(ISToken(USDC).balanceOf(address(this))<=usdcBefore+uint256(a1)) revert InsufficientProfit();
            require(ISToken(USDC).transfer(pool,uint256(a1)));
        } else if(swapMode==3) {
            require(a0>0&&a1<0&&a1!=type(int256).min&&uint256(-a1)==swapMaximum);
            require(ISToken(USDC).balanceOf(address(this))==usdcBefore+uint256(-a1));
            require(ISToken(USDC).approve(PSM,uint256(-a1)));
            ISPSM(PSM).swapExactIn(USDC,SUSDS,uint256(-a1),uint256(a0)+1,address(this),0);
            uint256 received=ISToken(SUSDS).balanceOf(address(this))-susdsBefore;
            require(received>uint256(a0));
            require(ISToken(SUSDS).transfer(pool,uint256(a0)));
            require(ISToken(SUSDS).approve(PSM,received-uint256(a0)));
            ISPSM(PSM).swapExactIn(SUSDS,USDC,received-uint256(a0),1,address(this),0);
        } else revert BadCallback();
    }
}
