// SPDX-License-Identifier: GPL-2.0-or-later
pragma solidity ^0.8.33;
import {ProtocolExecutor,BookExecutor,ProtocolBase,PEMarket,PEEVC} from "../src/ProtocolExecutor.sol";
import {CrossedBooksResolver,CrossedBooksMarket,CrossedBooksCollateralParams,CrossedBooksOffer,CrossedBooksTake} from "../src/CrossedBooksResolver.sol";
import {StructuralTokenMock,StructuralFactoryMock,StructuralPoolMock,VmStructural} from "./StructuralExecutor.t.sol";
contract SuiteOracleMock {function price() external pure returns(uint256){return 1e36;}}
contract SuiteBlueMock {
    function isAuthorized(address,address) external pure returns(bool){return true;}
    function accrueInterest(PEMarket calldata) external {}
    function market(bytes32) external pure returns(uint128,uint128,uint128,uint128,uint128,uint128){return(0,0,9999999,9000000,0,0);}
    function position(bytes32,address) external pure returns(uint256,uint128,uint128){return(0,10000000,13000000);}

    function flashLoan(address t,uint256 a,bytes calldata cb) external {
        StructuralTokenMock(t).transfer(msg.sender,a);ProtocolExecutor(msg.sender).onMorphoFlashLoan(a,cb);StructuralTokenMock(t).transferFrom(msg.sender,address(this),a);
    }
}
contract SuiteCometMock {
    address constant U=0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913;address constant C=address(0x111);
    bool public consumed;
    uint256 public pendingInventory;address public pendingBorrower;
    function prepareAbsorb(address a,uint256 n) external {pendingBorrower=a;pendingInventory=n;}
    function isLiquidatable(address a) external view returns(bool){return pendingInventory>0&&a==pendingBorrower;}
    function isBorrowCollateralized(address a) external view returns(bool){return pendingInventory==0||a!=pendingBorrower;}
    function borrowBalanceOf(address a) external view returns(uint256){return pendingInventory>0&&a==pendingBorrower?10000000:0;}
    function collateralBalanceOf(address a,address) external view returns(uint256){return a==pendingBorrower?pendingInventory:0;}
    function isAbsorbPaused() external pure returns(bool){return false;}
    function getReserves() external view returns(int256){return pendingInventory>0?int256(2000000):int256(0);}
    function targetReserves() external pure returns(uint256){return 1000000;}
    function isBuyPaused() external pure returns(bool){return false;}
    function numAssets() external pure returns(uint8){return 1;}
    function baseScale() external pure returns(uint256){return 1000000;}
    function getAssetInfo(uint8) external pure returns(uint8,address,address,uint64,uint64,uint64,uint64,uint128){return(0,C,address(0x333),1000000,0,0,0,0);}
    function getCollateralReserves(address) external view returns(uint256){return pendingInventory>0?0:StructuralTokenMock(C).balanceOf(address(this));}
    function quoteCollateral(address,uint256 a) external pure returns(uint256){return a*110/100;}
    function baseToken() external pure returns(address){return U;}
    function absorb(address,address[] calldata accounts) external {
        require(accounts.length==1&&accounts[0]==pendingBorrower&&pendingInventory>0);
        pendingInventory=0;consumed=false;
    }
    function consume() external {consumed=true;}
    function buyCollateral(address token,uint256,uint256 amount,address receiver) external {
        require(!consumed&&pendingInventory==0&&token==C);StructuralTokenMock(U).transferFrom(msg.sender,address(this),amount);StructuralTokenMock(C).transfer(receiver,amount*110/100);
    }
}
contract SuitePreMock {
    address public constant MORPHO=0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb;
    address constant U=0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913;address constant C=address(0x111);
    function marketParams() public pure returns(PEMarket memory){return PEMarket(U,C,address(0x333),address(2),8e17);}
    function preLiquidationParams() external pure returns(uint256,uint256,uint256,uint256,uint256,address){return(7e17,1e18,1e18,11e17,11e17,address(0x333));}
    function ID() external pure returns(bytes32){return keccak256(abi.encode(marketParams()));}
    function preLiquidate(address,uint256,uint256 shares,bytes calldata cb) external returns(uint256,uint256) {
        StructuralTokenMock(C).transfer(msg.sender,shares*110/100);ProtocolExecutor(msg.sender).onPreLiquidate(shares,cb);StructuralTokenMock(U).transferFrom(msg.sender,address(this),shares);return(shares*110/100,shares);
    }
}
contract SuiteEVCMock {
    address public account;address controller;bool busy;address testCollateral;
    function setTestCollateral(address v) external {testCollateral=v;}
    function getCollaterals(address) external view returns(address[] memory out){out=new address[](1);out[0]=testCollateral;}
    function batch(PEEVC.Item[] calldata items) external payable {
        require(!busy);busy=true;account=msg.sender;
        for(uint256 i;i<items.length;i++) {
            require(items[i].onBehalfOfAccount==(items[i].targetContract==address(this)?address(0):msg.sender));
            (bool ok,bytes memory err)=items[i].targetContract.call(items[i].data);if(!ok)assembly("memory-safe"){revert(add(err,32),mload(err))}
        }
        require(SuiteVaultMock(controller).debtOf(account)==0);busy=false;
    }
    function enableController(address a,address c) external {require(msg.sender==address(this)&&a==account);controller=c;}
    function enableCollateral(address a,address) external view {require(msg.sender==address(this)&&a==account);}
    function disableCollateral(address a,address) external view {require(msg.sender==address(this)&&a==account);}
}
contract SuiteVaultMock is StructuralTokenMock {
    address public constant EVC=0x5301c7dD20bD945D2013b48ed0DEE3A284ca8989;
    address public asset;mapping(address=>uint256) public debtOf;bool consumed;
    function initialize(address a) external {asset=a;}
    function cash() external view returns(uint256){return StructuralTokenMock(asset).balanceOf(address(this));}
    function convertToAssets(uint256 a) external pure returns(uint256){return a;}
    function hookConfig() external pure returns(address,uint32){return(address(0),0);}
    function consume() external {consumed=true;}
    function checkLiquidation(address,address,address) external view returns(uint256,uint256){return consumed?(0,0):(uint256(1e7),uint256(11e6));}
    function liquidate(address,address cv,uint256 a,uint256) external {require(!consumed);debtOf[msg.sender]+=a;SuiteVaultMock(cv).mint(msg.sender,a*110/100);consumed=true;}
    function redeem(uint256 shares,address receiver,address owner) external returns(uint256){require(msg.sender==owner);balanceOf[owner]-=shares;StructuralTokenMock(asset).transfer(receiver,shares);return shares;}
    function repay(uint256 a,address who) external returns(uint256){require(a==debtOf[who]);StructuralTokenMock(asset).transferFrom(msg.sender,address(this),a);debtOf[who]=0;return a;}
    function disableController() external view {require(msg.sender==EVC&&debtOf[SuiteEVCMock(EVC).account()]==0);}
}
contract SuiteMidnightMock {
    address constant U=0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913;address constant C=address(0x111);
    mapping(address=>uint256) units;bool public consumed;
    function marketId(CrossedBooksMarket memory m) public pure returns(bytes32){return keccak256(abi.encodePacked(uint8(0xff),m.midnight,uint256(0),keccak256(abi.encodePacked(hex"600b380380600b5f395ff3",abi.encode(m)))));}
    function market() public view returns(CrossedBooksMarket memory m){
        CrossedBooksCollateralParams[] memory cps=new CrossedBooksCollateralParams[](1);cps[0]=CrossedBooksCollateralParams(C,8e17,3e17,address(0x333));
        return CrossedBooksMarket(8453,address(this),U,cps,2000,0,address(0),address(0));
    }
    function toMarket(bytes32 id) external view returns(CrossedBooksMarket memory m){m=market();require(id==marketId(m));}
    function liquidationLocked(bytes32,address) external pure returns(bool){return false;}
    function debt(bytes32,address) external view returns(uint128){return consumed?0:10000000;}
    function collateralBitmap(bytes32,address) external pure returns(uint128){return 1;}
    function collateral(bytes32,address,uint256) external pure returns(uint128){return 11000000;}
    function consume() external {consumed=true;}
    function liquidate(CrossedBooksMarket calldata m,uint256 i,uint256,uint256 u,address borrower,bool,address receiver,address callback,bytes calldata cb) external returns(uint256,uint256){
        require(!consumed);consumed=true;StructuralTokenMock(C).transfer(receiver,u*110/100);
        BookExecutor(callback).onLiquidate(msg.sender,marketId(m),m,i,u*110/100,u,borrower,receiver,cb,0);
        StructuralTokenMock(U).transferFrom(msg.sender,address(this),u);return(u*110/100,u);
    }
    function take(CrossedBooksOffer memory o,bytes memory,uint256 u,address taker,address receiver,address callback,bytes memory cb) external returns(uint256 buyer,uint256 seller) {
        require(!consumed);buyer=u;seller=o.buy?u*110/100:u;
        if(!o.buy){units[taker]+=u;CrossedBooksResolver(callback).onBuy(marketId(o.market),o.market,buyer,u,0,taker,cb);StructuralTokenMock(U).transferFrom(msg.sender,address(this),buyer);}
        else {units[taker]-=u;StructuralTokenMock(U).transfer(receiver,seller);}
    }
}
contract ProtocolExecutorTest {
    VmStructural constant vm=VmStructural(address(uint160(uint256(keccak256("hevm cheat code")))));
    address constant U=0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913;address constant C=address(0x111);
    address constant B=0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb;
    address constant M=0xAdedD8ab6dE832766Fedf0FaC4992E5C4D3EA18A;
    address constant EVC=0x5301c7dD20bD945D2013b48ed0DEE3A284ca8989;
    address constant COMET=0xb125E6687d4313864e53df431d5425969c15Eb2F;
    address constant F=0x33128a8fC17869897dcE68Ed026d694621f6FDfD;address constant P=address(0x222);
    bytes32 constant HASH=bytes32(uint256(123));ProtocolExecutor bot;BookExecutor book;
    function setUp() public {
        vm.chainId(8453);vm.roll(101);vm.warp(1000);vm.setBlockhash(100,HASH);
        vm.etch(U,type(StructuralTokenMock).runtimeCode);vm.etch(C,type(StructuralTokenMock).runtimeCode);
        vm.etch(F,type(StructuralFactoryMock).runtimeCode);vm.etch(P,type(StructuralPoolMock).runtimeCode);
        StructuralPoolMock(P).initialize(F,C,U,1,1);StructuralFactoryMock(F).add(C,U,P);
        vm.etch(B,type(SuiteBlueMock).runtimeCode);vm.etch(COMET,type(SuiteCometMock).runtimeCode);vm.etch(M,type(SuiteMidnightMock).runtimeCode);vm.etch(EVC,type(SuiteEVCMock).runtimeCode);
        for(uint256 i;i<2;i++){address t=i==0?U:C;StructuralTokenMock(t).mint(B,1e12);StructuralTokenMock(t).mint(COMET,1e12);StructuralTokenMock(t).mint(M,1e12);StructuralTokenMock(t).mint(P,1e12);}
        bot=new ProtocolExecutor(address(this),8453,1e15,EVC);book=new BookExecutor(address(this),8453,1e15,EVC);bot.setPaused(false);book.setPaused(false);
    }
    function hops() private pure returns(ProtocolBase.Hop[] memory h){h=new ProtocolBase.Hop[](1);h[0]=ProtocolBase.Hop(P,C,U,1);}
    function empty() private pure returns(ProtocolBase.Hop[] memory h){h=new ProtocolBase.Hop[](0);}
    function plan(uint8 s,bytes memory args) private pure returns(ProtocolBase.Plan memory){return ProtocolBase.Plan(s,args,1e6,1e5,1e10,1006,100,HASH,102,7);}
    function compoundPlan() private pure returns(ProtocolBase.Plan memory){return plan(1,abi.encode(ProtocolBase.CompoundPlan(COMET,C,new address[](0),hops(),empty())));}
    function testCompoundFlashRepaymentAndProfit() public {
        StructuralTokenMock(U).mint(address(bot),777);StructuralTokenMock(C).mint(address(bot),888);
        require(bot.execute(compoundPlan())==1e5);require(StructuralTokenMock(U).balanceOf(address(this))==1e5);
        require(StructuralTokenMock(U).balanceOf(address(bot))==777&&StructuralTokenMock(C).balanceOf(address(bot))==888);
        require(StructuralTokenMock(U).allowance(address(bot),B)==0&&StructuralTokenMock(U).allowance(address(bot),COMET)==0);
    }
    function testCompetitionAndProfitFloorRevertEverything() public {
        ProtocolBase.Plan memory p=compoundPlan();p.minProfit=100001;vm.expectRevert();bot.execute(p);require(!bot.usedNonces(7));
        p.minProfit=100000;SuiteCometMock(COMET).consume();vm.expectRevert();bot.execute(p);require(StructuralTokenMock(U).balanceOf(address(this))==0);
    }
    function testPreLiquidationCallbackFunded() public {
        SuitePreMock pre=new SuitePreMock();StructuralTokenMock(C).mint(address(pre),1e12);
        require(bot.execute(plan(3,abi.encode(ProtocolBase.PrePlan(address(pre),address(7),1e6,hops(),empty()))))==1e5);
        require(StructuralTokenMock(U).allowance(address(bot),address(pre))==0);
    }
    function testEulerDeferredDebtIsFullyRepaid() public {
        SuiteVaultMock debt=new SuiteVaultMock();SuiteVaultMock coll=new SuiteVaultMock();debt.initialize(U);coll.initialize(C);StructuralTokenMock(C).mint(address(coll),1e12);
        require(bot.execute(plan(2,abi.encode(ProtocolBase.EulerPlan(address(debt),address(7),address(coll),hops(),empty()))))==1e5);
        require(debt.debtOf(address(bot))==0&&coll.balanceOf(address(bot))==0&&StructuralTokenMock(U).allowance(address(bot),address(debt))==0);
    }
    function testMidnightCallbackFunded() public {
        bytes32 id=SuiteMidnightMock(M).marketId(SuiteMidnightMock(M).market());
        require(book.execute(plan(4,abi.encode(ProtocolBase.MidnightPlan(id,address(7),0,false,hops(),empty()))))==1e5);
    }
    function testCrossedBooksUseExactBalancedUnits() public {
        CrossedBooksTake[] memory a=new CrossedBooksTake[](1);CrossedBooksTake[] memory b=new CrossedBooksTake[](1);
        CrossedBooksOffer memory o;o.market=SuiteMidnightMock(M).market();o.maker=address(7);o.maxUnits=1e6;o.maxAssets=1e6;
        a[0]=CrossedBooksTake(o,"",1e6);CrossedBooksOffer memory bid=abi.decode(abi.encode(o),(CrossedBooksOffer));bid.buy=true;b[0]=CrossedBooksTake(bid,"",1e6);
        require(book.execute(plan(5,abi.encode(a,b,empty())))==1e5);
        require(StructuralTokenMock(U).allowance(address(book.resolver()),M)==0);
    }
    function testIdleCallbacksAndWrongOwnerCannotTouchBalances() public {
        vm.expectRevert();bot.onMorphoFlashLoan(1,"");vm.expectRevert();bot.onPreLiquidate(1,"");vm.expectRevert();bot.eulerStep("");
        vm.prank(address(8));vm.expectRevert();bot.execute(compoundPlan());
    }
}
