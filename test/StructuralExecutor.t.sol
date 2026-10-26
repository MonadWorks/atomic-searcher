// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;
import {StructuralExecutor,BlueMarket} from "../src/StructuralExecutor.sol";
interface VmStructural {function chainId(uint256) external;function roll(uint256) external;function warp(uint256) external;function setBlockhash(uint256,bytes32) external;function etch(address,bytes calldata) external;function prank(address) external;function expectRevert() external;}
contract StructuralTokenMock {
    mapping(address=>uint256) public balanceOf;
    mapping(address=>mapping(address=>uint256)) public allowance;
    function mint(address a,uint256 v) external {balanceOf[a]+=v;}
    function transfer(address a,uint256 v) external returns(bool){balanceOf[msg.sender]-=v;balanceOf[a]+=v;return true;}
    function approve(address a,uint256 v) external returns(bool){allowance[msg.sender][a]=v;return true;}
    function transferFrom(address a,address b,uint256 v) external returns(bool){allowance[a][msg.sender]-=v;balanceOf[a]-=v;balanceOf[b]+=v;return true;}
}
contract StructuralFactoryMock {
    mapping(bytes32=>address) pools;
    function add(address a,address b,address p) external {pools[keccak256(abi.encode(a,b))]=p;}
    function getPool(address a,address b,uint24) external view returns(address){return pools[keccak256(abi.encode(a,b))];}
    function getPool(address a,address b,int24) external view returns(address){return pools[keccak256(abi.encode(a,b))];}
}
contract StructuralPoolMock {
    address public factory;address public token0;address public token1;uint24 public fee=500;
    int24 public tickSpacing=1;uint256 n;uint256 d;bool locked;
    function initialize(address f,address a,address b,uint256 n_,uint256 d_) external {factory=f;token0=a;token1=b;n=n_;d=d_;fee=500;tickSpacing=1;}
    function slot0() external pure returns(uint160,int24,uint16){return(uint160(1<<96),0,0);}
    function observations(uint256) external pure returns(uint32,int56,uint160,bool){return(0,0,0,true);}
    function swap(address receiver,bool z,int256 amount,uint160,bytes calldata) external returns(int256 a0,int256 a1){
        require(!locked);locked=true;
        uint256 input;uint256 output;
        if(amount>0){input=uint256(amount);output=z?input*n/d:input*d/n;}
        else{output=uint256(-amount);input=z?(output*d+n-1)/n:(output*n+d-1)/d;}
        address tin=z?token0:token1;address tout=z?token1:token0;
        uint256 beforeBalance=StructuralTokenMock(tin).balanceOf(address(this));
        StructuralTokenMock(tout).transfer(receiver,output);
        a0=z?int256(input):-int256(output);a1=z?-int256(output):int256(input);
        StructuralExecutor(payable(msg.sender)).uniswapV3SwapCallback(a0,a1,"");
        require(StructuralTokenMock(tin).balanceOf(address(this))==beforeBalance+input);locked=false;
    }
}
contract StructuralBlueMock {
    address constant USDC=0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913;
    address constant CBETH=0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22;
    mapping(address=>uint256) public debt;
    function setDebt(address a,uint256 v) external {debt[a]=v;}
    function idToMarketParams(bytes32) external pure returns(BlueMarket memory) {
        return BlueMarket(USDC,CBETH,0x97FF9CbD7E77348b2B8FfBB883bF29452aD18295,0x46415998764C29aB2a25CbeA6254146D50D22687,770000000000000000);
    }
    function liquidate(BlueMarket calldata,address borrower,uint256,uint256 shares,bytes calldata callbackData) external returns(uint256 seized,uint256 repaid){
        repaid=debt[borrower];require(repaid>0&&shares==repaid);debt[borrower]=0;seized=repaid*110/100;
        StructuralTokenMock(CBETH).transfer(msg.sender,seized);
        StructuralExecutor(payable(msg.sender)).onMorphoLiquidate(repaid,callbackData);
        StructuralTokenMock(USDC).transferFrom(msg.sender,address(this),repaid);
    }
}
contract StructuralPSMMock {
    address public constant usdc=0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913;
    address public constant susds=0x5875eEE11Cf8398102FdAd704C9E96607675467a;
    function swapExactIn(address a,address b,uint256 v,uint256 floor,address receiver,uint256) external returns(uint256){
        require(v>=floor);StructuralTokenMock(a).transferFrom(msg.sender,address(this),v);StructuralTokenMock(b).transfer(receiver,v);return v;
    }
}
contract StructuralExecutorTest {
    VmStructural constant vm=VmStructural(address(uint160(uint256(keccak256("hevm cheat code")))));
    address constant MORPHO=0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb;
    address constant USDC=0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913;
    address constant CBETH=0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22;
    address constant WETH=0x4200000000000000000000000000000000000006;
    address constant SUSDS=0x5875eEE11Cf8398102FdAd704C9E96607675467a;
    address constant PSM=0x1601843c5E9bC251A3272907010AFa41Fa18347E;
    address constant P1=0x47cA96Ea59C13F72745928887f84C9F52C3D7348;
    address constant P2=0xd0b53D9277642d899DF5C87A3966A349A798F224;
    address constant PP=0x4C9f68e780523fEB4c9bB1aad2E5CC3B6476892B;
    address constant F=0x33128a8fC17869897dcE68Ed026d694621f6FDfD;
    address constant SF=0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A;
    StructuralExecutor bot;bytes32 constant HASH=bytes32(uint256(123));
    function setUp() public {
        vm.chainId(8453);vm.roll(101);vm.warp(1000);vm.setBlockhash(100,HASH);
        address[4] memory tokens=[USDC,CBETH,WETH,SUSDS];
        for(uint256 i;i<4;i++)vm.etch(tokens[i],type(StructuralTokenMock).runtimeCode);
        vm.etch(F,type(StructuralFactoryMock).runtimeCode);vm.etch(SF,type(StructuralFactoryMock).runtimeCode);
        _pool(P1,SF,CBETH,WETH,1,1);_pool(P2,F,WETH,USDC,1,1);_pool(PP,F,SUSDS,USDC,100,110);
        vm.etch(MORPHO,type(StructuralBlueMock).runtimeCode);vm.etch(PSM,type(StructuralPSMMock).runtimeCode);
        StructuralTokenMock(CBETH).mint(MORPHO,1e15);StructuralTokenMock(USDC).mint(PSM,1e15);StructuralTokenMock(SUSDS).mint(PSM,1e15);
        bot=new StructuralExecutor(address(this),8453,1e15);bot.setPaused(false);
    }
    function _pool(address pool,address f,address a,address b,uint256 n,uint256 d) private {
        vm.etch(pool,type(StructuralPoolMock).runtimeCode);StructuralPoolMock(pool).initialize(f,a,b,n,d);
        StructuralFactoryMock(f).add(a,b,pool);StructuralTokenMock(a).mint(pool,1e15);StructuralTokenMock(b).mint(pool,1e15);
    }
    function _plan(uint8 mode,bytes memory args,uint256 amount,uint256 floor) private pure returns(StructuralExecutor.Plan memory) {
        return StructuralExecutor.Plan(mode,args,amount,floor,1e10,1006,100,HASH,102,7);
    }
    function _blue() private returns(StructuralExecutor.Plan memory) {
        StructuralBlueMock(MORPHO).setDebt(address(1),1000000);StructuralBlueMock(MORPHO).setDebt(address(2),2000000);
        StructuralExecutor.Request[] memory req=new StructuralExecutor.Request[](3);
        req[0]=StructuralExecutor.Request(address(1),1000000,1000000);
        req[1]=StructuralExecutor.Request(address(3),1000000,1000000); // competitor already cleared
        req[2]=StructuralExecutor.Request(address(2),2000000,2000000);
        return _plan(1,abi.encode(req),4000000,100000);
    }
    function testBatchSkipsClearedBorrowerPaysOnlyNewProfit() public {
        StructuralTokenMock(USDC).mint(address(bot),777);StructuralTokenMock(CBETH).mint(address(bot),888);StructuralTokenMock(WETH).mint(address(bot),999);
        uint256 profit=bot.execute(_blue());require(profit==300000);
        require(StructuralTokenMock(USDC).balanceOf(address(this))==profit);
        require(StructuralTokenMock(USDC).balanceOf(address(bot))==777&&StructuralTokenMock(CBETH).balanceOf(address(bot))==888&&StructuralTokenMock(WETH).balanceOf(address(bot))==999);
        require(StructuralTokenMock(USDC).allowance(address(bot),MORPHO)==0);
    }
    function testUnderfloorRollsBackDebtAndPayout() public {
        StructuralExecutor.Plan memory p=_blue();p.minProfit=300001;vm.expectRevert();bot.execute(p);
        require(StructuralBlueMock(MORPHO).debt(address(1))==1000000&&StructuralTokenMock(USDC).balanceOf(address(this))==0);
    }
    function testDebtBudgetCannotBorrowFromInventory() public {
        StructuralTokenMock(USDC).mint(address(bot),1e12);StructuralExecutor.Plan memory p=_blue();p.amountIn--;
        vm.expectRevert();bot.execute(p);require(StructuralTokenMock(USDC).balanceOf(address(bot))==1e12);
    }
    function testPsmBothDirectionsReturnUSDC() public {
        uint256 profit=bot.execute(_plan(2,abi.encode(PP,false),1000000,100000));require(profit==100000);
        StructuralPoolMock(PP).initialize(F,SUSDS,USDC,110,100);
        StructuralExecutor.Plan memory p=_plan(2,abi.encode(PP,true),1100000,100000);p.nonce=8;
        require(bot.execute(p)==100000&&StructuralTokenMock(SUSDS).balanceOf(address(bot))==0);
        require(StructuralTokenMock(USDC).allowance(address(bot),PSM)==0&&StructuralTokenMock(SUSDS).allowance(address(bot),PSM)==0);
    }
    function testUnauthorizedAndIdleCallbacksRejected() public {
        StructuralExecutor.Plan memory p=_blue();vm.prank(address(9));vm.expectRevert();bot.execute(p);
        vm.prank(MORPHO);vm.expectRevert();bot.onMorphoLiquidate(1,"");
        vm.prank(P1);vm.expectRevert();bot.uniswapV3SwapCallback(1,-1,"");
        vm.expectRevert();bot.liquidateOne(StructuralExecutor.Request(address(1),1,1));
    }
    function testExpiredReplayedAndPausedPlansReject() public {
        StructuralExecutor.Plan memory p=_blue();bot.execute(p);vm.expectRevert();bot.execute(p);
        p.nonce=8;p.deadline=999;vm.expectRevert();bot.execute(p);
        p.deadline=1006;p.anchorHash=bytes32(uint256(321));vm.expectRevert();bot.execute(p);
        bot.setPaused(true);p.anchorHash=HASH;vm.expectRevert();bot.execute(p);
    }
}
