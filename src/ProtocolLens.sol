// SPDX-License-Identifier: GPL-2.0-or-later
pragma solidity ^0.8.33;
import {MorphoStateLens,IBlueView,IBlueOracle} from "./MorphoStateLens.sol";
import {BlueMarket} from "./StructuralExecutor.sol";
interface PLPre {
    function MORPHO() external view returns(address);
    function ID() external view returns(bytes32);
    function marketParams() external view returns(BlueMarket memory);
    function preLiquidationParams() external view returns(uint256,uint256,uint256,uint256,uint256,address);
}
interface PLAuth {function isAuthorized(address,address) external view returns(bool);}
interface PLEVC {function getCollaterals(address) external view returns(address[] memory);}
interface PLVault {
    function EVC() external view returns(address);
    function asset() external view returns(address);
    function checkLiquidation(address,address,address) external view returns(uint256,uint256);
    function convertToAssets(uint256) external view returns(uint256);
    function maxRedeem(address) external view returns(uint256);
    function cash() external view returns(uint256);
    function hookConfig() external view returns(address,uint32);
}
contract ProtocolLens {
    address constant BLUE=0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb;
    address constant EVC=0x5301c7dD20bD945D2013b48ed0DEE3A284ca8989;
    struct PreState {uint256 shares;uint256 debt;uint256 collateral;uint256 price;uint256 lif;address loan;address token;}
    function pre(address target,address borrower) external returns(PreState memory o) {
        require(PLPre(target).MORPHO()==BLUE);
        BlueMarket memory m=PLPre(target).marketParams();bytes32 id=PLPre(target).ID();require(keccak256(abi.encode(m))==id);
        o.loan=m.loanToken;o.token=m.collateralToken;
        if(!PLAuth(BLUE).isAuthorized(borrower,target))return o;
        (uint256 preLltv,uint256 lcf1,uint256 lcf2,uint256 lif1,uint256 lif2,address oracle)=PLPre(target).preLiquidationParams();
        IBlueView(BLUE).accrueInterest(m);
        (,,uint128 assets,uint128 totalShares,,)=IBlueView(BLUE).market(id);
        (,uint128 borrowed,uint128 coll)=IBlueView(BLUE).position(id,borrower);
        uint256 price=IBlueOracle(oracle).price();if(price==0)return o;
        uint256 value=uint256(coll)*price/1e36;uint256 debt=_ceil(uint256(borrowed)*(uint256(assets)+1),uint256(totalShares)+1e6);
        if(debt==0||debt>value*m.lltv/1e18||debt<=value*preLltv/1e18)return o;
        uint256 q=(_ceil(debt*1e18,value)-preLltv)*1e18/(m.lltv-preLltv);
        uint256 lcf=lcf1+q*(lcf2-lcf1)/1e18;o.lif=lif1+q*(lif2-lif1)/1e18;
        o.shares=uint256(borrowed)*lcf/1e18;if(o.shares>borrowed)o.shares=borrowed;
        o.debt=_ceil(o.shares*(uint256(assets)+1),uint256(totalShares)+1e6);
        o.collateral=o.shares*(uint256(assets)+1)/(uint256(totalShares)+1e6)*o.lif/1e18*1e36/price;o.price=price;
    }
    struct EulerState {address collateralVault;address token;uint256 repay;uint256 shares;uint256 assets;uint256 cash;}
    function euler(address vault,address borrower,address executor) external view returns(address loan,EulerState[] memory rows) {
        require(PLVault(vault).EVC()==EVC);loan=PLVault(vault).asset();address[] memory cs=PLEVC(EVC).getCollaterals(borrower);
        require(cs.length<=16);rows=new EulerState[](cs.length);
        for(uint256 i;i<cs.length;i++) {
            try this.eulerOne(vault,borrower,executor,cs[i]) returns(EulerState memory r){rows[i]=r;}catch{}
        }
    }
    function eulerOne(address vault,address borrower,address executor,address cv) external view returns(EulerState memory r) {
        require(PLVault(cv).EVC()==EVC);r.collateralVault=cv;r.token=PLVault(cv).asset();
        (address loanHook,uint32 loanOps)=PLVault(vault).hookConfig();
        (address collateralHook,uint32 collateralOps)=PLVault(cv).hookConfig();
        if((loanHook==address(0)&&(loanOps&((1<<11)|(1<<7)))!=0)||(collateralHook==address(0)&&(collateralOps&(1<<3))!=0))return r;
        (r.repay,r.shares)=PLVault(vault).checkLiquidation(executor,borrower,cv);
        if(r.shares>0){r.assets=PLVault(cv).convertToAssets(r.shares);r.cash=PLVault(cv).cash();}
    }
    struct EulerResult {bool valid;address loan;EulerState[] positions;}
    function eulerBatch(address[] calldata vaults,address[] calldata borrowers,address executor) external view returns(EulerResult[] memory results) {
        require(vaults.length==borrowers.length&&vaults.length<=16);results=new EulerResult[](vaults.length);
        for(uint256 i;i<vaults.length;i++) {
            if(gasleft()<700000)break;
            try this.euler{gas:600000}(vaults[i],borrowers[i],executor) returns(address loan,EulerState[] memory rows){results[i]=EulerResult(true,loan,rows);}catch{}
        }
    }
    struct PreResult {bool valid;PreState position;}
    function preBatch(address[] calldata targets,address[] calldata borrowers) external returns(PreResult[] memory results) {
        require(targets.length==borrowers.length&&targets.length<=16);results=new PreResult[](targets.length);
        for(uint256 i;i<targets.length;i++) {
            if(gasleft()<700000)break;
            try this.pre{gas:600000}(targets[i],borrowers[i]) returns(PreState memory row){results[i]=PreResult(true,row);}catch{}
        }
    }
    function _ceil(uint256 a,uint256 b) private pure returns(uint256){return a/b+(a%b==0?0:1);}
}
