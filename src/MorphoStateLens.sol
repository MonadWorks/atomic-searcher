// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;
import {BlueMarket} from "./StructuralExecutor.sol";
interface IBlueView {
    function idToMarketParams(bytes32) external view returns(BlueMarket memory);
    function accrueInterest(BlueMarket calldata) external;
    function market(bytes32) external view returns(uint128,uint128,uint128,uint128,uint128,uint128);
    function position(bytes32,address) external view returns(uint256,uint128,uint128);
}
interface IBlueOracle {function price() external view returns(uint256);}
interface IPSMLens {function rateProvider() external view returns(address);function pocket() external view returns(address);}
interface IPSMRate {function getConversionRate() external view returns(uint256);}
interface IPSMBalance {function balanceOf(address) external view returns(uint256);}
interface IPSMPoolLens {function slot0() external view returns(uint160,int24,uint16,uint16,uint16,uint8,bool);function fee() external view returns(uint24);function liquidity() external view returns(uint128);}
/// @notice eth_call-only accrued state. Deployment has no administrative role.
contract MorphoStateLens {
    address constant MORPHO=0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb;
    struct Position {address borrower;uint256 collateral;uint256 shares;uint256 debt;uint256 boundary;}
    struct Inspection {uint256 price;uint256 timestamp;Position[] positions;}
    function inspectMarkets(bytes32[] calldata ids,address[][] calldata borrowers) external returns(Inspection[] memory results) {
        require(ids.length==borrowers.length && ids.length<=128);
        uint256 total;for(uint256 i;i<ids.length;i++)total+=borrowers[i].length;require(total<=128);
        results=new Inspection[](ids.length);
        for(uint256 i;i<ids.length;i++) {
            if(gasleft()<400000)break;
            uint256 available=gasleft()-300000;
            uint256 limit=available<1500000?available:1500000;
            try this.inspect{gas:limit}(ids[i],borrowers[i]) returns(uint256 price,uint256 timestamp,Position[] memory positions) {
                results[i]=Inspection(price,timestamp,positions);
            } catch {}
        }
    }
    function inspectPSM(address pool) external view returns(uint160 sqrt,uint24 fee,uint128 liquidity,uint256 rate,uint256 usdc,uint256 susds) {
        address psm=0x1601843c5E9bC251A3272907010AFa41Fa18347E;
        (sqrt,,,,,,)=IPSMPoolLens(pool).slot0();fee=IPSMPoolLens(pool).fee();liquidity=IPSMPoolLens(pool).liquidity();
        rate=IPSMRate(IPSMLens(psm).rateProvider()).getConversionRate();
        usdc=IPSMBalance(0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913).balanceOf(IPSMLens(psm).pocket());
        susds=IPSMBalance(0x5875eEE11Cf8398102FdAd704C9E96607675467a).balanceOf(psm);
    }
    function inspectPSMs(address[] calldata pools) external view returns(uint256[6][] memory values) {
        require(pools.length<=32);values=new uint256[6][](pools.length);
        for(uint256 i;i<pools.length;i++) {
            try this.inspectPSM{gas:250000}(pools[i]) returns(uint160 sqrt,uint24 fee,uint128 liquidity,uint256 rate,uint256 usdc,uint256 susds) {
                values[i]=[uint256(sqrt),uint256(fee),uint256(liquidity),rate,usdc,susds];
            } catch {}
        }
    }
    function inspect(bytes32 id,address[] calldata borrowers) external returns(uint256 price,uint256 timestamp,Position[] memory states) {
        require(borrowers.length<=128);
        BlueMarket memory m=IBlueView(MORPHO).idToMarketParams(id);
        require(keccak256(abi.encode(m))==id&&m.lltv>0);
        IBlueView(MORPHO).accrueInterest(m);
        (,,uint128 assets,uint128 shares,,)=IBlueView(MORPHO).market(id);
        price=IBlueOracle(m.oracle).price();timestamp=block.timestamp;require(price>0);
        states=new Position[](borrowers.length);
        for(uint256 i;i<borrowers.length;i++) {
            (,uint128 borrowed,uint128 collateral)=IBlueView(MORPHO).position(id,borrowers[i]);
            uint256 debt=ceil(uint256(borrowed)*(uint256(assets)+1),uint256(shares)+1e6);
            uint256 boundary=collateral==0?type(uint256).max:boundaryPrice(ceil(debt*1e18,m.lltv),collateral);
            states[i]=Position(borrowers[i],collateral,borrowed,debt,boundary);
        }
    }
    function boundaryPrice(uint256 threshold,uint256 collateral) internal pure returns(uint256) {
        uint256 tail=ceil((threshold%collateral)*1e36,collateral);
        uint256 whole=threshold/collateral;
        if(whole>(type(uint256).max-tail)/1e36)return type(uint256).max;
        return whole*1e36+tail;
    }
    function ceil(uint256 a,uint256 b) private pure returns(uint256) {return a/b+(a%b==0?0:1);}
}
