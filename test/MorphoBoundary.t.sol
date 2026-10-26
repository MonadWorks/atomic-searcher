// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;
import {MorphoStateLens} from "../src/MorphoStateLens.sol";
contract BoundaryHarness is MorphoStateLens {function boundary(uint256 threshold,uint128 collateral) external pure returns(uint256){return boundaryPrice(threshold,collateral);}}
contract MorphoBoundaryTest {
    BoundaryHarness lens=new BoundaryHarness();
    function testExtremeDebtDoesNotPoisonBatch() public view {require(lens.boundary(type(uint256).max,1)==type(uint256).max);}
    function testWideProductWithRepresentableResult() public view {require(lens.boundary(1e60,1e30)==1e66);}
    function testFuzzExactCeiling(uint128 threshold,uint128 collateral) public view {
        if(collateral==0)return;
        uint256 n=uint256(threshold)*1e36;
        require(lens.boundary(threshold,collateral)==n/collateral+(n%collateral==0?0:1));
    }
}

import {BlueMarket} from "../src/StructuralExecutor.sol";
interface VmBoundary {function etch(address,bytes calldata) external;}
contract LensBlueMock {
    function idToMarketParams(bytes32) external pure returns(BlueMarket memory){return BlueMarket(address(1),address(2),address(3000),address(4),800000000000000000);}
    function accrueInterest(BlueMarket calldata) external {}
    function market(bytes32) external pure returns(uint128,uint128,uint128,uint128,uint128,uint128){return(0,0,100,100000000,0,0);}
    function position(bytes32,address) external pure returns(uint256,uint128,uint128){return(0,10000000,100);}
}
contract LensOracleMock {function price() external pure returns(uint256){return 1e36;}}
contract MorphoMultiMarketTest {
    VmBoundary constant vm=VmBoundary(address(uint160(uint256(keccak256("hevm cheat code")))));
    function testInvalidMarketCannotCancelOtherInspections() public {
        vm.etch(0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb,type(LensBlueMock).runtimeCode);
        vm.etch(address(3000),type(LensOracleMock).runtimeCode);
        MorphoStateLens lens=new MorphoStateLens();bytes32 id=keccak256(abi.encode(BlueMarket(address(1),address(2),address(3000),address(4),800000000000000000)));
        bytes32[] memory ids=new bytes32[](3);ids[0]=id;ids[1]=bytes32(uint256(100));ids[2]=id;
        address[][] memory borrowers=new address[][](3);
        for(uint256 i;i<3;i++){borrowers[i]=new address[](1);borrowers[i][0]=address(5);}
        MorphoStateLens.Inspection[] memory rows=lens.inspectMarkets(ids,borrowers);
        require(rows[0].price==1e36 && rows[1].price==0 && rows[2].price==1e36);
        require(rows[0].positions[0].debt==10 && rows[2].positions[0].debt==10);
    }
    function testPriceScreenIncludesMarketsAfterTheFormer32MarketCutoff() public {
        vm.etch(0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb,type(LensBlueMock).runtimeCode);
        vm.etch(address(3000),type(LensOracleMock).runtimeCode);
        MorphoStateLens lens=new MorphoStateLens();bytes32 id=keccak256(abi.encode(BlueMarket(address(1),address(2),address(3000),address(4),800000000000000000)));
        bytes32[] memory ids=new bytes32[](48);address[][] memory borrowers=new address[][](48);
        for(uint256 i;i<48;i++){ids[i]=id;borrowers[i]=new address[](1);borrowers[i][0]=address(5);}
        MorphoStateLens.Inspection[] memory rows=lens.inspectMarkets(ids,borrowers);
        require(rows.length==48 && rows[47].price==1e36 && rows[47].positions[0].debt==10);
    }
}
