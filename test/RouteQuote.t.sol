// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;
import "../src/CLQuoter.sol";
contract QuotePoolMock {
    uint256 public changed;
    bool public partialFill;
    bool public broken;
    constructor(bool p,bool b){partialFill=p;broken=b;}
    function swap(address,bool direction,int256 amount,uint160,bytes calldata payload) external returns(int256,int256) {
        require(!broken);changed++;
        int256 used=partialFill?amount/2:amount;
        CLQuoter(msg.sender).uniswapV3SwapCallback(direction?used:-used*2,direction?-used*2:used,payload);
        return(0,0);
    }
}
contract RouteQuoteTest {
    function testBatchIsolatesBadAndPartialPoolsAndRestoresEveryPool() public {
        CLQuoter q=new CLQuoter();QuotePoolMock good=new QuotePoolMock(false,false);
        QuotePoolMock bad=new QuotePoolMock(false,true);QuotePoolMock partialFill=new QuotePoolMock(true,false);
        CLQuoter.Hop[][] memory paths=new CLQuoter.Hop[][](4);uint256[] memory amounts=new uint256[](4);
        address[4] memory pools=[address(good),address(bad),address(partialFill),address(good)];
        for(uint256 i;i<4;i++){paths[i]=new CLQuoter.Hop[](1);paths[i][0]=CLQuoter.Hop(pools[i],address(1),address(2),1);amounts[i]=100;}
        uint256[] memory out=q.quotePaths(paths,amounts,300000);
        require(out[0]==200&&out[1]==0&&out[2]==0&&out[3]==200);
        require(good.changed()==0&&partialFill.changed()==0);
    }
    function testDisconnectedOrRepeatedPathIsRejected() public {
        CLQuoter q=new CLQuoter();QuotePoolMock p=new QuotePoolMock(false,false);
        CLQuoter.Hop[] memory path=new CLQuoter.Hop[](2);
        path[0]=CLQuoter.Hop(address(p),address(1),address(2),1);
        path[1]=CLQuoter.Hop(address(p),address(2),address(3),1);
        try q.quotePath(path,100){revert("repeated pool accepted");}catch{}
    }
}
