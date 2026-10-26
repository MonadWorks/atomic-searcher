// SPDX-License-Identifier: MIT
pragma solidity ^0.8.33;
interface IV4Storage { function extsload(bytes32) external view returns(bytes32); }
/// @notice Bounded, atomic V4 pool/depth reads. No writes and no token calls.
contract V4StateLens {
    function context() external view returns(bool,uint256,uint256) {return(true,block.number,block.timestamp);}
    function read(address manager,bytes32[] calldata ids,int24[] calldata spacing,uint16 radius,uint16 tickLimit,bytes32[] calldata extra)
        external view returns(uint256,uint256,bytes32[] memory slots,bytes32[] memory values)
    {
        require(ids.length>0 && ids.length<=4 && ids.length==spacing.length && radius<=32 && tickLimit<=256 && extra.length<=512,"bounds");
        slots=new bytes32[](1288);values=new bytes32[](1288);uint256 count;
        for(uint256 i;i<ids.length;i++) {
            require(spacing[i]>0 && spacing[i]<=32767,"spacing");
            uint256 base=uint256(keccak256(abi.encode(ids[i],uint256(6))));
            bytes32 raw=IV4Storage(manager).extsload(bytes32(base));
            slots[count]=bytes32(base);values[count++]=raw;
            slots[count]=bytes32(base+3);values[count++]=IV4Storage(manager).extsload(bytes32(base+3));
            int24 tick=int24(uint24(uint256(raw)>>160));
            int256 compressed=int256(tick)/spacing[i];if(tick<0 && tick%spacing[i]!=0)--compressed;
            int256 center=compressed>>8;
            int256 minimum=(-int256(887272)/spacing[i]-(887272%uint24(spacing[i])==0?int256(0):int256(1)))>>8;
            int256 maximum=(int256(887272)/spacing[i])>>8;
            int256 lo=center-int256(uint256(radius));if(lo<minimum)lo=minimum;
            int256 hi=center+int256(uint256(radius));if(hi>maximum)hi=maximum;
            uint256 ticks;uint256 poolLimit=uint256(tickLimit)/ids.length;
            // Start at the price and alternate outward; a remote dense region
            // in one pool cannot consume the other pools' tick allowance.
            for(uint256 distance;distance<=uint256(radius)*2;distance++) {
                int256 offset=int256((distance+1)/2)*(distance%2==0?int256(-1):int256(1));
                int256 word=center+offset;if(word<lo || word>hi)continue;
                bytes32 bitmapSlot=keccak256(abi.encode(word,base+5));
                bytes32 bitmap=IV4Storage(manager).extsload(bitmapSlot);
                slots[count]=bitmapSlot;values[count++]=bitmap;
                uint256 bits=uint256(bitmap);
                int256 currentBit=compressed-(center<<8);
                for(uint256 step;bits!=0 && step<(word==center?512:256) && ticks<poolLimit;step++) {
                    int256 bit=word<center?int256(255-step):int256(step);
                    if(word==center)bit=currentBit+int256((step+1)/2)*(step%2==0?int256(-1):int256(1));
                    if(bit<0 || bit>255 || (bits&(uint256(1)<<uint256(bit)))==0)continue;
                    int256 boundary=((word<<8)+bit)*spacing[i];
                    if(boundary < -887272 || boundary > 887272)continue;
                    bytes32 tickSlot=keccak256(abi.encode(boundary,base+4));
                    slots[count]=tickSlot;values[count++]=IV4Storage(manager).extsload(tickSlot);ticks++;
                }
            }
        }
        for(uint256 i;i<extra.length;i++){slots[count]=extra[i];values[count++]=IV4Storage(manager).extsload(extra[i]);}
        assembly("memory-safe"){mstore(slots,count) mstore(values,count)}
        return(block.number,block.timestamp,slots,values);
    }
}
