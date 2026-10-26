"""Isolated hookless V4 discovery and exact-output research quotes.

Layout and fee arithmetic: Uniswap v4-core StateLibrary / ProtocolFeeLibrary.
Integer swap rounding: Uniswap v3/v4 core, GPL-2.0-or-later (as searcher.math).
No RPC, keys, transaction sender, timers, or production imports with side effects.
Unknown storage words are incomplete evidence, never assumed zero.
"""
import time
from functools import cached_property
from dataclasses import dataclass
from bisect import bisect_right
from collections import OrderedDict
from eth_abi import encode, decode
from eth_abi.exceptions import DecodingError
from eth_utils import keccak
from .math import (Q96, MIN_SQRT, MAX_SQRT, sqrt_at_tick,
                           amount0_delta, amount1_delta, ceildiv)

MANAGER = '0x498581ff718922c3f8e6a244956af099b2652b2b'
ZERO = '0x'+'00'*20
def topic(signature): return '0x'+keccak(text=signature).hex()
INITIALIZE = topic('Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)')
MODIFY = topic('ModifyLiquidity(bytes32,address,int24,int24,int256,bytes32)')
SWAP = topic('Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)')
KEY_TYPES = ['address','address','uint24','int24','address']

@dataclass(frozen=True)
class Key:
    currency0: str
    currency1: str
    fee: int
    spacing: int
    hooks: str = ZERO

    def __post_init__(self):
        if not (0 < int(self.currency0,16) < int(self.currency1,16) < 1<<160
                and self.hooks == ZERO and 0 <= self.fee < 1_000_000 and 0 < self.spacing <= 32767):
            raise ValueError('unsupported pool key')
    def values(self): return (self.currency0,self.currency1,self.fee,self.spacing,self.hooks)
    @cached_property
    def id(self): return '0x'+keccak(encode(KEY_TYPES,self.values())).hex()
    @cached_property
    def slot(self): return int.from_bytes(keccak(bytes.fromhex(self.id[2:])+(6).to_bytes(32,'big')),'big')

def initialization(log):
    """Manager address and PoolKey hash authenticate metadata; no winner hash input."""
    ts=log.get('topics',[])
    if log.get('removed') or log.get('address','').lower()!=MANAGER or len(ts)!=4 or ts[0].lower()!=INITIALIZE:
        return None
    try:
        a,b=decode(['address'],bytes.fromhex(ts[2][2:]))[0],decode(['address'],bytes.fromhex(ts[3][2:]))[0]
        raw=bytes.fromhex(log['data'][2:])
        if len(raw)!=160:return None
        fee,spacing,hooks,sqrt,tick=decode(['uint24','int24','address','uint160','int24'],raw)
        key=Key(a,b,fee,spacing,hooks)
        if key.id!=ts[1].lower() or not MIN_SQRT<=sqrt<MAX_SQRT or not -887272<=tick<=887272:return None
        return key
    except (ValueError,TypeError,KeyError,OverflowError,DecodingError):
        return None

class Registry:
    """Bounded event fixture registry. Reorgs invalidate metadata; caller replays canonical initialization."""
    def __init__(self, maximum=4096):
        if not 1<=maximum<=4096:raise ValueError('invalid registry capacity')
        self.keys={};self.maximum=maximum;self._pair_cursor=OrderedDict()
    def accept(self, log):
        if log.get('address','').lower()!=MANAGER:return set()
        ts=log.get('topics',[])
        if len(ts)<2:return set()
        pid=ts[1].lower()
        if log.get('removed'):
            self.keys.pop(pid,None)
            return set()
        key=initialization(log)
        if key:
            if pid not in self.keys and len(self.keys)>=self.maximum:return set()
            self.keys[pid]=key
        if pid not in self.keys or ts[0].lower() not in (INITIALIZE,MODIFY,SWAP):return set()
        changed=self.keys[pid]
        return {k.id for k in self.keys.values() if (k.currency0,k.currency1)==(changed.currency0,changed.currency1)}
    def pairs(self, settlement, limit=256):
        """Bounded page, full rotation for stable topology. Retain progress on churn.

        Cursor modulo the new total avoids repeatedly returning page zero, but
        does not promise complete coverage under arbitrary continuous topology changes.
        """
        if not isinstance(limit,int) or not 1<=limit<=256:raise ValueError('invalid pair page size')
        groups={}
        for k in self.keys.values():
            if settlement not in (k.currency0,k.currency1):continue
            groups.setdefault((k.currency0,k.currency1),[]).append(k)
        groups=[keys for keys in groups.values() if len(keys)>1]
        ends=[];total=0
        for keys in groups:total+=len(keys)*(len(keys)-1);ends.append(total)
        if not total:return []
        cursor=self._pair_cursor.get(settlement,0)%total;page=[]
        for step in range(min(limit,total)):
            absolute=(cursor+step)%total;g=bisect_right(ends,absolute)
            offset=absolute-(ends[g-1] if g else 0);keys=groups[g];n=len(keys)
            i,j=divmod(offset,n-1);j+=j>=i
            page.append((keys[i],keys[j]))
        self._pair_cursor[settlement]=(cursor+len(page))%total
        self._pair_cursor.move_to_end(settlement)
        while len(self._pair_cursor)>self.maximum:self._pair_cursor.popitem(last=False)
        return page

class CapacityLimit(ValueError):
    """An amount frontier, distinct from missing data or invalid pool state."""

class MissingState(ValueError):
    def __init__(self,message,slot=None):super().__init__(message);self.slot=slot
def signed(x,bits):return x-(1<<bits) if x&(1<<(bits-1)) else x
def total_fee(protocol_packed, lp, zero_for_one):
    if not 0<=protocol_packed<(1<<24) or not 0<=lp<1_000_000:raise ValueError('invalid fee')
    left,right=protocol_packed&4095,protocol_packed>>12
    if left>1000 or right>1000:raise ValueError('invalid protocol fee')
    protocol=left if zero_for_one else right
    return protocol+lp-protocol*lp//1_000_000

class Snapshot:
    def __init__(self, storage):
        self.storage={int(k,16):int(v,16) for k,v in storage.items()};self.reads=set();self.deadline_ns=None
    def word(self, slot):
        if slot not in self.storage:raise MissingState(f'missing storage word {slot:064x}',slot)
        self.reads.add(slot);return self.storage[slot]
    def mapping(self, slot, item):
        return self.word(int.from_bytes(keccak(encode(['int256','uint256'],[item,slot])),'big'))
    def state(self,key):
        s=self.word(key.slot)
        return s&((1<<160)-1),signed((s>>160)&((1<<24)-1),24),self.word(key.slot+3)&((1<<128)-1),(s>>184)&((1<<24)-1),(s>>208)&((1<<24)-1)
    def input_for_output(self,key,token_out,amount,*,include_state=False):
        if token_out not in (key.currency0,key.currency1) or not 0<amount<(1<<127):raise ValueError('invalid output')
        zero=token_out==key.currency1
        sqrt,tick,liquidity,protocol,lp=self.state(key)
        if not MIN_SQRT<=sqrt<MAX_SQRT:raise ValueError('uninitialized price')
        if lp!=key.fee:raise ValueError('fixed LP fee mismatch')
        fee=total_fee(protocol,lp,zero)
        if fee>=1_000_000:raise ValueError('exact output unavailable at full fee')
        remaining,paid=amount,0
        for step in range(4096):
            if step%16==0 and self.deadline_ns is not None and time.monotonic_ns()>self.deadline_ns:raise MissingState('quote computation deadline')
            if not remaining:return (paid,sqrt) if include_state else paid
            compressed=tick//key.spacing+(0 if zero else 1)
            word,bit=compressed>>8,compressed&255
            bitmap=self.mapping(key.slot+5,word)
            masked=bitmap&((1<<(bit+1))-1) if zero else bitmap&(((1<<256)-1)^((1<<bit)-1))
            next_compressed=(compressed-bit+(masked.bit_length()-1 if masked else 0)) if zero else (compressed-bit+((masked&-masked).bit_length()-1 if masked else 255))
            nxt=max(-887272,min(887272,next_compressed*key.spacing));target=sqrt_at_tick(nxt)
            target=max(MIN_SQRT+1,target) if zero else min(MAX_SQRT-1,target)
            delta_out=amount1_delta if zero else amount0_delta
            delta_in=amount0_delta if zero else amount1_delta
            available=delta_out(sqrt,target,liquidity,False)
            reached=remaining>=available
            if reached:new=target
            elif zero:new=sqrt-ceildiv(remaining*Q96,liquidity)
            else:
                denom=(liquidity<<96)-remaining*sqrt
                if denom<=0:raise MissingState('output exceeds price range')
                new=ceildiv((liquidity<<96)*sqrt,denom)
            consumed=delta_in(sqrt,new,liquidity,True)
            received=min(remaining,delta_out(sqrt,new,liquidity,False))
            paid+=consumed+ceildiv(consumed*fee,1_000_000-fee)
            remaining-=received
            if new==sqrt_at_tick(nxt):
                if masked:
                    net=signed(self.mapping(key.slot+4,nxt)>>128,128)
                    liquidity+=-net if zero else net
                    if not 0<=liquidity<(1<<128):raise ValueError('invalid liquidity')
                tick=nxt-1 if zero else nxt
            elif remaining:raise MissingState('price limit or incomplete fill')
            sqrt=new
            if remaining and sqrt in (MIN_SQRT+1,MAX_SQRT-1):raise MissingState('price limit')
        raise MissingState('crossing budget')
    def cycle(self,sell,buy,settlement,output):
        if sell.id==buy.id or (sell.currency0,sell.currency1)!=(buy.currency0,buy.currency1):raise ValueError('invalid pair')
        asset=sell.currency1 if settlement==sell.currency0 else sell.currency0
        debt,sell_limit=self.input_for_output(sell,settlement,output,include_state=True)
        if debt>=(1<<127):raise CapacityLimit('intermediate debt exceeds protocol int128 capacity')
        cost,buy_limit=self.input_for_output(buy,asset,debt,include_state=True)
        return {'output':output,'intermediate_debt':debt,'cost':cost,'gross_surplus':output-cost,
                'sell_limit':sell_limit,'buy_limit':buy_limit}

def key_from_input(calldata,pool_id,settlement):
    """Recover ABI PoolKeys from public calldata, authenticated by pool-id hash.

    Routers often embed PoolKey in nested bytes. Search bounded calldata around
    the settlement address; matching a five-word tuple is not sufficient until
    its full keccak equals the emitted pool id. Packed/custom calldata may yield
    no key; it never creates guessed metadata.
    """
    try:raw=bytes.fromhex(calldata.removeprefix('0x'));needle=encode(['address'],[settlement])
    except (ValueError,TypeError,AttributeError):return None
    if len(raw)>131072:return None
    start=0
    for _ in range(128):
        offset=raw.find(needle,start)
        if offset<0:return None
        start=offset+1
        for pos in (offset,offset-32):
            if pos<0 or pos+160>len(raw):continue
            try:key=Key(*decode(KEY_TYPES,raw[pos:pos+160]))
            except (ValueError,TypeError,DecodingError):continue
            if key.id==pool_id.lower():return key
    return None
