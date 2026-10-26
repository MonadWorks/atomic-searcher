"""Production adapters; one account executor owns signing, nonce and risk ledger."""
from dataclasses import dataclass
from functools import lru_cache
import json
from eth_abi import encode, decode
from eth_utils import keccak
from .core import ROOT, USDC, WETH, data, unhex, number, check

MORPHO='0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb'
MARKET='0x0ca10126f6c94cbd9cf0a48cc9516ae5e3dec5aa68303e6d988ee37c5149bf0d'
ORACLE='0x97ff9cbd7e77348b2b8ffbb883bf29452ad18295'
CBETH='0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22'
POOL1='0x47ca96ea59c13f72745928887f84c9f52c3d7348'
POOL2='0xd0b53d9277642d899df5c87a3966a349a798f224'
PSM='0x1601843c5e9bc251a3272907010afa41fa18347e'
SUSDS='0x5875eee11cf8398102fdad704c9e96607675467a'
PSM_POOLS=('0x4c9f68e780523feb4c9bb1aad2e5cc3b6476892b','0x7dd2f626865e71b099f80bab3ca25a7918c5e0dd')
LENS='0x000000000000000000000000000000000000c1d1'
PLAN='(uint8,bytes,uint256,uint256,uint256,uint64,uint64,bytes32,uint64,uint64)'
REQUESTS='(address,uint256,uint256)[]'
HOPS='(address,address,address,uint8)[]'
GENERIC=['bytes32',REQUESTS,HOPS,HOPS]

def enabled(config):return config.get('structural',{}).get('enabled') is True
@lru_cache(maxsize=4)
def artifact(name):return json.loads((ROOT/f'out/{name}.sol/{name}.json').read_text())
def cap(config):return int(next(t for t in config['settlement_tokens'] if t['address']==USDC)['max_input_raw'])

def deployment(config):
    check(config['chain_id']==8453 and 0<cap(config)<1<<255,'invalid structural deployment configuration')
    raw=unhex(artifact('StructuralExecutor')['bytecode']['object'])+encode(['address','uint256','uint256'],[config['owner'],8453,cap(config)])
    return {'creation_data':'0x'+raw.hex(),'creation_data_hash':'0x'+keccak(raw).hex()}

def verify_contract(rpc,config,contract,*,require_unpaused=True):
    from .execution import runtime_matches
    rpc.verify_chain()
    check(runtime_matches(rpc.request('eth_getCode',[contract,'latest']),artifact('StructuralExecutor')),'structural runtime mismatch')
    values=rpc.batch([('eth_call',[{'to':contract,'data':data(s)},'latest']) for s in ('owner()','chainId()','paused()','maxInput()')])
    check(decode(['address'],unhex(values[0]))[0].lower()==config['owner'],'structural owner mismatch')
    check(number(values[1])==8453==config['chain_id'],'structural chain mismatch')
    check(number(values[3])==cap(config),'structural principal cap mismatch')
    check(not require_unpaused or number(values[2])==0,'structural executor paused')
    return {'contract':contract.lower(),'paused':bool(number(values[2])),'execution_kind':'structural-v1'}

@dataclass(frozen=True)
class Route:
    strategy:int
    arguments:bytes
    pools:tuple
    execution_kind:str='structural-v1'
    path_tokens:tuple=()
    @property
    def tokens(self):
        if self.path_tokens:return self.path_tokens
        return (USDC,CBETH,WETH,USDC) if self.strategy==1 else (USDC,SUSDS,USDC)
    @property
    def key(self):return 'structural:'+str(self.strategy)+':'+keccak(self.arguments).hex()
    @property
    def input_token(self):
        return decode([HOPS,HOPS],self.arguments)[0][0][1] if self.strategy==4 else USDC

def cycle_route(cycle,settlement=()):
    """Pool-independent financing; all paths use the conversion executor."""
    cycle=tuple(cycle);settlement=tuple(settlement)
    check(2<=len(cycle)<=4,'invalid cycle length')
    start=cycle[0][1];check(start in (USDC,WETH),'unsupported funding asset')
    current=start;seen=set();pools=set()
    for pool,a,b,kind in cycle:
        check(a==current and a not in seen and a!=b and pool not in pools and kind in (1,2,3,4),'invalid funded cycle')
        current=b;seen.add(a);pools.add(pool)
    check(current==start,'funded cycle is not closed')
    check(len(settlement)<=4 and (bool(settlement)==(start!=USDC)),'missing cycle settlement')
    seen={start};pools=set()
    for pool,a,b,kind in settlement:
        check(a==current and b not in seen and pool not in pools and kind in (1,2,3,4),'invalid cycle settlement')
        current=b;seen.add(b);pools.add(pool)
    check(current==USDC,'cycle profit settlement mismatch')
    hops=cycle+settlement
    return Route(4,encode([HOPS,HOPS],[cycle,settlement]),tuple(h[0] for h in hops),
                 path_tokens=(USDC,start)+tuple(h[2] for h in hops))

def morpho_route(requests):
    check(0<len(requests)<=8,'invalid liquidation batch')
    return Route(1,encode([REQUESTS],[requests]),(POOL1,POOL2))

def market_route(market,requests,unwind,settlement=()):
    check(0<len(requests)<=8,'invalid liquidation batch')
    def validate(hops,start,end):
        check(len(hops)<=4 and (bool(hops) or start==end),'missing unwind path')
        current=start;visited={start};pools=set()
        for pool,a,b,kind in hops:
            check(a==current and b not in visited and pool not in pools and kind in (1,2,3,4),'invalid unwind path')
            current=b;visited.add(b);pools.add(pool)
        check(current==end,'unwind endpoint mismatch')
    validate(unwind,market.collateral,market.loan)
    validate(settlement,market.loan,USDC)
    hops=tuple(unwind)+tuple(settlement)
    tokens=(USDC,market.collateral)+tuple(h[2] for h in hops)
    return Route(3,encode(GENERIC,[unhex(market.id),requests,unwind,settlement]),tuple(h[0] for h in hops),path_tokens=tokens)

def psm_route(pool,reverse=False):
    check(isinstance(pool,str) and len(pool)==42,'invalid PSM pool')
    return Route(2,encode(['address','bool'],[pool,reverse]),(pool,))

def encode_plan(candidate,route,common):
    check(candidate.get('execution_kind')=='structural-v1' and candidate['route_key']==route.key,'structural candidate binding')
    check(tuple(candidate['pools'])==route.pools and tuple(candidate['tokens'])==route.tokens,'structural path binding')
    if route.strategy in (1,3):
        requests=decode([REQUESTS],route.arguments)[0] if route.strategy==1 else decode(GENERIC,route.arguments)[1]
        check(0<len(requests)<=8 and all(s>0 and d>0 for _,s,d in requests),'invalid liquidation amounts')
        check(common[1]==sum(d for _,_,d in requests),'liquidation debt budget binding')
    elif route.strategy==4:
        cycle,settlement=decode([HOPS,HOPS],route.arguments)
        check(route==cycle_route(cycle,settlement),'funded cycle binding')
    else:
        pool,reverse=decode(['address','bool'],route.arguments)
        check(route.strategy==2 and route.pools==(pool,),'invalid PSM route')
    return data(f'execute({PLAN})',[PLAN],[(route.strategy,route.arguments,*common[1:])])

def clock_call(route,context):
    from .statefeed import LENS as CLOCK_LENS
    if route.strategy in (3,4):
        a,b=decode([HOPS,HOPS],route.arguments) if route.strategy==4 else decode(GENERIC,route.arguments)[2:]
        hops=list(a)+list(b)
        calldata=(data('checkMixedClock(address[],int24[])',['address[]','int24[]'],[list(route.pools),[-1 if h[3]==4 else 0 if h[3]==3 else 1 for h in hops]]) if hops else data('contextClock()'))
    else:calldata=data('checkClock(address[])',['address[]'],[list(route.pools)])
    return ('eth_call',[{'to':CLOCK_LENS,'gas':hex(1_000_000),
        'data':calldata},'pending',
        {CLOCK_LENS:{'code':artifact('StateLens')['deployedBytecode']['object']}},context])

def inspect(rpc,borrowers,block='pending',context=None,market_id=MARKET,state_overrides=None):
    check(0<len(borrowers)<=128,'position read bound')
    call={'to':LENS,'gas':hex(6_000_000),'data':data('inspect(bytes32,address[])',['bytes32','address[]'],[unhex(market_id),borrowers])}
    args=[call,block,{**(state_overrides or {}),LENS:{'code':artifact('MorphoStateLens')['deployedBytecode']['object']}}]
    if context is not None:args.append(context)
    raw=rpc.request('eth_call',args)
    price,stamp,rows=decode(['uint256','uint256','(address,uint256,uint256,uint256,uint256)[]'],unhex(raw))
    check(price>0 and len(rows)==len(borrowers),'incomplete Morpho state')
    check([r[0].lower() for r in rows]==[a.lower() for a in borrowers],'position order mismatch')
    return price,stamp,[dict(zip(('borrower','collateral','shares','debt','boundary'),r)) for r in rows]


def inspect_markets(rpc,groups,block='pending',context=None,state_overrides=None):
    """One oracle-update view for multiple markets; exotic-market failures are isolated."""
    check(0<len(groups)<=128 and sum(len(a) for _,a in groups)<=128,'market batch bound')
    call={'to':LENS,'gas':hex(15_000_000),'data':data('inspectMarkets(bytes32[],address[][])',
        ['bytes32[]','address[][]'],[[unhex(id) for id,_ in groups],[a for _,a in groups]])}
    args=[call,block,{**(state_overrides or {}),LENS:{'code':artifact('MorphoStateLens')['deployedBytecode']['object']}}]
    if context is not None:args.append(context)
    results=decode(['(uint256,uint256,(address,uint256,uint256,uint256,uint256)[])[]'],unhex(rpc.request('eth_call',args)))[0]
    check(len(results)==len(groups),'incomplete market batch')
    out={}
    for (id,addresses),(price,stamp,rows) in zip(groups,results):
        if not price:continue
        check([r[0].lower() for r in rows]==[a.lower() for a in addresses],'market position order mismatch')
        if context:check(stamp==number(context['time']),'market batch clock mismatch')
        out[id]=(price,stamp,[dict(zip(('borrower','collateral','shares','debt','boundary'),r)) for r in rows])
    return out
