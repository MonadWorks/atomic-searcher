"""Typed callback-funded adapters. Every route settles fresh profit in USDC."""
from dataclasses import dataclass
from functools import lru_cache
import json
from eth_abi import encode,decode
from eth_utils import keccak
from .core import ROOT,USDC,check,data,unhex,number,address
from .structural import PLAN,HOPS,cap,artifact as common_artifact

EVC='0x5301c7dd20bd945d2013b48ed0dee3a284ca8989'
MIDNIGHT='0xadedd8ab6de832766fedf0fac4992e5c4d3ea18a'
MARKET='(uint256,address,address,(address,uint256,uint256,address)[],uint256,uint256,address,address)'
OFFER=f'({MARKET},bool,address,uint256,uint256,uint256,bytes32,address,bytes,address,address,bool,uint128,uint128,uint256)'
TAKE=f'({OFFER},bytes,uint256)'
TYPES={1:['address','address','address[]',HOPS,HOPS],2:['address','address','address',HOPS,HOPS],3:['address','address','uint256',HOPS,HOPS],4:['bytes32','address','uint256','bool',HOPS,HOPS],5:[TAKE+'[]',TAKE+'[]',HOPS],6:['(address,address,address,uint8,bytes)[]']}
NAMES={1:'compound',2:'euler',3:'preliquidation',4:'midnight_liquidation',5:'midnight_books',6:'venues'}
@lru_cache(maxsize=3)
def artifact(name='ProtocolExecutor'):
    check(name in ('ProtocolExecutor','BookExecutor','VenueExecutor'),'unknown protocol artifact')
    source='VenueExecutor.sol' if name=='VenueExecutor' else 'ProtocolExecutor.sol'
    return json.loads((ROOT/'out'/source/f'{name}.json').read_text())

def contract_names(config):
    return ('ProtocolExecutor','BookExecutor')+(('VenueExecutor',) if config.get('venues',{}).get('enabled',False) else ())

def enabled(config):return config.get('strategy_suite',{}).get('enabled',False) and config.get('structural',{}).get('enabled',False)

def deployment(config,name='ProtocolExecutor'):
    raw=unhex(artifact(name)['bytecode']['object'])+encode(['address','uint256','uint256','address'],[config['owner'],8453,cap(config),EVC])
    return {'creation_data':'0x'+raw.hex(),'creation_data_hash':'0x'+keccak(raw).hex()}

def verify_contract(rpc,config,contract,*,name='ProtocolExecutor',require_unpaused=True):
    from .execution import runtime_matches
    rpc.verify_chain()
    check(runtime_matches(rpc.request('eth_getCode',[contract,'latest']),artifact(name)),'protocol runtime mismatch')
    values=rpc.batch([('eth_call',[{'to':contract,'data':data(s)},'latest']) for s in ('owner()','chainId()','paused()','maxInput()','EVC()')])
    check(decode(['address'],unhex(values[0]))[0].lower()==config['owner'],'protocol owner mismatch')
    check(number(values[1])==8453==config['chain_id'],'protocol chain mismatch')
    check(number(values[3])==cap(config),'protocol input cap mismatch')
    check(decode(['address'],unhex(values[4]))[0].lower()==EVC,'protocol EVC mismatch')
    check(not require_unpaused or number(values[2])==0,'protocol executor paused')
    if name=='BookExecutor':
        child=decode(['address'],unhex(rpc.request('eth_call',[{'to':contract,'data':data('resolver()')},'latest'])))[0]
        check(runtime_matches(rpc.request('eth_getCode',[child,'latest']),common_artifact('CrossedBooksResolver')),'resolver runtime mismatch')
        midnight=decode(['address'],unhex(rpc.request('eth_call',[{'to':child,'data':data('MIDNIGHT()')},'latest'])))[0]
        check(midnight.lower()==MIDNIGHT,'resolver protocol mismatch')
    return {'contract':contract.lower(),'paused':bool(number(values[2])),'execution_kind':'protocol-v1','artifact':name}

@dataclass(frozen=True)
class Route:
    strategy:int
    arguments:bytes
    hops:tuple
    assets:tuple
    execution_kind:str='protocol-v1'
    @property
    def pools(self):return tuple(h[0] for h in self.hops)
    @property
    def tokens(self):return tuple(dict.fromkeys((USDC,*self.assets,*(h[1] for h in self.hops),*(h[2] for h in self.hops))))
    @property
    def key(self):return 'protocol:'+str(self.strategy)+':'+keccak(self.arguments).hex()
    @property
    def contract_name(self):return 'VenueExecutor' if self.strategy==6 else 'BookExecutor' if self.strategy>=4 else 'ProtocolExecutor'

def route(strategy,values,assets):
    check(strategy in TYPES and strategy!=6,'use cycle_route for venue cycles')
    hops=tuple(values[-1]) if strategy==5 else tuple(values[-2])+tuple(values[-1])
    check(all(h[3] in (1,2,3,4) for h in hops),'unknown swap type')
    encoded=encode(TYPES[strategy],values) if strategy==5 else encode(['('+','.join(TYPES[strategy])+')'],[tuple(values)])
    return Route(strategy,encoded,hops,tuple(assets))

def encode_plan(candidate,route,common):
    check(candidate.get('execution_kind')=='protocol-v1' and candidate['route_key']==route.key,'protocol route binding')
    check(tuple(candidate['pools'])==route.pools and tuple(candidate['tokens'])==route.tokens,'protocol path binding')
    check(route.strategy in TYPES and 0<common[1]<1<<255,'protocol amount binding')
    decode(TYPES[route.strategy] if route.strategy in (5,6) else ['('+','.join(TYPES[route.strategy])+')'],route.arguments)
    return data(f'execute({PLAN})',[PLAN],[(route.strategy,route.arguments,*common[1:])])

def clock_call(route,context):
    from .statefeed import LENS
    if route.strategy==6:
        # Complete execution checks the venue state; contextClock verifies
        # the actual overridden EVM clock, without treating V4 ids as V3 pools.
        return ('eth_call',[{'to':LENS,'gas':hex(100000),'data':data('contextClock()')},'pending',
                {LENS:{'code':common_artifact('StateLens')['deployedBytecode']['object']}},context])
    calldata=(data('checkMixedClock(address[],int24[])',['address[]','int24[]'],[list(route.pools),[-1 if h[3]==4 else 0 if h[3]==3 else 1 for h in route.hops]]) if route.pools else data('contextClock()'))
    return ('eth_call',[{'to':LENS,'gas':hex(1_000_000),'data':calldata},'pending',{LENS:{'code':common_artifact('StateLens')['deployedBytecode']['object']}},context])

def optional_address(value):
    from eth_utils import is_address
    check(is_address(value),"invalid optional address")
    return value.lower()

def wire_market(w):
    return (int(w['chain_id']),address(w['midnight']),address(w['loan_token']),[(address(c['token']),int(c['lltv']),int(c['liquidation_cursor']),address(c['oracle'])) for c in w['collaterals']],int(w['maturity']),int(w['rcf_threshold']),optional_address(w['enter_gate']),optional_address(w['liquidator_gate']))

def midnight_id(m):
    init=keccak(bytes.fromhex('600b380380600b5f395ff3')+encode([MARKET],[m]))
    return '0x'+keccak(b'\xff'+unhex(m[1])+b'\x00'*32+init).hex()

def wire_take(w,units):
    o=w['offer'];m=wire_market(o['market'])
    check(m[0]==8453 and m[1]==MIDNIGHT,'wrong offer chain')
    check(midnight_id(m)==w['market_id'].lower(),'offer market hash mismatch')
    check(isinstance(o['buy'],bool) and isinstance(o['reduce_only'],bool) and 0<units<=int(w['units']),'invalid offer amount')
    offer=(m,o['buy'],address(o['maker']),int(o['start']),int(o['expiry']),int(o['tick']),unhex(o['group']),optional_address(o['callback']),unhex(o['callback_data']),optional_address(o['receiver_if_maker_is_seller']),optional_address(o['ratifier']),o['reduce_only'],int(o['max_units']),int(o['max_assets']),int(o['continuous_fee_cap']))
    return offer,unhex(w['ratifier_data']),units
