"""V4 adapter: authenticated keys, atomic state, explicit exact-output semantics.

All execution goes through the existing account Executor. This module has no
signer and cannot send transactions. Hookless ERC20 cycles need only a final
settlement transfer; CL transfer-tax feedback does not describe these routes.
"""
from functools import lru_cache
from collections import OrderedDict
from dataclasses import asdict,dataclass
import json
import time
from eth_abi import decode,encode
from eth_utils import keccak
from .core import ROOT,USDC,check,data,number,unhex
from .context import simulation_context
from .costs import cost_budget,quote_profit_floor
from .v4_market import Key,Snapshot,MissingState,CapacityLimit,MANAGER,INITIALIZE,MODIFY,SWAP,initialization,Registry

MANAGER_HASH='0x83b2af6e9f3158defc2811cbcb0db71ecf8b2ba2abea39c39e370ac5c6f43eb6'
LENS='0x000000000000000000000000000000000000c1c7'
KEY_TYPE='(address,address,uint24,int24,address)'
PLAN_TYPE=f'({KEY_TYPE},{KEY_TYPE},address,uint128,uint160,uint160,uint256,uint256,uint64,uint64,bytes32,uint64,uint64)'
TOPICS=(INITIALIZE,MODIFY,SWAP)

def enabled(config):return config.get('v4',{}).get('enabled',False) is True
@lru_cache(maxsize=2)
def artifact(name):return json.loads((ROOT/f'out/{name}.sol/{name}.json').read_text())
def settlement_config(config):
    return next(t for t in config['settlement_tokens'] if t['address']==USDC)

def verify_manager(rpc):
    rpc.verify_chain()
    check(rpc.chain_id==8453,'V4 manager supports Base only')
    code=rpc.request('eth_getCode',[MANAGER,'latest'])
    check('0x'+keccak(unhex(code)).hex()==MANAGER_HASH,'V4 manager runtime differs from verified protocol')

def output_cap(config):
    return min(int(settlement_config(config)['max_input_raw']),(1<<127)-1)

def deployment(config):
    s=settlement_config(config);cap=output_cap(config)
    check(config['chain_id']==8453 and 0<cap<(1<<127),'invalid V4 deployment chain/cap')
    args=encode(['address','address','uint256','address','uint128'],[config['owner'],MANAGER,8453,USDC,cap])
    raw=unhex(artifact('V4AtomicPair')['bytecode']['object'])+args
    return {'creation_data':'0x'+raw.hex(),'creation_data_hash':'0x'+keccak(raw).hex(),'manager':MANAGER,'settlement':USDC,'max_output_raw':cap}

def verify_contract(rpc,config,contract,*,require_unpaused=True):
    from .execution import runtime_matches
    verify_manager(rpc)
    check(runtime_matches(rpc.request('eth_getCode',[contract,'latest']),artifact('V4AtomicPair')),'V4 executor runtime mismatch')
    calls=[data(x) for x in ('owner()','chainId()','manager()','paused()')]
    calls+=[data('maxOutput(address)',['address'],[USDC])]
    result=rpc.batch([('eth_call',[{'to':contract,'data':x},'latest']) for x in calls])
    owner,chain,manager,paused,cap=result
    check(decode(['address'],unhex(owner))[0].lower()==config['owner'],'V4 owner mismatch')
    check(number(chain)==config['chain_id'] and decode(['address'],unhex(manager))[0].lower()==MANAGER,'V4 chain/manager mismatch')
    check(number(cap)==output_cap(config),'V4 output cap mismatch')
    check(not require_unpaused or not number(paused),'V4 executor paused')
    return {'contract':contract,'paused':bool(number(paused)),'manager':MANAGER}

@dataclass(frozen=True)
class Route:
    sell:Key
    buy:Key
    settlement:str=USDC
    execution_kind:str='v4-pair-v1'
    @property
    def pools(self):return (self.sell.id,self.buy.id)
    @property
    def tokens(self):
        return (self.settlement,self.sell.currency1 if self.settlement==self.sell.currency0 else self.sell.currency0,self.settlement)
    @property
    def key(self):return 'v4:'+':'.join((*self.pools,self.settlement))

def encode_plan(candidate,route,common):
    check(candidate.get('execution_kind')=='v4-pair-v1' and candidate.get('amount_semantics')=='exact_settlement_output','V4 amount semantics missing')
    check(candidate['amount_in']==candidate['settlement_output_raw']==common[1],'V4 output binding mismatch')
    check(tuple(candidate['v4_keys'][0])==route.sell.values() and tuple(candidate['v4_keys'][1])==route.buy.values(),'V4 key binding mismatch')
    plan=(route.sell.values(),route.buy.values(),route.settlement,common[1],candidate['sell_limit'],candidate['buy_limit'],
          common[2],common[3],*common[4:])
    return data(f'execute({PLAN_TYPE})',[PLAN_TYPE],[plan])

def clock_call(context):
    # Hookless fixed-fee V4 has no oracle-time-dependent swap logic. Validate
    # the actual overridden EVM clock; complete execution separately rechecks price/fee state.
    return ('eth_call',[{'to':LENS,'data':data('context()'),'gas':hex(100000)},'pending',
                        {LENS:{'code':artifact('V4StateLens')['deployedBytecode']['object']}},context])

@dataclass
class Frame:
    anchor:dict
    context:dict
    snapshot:Snapshot
    keys:tuple
    requested_ns:int
    received_ns:int
    received_utc:float

class Reader:
    def __init__(self,rpc,config):
        self.rpc=rpc;self.config=config;self.extra=OrderedDict();self.verified=False;self.last_phase='idle'
    def read(self,keys):
        self.last_phase='protocol_verification'
        if not self.verified:verify_manager(self.rpc);self.verified=True
        if hasattr(self.rpc,'frame_reads'):
            with self.rpc.frame_reads():return self._read_frame(keys)
        return self._read_frame(keys)
    def _read_frame(self,keys):
        keys=tuple(keys);check(0<len(keys)<=4,'V4 read key bound')
        group=tuple(sorted(k.id for k in keys));extra=self.extra.get(group,{})
        self.last_phase='clock'
        anchor,pending=self.rpc.batch([('eth_getBlockByNumber',['latest',False]),('eth_getBlockByNumber',['pending',False])])
        check(anchor is not None and pending is not None,'V4 state clock missing')
        context=simulation_context(anchor,pending)
        cfg=self.config.get('v4',{});radius=int(cfg.get('bitmap_radius',16));ticks=int(cfg.get('tick_limit',128))
        payload=data('read(address,bytes32[],int24[],uint16,uint16,bytes32[])',
                     ['address','bytes32[]','int24[]','uint16','uint16','bytes32[]'],
                     [MANAGER,[unhex(k.id) for k in keys],[k.spacing for k in keys],radius,ticks,[n.to_bytes(32,'big') for n in sorted(extra)]])
        started=time.monotonic_ns()
        self.last_phase='atomic_state'
        raw,after=self.rpc.batch([('eth_call',[{'to':LENS,'data':payload,'gas':hex(8_000_000)},'pending',
                     {LENS:{'code':artifact('V4StateLens')['deployedBytecode']['object']}},context]),('eth_getBlockByNumber',['latest',False])])
        received=time.monotonic_ns()
        self.last_phase='decode_and_anchor_check'
        check(after is not None and after['hash']==anchor['hash'],'V4 state anchor changed')
        height,stamp,slots,values=decode(['uint256','uint256','bytes32[]','bytes32[]'],unhex(raw))
        check(height==number(context['number']) and stamp==number(context['time']) and len(slots)==len(values)<=1288,'V4 state/context mismatch')
        storage={}
        for slot,value in zip(slots,values):
            s='0x'+slot.hex();v='0x'+value.hex();check(s not in storage or storage[s]==v,'V4 inconsistent atomic slot');storage[s]=v
        self.last_phase='complete'
        return Frame(anchor,context,Snapshot(storage),keys,started,received,time.time())
    def request_missing(self,keys,missing):
        # Keep only slot addresses, never values. Empty/complete observations
        # must not evict another group's unfinished depth through LRU churn.
        if not missing:return
        group=tuple(sorted(k.id for k in keys));slots=self.extra.setdefault(group,OrderedDict())
        self.extra.move_to_end(group)
        for slot in sorted(missing):
            slots[slot]=None;slots.move_to_end(slot)
            while len(slots)>512:slots.popitem(last=False)
        # Sparse groups can share the original 128*512 total slot budget.
        # Independently bound group metadata even when each has only one slot.
        retained=sum(len(words) for words in self.extra.values())
        while len(self.extra)>4096 or retained>128*512:
            _,evicted=self.extra.popitem(last=False);retained-=len(evicted)

def evaluate(frame,config,routes=None):
    settlement=settlement_config(config);cap=output_cap(config)
    routes=routes or [Route(a,b) for a in frame.keys for b in frame.keys if a.id!=b.id and
                     (a.currency0,a.currency1)==(b.currency0,b.currency1) and USDC in (a.currency0,a.currency1)]
    budget=cost_budget(config['policy'],USDC,number(frame.anchor['baseFeePerGas']),2)
    floor=budget.settlement_cost_raw+quote_profit_floor(config,settlement,budget.settlement_cost_raw)
    candidates=[];missing=set();incomplete=0
    started=time.monotonic_ns();deadline=started+int(config['policy'].get('max_quote_ms',50)*1e6)
    limited=False
    # Each direction receives a budget. Integer output sizing expands from the
    # minimum economically meaningful output, then refines the profitable bracket.
    # Hookless fixed-fee two-pool cost is monotone/convex before integer rounding.
    for route_index,route in enumerate(routes):
        route_deadline=started+(deadline-started)*(route_index+1)//max(1,len(routes))
        frame.snapshot.deadline_ns=route_deadline
        best=None;observed={};amount=max(1,floor);previous=None;lower=1;upper=cap
        def quote(output):
            nonlocal best,incomplete,limited
            if time.monotonic_ns()>route_deadline:limited=True;return None
            if output in observed:return observed[output]
            try:q=frame.snapshot.cycle(route.sell,route.buy,USDC,output)
            except CapacityLimit:
                return None
            except MissingState as exc:
                incomplete+=1
                if exc.slot is not None:missing.add(exc.slot)
                if 'deadline' in str(exc):limited=True
                return None
            observed[output]=q
            if q['gross_surplus']>=floor and (best is None or q['gross_surplus']>best['gross_surplus']):best=q
            return q
        for _ in range(128):
            amount=min(amount,cap);q=quote(amount)
            if q is None:upper=amount;break
            if previous and q['gross_surplus']<previous['gross_surplus']:
                upper=amount;lower=max(1,previous['output']//2);break
            if amount==cap:lower=max(1,amount//2);upper=cap;break
            previous=q;amount=min(cap,amount*2)
        for _ in range(20):
            if upper-lower<=2 or time.monotonic_ns()>route_deadline:break
            a=lower+(upper-lower)//3;b=upper-(upper-lower)//3
            qa=quote(a);qb=quote(b)
            if qb is None:upper=b;continue
            if qa is None:upper=a;continue
            if qa['gross_surplus']<qb['gross_surplus']:lower=a
            else:upper=b
        if best:
            candidates.append({**best,'execution_kind':'v4-pair-v1','amount_semantics':'exact_settlement_output',
              'settlement_output_raw':best['output'],'amount_in':best['output'],'route_key':route.key,'pools':route.pools,'tokens':route.tokens,
              'v4_keys':[route.sell.values(),route.buy.values()],'cost_budget':asdict(budget),'minimum_gross_surplus':floor,
              'net_after_modelled_cost':best['gross_surplus']-budget.settlement_cost_raw})
    frame.snapshot.deadline_ns=None
    candidates.sort(key=lambda x:x['net_after_modelled_cost'],reverse=True)
    return {'candidates':candidates,'missing_slots':sorted(missing),'incomplete_sizes':incomplete,'search_time_limited':limited,
            'anchor':frame.anchor,'block':number(frame.anchor['number']),'execution_context':frame.context,
            'observation_age_ms':(time.monotonic_ns()-frame.requested_ns)/1e6,'scope':'complete atomic V4 state quotes; execution preflight separate'}
