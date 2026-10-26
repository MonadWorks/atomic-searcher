"""Compare both PSM directions and depth-scaled sizes before whole execution."""
from fractions import Fraction
from math import isqrt
from eth_abi import decode
from .core import data,unhex,MULTICALL,RPCError,safe_error
from .structural import LENS,artifact,psm_route


def native_screen(rpc,rows,minimum,context=None):
    """Price actual tick traversal, partial fills and PSM conversion in one call.

    These advisory quotes still precede the full executor call. Failed or
    unrecognized quote responses stay unknown and retain the full-call fallback;
    a failed quote is never interpreted as a zero sale.
    """
    from .route_preparation import QUOTER
    from .morpho_universe import optional_views
    optional_views(rpc,[])  # Authenticate the Multicall runtime once per reader.
    pools=list(dict.fromkeys(route.pools[0] for route,_,_ in rows))
    calls=[];indices=[]
    for route,amount,_ in rows:
        pool,reverse=decode(['address','bool'],route.arguments)
        if reverse:
            indices.append(None)
        else:
            indices.append(len(calls))
            calls.append((QUOTER,True,unhex(data('quote(address,bool,uint256)',
                ['address','bool','uint256'],[pool,False,amount]))))
    # Forward exact-input probes cover the currently observed false positive.
    # Reverse proposals keep the ordinary complete-execution path; no inferred
    # result or fixed-rate approximation can suppress an unmeasured direction.
    calls.append((LENS,True,unhex(data('inspectPSMs(address[])',['address[]'],[pools]))))
    payload=data('aggregate3((address,bool,bytes)[])',['(address,bool,bytes)[]'],[calls])
    args=[{'to':MULTICALL,'gas':hex(12_000_000),'data':payload},'pending',
          {QUOTER:{'code':artifact('CLQuoter')['deployedBytecode']['object']},
           LENS:{'code':artifact('MorphoStateLens')['deployedBytecode']['object']}}]
    if context is not None:args.append(context)
    raw=decode(['(bool,bytes)[]'],unhex(rpc.request('eth_call',args)))[0]
    if len(raw)!=len(calls) or not raw[-1][0]:raise ValueError('incomplete native PSM screen')
    snapshots=decode(['uint256[6][]'],raw[-1][1])[0]
    if len(snapshots)!=len(pools):raise ValueError('incomplete native PSM state')
    states=dict(zip(pools,snapshots));selected=[];evidence=[]
    for row,index in zip(rows,indices):
        route,amount,_=row;pool=route.pools[0]
        record={'route_key':route.key,'amount_in':amount,'status':'quote_unknown'}
        if index is None or not raw[index][0] or len(raw[index][1])!=64:
            selected.append(row);evidence.append(record);continue
        used,received=decode(['uint256','uint256'],raw[index][1])
        rate=states[pool][3];inventory=states[pool][4]
        if used<=0 or used>amount or received<=0 or rate<=0:
            selected.append(row);evidence.append(record);continue
        # This adapter's sUSDS -> USDC conversion is rate / 10**39.
        # Rounding matches the deployed PSM; repayment uses actual filled input.
        converted=received*rate//10**39
        gross=converted-used
        if converted>inventory:
            selected.append(row);evidence.append(record);continue
        record.update(status='native_quote_admitted' if gross>=minimum else 'not_profitable',
            actual_input=used,gross_surplus=gross,minimum_gross_surplus=minimum,
            evidence='native_pool_and_psm_quote',full_execution_checked=False)
        if gross>=minimum:selected.append(row)
        evidence.append(record)
    return selected,states,evidence

def proposals(pools,states,maximum,minimum):
    ranked=[]
    for pool,(sqrt,fee,liquidity,rate,usdc,susds) in zip(pools,states):
        if not sqrt or not rate or not liquidity or not 0<=fee<1000000:continue
        price=Fraction(sqrt*sqrt,1<<192);conversion=Fraction(rate,10**39);f=Fraction(1000000-fee,1000000)
        for reverse in (False,True):
            edge=(price/conversion if reverse else conversion/price)*f
            if edge<=1:continue
            # Constant-active-liquidity sizing hint. Tick crossings, pocket
            # inventory and exact repayment remain checked by full execution.
            root=isqrt(edge.numerator*10**36//edge.denominator)
            # reverse borrows an exact USDC output from the pool. Its cost
            # grows as the virtual USDC reserve is depleted, unlike exact-in.
            depth=Fraction(liquidity*sqrt,1<<96)/(1 if reverse else f)
            suggested=int(depth*(root-10**18)//(root if reverse else 10**18))
            inventory=int(susds*conversion) if reverse else usdc
            size=min(maximum,inventory,max(0,suggested))
            for divisor in (1,2,4,8):
                amount=size//divisor
                if amount<=0:continue
                expected=(amount-int(Fraction(amount,1)/(edge*(1-Fraction(amount,1)/depth))) if reverse else
                    int(amount*edge/(1+Fraction(amount,1)/depth))-amount)
                if expected<minimum:continue
                ranked.append((expected,psm_route(pool,reverse),amount,700000))
    ranked.sort(key=lambda row:-row[0]);seen=set();out=[]
    for _,route,amount,gas in ranked:
        key=(route.key,amount)
        if key not in seen:seen.add(key);out.append((route,amount,gas))
    return out

def discover(rpc,pools,maximum,minimum,context=None,*,on_snapshot=None,native=False,on_screening=None,screen_start=0):
    selected=pools[:32]
    args=[{'to':LENS,'gas':hex(9000000),'data':data('inspectPSMs(address[])',['address[]'],[selected])},'pending',
        {LENS:{'code':artifact('MorphoStateLens')['deployedBytecode']['object']}}]
    if context is not None:args.append(context)
    states=decode(['uint256[6][]'],unhex(rpc.request('eth_call',args)))[0]
    if len(states)!=len(selected):raise ValueError('incomplete PSM pool comparison')
    if on_snapshot is not None:on_snapshot(dict(zip(selected,states)))
    rows=proposals(selected,states,maximum,minimum)
    if native and rows:
        offset=screen_start%len(rows);rows=(rows[offset:]+rows[:offset])[:8]
        try:rows,states,evidence=native_screen(rpc,rows,minimum,context)
        except RPCError as exc:
            # An unavailable native quote is not negative economic evidence.
            # Preserve the bounded whole-execution fallback for these sizes.
            if on_screening is not None:on_screening([{'route_key':r.key,'amount_in':a,'status':'quote_unknown','reason':safe_error(exc)} for r,a,_ in rows])
            return rows
        if on_snapshot is not None:on_snapshot(states)
        if on_screening is not None:on_screening(evidence)
    return rows
