"""Bounded, diverse liquidation packages, ranked by realizable value hints."""
from .core import USDC, WETH, unhex
from .structural import market_route, cap
from .structural_runner import liquidation_batches, sized_requests
from .auction_selection import interleave

def packages(rows,price,maximum,lltv,decimals):
    if price is None:
        # Untrusted cached positions propose repay sizes only. The whole-call
        # batch checks present eligibility, balances and price before any bid.
        reqs=[]
        for r in rows:
            debt=(r['debt']*101+99)//100+1
            if r['shares']>0 and r['collateral']>0 and 0<debt<=maximum:reqs.append((r['borrower'],r['shares'],debt))
        full=[];group=[];total=0
        for request in reqs:
            if group and (len(group)==8 or total+request[2]>maximum):full.append(group);group=[];total=0
            group.append(request);total+=request[2]
        if group:full.append(group)
    else:full=liquidation_batches(rows,price,maximum,max_batch=8,lltv=lltv,loan_decimals=decimals)
    out=[];seen=set();families=[]
    for group in full:
        variants=[group]
        if len(group)>1:
            variants += [[x] for x in group[:2]]
            if len(group)>2:variants += [group[:2],group[:4]]
        variants += list(sized_requests(group))[1:]
        family=[]
        for variant in variants:
            key=tuple(variant)
            if key not in seen:seen.add(key);family.append(variant)
        families.append(family)
    return interleave(families)

def trials_for(universe,config,op,inspected,markets,minimum,liability_cap,limit=8,diagnostics=None):
    """Compare bundles/singles/partial sizes across markets in ONE whole-call batch.

    Cached swap curves choose promising paths and sizes, never authorize a bid.
    Preserve a single-position fallback so one bad member cannot monopolize work.
    """
    rb=getattr(universe,'route_book',None);by_market={}
    details=diagnostics if diagnostics is not None else []
    for m in markets:
        if m.id not in inspected:continue
        price,_,rows=inspected[m.id]
        groups=packages(rows,price,cap(config),m.lltv,m.loan_decimals)
        detail={'market':m.id,'positions':len(rows),'repayment_groups':len(groups),
                'unwind_and_settlement_paths':0,'bid_conversion_paths':0,'plans_generated':0,'reasons':[]}
        details.append(detail)
        if not groups:
            detail['reasons'].append('no_eligible_positions' if not rows else 'no_repayable_shares_within_budget')
            continue
        indexed={r['borrower']:r for r in rows};options=[]
        incentive=min(115*10**16,10**36//(10**18-3*10**17*(10**18-m.lltv)//10**18))
        for group_index,requests in enumerate(groups[:8]):
            debt=sum((indexed[a]['debt']*s+indexed[a]['shares']-1)//indexed[a]['shares'] for a,s,_ in requests)
            collateral=(sum(indexed[a]['collateral']*s//indexed[a]['shares'] for a,s,_ in requests) if price is None else
                sum(min(indexed[a]['collateral'],((indexed[a]['debt']*s//indexed[a]['shares'])*incentive*10**18+price-1)//price) for a,s,_ in requests))
            residual=max(1,debt*(incentive-10**18)//10**18)
            usd=residual if m.loan==USDC else max(1,int(residual*m.borrow_usd/max(1,m.borrow_assets)*10**6))
            paths=universe.routes(m,limit=3,amount=max(1,collateral),settlement_amount=residual)
            if rb:conversions=rb.ranked(USDC,WETH,usd,3)
            else:
                with universe.lock:conversions=list(universe.graph.paths(USDC,WETH,max_hops=2))
            detail['unwind_and_settlement_paths']=max(detail['unwind_and_settlement_paths'],len(paths))
            detail['bid_conversion_paths']=max(detail['bid_conversion_paths'],len(conversions))
            if not paths and 'no_prepared_liquidation_path' not in detail['reasons']:detail['reasons'].append('no_prepared_liquidation_path')
            if not conversions and 'no_prepared_bid_conversion' not in detail['reasons']:detail['reasons'].append('no_prepared_bid_conversion')
            # Diagonal pairs preserve alternative unwinds as well as conversions.
            choices=sorted(((i,j) for i in range(len(paths)) for j in range(len(conversions))),key=lambda x:(sum(x),x))[:4]
            for i,j in choices:
                a,b=paths[i];convert=conversions[j]
                realized=rb.estimate(a,collateral) if rb else None
                score=usd
                if realized is not None:
                    surplus=max(0,realized-debt)
                    settlement=rb.estimate(b,surplus) if surplus else 0
                    if settlement is not None:score=settlement
                converted=rb.estimate(convert,score) if rb else None
                # All estimates become the same WETH unit when available. The
                # USD fallback only ranks unpriced markets relative to each other.
                merit=(converted if converted is not None else score*10**12//5000)
                route=market_route(m,requests,a,b);amount=sum(r[2] for r in requests)
                plan=(unhex(op['userOpHash']),route.arguments,amount,convert,minimum,liability_cap,0,int(op['maxFeePerGas'],16) if isinstance(op['maxFeePerGas'],str) else op['maxFeePerGas'],int(op['deadline'],16) if isinstance(op['deadline'],str) else op['deadline'])
                options.append({'route':route,'plan':plan,'market':m.id,'score':merit,'borrowers':len(requests),'group':group_index,'conversion':tuple(convert)})
        if options:
            detail['plans_generated']=len(options)
            options.sort(key=lambda x:(-x['score'],x['group'],len(x['route'].pools)))
            by_market[m.id]=options
    # First pass covers the best package in every high-value market. Remaining
    # slots cover competing sizes, individual positions and alternative paths.
    ordered=sorted(by_market,key=lambda m:-by_market[m][0]['score']);selected=[];keys=set()
    def add(item):
        key=(item['route'].key,item['conversion'])
        if key not in keys and len(selected)<limit:keys.add(key);selected.append(item)
    for m in ordered[:4]:add(by_market[m][0])
    for m in ordered[:2]:
        for item in by_market[m]:
            if item['borrowers']==1 and item['group']!=by_market[m][0]['group']:add(item);break
    remaining=sorted((x for rows in by_market.values() for x in rows),key=lambda x:-x['score'])
    for item in remaining:add(item)
    return selected
