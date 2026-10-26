"""Fair, paginated identity discovery followed by fresh protocol state reads.

Index values never authorize a trade. Scanners yield only proposals; the suite
measures the complete financed execution before the common account executor.
"""
import time
from collections import deque
from eth_abi import decode
from .core import USDC,WETH,ZERO,check,data,unhex,address,number
from .structural import artifact,cap,MORPHO
from .protocol_execution import route,EVC,MIDNIGHT,MARKET,wire_take
from .morpho_universe import optional_views,ConversionGraph,query,borrower_pages
from .strategy_access import API,EULER_INDEX,pages,get

LENS='0x000000000000000000000000000000000000c1d4'
MID_LENS='0x000000000000000000000000000000000000c1d5'
COMETS=('0xb125e6687d4313864e53df431d5425969c15eb2f','0x9c4ec768c28520b50860ea7a15bd7213a9ff58bf','0x46e6b214b524310239732d51387075e0e70970bf')
WAD=10**18

def ceil(a,b):return (a+b-1)//b

def lens(rpc,name,signature,types,values,outputs):
    target=MID_LENS if name=='MidnightLiquidationLens' else LENS
    return decode(outputs,unhex(rpc.request('eth_call',[{'to':target,'gas':hex(8_000_000),'data':data(signature,types,values)},'pending',{target:{'code':artifact(name)['deployedBytecode']['object']}}])))

class Scanners:
    def __init__(self,rpc,client,config,universe,contracts,report,stop):
        self.rpc,self.client,self.config,self.u,self.contracts,self.report,self.stop=rpc,client,config,universe,contracts,report,stop
        self.discovered={};self.pair_due={};self.page=0;self.euler_cursor="";self.pre_skip=0;self.compound_cursor=0;self.books_cursor=0
        from .protocol_screen import ProtocolScreen
        self.profit_screen=ProtocolScreen(config,report)
    def paths(self,coll,loan,amount=1,loan_amount=None):
        if self.stop.is_set():return []
        now=time.monotonic();key=(coll,loan)
        # Refresh venue alternatives even when an older route already exists.
        if now-self.discovered.get(key,-300)>=300:
            pairs={(coll,loan),(coll,WETH),(loan,WETH),(loan,USDC),(coll,USDC)}
            with self.u.lock:
                pairs.update(self.u.graph.bridge_pairs(coll,loan))
                factories=list(self.u.graph.factories.values())
            needed=[p for p in pairs if p[0]!=p[1] and now>=self.pair_due.get(tuple(sorted(p)),0)]
            if needed:
                private=ConversionGraph(factories);private.discover_pairs(self.rpc,needed)
                with self.u.lock:
                    for p in private.pools.values():self.u.graph.add(p)
                for p in needed:self.pair_due[tuple(sorted(p))]=now+300
            self.discovered[key]=now
        rb=self.u.route_book
        if rb:
            a=rb.ranked(coll,loan,max(1,amount),2)
            result=[]
            for path in a:
                output=rb.estimate(path,amount)
                # Conversion input is loan-token surplus, never collateral raw units.
                surplus=(output-loan_amount if output is not None and loan_amount is not None else
                         output//20 if output is not None else (loan_amount or 1)//20)
                result.extend((path,b) for b in rb.ranked(loan,USDC,max(1,surplus),2))
            return result[:4]
        else:
            with self.u.lock:a=self.u.graph.direct(coll,loan);b=self.u.graph.direct(loan,USDC)
        return [(x,y) for x in a for y in b][:4]
    def compound(self):
        from .compound_accounts import CompoundAccounts
        accounts=CompoundAccounts(COMETS,self.config,self.stop,lambda **v:self.report(state='scanning',**v))
        while not self.stop.is_set():
            accounts.refresh()
            order=COMETS[self.compound_cursor:]+COMETS[:self.compound_cursor]
            for comet in order:
                self.compound_cursor=(COMETS.index(comet)+1)%len(COMETS)
                eligible=accounts.eligible(self.rpc,comet)
                v=optional_views(self.rpc,[(comet,data(s)) for s in ('getReserves()','targetReserves()','isBuyPaused()','numAssets()','baseToken()','baseScale()')])
                check(all(v),'Compound metadata incomplete')
                reserve=decode(['int256'],v[0])[0];target=int.from_bytes(v[1],'big')
                if eligible:
                    paused=optional_views(self.rpc,[(comet,data('isAbsorbPaused()'))])[0]
                    if paused is None or decode(['bool'],paused)[0]:eligible=[]
                # Absorption can open the sale gate and create inventory in
                # the SAME transaction. The complete call verifies both.
                if decode(['bool'],v[2])[0] or (reserve>=target and not eligible):
                    self.report(state='scanning',detail='collateral_sale_closed',market=comet);yield None;continue
                count=int.from_bytes(v[3],'big');check(count<=32,'Compound asset bound');loan=decode(['address'],v[4])[0];scale=int.from_bytes(v[5],'big')
                funding=optional_views(self.rpc,[(loan,data('balanceOf(address)',['address'],[MORPHO]))])[0]
                available=int.from_bytes(funding or b'','big')
                infos=optional_views(self.rpc,[(comet,data('getAssetInfo(uint8)',['uint8'],[i])) for i in range(count)])
                for raw in infos:
                    if not raw:continue
                    asset=decode(['uint8','address','address','uint64','uint64','uint64','uint64','uint128'],raw)[1]
                    vals=optional_views(self.rpc,[(comet,data('getCollateralReserves(address)',['address'],[asset])),(comet,data('quoteCollateral(address,uint256)',['address','uint256'],[asset,scale]))])
                    if not all(vals):yield None;continue
                    inventory,quoted=(int.from_bytes(x,'big') for x in vals)
                    self.report(state='scanning',detail='collateral_inventory_checked',market=comet,inventory_raw=inventory)
                    proposals=[([],inventory)] if reserve<target and inventory>0 else []
                    if eligible:
                        collateral=optional_views(self.rpc,[(comet,data('collateralBalanceOf(address,address)',
                            ['address','address'],[account,asset])) for account in eligible])
                        contributors=sorted([(int.from_bytes(raw,'big'),account) for account,raw in zip(eligible,collateral)
                                             if raw and int.from_bytes(raw,'big')>0],reverse=True)
                        if contributors:
                            proposals.append(([a for _,a in contributors],inventory+sum(n for n,_ in contributors)))
                            # A changed borrower must not prevent every other
                            # account from being attempted independently.
                            if len(contributors)>1:proposals.extend(([account],inventory+n) for n,account in contributors)
                    for absorb,inventory_after in proposals:
                        maximum=min(cap(self.config),available,inventory_after*scale//max(1,quoted))
                        if maximum<=0:continue
                        amounts=[a for a in dict.fromkeys(maximum//d for d in (1,2,4,8,16)) if a>0]
                        actual=optional_views(self.rpc,[(comet,data('quoteCollateral(address,uint256)',['address','uint256'],[asset,a])) for a in amounts])
                        sizes=[(amount,int.from_bytes(raw or b'','big')) for amount,raw in zip(amounts,actual)]
                        sizes=[(amount,received) for amount,received in sizes if 0<received<=inventory_after]
                        for a,b,amount in self.profit_screen.compound(self,comet,asset,loan,sizes):
                            yield route(1,[comet,asset,absorb,a,b],(asset,loan)),amount
                    yield None
                yield None
    def euler(self):
        while not self.stop.is_set():
            cursor=self.euler_cursor;seen=set()
            while not self.stop.is_set():
                q='{trackingVaultBalances(first:100,where:{debt_gt:"0",id_gt:"%s"},orderBy:id,orderDirection:asc){id account vault debt}}'%cursor
                response=self.client.post(EULER_INDEX,json={'query':q});response.raise_for_status();payload=response.json();check(not payload.get('errors'),'Euler index query failed')
                rows=payload['data']['trackingVaultBalances'];self.report(state='scanning',detail='borrower_page',cursor=cursor,identities_in_page=len(rows))
                for start in range(0,len(rows),8):
                    batch=rows[start:start+8]
                    for item in batch:
                        check(item['id']>cursor and item['id'] not in seen,'Euler cursor out of order');seen.add(item['id']);cursor=item['id']
                    snapshots=lens(self.rpc,'ProtocolLens','eulerBatch(address[],address[],address)',['address[]','address[]','address'],[[address(i['vault']) for i in batch],[address(i['account']) for i in batch],self.contracts['ProtocolExecutor']],['(bool,address,(address,address,uint256,uint256,uint256,uint256)[])[]'])[0]
                    check(len(snapshots)==len(batch),'Euler incomplete position batch')
                    for item,(valid,loan,positions) in zip(batch,snapshots):
                        if not valid:
                            self.report(state='scanning',detail='position_read_failed',market=item['vault']);continue
                        vault=address(item['vault']);borrower=address(item['account'])
                        for cv,token,repay,shares,assets,cash in positions:
                            if cv!=ZERO and token!=ZERO:self.paths(token,loan,assets or 10**18)
                            if not repay or not shares or not assets or not cash:continue
                            maximum=min(repay,repay*min(cash,assets)//assets,cap(self.config))
                            for amount in dict.fromkeys(maximum//d for d in (1,2,4)):
                                if amount:
                                    for a,b in self.paths(token,loan,max(1,assets*amount//repay),loan_amount=amount):yield route(2,[vault,borrower,cv,a,b],(loan,token,cv)),amount
                    self.euler_cursor=cursor
                    self.report(state='scanning',detail='positions_checked',identities=len(seen));yield None
                if len(rows)<100:break
            self.euler_cursor=''
            self.report(state='scanning',detail='borrower_catalog_complete',identities=len(seen));yield None
    def preliquidation(self):
        while not self.stop.is_set():
            skip=self.pre_skip
            while not self.stop.is_set():
                q='{markets(first:100,skip:%d,orderBy:UniqueKey,orderDirection:Asc,where:{chainId_in:[8453]}){items{marketId preLiquidations{items{address} pageInfo{countTotal}}} pageInfo{countTotal}}}'%skip
                page=query(self.client,q)['markets'];items=page['items'];check(items or skip==int(page['pageInfo']['countTotal']),'incomplete preliquidation catalog')
                for m in items:
                    targets=m['preLiquidations']['items']
                    check(len(targets)==int(m['preLiquidations']['pageInfo']['countTotal']),'preliquidation target pagination incomplete')
                    if not targets:continue
                    for borrowers,_ in borrower_pages(self.client,m['marketId']):
                        pairs=[(target['address'],borrower) for borrower in borrowers for target in targets]
                        for start in range(0,len(pairs),8):
                            group=pairs[start:start+8]
                            states=lens(self.rpc,'ProtocolLens','preBatch(address[],address[])',['address[]','address[]'],[[p[0] for p in group],[p[1] for p in group]],['(bool,(uint256,uint256,uint256,uint256,uint256,address,address))[]'])[0]
                            check(len(states)==len(group),'preliquidation incomplete position batch')
                            for (target,borrower),(valid,state) in zip(group,states):
                                if not valid:
                                    self.report(state='scanning',detail='position_read_failed',market=m['marketId']);continue
                                shares,debt,coll,price,lif,loan,token=state
                                self.paths(token,loan,coll or 10**18)
                                if shares and debt:
                                    for divisor in (1,2,4):
                                        sized=shares//divisor
                                        if not sized:continue
                                        budget=min(cap(self.config),ceil(debt,divisor)+2)
                                        for a,b in self.paths(token,loan,coll//divisor,loan_amount=budget):yield route(3,[target,borrower,sized,a,b],(loan,token)),budget
                            self.report(state='scanning',detail='positions_checked',market=m['marketId']);yield None
                skip+=len(items);self.pre_skip=skip;self.report(state='scanning',detail='market_page',markets_checked=skip)
                yield None
                if skip>=int(page['pageInfo']['countTotal']):self.pre_skip=0;break
    def midnight_liquidation(self):
        while not self.stop.is_set():
            candidates=pages(self.client,'/markets/midnight/liquidation-candidates',{'chain_ids':8453,'limit':100,'include_matured':'true','health_factor_lte':1.05},limit=100)
            for item in candidates:
                if int(item['chain_id'])!=8453:continue
                market=item['market_id'];borrower=address(item['borrower'])
                out='(bool,bool,bool,bool,bool,uint64,uint128,uint128,uint128,uint128,(uint8,uint128,uint256,uint256,uint256)[],'+MARKET+')'
                row=lens(self.rpc,'MidnightLiquidationLens','computeOne((bytes32,address,address))',['(bytes32,address,address)'],[(unhex(market),borrower,self.contracts['BookExecutor'])],[out])[0]
                valid,has_debt,healthy,locked,gate,stamp,debt,max_debt,bad,bitmap,slots,m=row
                if not valid or not has_debt or locked or not gate:yield None;continue
                matured=stamp>m[4]
                for index,coll,price,max_lif,lltv in slots:self.paths(m[3][index][0],m[2],coll or 10**18)
                if healthy and not matured:
                    self.report(state='scanning',detail='healthy_position_prepared',market=market);yield None;continue
                for index,coll,price,max_lif,lltv in slots:
                    if not coll or not price:continue
                    for post in ([False,True] if matured and not healthy else [matured]):
                        lif=min(max_lif,WAD+(max_lif-WAD)*(stamp-m[4])//3600) if post else max_lif
                        repayable=ceil(ceil(coll*price,10**36)*WAD,lif)
                        amount=min(debt-bad,repayable,cap(self.config))
                        if not post and lltv<WAD and lif*lltv<WAD*WAD:
                            rcf=ceil(max(0,debt-bad-max_debt)*WAD*WAD,WAD*WAD-lif*lltv)
                            if max(0,repayable-rcf)>=m[5]:amount=min(amount,rcf)
                        for divisor in (1,2,4):
                            sized=amount//divisor
                            if sized and (sized*lif//WAD)*10**36//price>0:
                                token=m[3][index][0];loan=m[2]
                                for a,b in self.paths(token,loan,min(coll,ceil(sized*lif,WAD)*10**36//price),loan_amount=sized):yield route(4,[unhex(market),borrower,index,post,a,b],(loan,token)),sized
                self.report(state='scanning',detail='position_checked',market=market);yield None
            self.report(state='scanning',detail='candidate_catalog_complete',identities=len(candidates));yield None
    def midnight_books(self):
        while not self.stop.is_set():
            markets=pages(self.client,'/v0/midnight/markets',{'chain_ids':8453,'listed':'true','active_only':'true','limit':100})
            # Depth is only a priority hint: unknown or zero indexed depth is still visited.
            markets.sort(key=lambda m:-(int(m.get('lend_depth') or 0)>0 and int(m.get('borrow_depth') or 0)>0))
            if markets:
                offset=self.books_cursor%len(markets);markets=markets[offset:]+markets[:offset]
            for m in markets:
                self.books_cursor+=1
                mid=m['market_id'];sides={side:get(self.client,f'/v0/midnight/books/{mid}/{side}/takeable-offers')['data'] for side in ('asks','bids')}
                asks=sorted((r for r in sides['asks'] if int(r['units'])>0 and not r['offer']['buy']),key=lambda r:int(r['offer']['tick']))
                bids=sorted((r for r in sides['bids'] if int(r['units'])>0 and r['offer']['buy']),key=lambda r:-int(r['offer']['tick']))
                ai=bi=0;remaining_a=[int(r['units']) for r in asks];remaining_b=[int(r['units']) for r in bids];taken_a={};taken_b={};units=0
                while ai<len(asks) and bi<len(bids) and len(taken_a)<8 and len(taken_b)<8:
                    if int(bids[bi]['offer']['tick'])<=int(asks[ai]['offer']['tick']):break
                    check(asks[ai]['market_id']==bids[bi]['market_id']==mid,'mixed order book')
                    n=min(remaining_a[ai],remaining_b[bi],cap(self.config)-units)
                    if not n:break
                    taken_a[ai]=taken_a.get(ai,0)+n;taken_b[bi]=taken_b.get(bi,0)+n;units+=n
                    remaining_a[ai]-=n;remaining_b[bi]-=n
                    if not remaining_a[ai]:ai+=1
                    if not remaining_b[bi]:bi+=1
                if units:
                    loan=address(m['loan_token']);paths=self.paths(loan,loan,units)
                    for _,conversion in paths:
                        yield route(5,[[wire_take(asks[i],n) for i,n in taken_a.items()],[wire_take(bids[i],n) for i,n in taken_b.items()],conversion],(loan,)),units
                self.report(state='scanning',detail='book_checked',market=mid,crossed=bool(units));yield None
