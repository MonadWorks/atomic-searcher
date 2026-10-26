"""Low-frequency protocol preparation, separate from the executable hot lane.

Records actual access/eligibility, never labels an indexed quote as execution.
No signer or transaction submission is available in this module.
"""
import asyncio
from collections import OrderedDict
import json
import threading
import time
import httpx
import websockets
from eth_abi import decode
from .core import RPC,USDC,ZERO,atomic_json,data,number,safe_error,check

ATLAS='0x583dcfef0d240dc80753f0f0b26513fee27d9b77'
CONTROL='0xa5e1a36938769cbd5a26f5e19d8fcb379f597c83'
ETH_AGGREGATOR='0x05c84a58fe042275b37db038baacd15f410c7bb0'
ETH_MARKET='0x8793cf302b8ffd655ab97bd1c695dbd967807e8367a65cb2f4edaf1380ba1bda'
EULER_INDEX='https://api.goldsky.com/api/public/project_cm4iagnemt1wp01xn4gh1agft/subgraphs/euler-simple-base/latest/gn'
API='https://api.morpho.org'

def get(client,path,**params):
    r=client.get(API+path,params=params);r.raise_for_status();return r.json()

def pages(client,path,params,limit=30):
    rows=[];cursor=None;seen=set()
    for _ in range(limit):
        page=get(client,path,**params,**({'cursor':cursor} if cursor else {}));rows.extend(page['data'])
        cursor=page.get('cursor')
        if not cursor:return rows
        check(cursor not in seen,'protocol cursor loop');seen.add(cursor)
    raise ValueError('protocol pagination exceeds preparation bound')

def midnight(client,now):
    markets=pages(client,'/v0/midnight/markets',{'chain_ids':8453,'listed':'true','active_only':'true','limit':100})
    check(len({m['market_id'] for m in markets})==len(markets),'duplicate Midnight markets')
    eligible=[m for m in markets if m['loan_token'].lower()==USDC and m['liquidator_gate'].lower()==ZERO]
    calendar=[];books=[]
    for m in sorted(eligible,key=lambda m:int(m['maturity']))[:8]:
        # total_units includes creditors. Read actual borrower debt separately.
        positions=pages(client,f"/v0/midnight/markets/{m['market_id']}/positions",{'chain_ids':8453,'types':'borrow','limit':100})
        borrowers=[p for p in positions if int(p.get('debt',0))>0]
        calendar.append({'market':m,'borrowers':borrowers,'matured':now>int(m['maturity']),
                         'debt_raw':sum(int(p['debt']) for p in borrowers)})
    unknown=[]
    for m in markets:
        if m.get('lend_depth') is None or m.get('borrow_depth') is None:unknown.append(m['market_id']);continue
        if int(m['lend_depth'])<=0 or int(m['borrow_depth'])<=0:continue
        sides={s:get(client,f"/v0/midnight/books/{m['market_id']}/{s}/takeable-offers") for s in ('asks','bids')}
        asks=[r for r in sides['asks']['data'] if int(r['units'])>0 and not r['offer']['buy']]
        bids=[r for r in sides['bids']['data'] if int(r['units'])>0 and r['offer']['buy']]
        a=min((int(r['offer']['tick']) for r in asks),default=None);b=max((int(r['offer']['tick']) for r in bids),default=None)
        books.append({'market':m['market_id'],'ask':a,'bid':b,'crossed':a is not None and b is not None and b>a,'offers':sides if a is not None and b is not None and b>a else None})
        if len(books)>=16:break
    return {'market_count':len(markets),'calendar':calendar,'books':books,'unknown_depth_markets':unknown,
            'execution_ready':False,'gate':'Midnight resolver and liquidation executor require independent production integration'}

def compound(rpc):
    head=rpc.header();block=number(head['number'])
    comets=('0xb125e6687d4313864e53df431d5425969c15eb2f','0x9c4ec768c28520b50860ea7a15bd7213a9ff58bf','0x46e6b214b524310239732d51387075e0e70970bf')
    signatures=('getReserves()','targetReserves()','isBuyPaused()','numAssets()')
    vals=rpc.views([(c,data(s)) for c in comets for s in signatures],block);rows=[];requests=[]
    for i,comet in enumerate(comets):
        v=vals[i*4:i*4+4];reserves=decode(['int256'],v[0])[0];target=decode(['uint256'],v[1])[0]
        paused=decode(['bool'],v[2])[0];count=decode(['uint8'],v[3])[0]
        check(count<=32,'unexpected Compound asset count')
        rows.append({'comet':comet,'sale_open':not paused and reserves<target,'collateral_inventory':[]})
        requests.extend((i,j) for j in range(count))
    metadata=rpc.views([(comets[i],data('getAssetInfo(uint8)',['uint8'],[j])) for i,j in requests],block)
    assets=[decode(['uint8','address','address','uint64','uint64','uint64','uint64','uint128'],raw) for raw in metadata]
    balances=rpc.views([(comets[i],data('getCollateralReserves(address)',['address'],[a[1]])) for (i,_),a in zip(requests,assets)],block)
    for (i,_),a,raw in zip(requests,assets,balances):
        balance=decode(['uint256'],raw)[0]
        if balance:rows[i]['collateral_inventory'].append({'asset':a[1],'reserves_raw':balance,'scale':a[3]})
    check(rpc.header(block)['hash']==head['hash'],'Compound inventory anchor changed')
    return {'block':block,'markets':rows,'execution_ready':False,'gate':'inventory/absorb route requires financed buy-and-resale adapter'}

def euler(client):
    q='{_meta{block{number timestamp}} trackingVaultBalances(first:100,where:{debt_gt:"0"},orderBy:id,orderDirection:asc){id account vault debt balance}}'
    response=client.post(EULER_INDEX,json={'query':q});response.raise_for_status();payload=response.json()
    check(not payload.get('errors'),'Euler borrower index query failed')
    return {'index':payload['data'],'scope':'first 100 debt records; seed, not eligible liquidation count',
            'execution_ready':False,'gate':'vault-specific EVC liquidation, redemption cash and swap route must pass full call'}

def preliquidation(client):
    q='{markets(first:20,orderBy:BorrowAssetsUsd,orderDirection:Desc,where:{chainId_in:[8453]}){items{marketId preLiquidations{items{address preLltv preLCF1 preLCF2 preLIF1 preLIF2 preLiquidationOracle} pageInfo{countTotal}}}}}'
    response=client.post(API+'/graphql',json={'query':q});response.raise_for_status();payload=response.json()
    check(not payload.get('errors'),'preliquidation discovery query failed')
    rows=[r for r in payload['data']['markets']['items'] if r['preLiquidations']['items']]
    return {'markets':rows,'execution_ready':False,'gate':'borrower authorization plus full preliquidation unwind must be verified'}

def svr_notice(payload,seen,universe=None,received_at=None):
    result=payload.get('params',{}).get('result',{});op=result.get('partial_user_operation',{})
    if number(op.get('chainId',0))!=8453 or op.get('to','').lower()!=ATLAS or op.get('control','').lower()!=CONTROL:return None
    hints=op.get('hints',{});feed=hints.get('aggregator','').lower();auction=result.get('auction_id')
    if not isinstance(auction,str) or not auction or auction in seen:return None
    markets=universe.notice_markets(feed) if universe else ([ETH_MARKET] if feed==ETH_AGGREGATOR else [])
    if not markets or number(hints.get('medianPrice',0))<=0:return None
    seen[auction]=None
    while len(seen)>2048:seen.popitem(last=False)
    return {'auction_id':auction,'market':markets[0],'markets':markets,'partial_user_operation':op,'aggregator':feed,'price':number(hints['medianPrice']),
        'deadline_block':number(op.get('deadline',0)),'required_gas_price_wei':number(op.get('maxFeePerGas',0)),
        'received_at':time.time() if received_at is None else received_at,'execution_ready':False,'stage':'notification_received'}

class AccessWorker:
    def __init__(self,config,stop,output,universe=None):
        self.config,self.stop,self.output=config,stop,output
        self.universe=universe
        self.thread=threading.Thread(target=self.run,name='protocol-preparation',daemon=True)
        self.svr_thread=threading.Thread(target=lambda:asyncio.run(self.svr()),name='svr-intake',daemon=True)
    async def svr(self):
        seen=OrderedDict();messages=0;matches=0
        while not self.stop.is_set():
            try:
                async with websockets.connect('wss://svr-bid-endpoint.chain.link/ws/solver',max_size=2**20,open_timeout=5,close_timeout=1,ping_interval=20,ping_timeout=10) as socket:
                    await socket.send(json.dumps({'jsonrpc':'2.0','id':1,'method':'solver_subscribe','params':['userOperations']}))
                    while not self.stop.is_set():
                        try:raw=await asyncio.wait_for(socket.recv(),1)
                        except asyncio.TimeoutError:continue
                        received_at=time.time()
                        payload=json.loads(raw);check(not payload.get('error'),'SVR subscription rejected');messages+=1
                        notice=svr_notice(payload,seen,self.universe,received_at)
                        if notice:
                            matches+=1;atomic_json(self.output/'svr-latest-auction.json',notice)
                            if self.universe:
                                with self.universe.lock:
                                    if not self.universe.atlas_forwarders.get(notice['aggregator']):
                                        self.universe.forwarder_requests.add(notice['aggregator'])
                                    self.universe.auctions.append(notice);self.universe.auction_wakeup.set()
                                for market in notice['markets']:self.universe.trigger(market)
                        if notice or messages%20==0:atomic_json(self.output/'svr-health.json',{'at':time.time(),'state':'connected','messages':messages,'mapped_auctions':matches,'bidding_configured':bool(self.config.get('structural',{}).get('atlas_enabled'))})
            except Exception as exc:
                atomic_json(self.output/'svr-health.json',{'at':time.time(),'state':'retrying','error':safe_error(exc),'bidding_configured':bool(self.config.get('structural',{}).get('atlas_enabled'))})
                for _ in range(30):
                    if self.stop.is_set():break
                    await asyncio.sleep(.1)
    def run(self):
        self.svr_thread.start()
        if self.config.get('strategy_suite',{}).get('enabled'):
            while not self.stop.wait(.5):pass
            self.svr_thread.join(timeout=7);return
        rpc=RPC(self.config['policy'].get('discovery_rpc_url'),timeout=4,cancel_event=self.stop)
        results={};checks=(('euler',euler),('preliquidation',preliquidation),('midnight',lambda c:midnight(c,time.time())),('compound',lambda c:compound(rpc)))
        try:
            with httpx.Client(timeout=8) as client:
                while not self.stop.is_set():
                    for name,fn in checks:
                        if self.stop.is_set():break
                        try:
                            if name=='compound':rpc.verify_chain()
                            results[name]={'at':time.time(),'state':'prepared',**fn(client)}
                        except Exception as exc:
                            if self.stop.is_set():break
                            results[name]={'at':time.time(),'state':'retrying','error':safe_error(exc),'execution_ready':False}
                        atomic_json(self.output/'strategy-access.json',results)
                        self.stop.wait(2)
                    self.stop.wait(300)
        finally:
            rpc.close();self.svr_thread.join(timeout=7)
