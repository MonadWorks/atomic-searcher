"""xStocks public discovery and authenticated RFQ preparation.

No signing, broadcasting or automatic account enrolment. Public prices and
indexed liquidity never become executable-profit evidence. Addresses come
from the issuer registry; every read frame is pinned to a canonical block.
"""
import argparse
from dataclasses import dataclass
from decimal import Decimal
import json
from pathlib import Path
import time

import httpx
from eth_abi import decode, encode
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import keccak
from .core import RPC, ZERO, check, number, data, atomic_json

API='https://api.xstocks.fi/api/v2'
NETWORKS={'Ethereum':1,'Arbitrum':42161,'Base':8453,'Ink':57073,'HyperEVM':999}
DEFAULT_RPC={'Ink':'https://rpc-gel.inkonchain.com'}
MESSAGE='(bytes32,uint256,(address,address,address,uint256),(address,address,address,uint256))'
PERMIT='(address,uint256,uint8,bytes32,bytes32)'


def address(value):
    check(isinstance(value,str) and value.startswith('0x') and len(value)==42,'invalid equity address')
    check(len(bytes.fromhex(value[2:]))==20 and value.lower()!=ZERO,'zero equity address')
    return value.lower()


def uint(value):
    check(type(value) is int or isinstance(value,str) and value.isdecimal(),'invalid equity integer')
    value=int(value);check(0<=value<2**256,'equity integer range');return value


class XStocks:
    def __init__(self,client=None,api_key=None):
        self.client=client or httpx.Client(timeout=15,follow_redirects=False)
        self.own=client is None;self.api_key=api_key;self.next_at=0

    def close(self):
        if self.own:self.client.close()

    def request(self,method,path,*,authenticated=False,params=None,body=None):
        check(path.startswith('/') and not path.startswith('//'),'issuer API path')
        if authenticated:check(bool(self.api_key),'issuer_account_required')
        time.sleep(max(0,self.next_at-time.monotonic()));self.next_at=time.monotonic()+.5
        headers={'X-API-KEY':self.api_key} if authenticated else {}
        response=self.client.request(method,API+path,params=params,json=body,headers=headers)
        # Never include response bodies or the authenticated request in errors.
        check(response.status_code==200,f'issuer_http_{response.status_code}')
        result=response.json();check(isinstance(result,dict),'issuer response shape');return result

    def assets(self,network=None,authenticated=False):
        assets={}
        for page in range(100):
            params={'page':page,'pageSize':100}
            if network:params['network']=network
            body=self.request('GET','/trades/xchange/assets' if authenticated else '/public/assets',
                authenticated=authenticated,params=params)
            rows=body.get('nodes');pagination=body.get('page',{})
            check(isinstance(rows,list) and pagination.get('currentPage')==page and
                  type(pagination.get('hasNextPage')) is bool,'issuer pagination shape')
            previous=len(assets)
            for row in rows:
                key=row['id'];check(key not in assets,'duplicate issuer asset page');assets[key]=row
            if not pagination['hasNextPage']:return list(assets.values())
            check(len(assets)>previous,'issuer pagination did not advance')
        raise ValueError('issuer catalog exceeded bounded pagination')

    def wallets(self):return self.request('GET','/client/registered-wallets',authenticated=True)

    def rfq(self,identifier,side,network,stablecoin,quantity,*,receiver=None,soft=True):
        check(side in ('Buy','Sell') and network in NETWORKS,'RFQ side/network')
        amount=Decimal(str(quantity));check(amount.is_finite() and amount>0,'RFQ quantity')
        body=dict(identifier=identifier,side=side,network=network,stablecoinSymbol=stablecoin,quantity=str(amount))
        if not soft:
            receiver=address(receiver)
            body.update(paymentWalletIdentifier=receiver,receivingWalletIdentifier=receiver)
        return self.request('POST','/trades/xchange/rfq'+('/soft' if soft else ''),authenticated=True,body=body)


def inventory(assets,network):
    check(network in NETWORKS,'unsupported EVM inventory network')
    result=[]
    for asset in assets:
        for dep in asset.get('deployments',[]):
            if dep['network']!=network:continue
            token=address(dep['address']);wrapper=dep.get('wrapperAddressV2')
            stablecoins=[]
            for stable in dep.get('stablecoins',[]):
                if stable.get('supportsAtomicSwaps') and (stable.get('issuance') or stable.get('redemption')):
                    stablecoins.append({**stable,'address':address(stable['address'])})
            trading=asset.get('trading') or {}
            result.append(dict(symbol=asset['symbol'],id=asset['id'],network=network,
                chain_id=NETWORKS[network],token=token,wrapper=address(wrapper) if wrapper else None,
                atomic_supported=dep.get('supportsAtomicSwaps') is True,stablecoins=stablecoins,
                issuer_open=trading.get('openNow') is True and not asset.get('isTradingHalted',True)
                    and not trading.get('isTradingHalted',True),
                next_change_at=trading.get('nextChangeAt'),limits=trading.get('limitsPerPeriod',{}),
                issuer_account_verified=False,execution_connected=False))
    return result


def view(rpc,target,signature,types=(),args=(),returns=('uint256',),block='latest'):
    raw=rpc.request('eth_call',[{'to':target,'data':data(signature,list(types),list(args))},block])
    values=decode(list(returns),bytes.fromhex(raw[2:]))
    return values[0] if len(values)==1 else values


def inspect_wrapper(rpc,item,block):
    wrapper=item['wrapper'];check(wrapper is not None,'current wrapper unavailable')
    check(address(view(rpc,wrapper,'asset()',returns=('address',),block=block))==item['token'],'wrapper underlying mismatch')
    decimals=view(rpc,wrapper,'decimals()',returns=('uint8',),block=block)
    token_decimals=view(rpc,item['token'],'decimals()',returns=('uint8',),block=block)
    check(decimals<=36 and token_decimals<=36,'unsupported equity decimals')
    shares=10**decimals
    underlying=view(rpc,wrapper,'previewRedeem(uint256)',('uint256',),(shares,),block=block)
    deposit=view(rpc,wrapper,'previewDeposit(uint256)',('uint256',),(underlying,),block=block)
    return dict(share_decimals=decimals,asset_decimals=token_decimals,
                shares=shares,redeem_assets=underlying,redeposit_shares=deposit)


def inspect_v3(rpc,pool,item,block):
    pool=address(pool)
    t0=address(view(rpc,pool,'token0()',returns=('address',),block=block))
    t1=address(view(rpc,pool,'token1()',returns=('address',),block=block))
    check(item['wrapper'] in (t0,t1) or item['token'] in (t0,t1),'pool stock address mismatch')
    fee=view(rpc,pool,'fee()',returns=('uint24',),block=block)
    factory=address(view(rpc,pool,'factory()',returns=('address',),block=block))
    actual=view(rpc,factory,'getPool(address,address,uint24)',('address','address','uint24'),
        (t0,t1,fee),returns=('address',),block=block)
    check(address(actual)==pool,'pool factory lookup mismatch')
    liquidity=view(rpc,pool,'liquidity()',returns=('uint128',),block=block)
    check(0<fee<1_000_000 and liquidity>0,'inactive V3 pool')
    return dict(pool=pool,token0=t0,token1=t1,fee=fee,liquidity=liquidity,factory=factory,
                identity_consistent=True,execution_allowlisted=False)


@dataclass(frozen=True)
class FirmSwap:
    contract:str
    quote_id:str
    expires:int
    input_token:str
    input_amount:int
    output_token:str
    output_amount:int
    calldata:str


def prepare_swap(quote_response,*,network,receiver,contract,signer,input_token,output_token,
                 max_input,min_output,now=None,expiry_buffer=10,used_ids=()):
    """Encode only a signed, recipient-bound RFQ. This is not a cycle preflight.

    Contract and issuer signer must be independently pinned from onboarding;
    never trust the same quote response to supply its own trust roots.
    """
    check(network in NETWORKS,'RFQ network')
    check(quote_response.get('generalStatus')=='Accepted' and
          quote_response.get('blockchainStatus')=='PendingExecution','RFQ not executable')
    c=quote_response['contract'];payload=quote_response['signaturePayload'];domain=payload['domain'];m=payload['message']
    types=payload['types'];fields=types[payload['primaryType']]
    check(len(fields)==4 and [(f['name'],f['type']) for f in fields[:2]]==
          [('quoteId','bytes32'),('expiration','uint256')] and
          [f['name'] for f in fields[2:]]==['incomingTransfer','outgoingTransfer'],'RFQ message schema')
    for field in fields[2:]:
        check([(f['name'],f['type']) for f in types[field['type']]]==
              [('from','address'),('to','address'),('token','address'),('amount','uint256')],'RFQ transfer schema')
    contract=address(contract);receiver=address(receiver)
    check(c['network']==network and address(c['address'])==contract,'RFQ contract mismatch')
    check(uint(domain['chainId'])==NETWORKS[network] and address(domain['verifyingContract'])==contract,'RFQ domain mismatch')
    expires=uint(m['expiration']);check(type(expiry_buffer) is int and expiry_buffer>=1,'RFQ expiry buffer')
    check(expires>=(time.time() if now is None else now)+expiry_buffer,'RFQ expired or too close')
    qid=m['quoteId'];check(isinstance(qid,str) and qid.startswith('0x') and len(qid)==66,'RFQ id')
    qid='0x'+bytes.fromhex(qid[2:]).hex();check(qid not in used_ids,'RFQ already used')
    incoming=m['incomingTransfer'];outgoing=m['outgoingTransfer']
    def transfer(row):return (address(row['from']),address(row['to']),address(row['token']),uint(row['amount']))
    incoming=transfer(incoming);outgoing=transfer(outgoing)
    check(incoming[0]==receiver and outgoing[1]==receiver,'RFQ receiver mismatch')
    check(incoming[2]==address(input_token) and outgoing[2]==address(output_token) and incoming[2]!=outgoing[2],'RFQ token mismatch')
    check(0<incoming[3]<=uint(max_input) and outgoing[3]>=uint(min_output)>0,'RFQ amount bounds')
    signature=quote_response['signature'];check(isinstance(signature,str) and signature.startswith('0x'),'RFQ signature missing')
    signature_bytes=bytes.fromhex(signature[2:]);check(len(signature_bytes)==65,'RFQ signature length')
    recovered=Account.recover_message(encode_typed_data(full_message=payload),signature=signature_bytes)
    check(address(recovered)==address(signer),'RFQ issuer signature mismatch')
    message=(bytes.fromhex(qid[2:]),expires,incoming,outgoing)
    call='executeSwap('+MESSAGE+',bytes,'+PERMIT+')'
    calldata='0x'+(keccak(text=call)[:4]+encode([MESSAGE,'bytes',PERMIT],
        [message,signature_bytes,(ZERO,0,0,bytes(32),bytes(32))])).hex()
    return FirmSwap(contract,qid,expires,incoming[2],incoming[3],outgoing[2],outgoing[3],calldata)


def net_cycle(*,borrowed,repaid,final_balance,baseline,network_cost,bid_cost,min_net):
    """Amounts are integers in the SAME funding token, from a complete call.

    The caller must obtain final_balance AFTER every swap, wrap, RFQ and loan
    repayment. A USDG balance cannot be counted as USDC without a conversion.
    """
    values=[borrowed,repaid,final_balance,baseline,network_cost,bid_cost,min_net]
    for value in values:uint(value)
    check(borrowed>0 and repaid>=borrowed and min_net>0,'cycle debt/profit bounds')
    net=final_balance-baseline-network_cost-bid_cost
    return dict(net=net,qualifies=net>=min_net,source='complete_execution_required')


def indexed_markets(client,items,network,finish,checkpoint=None):
    # Official API supports up to 30 addresses per request. Discover the whole
    # registered universe before spending chain reads on individual pools.
    by_token={token:item for item in items for token in (item['token'],item['wrapper']) if token}
    tokens=list(by_token);pools={};queried=set()
    for offset in range(0,len(tokens),30):
        if time.monotonic()>=finish:break
        batch=tokens[offset:offset+30]
        response=client.client.get('https://api.dexscreener.com/tokens/v1/'+network.lower()+'/'+','.join(batch),timeout=15)
        check(response.status_code==200,'pool index unavailable')
        rows=response.json();check(isinstance(rows,list),'pool index shape')
        queried.update(batch)
        for row in rows:
            if row.get('chainId')!=network.lower():continue
            for side in ('baseToken','quoteToken'):
                token=(row.get(side,{}).get('address') or '').lower()
                if token in by_token:
                    key=by_token[token]['token'];pools.setdefault(key,{})[row['pairAddress']]=row
        if checkpoint:checkpoint(len(queried),len(tokens))
        time.sleep(.25)
    for item in items:
        item['index_checked']=all(t in queried for t in (item['token'],item['wrapper']) if t)
        item['indexed_pools']=len(pools.get(item['token'],{}))
    return {token:list(rows.values()) for token,rows in pools.items()}


def snapshot(client,network,rpc=None,*,seconds=300,checkpoint=None):
    assets=client.assets(network);items=inventory(assets,network)
    result=dict(at=time.time(),network=network,chain_id=NETWORKS[network],assets=items,
        coverage='issuer registry; indexed DEX discovery is not exhaustive',
        firm_quotes=0,complete_execution_checks=0,submitted=0,read_complete=False,assets_checked=0,
        issuer_access='not_configured',execution_connected=False)
    if checkpoint:checkpoint(result)
    if rpc is None:return result
    finish=time.monotonic()+seconds
    rpc.verify_chain();check(rpc.chain_id==NETWORKS[network],'equity reader chain mismatch')
    def progress(done,total):
        result.update(index_addresses_checked=done,index_addresses_total=total)
        if checkpoint:checkpoint(result)
    discovered=indexed_markets(client,items,network,finish,progress)
    # Existing liquidity is a prioritization hint only, never profit evidence.
    active=sorted((item for item in items if item['indexed_pools']),
        key=lambda item:-max((p.get('liquidity') or {}).get('usd',0) or 0 for p in discovered[item['token']]))
    for item in active:
        if time.monotonic()>=finish:break
        result['assets_checked']+=1
        try:
            head=rpc.header();block=head['number']
            if item['wrapper']:item['wrapper_state']=inspect_wrapper(rpc,item,block)
            item['pools']=[]
            for pool in discovered[item['token']]:
                row={'pool':pool['pairAddress'],'dex':pool.get('dexId')}
                try:row.update(inspect_v3(rpc,pool['pairAddress'],item,block))
                except Exception as exc:row['read_state']=type(exc).__name__
                item['pools'].append(row)
            check(rpc.header(number(block))['hash']==head['hash'],'equity frame changed')
            item['block']=number(block);item['block_hash']=head['hash']
        except Exception as exc:
            item['read_state']=type(exc).__name__;item.pop('wrapper_state',None);item.pop('pools',None)
        if checkpoint:checkpoint(result)
    result['read_complete']=all(item['index_checked'] for item in items) and result['assets_checked']==len(active)
    result['completed_at']=time.time()
    return result


def main():
    parser=argparse.ArgumentParser(description='Refresh public equity market preparation; no wallet access or trading.')
    parser.add_argument('--network',choices=tuple(NETWORKS),default='Ink')
    parser.add_argument('--rpc');parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--seconds',type=int,default=300)
    args=parser.parse_args();client=XStocks();url=args.rpc or DEFAULT_RPC.get(args.network)
    rpc=RPC(url,chain_id=NETWORKS[args.network],timeout=5) if url else None
    if rpc:rpc.minimum_request_interval=.25
    try:
        check(1<=args.seconds<=3600,'equity read budget')
        result=snapshot(client,args.network,rpc,seconds=args.seconds,checkpoint=lambda result:atomic_json(args.output,result));atomic_json(args.output,result)
        print(json.dumps(dict(output=str(args.output),assets=len(result['assets']),execution_connected=False)))
    finally:
        client.close()
        if rpc:rpc.close()


if __name__=='__main__':main()
