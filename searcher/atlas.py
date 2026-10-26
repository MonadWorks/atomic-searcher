"""Atlas payloads, fee estimates and full oracle-update + solver simulation.

All discovery and quoting is keyless. Full Atlas simulation accepts an
already signed solver operation because Chainlink authenticates its bundler. The chain simulator executes the supplied real oracle report; the
price override is used solely to rank and measure a potential liquidation.
"""
import time
from eth_abi import encode,decode
from eth_utils import keccak,to_checksum_address
from eth_account.messages import encode_typed_data,encode_defunct
from .core import ZERO,USDC,WETH,data,number,unhex,check
from .structural import HOPS,GENERIC,artifact,LENS
from .strategy_access import ATLAS,CONTROL

U='(address,address,uint256,uint256,uint256,uint256,uint256,address,address,uint32,uint32,uint32,uint24,address,bytes,bytes)'
S='(address,address,uint256,uint256,uint256,uint256,address,address,bytes32,address,uint256,bytes,bytes)'
D='(address,address,uint256,uint256,address,address,bytes32,bytes32,bytes)'
AP='(bytes32,bytes,uint256,'+HOPS+',uint256,uint256,uint256,uint256,uint64)'
SOLVER_FIELDS=list(zip(('from','to','value','gas','maxFeePerGas','deadline','solver','control','userOpHash','bidToken','bidAmount','data'),
                      ('address','address','uint256','uint256','uint256','uint256','address','address','bytes32','address','uint256','bytes')))
GATEWAY='https://svr-bid-endpoint.chain.link/'
QUERY_GATEWAY='https://solver-query-api-fra.fastlane-labs.xyz/'

def metadata(rpc):
    calls=[(ATLAS,s) for s in ('SIMULATOR()','VERIFICATION()')]+[(CONTROL,s) for s in ('CALL_CONFIG()','getDAppGasLimit()','getSolverGasLimit()','getBundlerSurchargeRate()','authorizedUserOpSigner()')]+[(ATLAS,'getAtlasSurchargeRate()')]
    from .morpho_universe import optional_views
    raw=optional_views(rpc,[(a,data(s)) for a,s in calls])
    check(len(raw)==len(calls) and all(v and len(v)==32 for v in raw),'incomplete auction metadata')
    vals=['0x'+v.hex() for v in raw]
    return dict(simulator=decode(['address'],unhex(vals[0]))[0],verification=decode(['address'],unhex(vals[1]))[0],
        call_config=number(vals[2]),dapp_gas=number(vals[3]),solver_gas=number(vals[4]),surcharge=number(vals[5]),authorized_signer=decode(['address'],unhex(vals[6]))[0],atlas_surcharge=number(vals[7]),at=time.time())

def validate_notice(notice,height,now=None,max_age=1.5):
    op=notice['partial_user_operation'];now=time.time() if now is None else now
    check(number(op['chainId'])==8453 and op['to'].lower()==ATLAS and op['control'].lower()==CONTROL and op['dapp'].lower()==CONTROL,'wrong Atlas domain')
    check(number(op['deadline'])>=height and 0<=now-notice['received_at']<=max_age,'auction window expired')
    check(len(unhex(op['userOpHash']))==32 and number(op['userOpHash'])>0 and number(op['maxFeePerGas'])>0,'invalid auction fields')
    hints=op['hints'];forward=unhex(hints['forwardData'])
    check(forward[:4]==unhex(data('forward(address,bytes)')),'unsupported oracle forwarding entry')
    aggregator,report_call=decode(['address','bytes'],forward[4:])
    check(aggregator.lower()==hints['aggregator'].lower(),'auction feed binding mismatch')
    check(report_call[:4]==unhex(data('transmitSecondary(bytes32[3],bytes,bytes32[],bytes32[],bytes32)')),'unsupported oracle report')
    _,report,_,_,_=decode(['bytes32[3]','bytes','bytes32[]','bytes32[]','bytes32'],report_call[4:])
    stamp,_,observations,_=decode(['uint32','bytes32','int192[]','int192'],report)
    check(len(observations)>0 and list(observations)==sorted(observations),'invalid oracle observations')
    median=observations[len(observations)//2]
    check(median>0 and median==number(hints['medianPrice']),'report/median mismatch')
    check(not hints.get('rawReport') or unhex(hints['rawReport'])==report,'raw report binding mismatch')
    return op,aggregator.lower(),median,stamp


def report_override(feed,answer,stamp):
    """Temporary constant latestRoundData result. Never used in final simulation."""
    result=encode(['uint80','int256','uint256','uint256','uint80'],[1,answer,stamp,stamp,1])
    # Runtime copies the following 160 bytes to memory and returns them.
    prefix=bytearray.fromhex('60a0600060003960a06000f3');prefix[3]=len(prefix)
    code=bytes(prefix)+result
    return {feed:{'code':'0x'+code.hex()}}


def forwarders(rpc,feeds,block='latest'):
    """Discover authorized forwarding contracts from each feed's transmitter set."""
    from .morpho_universe import optional_views
    feeds=sorted(set(feeds));raw=optional_views(rpc,[(f,data('getTransmitters()')) for f in feeds],block)
    candidates={}
    for feed,value in zip(feeds,raw):
        if not value:continue
        try:candidates[feed]=decode(['address[]'],value)[0]
        except Exception:continue
    transmitters=sorted({t for rows in candidates.values() for t in rows})
    auth=optional_views(rpc,[(t,data('getAuthorizedSenders()')) for t in transmitters],block)
    allowed=set();senders={}
    for t,value in zip(transmitters,auth):
        if value:
            try:
                rows=decode(['address[]'],value)[0]
                if CONTROL in rows:allowed.add(t);senders[t]=[a for a in rows if a!=CONTROL]
            except Exception:pass
    checks=optional_views(rpc,[(CONTROL,data('verifyOracleWhitelist(address)',['address'],[t])) for t in sorted(allowed)],block)
    allowed={t for t,v in zip(sorted(allowed),checks) if v is not None}
    return {f:[{'address':t,'bundlers':senders[t]} for t in sorted(set(ts)&allowed) if senders[t]] for f,ts in candidates.items()}


def user_tuple(op,meta):
    feed=op['hints']['aggregator'].lower()
    choices=meta.get('forwarders',{}).get(feed,[])
    check(choices,'authorized oracle forwarding contract not prepared')
    source=op.get('from',ZERO)
    if source==ZERO:source=meta['authorized_signer']
    wrapped=unhex(data('update(address,bytes)',['address','bytes'],[choices[0]['address'],unhex(op['hints']['forwardData'])]))
    return (source,ATLAS,number(op.get('value',0)),number(op['gas']),number(op['maxFeePerGas']),number(op.get('nonce',op['userOpHash'])),
        number(op['deadline']),op['dapp'],CONTROL,meta['call_config'],meta['dapp_gas'],meta['solver_gas'],meta['surcharge'],
        ZERO,wrapped,b'')

def solver_tuple(op):
    return tuple(unhex(op[k]) if typ in ('bytes32','bytes') and isinstance(op[k],str) else number(op[k]) if typ=='uint256' else op[k]
                 for k,typ in SOLVER_FIELDS)+(unhex(op.get('signature','0x')),)

def solution(owner,contract,op,gas,bid,plan):
    return {'from':owner,'to':ATLAS,'value':0,'gas':gas,'maxFeePerGas':number(op['maxFeePerGas']),
        'deadline':number(op['deadline']),'solver':contract,'control':CONTROL,'userOpHash':op['userOpHash'],
        'bidToken':ZERO,'bidAmount':bid,'data':'0x'+encode([AP],[plan]).hex(),'signature':'0x'}

def max_charges(rpc,meta,op,solvers,block='pending'):
    fee=number(op['maxFeePerGas']);user=user_tuple(op,meta)
    calls=[('eth_call',[{'to':meta['simulator'],'gasPrice':hex(fee),
        'data':data(f'estimateMaxSolverWinGasCharge({U},{S})',[U,S],[user,solver_tuple(s)])},block]) for s in solvers]
    if len(calls)>1 and getattr(rpc,'multicall_verified',False):
        from .core import MULTICALL
        payload=[(meta['simulator'],True,unhex(args[0]['data'])) for _,args in calls]
        raw=rpc.request('eth_call',[{'to':MULTICALL,'gas':hex(8000000),'gasPrice':hex(fee),
            'data':data('aggregate3((address,bool,bytes)[])',['(address,bool,bytes)[]'],[payload])},block])
        rows=decode(['(bool,bytes)[]'],unhex(raw))[0]
        check(len(rows)==len(calls) and all(ok and len(value)==32 for ok,value in rows),'incomplete auction gas estimates')
        estimates=['0x'+value.hex() for _,value in rows]
    else:estimates=rpc.batch(calls) if len(calls)>1 else [rpc.request(*calls[0])]
    # Include dispatch/post-settlement reservation observed in real Atlas shortfall.
    margin=(150000*fee*(10000+meta['surcharge']+meta['atlas_surcharge'])+9999)//10000
    return [((number(v)+margin+fee-1)//fee)*fee for v in estimates]

def max_charge(rpc,meta,op,solver,block='pending'):
    return max_charges(rpc,meta,op,[solver],block)[0]

def measure_charges(rpc,meta,op,owner,executor,plans,per_plan,gas_limit,overrides,clock):
    """One discarded call: exact liquidation/swaps + protocol fee estimator.

    Only this unsigned quote installs temporary helper code at the owner. The
    final signed simulation uses the real owner and authentic report, separately.
    """
    helper=artifact('CLQuoter')['deployedBytecode']['object']
    temporary={**overrides,owner:{'code':helper if helper.startswith('0x') else '0x'+helper}}
    fee=number(op['maxFeePerGas'])
    call={'from':owner,'to':owner,'gas':hex(min(15_000_000,per_plan*len(plans)+300000*len(plans)+500000)),
          'gasPrice':hex(fee),'data':data(f'quoteAuctionDetails(address,address,{U},{AP}[],uint256,uint256)',
          ['address','address',U,AP+'[]','uint256','uint256'],
          [executor,meta['simulator'],user_tuple(op,meta),plans,per_plan,gas_limit])}
    rows,failures=decode(['uint256[4][]','bytes[]'],unhex(rpc.request('eth_call',[call,'pending',temporary,clock])))
    check(len(failures)==len(plans),'incomplete auction failure details')
    rpc.auction_quote_diagnostics=[{'plan_index':i,'revert_data':'0x'+raw[:256].hex(),
        'classification':'execution_reverted' if raw else 'empty_revert_or_plan_gas_limit'}
        for i,(row,raw) in enumerate(zip(rows,failures)) if row[1]==0]
    check(len(rows)==len(plans),'incomplete auction quote/fee batch')
    margin=(150000*fee*(10000+meta['surcharge']+meta['atlas_surcharge'])+9999)//10000
    # measureAuctions returns (0, 0) when a child execution reverts. Preserve
    # that missing measurement rather than presenting padded dispatch gas as
    # a completed zero-profit execution.
    return [(gross,gas if used>0 else 0,
             None if used==0 or charge==2**256-1 else ((charge+margin+fee-1)//fee)*fee)
            for gross,used,gas,charge in rows]


def full_simulation(rpc,meta,op,solver,block='pending',overrides=None,context=None):
    # Chainlink's control also authenticates the bundler against the selected
    # forwarder. A real solver signature is required even in Simulator mode.
    forwarding=meta['forwarders'][op['hints']['aggregator'].lower()][0]
    check(forwarding['bundlers'],'oracle bundler not prepared')
    check(len(unhex(solver.get('signature','0x')))==65,'full simulation requires signed solver operation')
    d=(ZERO,ATLAS,0,number(op['deadline']),CONTROL,forwarding['bundlers'][0],unhex(op['userOpHash']),bytes(32),b'')
    call={'from':solver['from'],'to':meta['simulator'],'gas':hex(15_000_000),'gasPrice':hex(number(op['maxFeePerGas'])),
        'data':data(f'simSolverCall({U},{S},{D})',[U,S,D],[user_tuple(op,meta),solver_tuple(solver),d])}
    args=[call,block]
    if overrides is not None or context is not None:args.append(overrides or {})
    if context is not None:args.append(context)
    success,result,reason=decode(['bool','uint8','uint256'],unhex(rpc.request('eth_call',args)))
    check(success and result==6,'Atlas full simulation failed: result=%s reason=%s'%(result,reason))
    return {'full_simulation_passed':True,'oracle_report_applied':True,'simulation_result':result}

def sign_solution(signer,meta,solver):
    message={k:solver[k] for k,_ in SOLVER_FIELDS}
    typed=encode_typed_data(domain_data={'name':'AtlasVerification','version':'1.6.4','chainId':8453,'verifyingContract':meta['verification']},
        message_types={'SolverOperation':[{'name':k,'type':t} for k,t in SOLVER_FIELDS]},message_data=message)
    signed=signer.sign_message(typed)
    return {**solver,'signature':'0x'+bytes(signed.signature).hex()}

def submit_solution(client,auction_id,solver):
    raw={k:hex(number(v)) if dict(SOLVER_FIELDS).get(k)=='uint256' else v for k,v in solver.items()}
    response=client.post(GATEWAY,json={'jsonrpc':'2.0','id':auction_id,'method':'solver_submitSolverOperation',
        'params':[{'auction_id':auction_id,'auction_solution':raw}]})
    response.raise_for_status();payload=response.json();check(not payload.get('error'),'Atlas submission rejected: '+str(payload.get('error'))[:160])
    return {'gateway_accepted':True}

def query_result(client,signer,auction_id,user_hash):
    owner=to_checksum_address(signer.address)
    signature=signer.sign_message(encode_defunct(text=f'{auction_id}:{user_hash}:{owner}')).signature
    response=client.post(QUERY_GATEWAY,json={'jsonrpc':'2.0','id':auction_id,'method':'solver_getSolverOperationResult',
        'params':[{'auctionId':auction_id,'userOperationHash':user_hash,'solverOperationFrom':owner,'signature':'0x'+bytes(signature).hex()}]})
    response.raise_for_status();payload=response.json();check(not payload.get('error'),'Atlas result unavailable')
    return payload.get('result')
