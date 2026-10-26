import copy
import httpx
import pytest
from eth_abi import decode
from eth_account import Account
from eth_account.messages import encode_typed_data
from searcher.core import Reject
from searcher.equities import XStocks,inventory,prepare_swap,inspect_wrapper,net_cycle,MESSAGE,PERMIT

A='0x'+'11'*20;B='0x'+'22'*20;C='0x'+'33'*20;D='0x'+'44'*20;E='0x'+'55'*20

def test_catalog_paginates_without_sending_account_key_to_public_endpoints():
    calls=[]
    def handler(request):
        calls.append(request);assert 'x-api-key' not in request.headers
        page=int(request.url.params['page'])
        return httpx.Response(200,json={'nodes':[{'id':str(page)}],
            'page':{'currentPage':page,'hasNextPage':page==0}})
    with httpx.Client(transport=httpx.MockTransport(handler)) as session:
        api=XStocks(session,api_key='test-key');assert api.assets('Ink')==[{'id':'0'},{'id':'1'}]
        assert len(calls)==2

def test_catalog_does_not_silently_accept_repeating_or_unfinished_pages():
    def handler(request):return httpx.Response(200,json={'nodes':[{'id':'same'}],
        'page':{'currentPage':int(request.url.params['page']),'hasNextPage':True}})
    with httpx.Client(transport=httpx.MockTransport(handler)) as session:
        with pytest.raises(Reject,match='duplicate'):XStocks(session).assets()

def test_inventory_uses_current_wrapper_and_exact_chain_not_ticker():
    a=dict(id='asset',symbol='TSLAx',isTradingHalted=False,trading={'openNow':True,'isTradingHalted':False},
        deployments=[dict(network='Ink',address=A,wrapperAddress=B,wrapperAddressV2=C,
            supportsAtomicSwaps=True,stablecoins=[dict(address=D,supportsAtomicSwaps=True,issuance=True)]),
            dict(network='Base',address=E,stablecoins=[])])
    row=inventory([a],'Ink')[0]
    assert row['wrapper']==C and row['token']==A and row['chain_id']==57073
    assert row['issuer_open'] and not row['issuer_account_verified'] and not row['execution_connected']
    assert inventory([a],'Ethereum')==[]

def firm():
    signer=Account.create();types={
      'EIP712Domain':[{'name':k,'type':t} for k,t in [('name','string'),('version','string'),('chainId','uint256'),('verifyingContract','address')]],
      'SwapMessage':[{'name':k,'type':t} for k,t in [('quoteId','bytes32'),('expiration','uint256'),('incomingTransfer','Transfer'),('outgoingTransfer','Transfer')]],
      'Transfer':[{'name':k,'type':t} for k,t in [('from','address'),('to','address'),('token','address'),('amount','uint256')]]}
    message={'quoteId':'0x'+'ab'*32,'expiration':1100,
        'incomingTransfer':{'from':A,'to':B,'token':C,'amount':'1000000'},
        'outgoingTransfer':{'from':B,'to':A,'token':D,'amount':'2000000'}}
    payload={'types':types,'primaryType':'SwapMessage','domain':{'name':'fixture','version':'1','chainId':57073,'verifyingContract':E},'message':message}
    signature=signer.sign_message(encode_typed_data(full_message=payload)).signature
    q={'generalStatus':'Accepted','blockchainStatus':'PendingExecution','contract':{'network':'Ink','address':E},
       'signaturePayload':payload,'signature':'0x'+signature.hex()}
    args=dict(network='Ink',receiver=A,contract=E,signer=signer.address,input_token=C,output_token=D,
              max_input=1_000_000,min_output=2_000_000,now=1000)
    return q,args

def test_firm_rfq_calldata_preserves_exact_transfers_and_empty_permit():
    q,args=firm();swap=prepare_swap(q,**args)
    message,signature,permit=decode([MESSAGE,'bytes',PERMIT],bytes.fromhex(swap.calldata[10:]))
    assert message[2]==(A,B,C,1_000_000) and message[3]==(B,A,D,2_000_000)
    assert permit[1:3]==(0,0) and len(signature)==65

@pytest.mark.parametrize('change',["receiver","token","domain","expired","unsigned","tampered","used","overspend","underpay","wrong_signer","soft"])
def test_rfq_rejects_non_executable_or_changed_authorization(change):
    q,args=firm();m=q['signaturePayload']['message']
    if change=='receiver':m['outgoingTransfer']['to']=B
    elif change=='token':m['outgoingTransfer']['token']=C
    elif change=='domain':q['signaturePayload']['domain']['chainId']=8453
    elif change=='expired':args['now']=1095
    elif change=='unsigned':q['signature']=None
    elif change=='tampered':m['outgoingTransfer']['amount']='3000000'
    elif change=='used':args['used_ids']=[m['quoteId']]
    elif change=='overspend':args['max_input']=999999
    elif change=='underpay':args['min_output']=2000001
    elif change=='wrong_signer':args['signer']=B
    elif change=='soft':q['blockchainStatus']='NotReady'
    with pytest.raises((Reject,ValueError)):prepare_swap(q,**args)

def test_api_hard_rfq_binds_both_addresses_and_never_retries_mutating_request():
    calls=[]
    def handler(request):
        import json
        body=json.loads(request.content);calls.append(body)
        assert body['paymentWalletIdentifier']==A==body['receivingWalletIdentifier']
        assert request.headers['x-api-key']=='test'
        return httpx.Response(429,json={'message':'sensitive issuer account context'})
    with httpx.Client(transport=httpx.MockTransport(handler)) as session:
        with pytest.raises(Reject,match='issuer_http_429') as exc:
            XStocks(session,'test').rfq('TSLAx','Sell','Ink','USDC','1.2',receiver=A,soft=False)
        assert len(calls)==1 and 'sensitive' not in str(exc.value)

def test_missing_account_fails_before_network():
    with httpx.Client(transport=httpx.MockTransport(lambda _:pytest.fail('unexpected request'))) as session:
        with pytest.raises(Reject,match='issuer_account_required'):XStocks(session).rfq('TSLAx','Buy','Ink','USDC','1')

def test_wrapper_reads_use_same_block_and_preserve_real_share_conversion(monkeypatch):
    import searcher.equities as module
    seen=[]
    def view(rpc,target,signature,types=(),args=(),returns=(),block=None):
        seen.append(block)
        return {'asset()':A,'decimals()':18,'previewRedeem(uint256)':12*10**17,'previewDeposit(uint256)':10**18-1}[signature]
    monkeypatch.setattr(module,'view',view)
    state=inspect_wrapper(None,{'wrapper':B,'token':A},'0x123')
    assert set(seen)=={'0x123'} and state['redeem_assets']!=state['shares']
    assert state['redeposit_shares']==10**18-1

def test_profit_accounting_uses_after_repayment_balance_and_all_external_costs():
    result=net_cycle(borrowed=10**9,repaid=1000400000,baseline=9000000,final_balance=9200000,
                     network_cost=80000,bid_cost=30000,min_net=100000)
    assert result['net']==90000 and not result['qualifies']

def test_index_batches_all_registry_addresses_and_rejects_other_chains():
    import time
    from searcher.equities import indexed_markets
    items=[{'token':'0x'+format(i+1,'040x'),'wrapper':None} for i in range(35)]
    calls=[]
    def handler(request):
        batch=request.url.path.split('/')[-1].split(',');calls.append(batch)
        assert len(batch)<=30 and 'x-api-key' not in request.headers
        rows=[dict(chainId='base',pairAddress='bad',baseToken={'address':batch[0]},quoteToken={'address':E}),
              dict(chainId='ink',pairAddress=B,baseToken={'address':items[0]['token']},quoteToken={'address':items[-1]['token']})]
        return httpx.Response(200,json=rows)
    with httpx.Client(transport=httpx.MockTransport(handler)) as session:
        pools=indexed_markets(XStocks(session),items,'Ink',time.monotonic()+10)
    assert len(calls)==2 and all(item['index_checked'] for item in items)
    assert len(pools[items[0]['token']])==1 and len(pools[items[-1]['token']])==1
    assert all(pool['pairAddress']!='bad' for rows in pools.values() for pool in rows)
