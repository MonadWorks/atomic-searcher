import json,time,threading
from types import SimpleNamespace
import pytest
from eth_abi import encode,decode
from searcher.core import load_config,data,Reject,USDC,WETH
from searcher.ledger import RiskLedger
from searcher.atlas_runtime import AuctionManager
from searcher import atlas_runtime as ar
from test_atlas import notice
from test_morpho_universe import make_market

@pytest.fixture
def manager(tmp_path,monkeypatch):
    cfg=load_config();m=make_market();n=notice();n['received_at']=time.time();n['markets']=[m.id]
    cfg['policy']['min_net_profit_fee_bps']=0
    minimum=10**12;gross=10**14;fee=10**11
    class RPC:
        def request(self,method,args):
            assert method=='eth_call'
            if args[0]['data'].startswith(data('balanceOfBonded(address)')[:10]):return hex(10**15)
            assert args[0]['data'].startswith(data(f'measureAuctions({ar.atlas.AP}[],uint256)')[:10])
            plans,_=decode([ar.atlas.AP+'[]','uint256'],bytes.fromhex(args[0]['data'][10:]));self.plan_count=len(plans)
            return '0x'+encode(['uint256[2][]'],[[(gross+i*100,100000) for i in range(len(plans))]]).hex()
    rpc=RPC();ledger=RiskLedger(tmp_path/'ledger.sqlite',10**16,3)
    head={'number':'0x63','timestamp':'0x100','hash':'0x'+'11'*32}
    context={'requested_ns':time.monotonic_ns(),'head':head,'pending_head':None,'weth_profit_floor':minimum,'weth_profit_usdc_raw':100000}
    e=SimpleNamespace(rpc=rpc,db=ledger.db,ledger=ledger,config=cfg,context=context,owner=cfg['owner'],signer=object(),structural_contract='0x'+'44'*20)
    e.bond_snapshot={'requested_ns':time.monotonic_ns(),'block':99,'block_hash':head['hash'],'balance':10**15}
    book=SimpleNamespace(batch=lambda n:['0x'+'11'*20]);paths=[([('0x'+'22'*20,m.collateral,USDC,1)],[])]
    u=SimpleNamespace(atlas_meta={'at':time.time(),'solver_gas':6000000},atlas_forwarders={},markets={m.id:m},books={m.id:book},
        lock=threading.Lock(),routes=lambda *a,**k:paths,graph=SimpleNamespace(paths=lambda *a,**k:[[('0x'+'33'*20,USDC,WETH,1)]]))
    monkeypatch.setattr(ar,'inspect_markets',lambda *a,**k:{m.id:(10**36,258,[{'borrower':'0x'+'11'*20,'debt':10000,'shares':10000,'collateral':12000,'boundary':2*10**36}])})
    monkeypatch.setattr(ar,'liquidation_batches',lambda *a,**k:[[('0x'+'11'*20,10000,11000)]])
    def quotes(rpc,meta,op,owner,executor,plans,per_plan,gas_limit,overrides,clock):
        raw=rpc.request('eth_call',[{'data':data(f'measureAuctions({ar.atlas.AP}[],uint256)',[ar.atlas.AP+'[]','uint256'],[plans,per_plan])}])
        return [(gross,(used+150000)*115//100,fee) for gross,used in decode(['uint256[2][]'],bytes.fromhex(raw[2:]))[0]]
    monkeypatch.setattr(ar.atlas,'measure_charges',quotes)
    monkeypatch.setattr(ar.atlas,'sign_solution',lambda signer,meta,solver:{**solver,'signature':'0x'+'00'*65})
    monkeypatch.setattr(ar.atlas,'full_simulation',lambda *a,**k:{'full_simulation_passed':True})
    sent=[]
    def send(client,id,solver):sent.append(solver);return {'gateway_accepted':True}
    monkeypatch.setattr(ar.atlas,'submit_solution',send)
    a=AuctionManager(e,u,tmp_path)
    yield a,n,sent,gross,fee
    a.close();ledger.close()

def test_best_batch_quote_selected_then_persisted_before_gateway(manager):
    a,n,sent,gross,fee=manager
    out=a.process(n)
    assert out['status']=='auction_submitted' and len(sent)==1
    assert sent[0]['bidAmount']==gross+(a.e.rpc.plan_count-1)*100-fee-max(10**12,2*10**12)
    assert a.e.db.execute('SELECT state,kind FROM attempts').fetchone()==('submitted','auction')
    payload=a.e.db.execute('SELECT payload FROM auctions').fetchone()[0]
    assert 'signature' not in payload and json.loads(payload)['operations'][0]['full_simulation_passed']

def test_signed_full_simulation_failure_keeps_disclosed_authorization_reserved(manager,monkeypatch):
    a,n,sent,_,_=manager
    def fail(*a,**k):raise Reject('full simulation failed')
    monkeypatch.setattr(ar.atlas,'full_simulation',fail)
    with pytest.raises(Reject):a.process(n)
    assert not sent and a.e.db.execute('SELECT state,actual FROM attempts').fetchone()==('submitted',None)

def test_ambiguous_gateway_submission_remains_reserved(manager,monkeypatch):
    a,n,_,_,_=manager
    def timeout(*a,**k):raise TimeoutError('no response')
    monkeypatch.setattr(ar.atlas,'submit_solution',timeout)
    assert a.process(n)['submission_unresolved']
    assert a.e.db.execute('SELECT state FROM attempts').fetchone()[0]=='submitted'

def test_crashed_reservation_before_payload_is_recoverable(tmp_path):
    l=RiskLedger(tmp_path/'ledger.sqlite',100,3);l.reserve('auction:crash',10,kind='auction')
    a=AuctionManager(SimpleNamespace(db=l.db),None,tmp_path)
    try:assert l.db.execute('SELECT state,actual FROM attempts').fetchone()==('cancelled','0')
    finally:a.close();l.close()

def test_stale_bond_context_never_creates_a_bid(manager):
    a,n,sent,_,_=manager;a.e.bond_snapshot['requested_ns']=time.monotonic_ns()-4_000_000_000
    with pytest.raises(Reject,match='bond context'):a.process(n)
    assert not sent and not a.e.db.execute('SELECT 1 FROM attempts').fetchone()

def test_second_signed_simulation_failure_keeps_both_authorizations_reserved(manager,monkeypatch):
    a,n,sent,_,fee=manager;a.process(n)
    n['auction_id']='parallel';n['partial_user_operation']['userOpHash']='0x'+'55'*32
    def fail(*a,**k):raise Reject('parallel simulation failed')
    monkeypatch.setattr(ar.atlas,'full_simulation',fail)
    with pytest.raises(Reject):a.process(n)
    assert len(sent)==1 and a.e.db.execute('SELECT state,reserved FROM attempts').fetchone()==('submitted',str(2*fee))
    assert len(json.loads(a.e.db.execute('SELECT payload FROM auctions').fetchone()[0])['operations'])==2

def test_prepared_positions_use_one_price_screen_and_still_require_full_execution(manager,monkeypatch):
    a,n,sent,_,_=manager;m=next(iter(a.u.markets.values()))
    a.u.books[m.id].proposals=lambda n:[{'borrower':'0x'+'11'*20,'debt':10000,'shares':10000,'collateral':12000,'boundary':2*10**36}]
    calls=[];original=ar.inspect_markets
    def screen(*args,**kwargs):
        calls.append(args[1]);return original(*args,**kwargs)
    monkeypatch.setattr(ar,'inspect_markets',screen)
    assert a.process(n)['status']=='auction_submitted'
    assert calls==[[(m.id,['0x'+'11'*20])]]
    assert sent and json.loads(a.e.db.execute('SELECT payload FROM auctions').fetchone()[0])['operations'][0]['full_simulation_passed']

def test_optimistic_prepared_hint_with_zero_whole_call_profit_never_signs(manager,monkeypatch):
    a,n,sent,_,_=manager;m=next(iter(a.u.markets.values()))
    a.u.books[m.id].proposals=lambda n:[{'borrower':'0x'+'11'*20,'debt':10000,'shares':10000,'collateral':12000,'boundary':2*10**36}]
    def zero(method,args):
        plans,_=decode([ar.atlas.AP+'[]','uint256'],bytes.fromhex(args[0]['data'][10:]))
        return '0x'+encode(['uint256[2][]'],[[(0,0)]*len(plans)]).hex()
    a.e.rpc.request=zero
    def forbidden(*a,**k):raise AssertionError('zero-profit hint was signed')
    monkeypatch.setattr(ar.atlas,'sign_solution',forbidden)
    result=a.process(n)
    assert result['status']=='no_executable_auction' and not sent
    assert result['positions_proposed']==1 and result['positive_quoted_gross_wei']==0

def test_accounting_worker_start_does_not_cancel_execution_reservation(manager):
    a,_,_,_,_=manager;a.e.ledger.reserve('still-signing',1,kind='auction')
    other=AuctionManager(a.e,None,a.output,recover=False)
    try:assert a.e.db.execute("SELECT state FROM attempts WHERE id='still-signing'").fetchone()==('reserved',)
    finally:other.close()


def test_parallel_notifications_reserve_both_fees_and_merge_gateway_results(manager,monkeypatch,tmp_path):
    import copy
    from searcher.auction_selection import AuctionScreens
    a,n,sent,_,fee=manager
    a.u.auction_screens=AuctionScreens();screens=[];quotes=[];original_screen=ar.inspect_markets;original_quote=ar.atlas.measure_charges
    def screen(*args,**kwargs):screens.append(1);return original_screen(*args,**kwargs)
    def quote(*args,**kwargs):quotes.append(args[2]['userOpHash']);return original_quote(*args,**kwargs)
    monkeypatch.setattr(ar,'inspect_markets',screen);monkeypatch.setattr(ar.atlas,'measure_charges',quote)
    gate=threading.Barrier(2);lock=threading.RLock();errors=[];results=[]
    def full(*args,**kwargs):gate.wait(timeout=2);return {'full_simulation_passed':True}
    monkeypatch.setattr(ar.atlas,'full_simulation',full)
    path=a.e.db.execute('PRAGMA database_list').fetchone()[2]
    def run(index):
        ledger=RiskLedger(path,10**16,3);other=None
        try:
            account=SimpleNamespace(**vars(a.e));account.db=ledger.db;account.ledger=ledger;account.reservation_lock=lock
            other=AuctionManager(account,a.u,tmp_path,recover=False)
            notice=copy.deepcopy(n);notice['auction_id']=f'parallel-{index}'
            notice['partial_user_operation']['userOpHash']='0x'+f'{index+1:02x}'*32
            results.append(other.process(notice))
        except Exception as exc:errors.append(str(exc))
        finally:
            if other:other.close()
            ledger.close()
    threads=[threading.Thread(target=run,args=(i,)) for i in range(2)]
    for t in threads:t.start()
    for t in threads:t.join(4)
    assert not errors and len(results)==2
    assert a.e.db.execute('SELECT reserved,state FROM attempts').fetchall()==[(str(2*fee),'submitted')]
    operations=json.loads(a.e.db.execute('SELECT payload FROM auctions').fetchone()[0])['operations']
    assert len(operations)==2 and all(x['gateway']=='accepted' and x['full_simulation_passed'] for x in operations)
    assert len(sent)==2
    assert len(screens)==1 and len(quotes)==2 and len(set(quotes))==2


def test_large_healthy_markets_do_not_displace_smaller_eligible_market(manager,monkeypatch):
    from dataclasses import replace
    a,n,sent,_,_=manager;base=next(iter(a.u.markets.values()))
    markets=[replace(base,id='0x'+f'{i+20:02x}'*32,borrow_usd=10**9//(i+1)) for i in range(6)]
    row={'borrower':'0x'+'11'*20,'debt':10000,'shares':10000,'collateral':12000,'boundary':2*10**36}
    a.u.markets={m.id:m for m in markets};a.u.books={m.id:SimpleNamespace(proposals=lambda _: [row]) for m in markets}
    n['markets']=list(a.u.markets)
    # One broken market is isolated; four large healthy markets are screened.
    monkeypatch.setattr(ar,'inspect_markets',lambda *args,**kw:{m.id:(10**36 if i==5 else 3*10**36,258,[row]) for i,m in enumerate(markets) if i!=0})
    out=a.process(n,submit=False)
    assert out['status']=='quoted_only' and out['market']==markets[-1].id
    assert not sent


def test_same_report_covers_48_markets_and_finds_last_profitable_market(manager,monkeypatch):
    from dataclasses import replace
    a,n,sent,_,_=manager;base=next(iter(a.u.markets.values()))
    markets=[replace(base,id='0x'+f'{i+20:02x}'*32,borrow_usd=10**9//(i+1)) for i in range(48)]
    row={'borrower':'0x'+'11'*20,'debt':10000,'shares':10000,'collateral':12000,'boundary':2*10**36}
    a.u.markets={m.id:m for m in markets};a.u.books={m.id:SimpleNamespace(proposals=lambda _: [row]) for m in markets}
    n['markets']=list(a.u.markets);calls=[]
    def inspect(rpc,groups,*args):
        calls.append(groups)
        return {id:(10**36 if id==markets[-1].id else 3*10**36,258,[row]) for id,_ in groups}
    monkeypatch.setattr(ar,'inspect_markets',inspect)
    out=a.process(n,submit=False)
    assert len(calls)==1 and len(calls[0])==48
    assert out['status']=='quoted_only' and out['market']==markets[-1].id
    assert out['market_coverage']=={'mapped_markets':48,'requested_markets':48,'read_markets':48,'unread_markets':[],'deferred_markets':0}
    assert not sent and a.audit['trials']


def test_unrepayable_dust_gets_explicit_reason_without_rpc_quote_or_signature(manager,monkeypatch):
    a,n,sent,_,_=manager;m=next(iter(a.u.markets.values()))
    # Actual WETH/cbBTC residual observed on 2026-10-18: collateral is worth
    # one satoshi before the liquidation incentive and rounds to zero after it.
    row={'borrower':'0x'+'11'*20,'debt':2,'shares':997714,'collateral':465121084796,'boundary':6449933357280043335135256}
    monkeypatch.setattr(ar,'inspect_markets',lambda *args:{m.id:(2958007667792597978992389,258,[row])})
    def forbidden(*args):raise AssertionError('unrepayable residual entered quote')
    monkeypatch.setattr(ar.atlas,'measure_charges',forbidden)
    out=a.process(n)
    assert out['packages_compared']==0 and out['reasons']==['no_repayable_shares_within_budget']
    assert not sent and not a.e.db.execute('SELECT 1 FROM attempts').fetchone()


def test_failed_measurement_requests_fresh_borrower_state_outside_hot_path(manager,monkeypatch):
    a,n,sent,_,_=manager;m=next(iter(a.u.markets.values()));refreshed=[];triggered=[]
    a.u.books[m.id].add=lambda addresses:refreshed.extend(addresses)
    a.u.trigger=lambda id,priority:triggered.append((id,priority))
    monkeypatch.setattr(ar.atlas,'measure_charges',lambda rpc,meta,op,owner,executor,plans,*args:[(0,0,None)]*len(plans))
    assert a.process(n)['status']=='no_executable_auction'
    assert refreshed==['0x'+'11'*20] and triggered==[(m.id,True)]
    assert not sent


def test_gateway_warming_does_not_submit_or_hold_execution_lock():
    from searcher.atlas_runtime import AuctionWorker
    worker=AuctionWorker.__new__(AuctionWorker);worker.log_lock=threading.Lock();worker.stop=threading.Event();calls=[]
    class Client:
        def __init__(self,fail):self.fail=fail
        def head(self,url,timeout):
            assert worker.log_lock.acquire(blocking=False)
            worker.log_lock.release();calls.append(self.fail)
            if len(calls)==2:worker.stop.set()
            if self.fail:raise TimeoutError('cold connection')
        def post(self,*args,**kwargs):raise AssertionError('keepalive submitted a payload')
    worker.gateway_clients={Client(True),Client(False)}
    worker.warm_gateway()
    assert sorted(calls)==[False,True]


def test_live_manager_quotes_valuable_position_outside_first_eight_risk_hints(manager,monkeypatch):
    from searcher.structural_runner import BorrowerBook
    from test_auction_readiness import row
    from searcher.structural import GENERIC
    a,n,sent,_,_=manager;m=next(iter(a.u.markets.values()))
    book=BorrowerBook();dust=[row(i,90,100) for i in range(1,80)];large=row(100,880000,1000000)
    book.update([*dust,large]);a.u.books[m.id]=book
    reads=[]
    def inspect(rpc,groups,*args):
        reads.append(groups);return {m.id:(10**36,258,[large])}
    monkeypatch.setattr(ar,'inspect_markets',inspect)
    def measured(rpc,meta,op,owner,executor,plans,*args):
        return [(10**14 if any(addr==large['borrower'] for addr,_,_ in decode(GENERIC,p[1])[1]) else 0,
                 500000,10**11) for p in plans]
    monkeypatch.setattr(ar.atlas,'measure_charges',measured)
    result=a.process(n,submit=False)
    assert result['status']=='quoted_only'
    assert reads==[[(m.id,[large['borrower']])]]
    assert result['position_selection'][m.id]['known_positions']==80
    plan=decode([ar.atlas.AP],bytes.fromhex(result['unsigned_solver']['data'][2:]))[0]
    requests=decode(GENERIC,plan[1])[1]
    assert any(address==large['borrower'] for address,_,_ in requests)
    assert not sent and a.e.db.execute('SELECT 1 FROM attempts').fetchone() is None
