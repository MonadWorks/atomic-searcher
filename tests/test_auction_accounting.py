import json,time
from types import SimpleNamespace
import pytest
from eth_abi import encode
from searcher.atlas_runtime import AuctionManager,EXECUTED,BOND,UNBOND
from searcher.ledger import RiskLedger
from searcher.core import Reject

OWNER='0x'+'12'*20;CONTRACT='0x'+'23'*20;HASH='0x'+'34'*32;TX='0x'+'45'*32
class Chain:
    def __init__(self):self.height=104;self.branch='a';self.bond=10010;self.executed=True;self.unavailable=False
    def header(self,n=None):
        if self.unavailable:raise RuntimeError('provider offline')
        n=self.height if n is None else n
        return {'number':hex(n),'hash':'0x'+format(n,'062x')+('aa' if n<104 or self.branch=='a' else 'bb')}
    def request(self,m,args):
        if m=='eth_call':return hex(self.bond)
        if m=='eth_getBlockByNumber':return self.header(99)
        if m=='eth_getTransactionReceipt':return {'status':'0x1','blockHash':self.header(103)['hash']}
        assert m=='eth_getLogs'
        q=args[0]
        if q['address']==CONTRACT:
            return [{'topics':[EXECUTED,HASH],'data':'0x'+encode(['uint256']*4,[200,100,100,50]).hex(),
                'transactionHash':TX,'blockNumber':hex(103),'blockHash':self.header(103)['hash']}] if self.executed else []
        return [{'topics':[BOND],'data':'0x'+encode(['uint256'],[50]).hex()},
                {'topics':[UNBOND],'data':'0x'+encode(['uint256','uint256'],[10,200]).hex()}]

def manager(tmp_path):
    ledger=RiskLedger(tmp_path/'ledger.sqlite',1000,3);rpc=Chain()
    e=SimpleNamespace(db=ledger.db,ledger=ledger,rpc=rpc,owner=OWNER)
    a=AuctionManager(e,None,tmp_path);ledger.reserve('auction:one',50,kind='auction');ledger.submitted('auction:one',None)
    payload={'start_block':100,'start_hash':rpc.header(100)['hash'],'deadline':102,'bond_before':'10000','owner':OWNER,'contract':CONTRACT,'operations':[{'user_hash':HASH,'auction_id':'1'}]}
    with e.db:e.db.execute('INSERT INTO auctions(attempt,payload) VALUES(?,?)',('auction:one',json.dumps(payload)))
    return a,ledger,rpc

def test_bid_is_removed_before_fee_and_deposits_are_not_income(tmp_path):
    a,l,r=manager(tmp_path)
    try:
        a.reconcile();out=json.loads(l.db.execute('SELECT outcome FROM auctions').fetchone()[0])
        assert out['actual_fee_wei']==30 and out['payout_wei']==100 and out['net_profit_wei']==70
        assert l.db.execute('SELECT state,actual FROM attempts').fetchone()==('success','30')
    finally:a.close();l.close()

def test_provider_failure_retains_liability(tmp_path):
    a,l,r=manager(tmp_path);r.unavailable=True
    try:
        with pytest.raises(RuntimeError):a.reconcile()
        assert l.db.execute('SELECT state,reserved FROM attempts').fetchone()==('submitted','50')
    finally:a.close();l.close()

def test_reorg_reopens_settlement_and_expiry_can_reopen_again(tmp_path):
    a,l,r=manager(tmp_path)
    try:
        a.reconcile();r.branch='b';a.last_reconcile=0;a.reconcile()
        assert l.db.execute('SELECT state,actual FROM attempts').fetchone()==('submitted',None)
        r.bond=10040;r.executed=False;a.last_reconcile=0;a.reconcile()
        assert l.db.execute('SELECT state,actual FROM attempts').fetchone()==('cancelled','0')
        l.reopen('auction:one')
        assert l.db.execute('SELECT state FROM attempts').fetchone()[0]=='submitted'
    finally:a.close();l.close()

def test_idle_account_does_not_poll_extra_headers(tmp_path):
    l=RiskLedger(tmp_path/'ledger.sqlite',100,3)
    e=SimpleNamespace(db=l.db,ledger=l,rpc=None,owner=OWNER);a=AuctionManager(e,None,tmp_path)
    try:a.reconcile()
    finally:a.close();l.close()

def test_status_uses_net_receipt_and_never_counts_bond_as_profit(tmp_path):
    import importlib.util
    from pathlib import Path
    path=Path(__file__).resolve().parents[1]/'tools/live-view/view.py'
    spec=importlib.util.spec_from_file_location('auction_status_view',path);view=importlib.util.module_from_spec(spec);spec.loader.exec_module(view)
    a,l,r=manager(tmp_path)
    try:
        l.db.execute('CREATE TABLE intents(attempt TEXT,tx_hash TEXT,payload TEXT,outcome TEXT,finalized INTEGER)');l.db.commit()
        a.reconcile();state=view.read_ledger(tmp_path/'ledger.sqlite',CONTRACT)
        trade=state['trades'][0]
        assert trade['label']=='竞标成交' and trade['symbol']=='ETH'
        assert float(trade['gross'])==100/10**18 and float(trade['fee_eth'])==30/10**18
        assert float(trade['net_eth'])==70/10**18
    finally:a.close();l.close()

def test_receipt_logs_use_the_provider_that_supports_them(tmp_path):
    a,l,r=manager(tmp_path)
    class NoLogs:
        def header(self,*a):raise AssertionError('execution node used for receipt logs')
        def request(self,*a):raise AssertionError('execution node used for receipt logs')
    a.e.reconciliation_rpc=r;a.e.rpc=NoLogs()
    try:
        a.reconcile();assert l.db.execute('SELECT state FROM attempts').fetchone()[0]=='success'
    finally:a.close();l.close()


def test_liability_extended_during_receipt_read_is_not_settled_from_old_snapshot(tmp_path):
    a,l,r=manager(tmp_path);request=r.request;changed=False
    def race(method,args):
        nonlocal changed
        if method=='eth_getLogs' and not changed:
            changed=True;l.extend('auction:one',100)
            payload=json.loads(l.db.execute('SELECT payload FROM auctions').fetchone()[0])
            payload['deadline']=110;payload['operations'].append({'user_hash':HASH,'auction_id':'2'})
            with l.db:l.db.execute('UPDATE auctions SET payload=?',(json.dumps(payload),))
        return request(method,args)
    r.request=race
    try:
        a.reconcile()
        assert l.db.execute('SELECT state,reserved,actual FROM attempts').fetchone()==('submitted','100',None)
        assert l.db.execute('SELECT outcome FROM auctions').fetchone()[0] is None
    finally:a.close();l.close()
