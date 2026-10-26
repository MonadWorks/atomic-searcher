import gzip
import importlib.util
import json
from pathlib import Path
import sqlite3
import pytest

spec=importlib.util.spec_from_file_location('live_view',Path(__file__).with_name('view.py'))
view=importlib.util.module_from_spec(spec);spec.loader.exec_module(view)

def test_actual_money_excludes_estimates_and_exposes_no_signed_payload(tmp_path):
    path=tmp_path/'ledger.sqlite'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE attempts(id,state,tx_hash,actual,finished_at)')
        db.execute('CREATE TABLE intents(attempt,payload,outcome,finalized)')
        cases=[('success',view.USDC,3000000,10**12,False),
               ('failed',view.USDC,0,2*10**12,True),
               ('success',view.WETH,10**15,3*10**12,False),
               ('submitted',view.USDC,999999999999999,None,False)]
        for index,(state,token,gross,fee,old) in enumerate(cases):
            tx='0x'+f'{index+1:064x}'
            db.execute('INSERT INTO attempts VALUES(?,?,?,?,?)',(index,state,tx,str(fee) if fee else None,100+index))
            payload={'prepared_at':90,'settlement':token,'contract':'old' if old else 'current',
                     'simulated_gross_profit':999999999999999,'fee_in_settlement':100000000000,
                     'raw_transaction':'SECRET_SIGNED_PAYLOAD'}
            outcome={'gross_profit_raw':gross,'included_tx_hash':tx,'net_after_reserved_fee_raw':88888888888} if state!='submitted' else None
            db.execute('INSERT INTO intents VALUES(?,?,?,?)',(index,json.dumps(payload),json.dumps(outcome) if outcome else None,1))
    data=view.read_ledger(path,'current')
    assert data['gross_usdc']=='3' and data['gross_weth']=='0.001'
    assert data['fees_eth']=='0.000006'
    assert data['trades'][0]['fee_eth'] is None and data['trades'][0]['gross'] is None
    assert data['trades'][1]['net_eth']=='0.000997'
    assert data['trades'][2]['earlier_executor'] and data['trades'][2]['label']=='交易回滚'
    assert 'SECRET_SIGNED_PAYLOAD' not in json.dumps(data)
    assert '88888888888' not in json.dumps(data)

def test_precise_amount_and_verified_explorer_links():
    assert view.amount(1)=='0.000000000000000001'
    assert view.amount(-1)=='-0.000000000000000001'
    large=2**255-1
    output=view.amount(large)
    assert int(output.replace('.',''))==large
    assert view.transaction_url('javascript:alert(1)') is None
    assert view.transaction_url('0x'+'a'*64).startswith('https://basescan.org/tx/')

def test_view_reads_opportunities_but_does_not_use_them_as_income(tmp_path,monkeypatch):
    config=tmp_path/'config';config.mkdir()
    (config/'base.json').write_text(json.dumps({'owner':'0x'+'1'*40,'chain_id':8453}))
    session=tmp_path/'evidence/session-test';folder=session/'pipeline';folder.mkdir(parents=True)
    (tmp_path/'evidence/runtime.json').write_text(json.dumps({'session':str(session)}))
    now=view.time.time()
    (folder/'health.json').write_text(json.dumps({'state':'running','executor_connected':True}))
    (folder/'decision-1.json').write_text(json.dumps({'observed_at':now,'candidates':[]}))
    (folder/'activity-stream-health.json').write_text(json.dumps({'state':'budget_exhausted','retry_at':now+100}))
    candidate={'tokens':[view.USDC,view.WETH,view.USDC],'net_after_modelled_cost':1000000,'status':'execution_rejected'}
    (folder/'opportunity-1.json.gz').write_bytes(gzip.compress(json.dumps({'received_utc':now,'decision':{'candidates':[candidate]}}).encode()))
    monkeypatch.setattr(view,'active_lock',lambda _:True)
    result=view.Reader(tmp_path).snapshot()
    assert result['state']=='running' and result['executor_connected']
    assert result['opportunities'][0]['estimate'] is None
    assert 'HTTP' in result['stream']
    assert '本机' in result['stream'] and '不是服务商' in result['stream_note']
    assert result['ledger']['available'] is False
    assert 'gross_usdc' not in result['ledger']
    monkeypatch.setattr(view,'active_lock',lambda _:False)
    stopped=view.Reader(tmp_path).snapshot()
    assert stopped['state']=='stopped' and not stopped['executor_connected']
    assert '已停止' in stopped['stream']

def test_brief_queue_wait_is_not_shown_as_lost_connection(tmp_path,monkeypatch):
    folder=tmp_path/'evidence/session-test/pipeline';folder.mkdir(parents=True)
    (tmp_path/'evidence/runtime.json').write_text(json.dumps({'session':str(folder.parent)}))
    (folder/'decision-1.json').write_text(json.dumps({'observed_at':view.time.time()}))
    (folder/'health.json').write_text(json.dumps({'state':'degraded','feed':{'state':'connected'}}))
    (folder/'activity-stream-health.json').write_text(json.dumps({'state':'connected','http_only_pools':2,
       'budget_kind':'local_request_units','local_units':10,'local_daily_limit':80000}))
    monkeypatch.setattr(view,'active_lock',lambda _:True)
    result=view.Reader(tmp_path).snapshot()
    assert result['state']=='running' and '分流' in result['stream']
    assert '10 / 80000' in result['stream_note']
    (folder/'decision-1.json').write_text(json.dumps({'observed_at':view.time.time()-60}))
    assert view.Reader(tmp_path).snapshot()['state']=='waiting'


@pytest.mark.parametrize("delay", [90, 14400])
def test_stream_pacing_reports_retry_without_claiming_daily_exhaustion(tmp_path,monkeypatch,delay):
    folder=tmp_path/'evidence/session-test/pipeline';folder.mkdir(parents=True)
    (tmp_path/'evidence/runtime.json').write_text(json.dumps({'session':str(folder.parent)}))
    now=view.time.time()
    (folder/'decision-1.json').write_text(json.dumps({'observed_at':now}))
    (folder/'health.json').write_text(json.dumps({'state':'running','executor_connected':True}))
    (folder/'activity-stream-health.json').write_text(json.dumps({
        'state':'budget_pacing','retry_at':now+delay,'budget_kind':'local_request_units',
        'local_units':1100,'local_daily_limit':80000}))
    monkeypatch.setattr(view,'active_lock',lambda _:True)
    result=view.Reader(tmp_path).snapshot()
    assert result['state']=='running' and result['executor_connected']
    assert result['stream_retry_at']==now+delay
    assert 'HTTP 持续运行' in result['stream']
    assert '自动重试' in result['stream_note']
    assert '短暂' not in result['stream_note']
    assert '已用完' not in result['stream'] and '已触发' not in result['stream_note']


def test_live_feed_with_broken_calculation_is_reported_as_fault(tmp_path,monkeypatch):
    folder=tmp_path/'evidence/session-test/pipeline';folder.mkdir(parents=True)
    (tmp_path/'evidence/runtime.json').write_text(json.dumps({'session':str(folder.parent),'executor_connected':True}))
    (folder/'decision-1.json').write_text(json.dumps({'observed_at':view.time.time()-720}))
    (folder/'health.json').write_text(json.dumps({'state':'recovering','error':'amount out of range'}))
    (folder/'activity-stream-health.json').write_text(json.dumps({'state':'connected'}))
    monkeypatch.setattr(view,'active_lock',lambda _:True)
    result=view.Reader(tmp_path).snapshot()
    assert result['state']=='fault' and not result['executor_connected']
    assert '零输出' in result['activity'] and '中断' in result['activity']
    assert result['stream_state']=='connected'


def test_network_retry_keeps_unlocked_executor_visible(tmp_path,monkeypatch):
    folder=tmp_path/'evidence/session-test/pipeline';folder.mkdir(parents=True)
    (tmp_path/'evidence/runtime.json').write_text(json.dumps({'session':str(folder.parent),'executor_connected':True}))
    (folder/'decision-1.json').write_text(json.dumps({'observed_at':view.time.time()-3}))
    (folder/'health.json').write_text(json.dumps({'state':'degraded','executor_connected':True,
        'feed':{'state':'retrying','error':'RPC total request deadline exceeded'}}))
    monkeypatch.setattr(view,'active_lock',lambda _:True)
    data=view.Reader(tmp_path).snapshot()
    assert data['state']=='waiting' and data['executor_connected']
    assert '自动重试' in data['activity']
    monkeypatch.setattr(view,'active_lock',lambda _:False)
    data=view.Reader(tmp_path).snapshot()
    assert data['state']=='stopped' and not data['executor_connected']


def test_rechecked_route_displays_current_quote_and_drops_expired_estimate(tmp_path,monkeypatch):
    folder=tmp_path/'evidence/session-test/pipeline';folder.mkdir(parents=True)
    (tmp_path/'evidence/runtime.json').write_text(json.dumps({'session':str(folder.parent)}))
    now=view.time.time()
    original={'route_key':'r','tokens':[view.USDC,view.WETH,view.USDC],
              'net_after_modelled_cost':9000000,'status':'whole_cycle_passed'}
    recheck={'received_utc':now,'decision':{'candidates':[dict(original,net_after_modelled_cost=2000000)]}}
    candidate=dict(original,execution_recheck=recheck)
    file=folder/'opportunity-1.json.gz'
    def write():file.write_bytes(gzip.compress(json.dumps({'received_utc':now-5,'decision':{'candidates':[candidate]}}).encode()))
    write();result=view.Reader(tmp_path).snapshot()
    assert result['opportunities'][0]['estimate']=='2' and result['opportunities'][0]['at']==now
    candidate['status']='recheck_below_floor';recheck['decision']['candidates']=[]
    write();result=view.Reader(tmp_path).snapshot()
    assert result['opportunities'][0]['estimate'] is None
    assert result['opportunities'][0]['label']=='重读后价差不足'


@pytest.mark.parametrize('status,label', [
    ('sampled_transfer_backoff','转账规则待复核，暂缓此路线'),
    ('recheck_budget_exhausted','复核计算达到时限，等待新报价'),
    ('recheck_failed','路线重读失败，尚未进入整笔校验'),
    ('execution_rejected','执行前校验未通过'),
    ('expired_before_probe','价格已过期')])
def test_deferred_candidate_does_not_present_model_quote_as_available_profit(tmp_path,status,label):
    folder=tmp_path/'evidence/session-test/pipeline';folder.mkdir(parents=True)
    (tmp_path/'evidence/runtime.json').write_text(json.dumps({'session':str(folder.parent)}))
    candidate={'tokens':[view.USDC,view.WETH,view.USDC],'net_after_modelled_cost':9000000,
               'status':status}
    (folder/'opportunity-1.json.gz').write_bytes(gzip.compress(json.dumps({
        'received_utc':view.time.time(),'decision':{'candidates':[candidate]}}).encode()))
    result=view.Reader(tmp_path).snapshot()['opportunities'][0]
    assert result['estimate'] is None and result['label']==label


@pytest.mark.parametrize('failure,expected', [
    ({'cause':'snapshot anchor changed during read'}, '复核时区块已更新，等待新报价'),
    ({'cause':'execution context stale'}, '整笔校验前数据已过期'),
    ({'revert_name':'BadCallback'}, '合约拒绝：回调或到账金额不符'),
    ({'revert_name':'InsufficientProfit'}, '实际回款低于利润门槛'),
    ({'revert_name':'UniswapV2Invariant'}, '兑换池余额校验未通过'),
    ({'cause':'UniswapV2: TRANSFER_FAILED','validation_deadline_reached':True}, '校验超时，尚未确认可成交'),
    ({'cause':'UniswapV2: TRANSFER_FAILED'}, '转账步骤失败，尚未确认可成交'),
    ({'cause':'https://secret.example/token'}, '执行前校验未通过')])
def test_rejection_exposes_actual_stage_without_claiming_tax_or_leaking_endpoints(failure,expected):
    assert view.rejection_label({'execution_failure':failure})==expected
