"""Local, read-only view of existing bot evidence. Never imports a signer or RPC."""
from __future__ import annotations
import argparse
from collections import Counter
import fcntl
import gzip
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import threading
import time
import webbrowser
from urllib.request import urlopen

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
WETH = '0x4200000000000000000000000000000000000006'
USDC = '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913'
SYMBOLS = {WETH:'WETH',USDC:'USDC',
 '0x22af33fe49fd1fa80c7149773dde5890d3c76f3b':'BNKR',
 '0x4f9fd6be4a90f2620860d680c0d4d5fb53d1a825':'AIXBT',
 '0x940181a94a35a4569e4529a3cdfb74e38fd98631':'AERO',
 '0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf':'cbBTC',
 '0x029c58a909fbe3d4be85a24f414dda923a3fde0f':'IRA'}

def read_json(path, default=None):
    try:return json.loads(path.read_text())
    except (OSError,ValueError):return {} if default is None else default

def amount(raw, decimals=18):
    value=int(raw or 0);absolute=abs(value);scale=10**decimals
    fraction=f'{absolute%scale:0{decimals}d}'.rstrip('0') if decimals else ''
    return ('-' if value<0 else '')+str(absolute//scale)+('.'+fraction if fraction else '')

def symbol(address):
    return SYMBOLS.get(address, address[:6]+'…'+address[-4:] if address else '未知资产')

def transaction_url(value):
    return 'https://basescan.org/tx/'+value if isinstance(value,str) and re.fullmatch(r'0x[0-9a-fA-F]{64}',value) else None

def rejection_label(candidate):
    """Describe the stage actually reached; never infer a tax from a generic revert."""
    failure=candidate.get('execution_failure') or {}
    reason=str(failure.get('cause') or candidate.get('error') or '')
    if 'snapshot anchor changed' in reason:
        return '复核时区块已更新，等待新报价'
    if 'execution context stale' in reason or 'candidate expired' in reason:
        return '整笔校验前数据已过期'
    name=failure.get('revert_name')
    if name=='BadCallback' and failure.get('transfer_retry_after_seconds'):
        return '已知异常路线复核失败，已延后重试'
    if name=='BadCallback':
        return '合约拒绝：回调或到账金额不符'
    if name=='InsufficientProfit':
        return '实际回款低于利润门槛'
    if name=='UniswapV2Invariant':
        return '兑换池余额校验未通过'
    # A token transfer wrapper may also mask insufficient gas. A timed-out
    # retry does not establish an intrinsic token failure.
    if failure.get('validation_deadline_reached'):
        return '校验超时，尚未确认可成交'
    if name=='TransferFailed' or 'TRANSFER_FAILED' in reason:
        return '转账步骤失败，尚未确认可成交'
    if name=='InvalidPlan':
        return '交易参数或有效期校验未通过'
    if name=='InvalidPool':
        return '交易池校验未通过'
    return '执行前校验未通过'

def active_lock(path):
    try:
        with path.open('r') as handle:
            try:fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:return True
    except OSError:pass
    return False

def read_ledger(path, contract):
    if not path.exists():return {'available':False,'trades':[],'reason':'尚无交易账本'}
    with sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=.2) as db:
        db.execute('PRAGMA query_only=ON')
        # Return selected fields only: raw signed transactions never enter this process.
        has_kind='kind' in {r[1] for r in db.execute('PRAGMA table_info(attempts)')}
        filter_kind=" WHERE a.kind='transaction'" if has_kind else ''
        rows=db.execute("""SELECT a.state,a.tx_hash,a.actual,a.finished_at,
          json_extract(i.payload,'$.prepared_at'),json_extract(i.payload,'$.settlement'),
          json_extract(i.payload,'$.contract'),i.outcome,i.finalized
          FROM attempts a LEFT JOIN intents i ON a.id=i.attempt"""+filter_kind+" ORDER BY a.rowid DESC").fetchall()
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='auctions'").fetchone():
            for state,fee,finished,payload,raw,finalized in db.execute("SELECT a.state,a.actual,a.finished_at,x.payload,x.outcome,x.finalized FROM attempts a JOIN auctions x ON a.id=x.attempt WHERE a.kind='auction'"):
                op=json.loads(payload);out=json.loads(raw) if raw else {};executed=out.get('executions',[])
                tx=executed[0]['transaction_hash'] if executed else None
                outcome={'auction':True,'simulation_only':bool(op.get('operations')) and all(o.get('gateway') in ('signed_simulation_pending','simulation_not_submitted') for o in op['operations']),'auction_status':out.get('status'),'gross_profit_raw':out.get('payout_wei',0),'included_tx_hash':tx}
                at=min((o.get('at',0) for o in op.get('operations',[])),default=0)
                rows.append((state,tx,fee,finished,at,WETH,op.get('contract'),json.dumps(outcome),finalized))
        rows.sort(key=lambda r:r[3] or r[4] or 0,reverse=True)
    totals=Counter();trades=[];states=Counter()
    for state,tx,fee,finished,prepared,settlement,executed_contract,raw_outcome,finalized in rows:
        outcome=json.loads(raw_outcome) if raw_outcome else {}
        settled=state in ('success','failed')
        actual_fee=int(fee) if fee is not None and settled else None
        gross=int(outcome.get('gross_profit_raw',0)) if state=='success' else 0
        if actual_fee is not None:totals['fee']+=actual_fee
        if state=='success':totals[settlement]+=gross
        states[state]+=1
        if len(trades)<50:
            tx=outcome.get('included_tx_hash') or tx
            label={'reserved':'准备提交','submitted':'等待回执','success':'执行成功','failed':'交易回滚','cancelled':'提交前取消'}.get(state,state)
            if outcome.get('cancelled'):label='已取消交易'
            if outcome.get('auction'):
                label={'success':'竞标成交','failed':'竞标执行失败','submitted':'竞标已发送 · 核对成交','reserved':'竞标执行校验','cancelled':'竞标结束 · 无费用'}.get(state,label)
            if outcome.get('simulation_only') and state=='submitted':label='竞标执行校验 · 核对费用'
            net=amount(gross-actual_fee) if settled and settlement==WETH and actual_fee is not None else None
            trades.append({'state':state,'label':label,'at':finished or prepared,'tx':tx,
              'url':transaction_url(tx),'symbol':'ETH' if outcome.get('auction') else symbol(settlement),
              'gross':amount(gross,6 if settlement==USDC else 18) if settled and settlement in (USDC,WETH) else None,
              'fee_eth':amount(actual_fee) if actual_fee is not None else None,
              'net_eth':net,'finalized':bool(finalized),'earlier_executor':bool(contract and executed_contract!=contract)})
    return {'available':True,'trades':trades,'states':dict(states),
      'gross_usdc':amount(totals[USDC],6),'gross_weth':amount(totals[WETH]),'fees_eth':amount(totals['fee']),
      'scope':'实际交易账本；竞标收入已扣出价，网络费另列。部署与保证金存入不算交易收入。'}

class Reader:
    def __init__(self,root):
        self.root=root;self.lock=threading.Lock();self.cache={};self.cached_at=0;self.opportunities={}

    def snapshot(self):
        with self.lock:
            now=time.time()
            if now-self.cached_at<2:return self.cache
            config=read_json(self.root/'config/base.json');evidence=self.root/'evidence'
            runtime=read_json(evidence/'runtime.json');pipeline={};stream={};decision={};latest_at=None;events=[];coverage={};auction={};svr={};suite={}
            session=Path(runtime.get('session','/nonexistent')).resolve()
            if session.is_relative_to(evidence.resolve()) and session.exists():
                folder=session/'pipeline'
                suite=read_json(folder/'strategy-suite.json')
                coverage=read_json(folder/'market-coverage.json');auction=read_json(folder/'atlas-latest-decision.json');svr=read_json(folder/'svr-health.json')
                pipeline=read_json(folder/'health.json');stream=read_json(folder/'activity-stream-health.json')
                paths=sorted(folder.glob('decision-*.json'),key=lambda p:int(p.stem.split('-')[-1]))
                if paths:
                    decision=read_json(paths[-1]);latest_at=decision.get('observed_at')
                files=sorted(folder.glob('opportunity-*.json.gz'),key=lambda p:int(p.name.split('-')[1].split('.')[0]))[-20:]
                for file in files:
                    if str(file) in self.opportunities:continue
                    try:
                        with gzip.open(file,'rb') as f:
                            raw=f.read(12*1024**2+1)
                        if len(raw)>12*1024**2:continue
                        record=json.loads(raw);report=record.get('decision',{})
                        entries=[]
                        for candidate in report.get('candidates',[])[:8]:
                            token=candidate.get('tokens',[''])[0]
                            status=candidate.get('status','quoted_only')
                            label={'submitted':'已提交交易','execution_rejected':'执行前校验未通过','expired_before_probe':'价格已过期',
                                   'recheck_failed':'路线重读失败，尚未进入整笔校验',
                                   'recheck_below_floor':'重读后价差不足','sampled_transfer_backoff':'转账规则待复核，暂缓此路线',
                                   'recheck_budget_exhausted':'复核计算达到时限，等待新报价',
                                   'whole_cycle_passed':'整笔验证通过','quoted_only':'发现报价候选'}.get(status,'等待校验')
                            if status=='execution_rejected':label=rejection_label(candidate)
                            recheck=candidate.get('execution_recheck')
                            priced=(next((c for c in recheck.get('decision',{}).get('candidates',[])
                                          if c.get('route_key')==candidate.get('route_key')),None)
                                    if recheck else candidate)
                            # A rejected or stale quote is not an available profit.
                            if status not in ('quoted_only','whole_cycle_passed','submitted'):priced=None
                            entries.append({'at':(recheck or {}).get('received_utc',record.get('received_utc',report.get('observed_at'))),
                              'route':' → '.join(symbol(t) for t in candidate.get('tokens',[])),
                              'estimate':amount(priced.get('net_after_modelled_cost',0),6 if token==USDC else 18) if priced and token in (USDC,WETH) else None,
                              'symbol':symbol(token),'label':label,'url':transaction_url(candidate.get('execution',{}).get('tx_hash'))})
                        self.opportunities[str(file)]=entries
                    except (OSError,ValueError,EOFError):continue
                current={str(p) for p in files}
                self.opportunities={p:v for p,v in self.opportunities.items() if p in current}
                events=sorted([v for rows in self.opportunities.values() for v in rows],key=lambda x:x['at'] or 0,reverse=True)[:20]
            running=active_lock(evidence/'runtime.lock')
            age=now-latest_at if latest_at else None
            state='running' if running and age is not None and age<15 else 'waiting' if running else 'stopped'
            if running and pipeline.get('state')=='waiting_for_data':state='waiting'
            if running and pipeline.get('state')=='recovering' and pipeline.get('error'):state='fault'
            if running and pipeline.get('state')=='degraded' and pipeline.get('feed',{}).get('state')!='connected':state='waiting'
            stream_label={'connected':'Chainstack 推送已连接','budget_exhausted':'本机推送预算已用完 · 使用 HTTP',
              'retrying':'推送重连中 · 使用 HTTP','connecting':'正在连接推送','subscribing':'正在更新推送范围',
              'waiting_for_pools':'等待推送订阅 · 使用 HTTP',
              'budget_pacing':'推送按额度调节中 · HTTP 持续运行',
              'not_configured':'使用 HTTP 数据','stopped':'推送已停止'}.get(stream.get('state'),'读取数据状态中')
            stream_note=''
            if stream.get('state')=='budget_exhausted':
                reason='本机每日请求预算' if stream.get('budget_kind')=='local_request_units' else '旧版本的本机每小时字节限额'
                stream_note=reason+'已触发；这不是服务商剩余额度。'
            elif stream.get('state')=='budget_pacing':
                stream_note='按今日剩余额度分配推送用量，到显示的时间自动重试；HTTP 继续获取事件并检查已发现的可用路线。'
            elif stream.get('state')=='connected' and stream.get('http_only_pools'):
                stream_label='推送与 HTTP 分流运行'
                stream_note='高频池使用 HTTP 查询，其他池使用推送；定期重新尝试高频池。'
            if stream.get('budget_kind')=='local_request_units':
                stream_note+=' 本机今日已接收 '+str(stream.get('local_units',0))+' / '+str(stream.get('local_daily_limit',0))+' 次消息；按 UTC 日累计。'
            owner=config.get('owner','')
            account=evidence/f"account-{config.get('chain_id',8453)}-{owner}"
            installation=read_json(account/('installation-structural.json' if config.get('structural',{}).get('enabled') else 'installation.json'))
            try:ledger=read_ledger(account/'transactions.sqlite',installation.get('contract'))
            except (OSError,sqlite3.Error,ValueError,TypeError):ledger={'available':False,'trades':[],'reason':'账本暂时不可读取，稍后自动重试'}
            rejects=decision.get('rejected',{})
            if events and events[0].get('at') and now-events[0]['at']<15:activity='发现候选机会，正在按执行结果更新记录。'
            elif decision.get('deep_read_routes'):activity='正在核对不同成交量对应的价格和滑点，筛选扣费后仍有利润的路线。'
            else:activity='正在检查价差，等待满足利润和费用条件的机会。'
            if pipeline.get('strategy_mode')=='structural':
                # Structural auctions use their own connection, not the optional
                # pool-event stream used by the DEX scanner.
                stream=svr or {'state':'not_configured'}
                stream_note='竞价推送正在重连；借款与兑换价格扫描继续。' if stream.get('state')=='retrying' else ''
                stream_label='预言机竞价推送 + 借款与兑换价格' if svr.get('state')=='connected' else '直接读取借款与兑换价格'
                if not events:
                    activity='正在更新临界借款和清算线，同时核对 PSM 兑换价差；满足净利润门槛后自动成交。'
                    if coverage:
                        activity='正在逐市场检查临界借款、匹配卖出路线，并核对 PSM 价差。'
                        if not coverage.get('catalog_complete'):activity='正在补齐市场与借款仓位，同时检查已准备路线。'
                    if auction.get('status')=='auction_submitted' and now-auction.get('at',0)<10:
                        activity='竞标已发送，正在核对链上结果；成交和实际收入会记到账本。'
                    elif auction.get('status')=='no_executable_auction' and 0<=now-auction.get('at',0)<60:
                        activity='已处理最新拍卖，尚未形成可执行方案；继续检查借款与兑换价格。'
            if suite and running:
                lanes=suite.get('lanes',{})
                active=[k for k,v in lanes.items() if v.get('execution_ready') and v.get('state') in ('scanning','candidate_ready') and now-v.get('at',0)<60]
                if active:activity='清算、跨池价差和订单簿策略正在分别检查；整笔执行扣费后达到利润门槛，会自动提交。'
            if state=='stopped':
                activity='启动交易程序后，这里会继续显示它的活动。已有交易记录保留。'
                stream_label='交易程序已停止'
                stream_note=''
            elif state=='waiting':activity=('节点连接正在自动重试，恢复后继续扫描。右侧保留最近一次完成扫描的时间。'
                if pipeline.get('feed',{}).get('state')=='retrying' else
                '正在等待下一次有效计算，程序会自动继续。右侧保留最近一次完成扫描的时间。')
            elif state=='fault':
                reason=pipeline.get('error','')
                activity=('金额优化遇到零输出边界错误，机会计算已中断。修复版本已准备，重新启动后恢复扫描。'
                          if reason=='amount out of range' else '机会计算发生错误，当前扫描已中断，需要检查运行日志。')
            self.cache={'updated_at':now,'state':state,'activity':activity,'last_data_at':latest_at,
                'executor_connected':bool(running and state!='fault' and pipeline.get('executor_connected',runtime.get('executor_connected'))),'stream':stream_label,
                'stream_state':stream.get('state'),'stream_retry_at':stream.get('retry_at') if running else None,
                'stream_note':stream_note,
                'strategy_suite':suite,'strategy_coverage':{k:v for k,v in coverage.items() if k!='markets'},'latest_auction':auction,'ledger':ledger,'opportunities':events,'contract':installation.get('contract'),
                'account_url':'https://basescan.org/address/'+owner if re.fullmatch(r'0x[0-9a-fA-F]{40}',owner) else None}
            self.cached_at=now
            return self.cache

def serve():
    runtime=HERE/'runtime';runtime.mkdir(exist_ok=True)
    lock=(runtime/'view.lock').open('a+')
    try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:return
    reader=Reader(ROOT)
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):
            if self.headers.get('Host')!=f'127.0.0.1:{self.server.server_port}':
                self.send_error(403);return
            if self.path=='/api/status':
                try:body=json.dumps(reader.snapshot(),ensure_ascii=False).encode()
                except Exception:self.send_error(503,'Read temporarily unavailable');return
                content_type='application/json; charset=utf-8'
            elif self.path in ('/','/app.js','/style.css'):
                path=HERE/({'/':'index.html','/app.js':'app.js','/style.css':'style.css'}[self.path])
                body=path.read_bytes();content_type={'/':'text/html','/app.js':'text/javascript','/style.css':'text/css'}[self.path]+'; charset=utf-8'
            else:self.send_error(404);return
            self.send_response(200)
            self.send_header('Content-Type',content_type);self.send_header('Content-Length',str(len(body)))
            self.send_header('Cache-Control','no-store');self.send_header('X-Content-Type-Options','nosniff')
            self.send_header('Referrer-Policy','no-referrer')
            self.send_header('Content-Security-Policy',"default-src 'self'; connect-src 'self'; script-src 'self'; style-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'none'")
            self.end_headers();self.wfile.write(body)
    previous=read_json(runtime/'server.json').get('url','')
    match=re.fullmatch(r'http://127\.0\.0\.1:([0-9]+)/',previous)
    port=int(match[1]) if match and 1024<=int(match[1])<=65535 else 0
    try:server=ThreadingHTTPServer(('127.0.0.1',port),Handler)
    except OSError:server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    (runtime/'server.json').write_text(json.dumps({'pid':os.getpid(),'url':f'http://127.0.0.1:{server.server_port}/'}))
    server.serve_forever()

def start(open_browser=False):
    runtime=HERE/'runtime';runtime.mkdir(exist_ok=True)
    def current():
        state=read_json(runtime/'server.json');url=state.get('url','')
        if not re.fullmatch(r'http://127\.0\.0\.1:[0-9]+/',url):return None
        try:
            with urlopen(url+'api/status',timeout=1) as response:
                if 'ledger' in json.load(response):return url
        except Exception:pass
    url=current()
    if not url:
        with (runtime/'server.log').open('a') as log:
            subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'--serve'],stdin=subprocess.DEVNULL,
              stdout=log,stderr=log,start_new_session=True,close_fds=True)
        for _ in range(30):
            time.sleep(.1);url=current()
            if url:break
    if not url:raise SystemExit('查看页面未能启动，请检查 tools/live-view/runtime/server.log')
    if open_browser:webbrowser.open(url)
    print(url)

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--serve',action='store_true');parser.add_argument('--open',action='store_true')
    args=parser.parse_args()
    serve() if args.serve else start(args.open)
