'use strict';
const byId=id=>document.getElementById(id);
const put=(id,text)=>{byId(id).textContent=text;};
const clock=timestamp=>timestamp?new Date(timestamp*1000).toLocaleString('zh-CN',{month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false}):'—';
function number(value){if(value===null||value===undefined)return '—';const parts=String(value).split('.');let fraction=(parts[1]||'').replace(/0+$/,'');if(fraction.length>10)fraction=fraction.slice(0,10)+'…';return parts[0].replace(/\B(?=(\d{3})+(?!\d))/g,',')+(fraction?'.'+fraction:'');}
function node(tag,text,cls){const el=document.createElement(tag);if(text!==undefined)el.textContent=text;if(cls)el.className=cls;return el;}
function external(url,label){const link=node('a',label);link.href=url;link.target='_blank';link.rel='noopener noreferrer';return link;}
function metric(id,value){const element=byId(id);element.textContent=number(value);element.title=value===undefined?'':String(value);}
function render(data){
  const status={running:['自动交易运行中','正在寻找机会'],waiting:['等待有效计算','扫描暂未更新'],fault:['计算故障','机会扫描已中断'],stopped:['程序已停止','交易程序当前未运行']}[data.state]||['状态待确认','正在核对运行状态'];
  put('run-label',status[0]);put('state',status[1]);put('activity',data.activity);put('executor',data.executor_connected?(data.state==='running'?'已连接 · 满足条件自动执行':'已连接 · 等待扫描恢复'):data.state==='fault'?'等待计算恢复':'未连接');put('stream',data.stream);put('data-time',clock(data.last_data_at));
  byId('stream').className=data.stream_state==='connected'?'':'warning';
  const streamNote=(data.stream_note||'')+(data.stream_retry_at?' 计划于 '+clock(data.stream_retry_at)+' 重试推送。':'');
  put('stream-note',streamNote);byId('stream-note').hidden=!streamNote;
  put('refreshed',new Date(data.updated_at*1000).toLocaleTimeString('zh-CN',{hour12:false})+' 更新');byId('refresh-light').className='dot';
  const ledger=data.ledger;metric('usdc',ledger.available?ledger.gross_usdc:undefined);metric('weth',ledger.available?ledger.gross_weth:undefined);metric('fee',ledger.available?ledger.fees_eth:undefined);
  put('accounting-note',ledger.available?ledger.scope:ledger.reason);
  const wallet=byId('wallet-link');wallet.hidden=!data.account_url;if(data.account_url)wallet.href=data.account_url;
  const body=byId('trades');body.replaceChildren();
  for(const trade of ledger.trades){
    const row=node('tr');row.append(node('td',clock(trade.at)));
    const result=node('td');result.append(node('span',trade.label,trade.state==='success'?'good':trade.state==='failed'?'bad':'pending'));
    result.append(node('span',trade.earlier_executor?'历史执行器':'当前执行器','small'));
    if(['success','failed'].includes(trade.state)&&!trade.finalized)result.append(node('span','已入块，待最终确认','small'));
    row.append(result);
    const gross=node('td',trade.gross===null?'等待回执':number(trade.gross)+' '+trade.symbol);gross.title=trade.gross===null?'':trade.gross+' '+trade.symbol;
    if(trade.net_eth!==null)gross.append(node('span','扣费后 '+number(trade.net_eth)+' ETH 等值','small'));row.append(gross);
    const fee=node('td',trade.fee_eth===null?'等待回执':number(trade.fee_eth)+' ETH');fee.title=trade.fee_eth===null?'':trade.fee_eth+' ETH';row.append(fee);
    const proof=node('td');proof.append(trade.url?external(trade.url,'查看链上 ↗'):node('span','尚未提交'));row.append(proof);body.append(row);
  }
  byId('trades-empty').hidden=ledger.trades.length>0;byId('trades-table').hidden=!ledger.trades.length;
  put('trades-empty',ledger.available?'还没有套利交易。提交后会先显示“等待回执”，入块后显示结果和实际费用。':ledger.reason);
  const events=byId('opportunities');events.replaceChildren();
  for(const event of data.opportunities){const row=node('div',undefined,'opportunity');row.append(node('div',clock(event.at),'small'));const route=node('div');route.append(node('div',event.route,'route'));route.append(node('span',event.label,'small'));row.append(route);const estimate=node('div',event.estimate===null?'—':'报价估算 '+number(event.estimate)+' '+event.symbol,'estimate');if(event.url)estimate.append(external(event.url,' 链上记录 ↗'));row.append(estimate);events.append(row);}
  byId('opportunities-empty').hidden=!!data.opportunities.length;
  if(!data.opportunities.length)put('opportunities-empty',data.state==='fault'?'机会计算已中断。修复恢复扫描后，这里继续显示候选及执行结果。':data.state==='running'?'正在寻找符合条件的价差。出现候选后，这里显示路线、预计净收益及处理结果。':'等待扫描恢复，已有交易结果保留在上方。');
  const contract=byId('contract');contract.replaceChildren();if(/^0x[0-9a-fA-F]{40}$/.test(data.contract||''))contract.append(external('https://basescan.org/address/'+data.contract,'当前执行合约 ↗'));
}
let busy=false;
async function refresh(){if(busy)return;busy=true;try{const response=await fetch('/api/status',{cache:'no-store'});if(!response.ok)throw new Error('unavailable');render(await response.json());byId('error').hidden=true;}catch{put('error','查看页面暂时无法读取最新记录，正在重试。下方保留的是上次数据。');byId('error').hidden=false;byId('refresh-light').className='dot warn';}finally{busy=false;}}
byId('refresh').addEventListener('click',refresh);refresh();setInterval(refresh,2000);
