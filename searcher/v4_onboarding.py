"""Additional V4 deployment, sharing the account lock and setup fee policy."""
import fcntl
from pathlib import Path
import time
from .core import address,atomic_json,data,check
from .v4 import deployment,verify_contract,verify_manager
from .onboarding import (account_directory,require_current_validation,ensure_migration_account_idle,
                         submit_setup_step)

def ensure_setup_account_ready(rpc,base):
    """Never reuse a nonce whose earlier accounting can still be orphaned."""
    import json,sqlite3
    from .core import number
    from .onboarding import SetupPending
    from .core import Reject
    try:ensure_migration_account_idle(base)
    except Reject as exc:raise SetupPending('正在恢复原交易，确认后继续V4设置。') from exc
    path=base/'transactions.sqlite'
    if not path.exists():return
    with sqlite3.connect(f'file:{path}?mode=ro',uri=True) as db:
        tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'intents' not in tables:return
        rows=db.execute("SELECT i.outcome FROM attempts a JOIN intents i ON a.id=i.attempt WHERE a.state IN ('success','failed')").fetchall()
    if not rows:return
    finalized=rpc.request('eth_getBlockByNumber',['finalized',False])
    if finalized is None:raise SetupPending('等待已结算交易最终确认，再创建额外V4合约。')
    for (raw,) in rows:
        check(raw is not None,'settled account outcome missing before V4 setup')
        outcome=json.loads(raw);height=outcome['block']
        if height>number(finalized['number']):raise SetupPending('等待已结算交易最终确认，再创建额外V4合约。')
        header=rpc.request('eth_getBlockByNumber',[hex(height),False])
        if header is None or header['hash']!=outcome['block_hash']:
            raise SetupPending('旧交易结算正在恢复，恢复后再创建V4合约。')

def setup_executor(rpc,config,signer,directory=None,require_validation=True):
    if require_validation:require_current_validation()
    base=Path(directory) if directory else account_directory(config)
    base.mkdir(parents=True,exist_ok=True,mode=0o700)
    with (base/'executor.lock').open('a+') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        verify_manager(rpc)
        dep=deployment(config)
        # V4 is an additional executor, not a replacement/migration of CL/V2.
        # Its journals must never enter AtomicCycle predecessor discovery.
        journal=base/'v4'/dep['creation_data_hash'][2:]
        if not (journal/'setup-deploy.json').exists():ensure_setup_account_ready(rpc,base)
        receipt=submit_setup_step(rpc,config,signer,journal/'setup-deploy.json',dep['creation_data'])
        contract=address(receipt['contractAddress'])
        state=verify_contract(rpc,config,contract,require_unpaused=False)
        if state['paused'] or (journal/'setup-unpause.json').exists():
            if not (journal/'setup-unpause.json').exists():ensure_setup_account_ready(rpc,base)
            submit_setup_step(rpc,config,signer,journal/'setup-unpause.json',data('setPaused(bool)',['bool'],[False]),contract)
        state=verify_contract(rpc,config,contract)
        state.update(verified_at=time.time(),deployment_tx=receipt['transactionHash'],execution_kind='v4-pair-v1',
                     deployment_journal_directory=str(journal),trading_started=False)
        atomic_json(base/'installation-v4.json',state)
        return state

def preflight(rpc,config,directory=None):
    """Unsigned setup cost check before local wallet unlock; no account override."""
    from .onboarding import fee_fields,SETUP_STEP_FEE_CAP
    from .execution import FeeOracle
    from .core import number
    verify_manager(rpc)
    dep=deployment(config)
    base=Path(directory) if directory else account_directory(config)
    installed=base/'installation-v4.json'
    if installed.exists():
        import json
        verified=verify_contract(rpc,config,json.loads(installed.read_text())['contract'],require_unpaused=False)
        return {**verified,'state':'resume_existing_setup' if verified['paused'] else 'ready','broadcast':False,'balance_sufficient':True}
    if (base/'v4'/dep['creation_data_hash'][2:]/'setup-deploy.json').exists():
        return {'state':'resume_existing_setup','broadcast':False}
    fee=fee_fields(rpc,config)
    call={'from':config['owner'],'value':'0x0','data':dep['creation_data'],'gas':hex(6_000_000),
          **{k:hex(v) for k,v in fee.items()}}
    gas=number(rpc.request('eth_estimateGas',[call,'pending']))
    gas_limit=(gas*120+99)//100;check(gas_limit<=6_000_000,'V4 deployment gas cap exceeded')
    oracle=FeeOracle(rpc,config)
    deploy_cost=oracle.maximum_fee(dep['creation_data'],gas_limit,fee['maxFeePerGas'])
    unpause_cost=oracle.maximum_fee(data('setPaused(bool)',['bool'],[False]),150000,fee['maxFeePerGas'])
    check(max(deploy_cost,unpause_cost)<=SETUP_STEP_FEE_CAP,'V4 setup exceeds existing per-step fee cap')
    balance=number(rpc.request('eth_getBalance',[config['owner'],'pending']))
    needed=deploy_cost+unpause_cost+int(config['policy']['max_total_fee_wei'])
    return {'manager_verified':True,'deployment_gas':gas_limit,'deployment_fee_budget_wei':deploy_cost,
            'unpause_fee_budget_wei':unpause_cost,'balance_sufficient':balance>=needed,'broadcast':False,
            'scope':'unsigned new-deployment estimate; existing journals are resumed during launch'}
