"""Resumable additional deployment under the existing setup and account locks."""
import fcntl
import json
import time
from pathlib import Path
from .core import address,atomic_json,data,check,number
from .protocol_execution import deployment as _deployment,verify_contract as _verify,artifact
from .onboarding import account_directory,require_current_validation,submit_setup_step,fee_fields,SETUP_STEP_FEE_CAP,setup_gas_limit
from .v4_onboarding import ensure_setup_account_ready

def setup_executor(rpc,config,signer,directory=None,require_validation=True,name="ProtocolExecutor"):
    deployment=lambda c:_deployment(c,name)
    verify_contract=lambda rpc,c,a,**kw:_verify(rpc,c,a,name=name,**kw)
    if require_validation:require_current_validation()
    base=Path(directory) if directory else account_directory(config)
    base.mkdir(parents=True,exist_ok=True,mode=0o700)
    with (base/'executor.lock').open('a+') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        dep=deployment(config);journal=base/name/dep['creation_data_hash'][2:]
        if not (journal/'setup-deploy.json').exists():ensure_setup_account_ready(rpc,base)
        receipt=submit_setup_step(rpc,config,signer,journal/'setup-deploy.json',dep['creation_data'],capped_deployment=True)
        contract=address(receipt['contractAddress']);state=verify_contract(rpc,config,contract,require_unpaused=False)
        if state['paused'] or (journal/'setup-unpause.json').exists():
            if not (journal/'setup-unpause.json').exists():ensure_setup_account_ready(rpc,base)
            submit_setup_step(rpc,config,signer,journal/'setup-unpause.json',data('setPaused(bool)',['bool'],[False]),contract)
        state=verify_contract(rpc,config,contract)
        state.update(verified_at=time.time(),deployment_tx=receipt['transactionHash'],deployment_journal_directory=str(journal),trading_started=False)
        atomic_json(base/f'installation-{name}.json',state)
        return state

def preflight(rpc,config,directory=None,name="ProtocolExecutor"):
    deployment=lambda c:_deployment(c,name)
    verify_contract=lambda rpc,c,a,**kw:_verify(rpc,c,a,name=name,**kw)
    from .execution import FeeOracle
    base=Path(directory) if directory else account_directory(config)
    dep=deployment(config);installed=base/f'installation-{name}.json';journal=base/name/dep['creation_data_hash'][2:]
    if installed.exists():
        from .execution import runtime_matches
        old=json.loads(installed.read_text())['contract']
        if runtime_matches(rpc.request('eth_getCode',[old,'latest']),artifact(name)):
            state=verify_contract(rpc,config,old,require_unpaused=False)
            bond={"additional_balance_needed_wei":0};balance=number(rpc.request('eth_getBalance',[config['owner'],'pending']))
            unpause=0
            if state['paused']:
                fee=fee_fields(rpc,config)
                unpause=FeeOracle(rpc,config).maximum_fee(data('setPaused(bool)',['bool'],[False]),150000,fee['maxFeePerGas'])
                check(unpause<=SETUP_STEP_FEE_CAP,'structural unpause exceeds setup fee cap')
            needed=bond['additional_balance_needed_wei']+int(config['policy']['max_total_fee_wei'])+unpause
            return {**state,'state':'resume_existing_setup' if state['paused'] else 'ready','atlas':bond,'broadcast':False,
                    'unpause_fee_budget_wei':unpause,'required_balance_wei':needed,'balance_sufficient':balance>=needed}
    if (base/name/dep['creation_data_hash'][2:]/'setup-deploy.json').exists():return {'state':'resume_existing_setup','broadcast':False}
    fee=fee_fields(rpc,config)
    call={'from':config['owner'],'data':dep['creation_data'],'value':'0x0','gas':hex(6_000_000),**{k:hex(v) for k,v in fee.items()}}
    gas=setup_gas_limit(number(rpc.request('eth_estimateGas',[call,'pending'])),capped_deployment=True)
    call['gas']=hex(gas)
    check(len(rpc.request('eth_call',[call,'pending']))>2,'protocol constructor call returned empty runtime')
    oracle=FeeOracle(rpc,config)
    deploy=oracle.maximum_fee(dep['creation_data'],gas,fee['maxFeePerGas'])
    unpause=oracle.maximum_fee(data('setPaused(bool)',['bool'],[False]),150000,fee['maxFeePerGas'])
    check(max(deploy,unpause)<=SETUP_STEP_FEE_CAP,'structural setup exceeds existing fee cap')
    balance=number(rpc.request('eth_getBalance',[config['owner'],'pending']))
    bond={"additional_balance_needed_wei":0}
    needed=deploy+unpause+int(config['policy']['max_total_fee_wei'])+bond['additional_balance_needed_wei']
    return {'state':'deployment_needed','deployment_gas':gas,'deployment_fee_budget_wei':deploy,
        'atlas':bond,'unpause_fee_budget_wei':unpause,'balance_sufficient':balance>=needed,'required_balance_wei':needed,'broadcast':False}


def preflight_all(rpc,config,directory=None):
    rows={name:preflight(rpc,config,directory,name=name) for name in __import__('searcher.protocol_execution',fromlist=['contract_names']).contract_names(config)}
    fees=sum(int(row.get('deployment_fee_budget_wei',0))+int(row.get('unpause_fee_budget_wei',0)) for row in rows.values())
    needed=fees+int(config['policy']['max_total_fee_wei'])
    balance=number(rpc.request('eth_getBalance',[config['owner'],'pending']))
    check(balance>=needed,'余额需覆盖全部新增部署并预留一次交易费用。')
    return {'contracts':rows,'setup_fee_budget_wei':fees,'required_balance_wei':needed,'balance_sufficient':True,'broadcast':False}
