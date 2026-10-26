"""Resumable additional deployment under the existing setup and account locks."""
import fcntl
import json
import time
from pathlib import Path
from .core import address,atomic_json,data,check,number
from .structural import deployment,verify_contract
from .onboarding import account_directory,require_current_validation,submit_setup_step,fee_fields,SETUP_STEP_FEE_CAP,setup_gas_limit
from .v4_onboarding import ensure_setup_account_ready

def setup_executor(rpc,config,signer,directory=None,require_validation=True):
    if require_validation:require_current_validation()
    base=Path(directory) if directory else account_directory(config)
    base.mkdir(parents=True,exist_ok=True,mode=0o700)
    with (base/'executor.lock').open('a+') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        dep=deployment(config);journal=base/'structural'/dep['creation_data_hash'][2:]
        if not (journal/'setup-deploy.json').exists():ensure_setup_account_ready(rpc,base)
        receipt=submit_setup_step(rpc,config,signer,journal/'setup-deploy.json',dep['creation_data'],capped_deployment=True)
        contract=address(receipt['contractAddress']);state=verify_contract(rpc,config,contract,require_unpaused=False)
        if state['paused'] or (journal/'setup-unpause.json').exists():
            if not (journal/'setup-unpause.json').exists():ensure_setup_account_ready(rpc,base)
            submit_setup_step(rpc,config,signer,journal/'setup-unpause.json',data('setPaused(bool)',['bool'],[False]),contract)
        if config.get('structural',{}).get('atlas_enabled'):
            from .atlas_onboarding import setup_bond
            setup_bond(rpc,config,signer,journal)
        state=verify_contract(rpc,config,contract)
        state.update(verified_at=time.time(),deployment_tx=receipt['transactionHash'],deployment_journal_directory=str(journal),trading_started=False)
        atomic_json(base/'installation-structural.json',state)
        return state

def preflight(rpc,config,directory=None):
    from .execution import FeeOracle
    base=Path(directory) if directory else account_directory(config)
    dep=deployment(config);installed=base/'installation-structural.json';journal=base/'structural'/dep['creation_data_hash'][2:]
    if installed.exists():
        from .execution import runtime_matches
        from .structural import artifact
        old=json.loads(installed.read_text())['contract']
        if runtime_matches(rpc.request('eth_getCode',[old,'latest']),artifact('StructuralExecutor')):
            state=verify_contract(rpc,config,old,require_unpaused=False)
            from .atlas_onboarding import preflight as bond_preflight
            bond=bond_preflight(rpc,config,journal);balance=number(rpc.request('eth_getBalance',[config['owner'],'pending']))
            unpause=0
            if state['paused']:
                fee=fee_fields(rpc,config)
                unpause=FeeOracle(rpc,config).maximum_fee(data('setPaused(bool)',['bool'],[False]),150000,fee['maxFeePerGas'])
                check(unpause<=SETUP_STEP_FEE_CAP,'structural unpause exceeds setup fee cap')
            needed=bond['additional_balance_needed_wei']+int(config['policy']['max_total_fee_wei'])+unpause
            return {**state,'state':'resume_existing_setup' if state['paused'] else 'ready','atlas':bond,'broadcast':False,
                    'unpause_fee_budget_wei':unpause,'required_balance_wei':needed,'balance_sufficient':balance>=needed}
    if (base/'structural'/dep['creation_data_hash'][2:]/'setup-deploy.json').exists():return {'state':'resume_existing_setup','broadcast':False}
    fee=fee_fields(rpc,config)
    call={'from':config['owner'],'data':dep['creation_data'],'value':'0x0','gas':hex(6_000_000),**{k:hex(v) for k,v in fee.items()}}
    gas=setup_gas_limit(number(rpc.request('eth_estimateGas',[call,'pending'])),capped_deployment=True)
    call['gas']=hex(gas)
    check(len(rpc.request('eth_call',[call,'pending']))>2,'structural constructor returned empty runtime')
    oracle=FeeOracle(rpc,config)
    deploy=oracle.maximum_fee(dep['creation_data'],gas,fee['maxFeePerGas'])
    unpause=oracle.maximum_fee(data('setPaused(bool)',['bool'],[False]),150000,fee['maxFeePerGas'])
    check(max(deploy,unpause)<=SETUP_STEP_FEE_CAP,'structural setup exceeds existing fee cap')
    balance=number(rpc.request('eth_getBalance',[config['owner'],'pending']))
    from .atlas_onboarding import preflight as bond_preflight
    bond=bond_preflight(rpc,config,journal)
    needed=deploy+unpause+int(config['policy']['max_total_fee_wei'])+bond['additional_balance_needed_wei']
    return {'state':'deployment_needed','deployment_gas':gas,'deployment_fee_budget_wei':deploy,
        'atlas':bond,'unpause_fee_budget_wei':unpause,'balance_sufficient':balance>=needed,'required_balance_wei':needed,'broadcast':False}
