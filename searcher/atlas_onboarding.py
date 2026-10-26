"""User-launched, bounded Atlas bond setup using the existing resumable journal."""
import json
import re
from pathlib import Path
from .core import number,data,check,atomic_json
from .strategy_access import ATLAS
from .onboarding import submit_setup_step,fee_fields,SETUP_STEP_FEE_CAP

def increase_plan(config,journal,bonded):
    """An explicitly authorized allocation; restarts never create another one.

    Existing accounts may add only the approved increment, even if fees have
    already depleted their original bond. A fresh account may fund the target
    once. Persist even a zero allocation so external funding cannot later turn
    a restart into permission to replenish consumed funds.
    """
    increase=config['structural']['atlas_bond_increase']
    ident=increase['id'];target=int(config['structural']['atlas_bond_wei'])
    check(isinstance(ident,str) and re.fullmatch(r'[a-z0-9][a-z0-9-]{0,63}',ident),
          'invalid Atlas allocation id')
    additional=int(increase['max_additional_wei'])
    check(0<additional<target,'invalid Atlas additional allocation')
    legacy=bool(journal and (Path(journal)/'setup-atlas-bond.json').exists())
    limit=additional if legacy else target
    identity={'id':ident,'chain_id':config['chain_id'],'owner':config['owner'],
              'target_bond_wei':target,'max_additional_wei':additional,
              'legacy_allocation_present':legacy}
    path=Path(journal)/f'setup-atlas-bond-{ident}.json' if journal else None
    plan_path=Path(journal)/f'atlas-allocation-{ident}.json' if journal else None
    plan={'identity':identity,'deposit_wei':min(limit,max(0,target-bonded))}
    if plan_path and plan_path.exists():
        plan=json.loads(plan_path.read_text())
        check(plan.get('identity')==identity,'Atlas allocation configuration changed')
        check(type(plan.get('deposit_wei')) is int and 0<=plan['deposit_wei']<=limit,
              'Atlas allocation exceeds authorized deposit')
    else:
        check(not (path and path.exists()),'Atlas allocation record missing')
    return plan,plan_path,path


def preflight(rpc,config,journal=None):
    if not config.get('structural',{}).get('atlas_enabled'):return {'enabled':False,'additional_balance_needed_wei':0}
    target=int(config['structural']['atlas_bond_wei'])
    check(0<target<=2*int(config['policy']['max_total_fee_wei']),'Atlas bond exceeds two-attempt budget')
    bonded=number(rpc.request('eth_call',[{'to':ATLAS,'data':data('balanceOfBonded(address)',['address'],[config['owner']])},'pending']))
    path=journal/'setup-atlas-bond.json' if journal else None
    # A completed deposit is a one-time setup allocation, never an automatic refill.
    funded=bool(path and path.exists())
    amount=0 if funded else max(0,target-bonded);fee=0
    if config['structural'].get('atlas_bond_increase'):
        plan,plan_path,path=increase_plan(config,journal,bonded)
        funded=bool(plan_path and plan_path.exists() and (not plan['deposit_wei'] or path.exists()))
        amount=0 if funded else plan['deposit_wei']
    if amount:
        from .execution import FeeOracle
        fields=fee_fields(rpc,config);calldata=data('depositAndBond(uint256)',['uint256'],[amount])
        gas=(number(rpc.request('eth_estimateGas',[{'from':config['owner'],'to':ATLAS,'value':hex(amount),'data':calldata},'pending']))*120+99)//100
        check(gas<=150000,'Atlas bond gas cap');fee=FeeOracle(rpc,config).maximum_fee(calldata,gas,fields['maxFeePerGas'])
        check(fee<=SETUP_STEP_FEE_CAP,'Atlas bond setup fee cap')
    return {'enabled':True,'bonded_wei':bonded,'target_bond_wei':target,'setup_allocation_used':funded,'available_for_bidding':bonded>0,'deposit_wei':amount,'fee_budget_wei':fee,'additional_balance_needed_wei':amount+fee}

def setup_bond(rpc,config,signer,journal):
    if not config.get('structural',{}).get('atlas_enabled'):return
    journal=Path(journal)
    path=journal/'setup-atlas-bond.json'
    increase=config['structural'].get('atlas_bond_increase')
    if path.exists():
        # Resuming always reuses exactly the original funded intent.
        amount=int(json.loads(path.read_text())['identity']['value'])
    elif not increase:
        state=preflight(rpc,config);amount=state['deposit_wei']
    else:amount=0
    if amount:submit_setup_step(rpc,config,signer,path,data('depositAndBond(uint256)',['uint256'],[amount]),ATLAS,value=amount)
    if increase:
        # Finish the original transaction before fixing the additional amount.
        # A failure or interruption therefore never advances to another nonce.
        state=preflight(rpc,config,journal)
        plan,plan_path,path=increase_plan(config,journal,state['bonded_wei'])
        if not plan_path.exists():atomic_json(plan_path,plan)
        amount=plan['deposit_wei']
        if amount:submit_setup_step(rpc,config,signer,path,data('depositAndBond(uint256)',['uint256'],[amount]),ATLAS,value=amount)
