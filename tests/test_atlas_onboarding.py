import json
import pytest
from searcher.core import load_config,Reject,data
from searcher.atlas_onboarding import preflight,setup_bond
from searcher import atlas_onboarding

def test_consumed_setup_allocation_is_not_automatically_refilled(tmp_path):
    cfg=load_config();cfg['structural'].pop('atlas_bond_increase',None)
    cfg['structural'].update(atlas_enabled=True,atlas_bond_wei='100')
    path=tmp_path/'setup-atlas-bond.json';path.write_text(json.dumps({'identity':{'value':'100'}}))
    class RPC:
        def request(self,method,args):assert method=='eth_call';return '0x0'
    state=preflight(RPC(),cfg,tmp_path)
    assert state['setup_allocation_used'] and state['deposit_wei']==state['additional_balance_needed_wei']==0
    assert not state['available_for_bidding']

def test_resume_reuses_original_deposit_intent(tmp_path,monkeypatch):
    path=tmp_path/'setup-atlas-bond.json';path.write_text(json.dumps({'identity':{'value':'100'}}));calls=[]
    monkeypatch.setattr(atlas_onboarding,'submit_setup_step',lambda *a,**k:calls.append((a,k)))
    cfg=load_config();cfg['structural'].pop('atlas_bond_increase',None)
    cfg['structural'].update(atlas_enabled=True,atlas_bond_wei='100')
    setup_bond(None,cfg,None,tmp_path)
    assert len(calls)==1 and calls[0][1]['value']==100 and calls[0][0][3]==path


INITIAL=200_000_000_000_000
ADDITIONAL=400_000_000_000_000
TARGET=INITIAL+ADDITIONAL


def test_public_example_has_no_auction_funding_authorization(tmp_path):
    cfg=load_config()
    assert cfg['structural']['atlas_enabled'] is False
    assert int(cfg['structural']['atlas_bond_wei']) == 0
    assert 'atlas_bond_increase' not in cfg['structural']
    class NoNetwork:
        def request(self, *args): pytest.fail('Default auction settings accessed RPC')
    setup_bond(NoNetwork(),cfg,None,tmp_path)
    assert preflight(NoNetwork(),cfg,tmp_path)['additional_balance_needed_wei'] == 0
    assert list(tmp_path.iterdir()) == []


@pytest.fixture
def allocation(monkeypatch,tmp_path):
    from searcher.execution import FeeOracle
    cfg=load_config()
    cfg['policy']['max_total_fee_wei']=str(TARGET//2)
    cfg['structural'].update(atlas_enabled=True,atlas_bond_wei=str(TARGET),
        atlas_bond_increase={'id':'test-allocation','max_additional_wei':str(ADDITIONAL)})
    class RPC:
        bonded=INITIAL
        def request(self,method,args):
            if method=='eth_call':
                assert args[0]['data'].startswith(data('balanceOfBonded(address)')[:10])
                return hex(self.bonded)
            assert method=='eth_estimateGas'
            return hex(50_001)
    monkeypatch.setattr(atlas_onboarding,'fee_fields',lambda *a:{'maxFeePerGas':1})
    monkeypatch.setattr(FeeOracle,'maximum_fee',lambda self,calldata,gas,price:gas*price)
    legacy=tmp_path/'setup-atlas-bond.json'
    legacy.write_text(json.dumps({'identity':{'value':INITIAL}}))
    sent=[]
    def submit(rpc,config,signer,path,calldata,target,*,value):
        assert calldata==data('depositAndBond(uint256)',['uint256'],[value])
        if not path.exists():
            path.write_text(json.dumps({'identity':{'value':value}}))
            rpc.bonded+=value;sent.append((path.name,value))
        assert json.loads(path.read_text())['identity']['value']==value
    monkeypatch.setattr(atlas_onboarding,'submit_setup_step',submit)
    return cfg,RPC(),legacy,sent


def test_authorized_increase_funds_only_difference_and_never_refills(allocation,tmp_path):
    cfg,rpc,legacy,sent=allocation
    before=legacy.read_bytes()
    state=preflight(rpc,cfg,tmp_path)
    assert state['deposit_wei']==ADDITIONAL
    assert state['additional_balance_needed_wei']==ADDITIONAL+60_002
    assert list(tmp_path.iterdir())==[legacy]  # Read-only startup check.
    setup_bond(rpc,cfg,None,tmp_path)
    assert rpc.bonded==TARGET and sent==[('setup-atlas-bond-test-allocation.json',ADDITIONAL)]
    rpc.bonded=0  # Fees consume the bond after startup.
    assert preflight(rpc,cfg,tmp_path)['additional_balance_needed_wei']==0
    setup_bond(rpc,cfg,None,tmp_path)
    assert rpc.bonded==0 and len(sent)==1 and legacy.read_bytes()==before


def test_consumed_legacy_bond_does_not_increase_new_authorization(allocation,tmp_path):
    cfg,rpc,_,sent=allocation;rpc.bonded=0
    assert preflight(rpc,cfg,tmp_path)['deposit_wei']==ADDITIONAL
    setup_bond(rpc,cfg,None,tmp_path)
    assert rpc.bonded==ADDITIONAL and sent[0][1]==ADDITIONAL


@pytest.mark.parametrize('bonded',[TARGET,TARGET+1,TARGET-123])
def test_external_funding_reduces_allocation_and_zero_is_durable(allocation,tmp_path,bonded):
    cfg,rpc,_,sent=allocation;rpc.bonded=bonded
    setup_bond(rpc,cfg,None,tmp_path)
    plan=json.loads((tmp_path/'atlas-allocation-test-allocation.json').read_text())
    assert plan['deposit_wei']==max(0,TARGET-bonded)
    rpc.bonded=0
    setup_bond(rpc,cfg,None,tmp_path)
    assert rpc.bonded==0 and len(sent)==int(bonded<TARGET)


def test_fresh_account_gets_target_once_without_legacy_double_funding(allocation,tmp_path):
    cfg,rpc,legacy,sent=allocation;legacy.unlink();rpc.bonded=0
    assert preflight(rpc,cfg,tmp_path)['deposit_wei']==TARGET
    setup_bond(rpc,cfg,None,tmp_path)
    assert rpc.bonded==TARGET and not legacy.exists()
    rpc.bonded=0
    setup_bond(rpc,cfg,None,tmp_path)
    assert rpc.bonded==0 and len(sent)==1


def test_interruption_preserves_amount_before_transaction_is_created(allocation,tmp_path,monkeypatch):
    cfg,rpc,legacy,sent=allocation
    submit=atlas_onboarding.submit_setup_step
    def interrupted(*args,**kwargs):
        if args[3]!=legacy:raise TimeoutError('interrupted before signing')
        return submit(*args,**kwargs)
    monkeypatch.setattr(atlas_onboarding,'submit_setup_step',interrupted)
    with pytest.raises(TimeoutError):setup_bond(rpc,cfg,None,tmp_path)
    plan_path=tmp_path/'atlas-allocation-test-allocation.json';planned=plan_path.read_bytes()
    rpc.bonded=0
    assert preflight(rpc,cfg,tmp_path)['deposit_wei']==ADDITIONAL
    monkeypatch.setattr(atlas_onboarding,'submit_setup_step',submit)
    setup_bond(rpc,cfg,None,tmp_path)
    assert sent[0][1]==ADDITIONAL and plan_path.read_bytes()==planned


def test_interruption_after_submission_reuses_one_intent(allocation,tmp_path,monkeypatch):
    cfg,rpc,legacy,sent=allocation
    submit=atlas_onboarding.submit_setup_step
    def interrupted(*args,**kwargs):
        result=submit(*args,**kwargs)
        if args[3]!=legacy:raise TimeoutError('lost response')
        return result
    monkeypatch.setattr(atlas_onboarding,'submit_setup_step',interrupted)
    with pytest.raises(TimeoutError):setup_bond(rpc,cfg,None,tmp_path)
    assert len(sent)==1 and preflight(rpc,cfg,tmp_path)['deposit_wei']==0
    monkeypatch.setattr(atlas_onboarding,'submit_setup_step',submit)
    setup_bond(rpc,cfg,None,tmp_path)
    assert len(sent)==1 and rpc.bonded==TARGET


def test_unconfirmed_legacy_step_must_finish_before_increase(allocation,tmp_path,monkeypatch):
    cfg,rpc,legacy,sent=allocation
    def pending(*args,**kwargs):
        assert args[3]==legacy
        raise TimeoutError('original deposit pending')
    monkeypatch.setattr(atlas_onboarding,'submit_setup_step',pending)
    with pytest.raises(TimeoutError):setup_bond(rpc,cfg,None,tmp_path)
    assert list(tmp_path.iterdir())==[legacy] and not sent


@pytest.mark.parametrize('change',['target','amount','owner','id','missing_plan','over_budget'])
def test_changed_or_corrupt_allocation_rejected_without_another_deposit(allocation,tmp_path,change):
    cfg,rpc,_,sent=allocation
    setup_bond(rpc,cfg,None,tmp_path)
    path=tmp_path/'atlas-allocation-test-allocation.json'
    if change=='target':cfg['structural']['atlas_bond_wei']=str(TARGET-1)
    elif change=='amount':cfg['structural']['atlas_bond_increase']['max_additional_wei']='1'
    elif change=='owner':cfg['owner']='0x'+'ab'*20
    elif change=='id':cfg['structural']['atlas_bond_increase']['id']='../bad'
    elif change=='missing_plan':path.unlink()
    else:
        plan=json.loads(path.read_text());plan['deposit_wei']=TARGET;path.write_text(json.dumps(plan))
    with pytest.raises(Reject):preflight(rpc,cfg,tmp_path)
    assert len(sent)==1


def test_disabled_atlas_never_allocates(allocation,tmp_path):
    cfg,rpc,legacy,sent=allocation;cfg['structural']['atlas_enabled']=False
    setup_bond(rpc,cfg,None,tmp_path)
    assert preflight(rpc,cfg,tmp_path)['additional_balance_needed_wei']==0
    assert list(tmp_path.iterdir())==[legacy] and not sent
