import importlib.util
import json
import time
from pathlib import Path
from searcher.structural_runner import publish_decision

def test_new_strategy_activity_and_current_executor_are_visible(tmp_path,monkeypatch):
    path=Path(__file__).resolve().parents[1]/'tools/live-view/view.py'
    spec=importlib.util.spec_from_file_location('structural_status_view',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    config={'chain_id':8453,'owner':'0x'+'11'*20,'structural':{'enabled':True}}
    (tmp_path/'config').mkdir();(tmp_path/'config/base.json').write_text(json.dumps(config))
    session=tmp_path/'evidence/session-test';out=session/'pipeline';out.mkdir(parents=True)
    (tmp_path/'evidence/runtime.json').write_text(json.dumps({'session':str(session),'state':'running','executor_connected':True}))
    (out/'health.json').write_text(json.dumps({'state':'running','strategy_mode':'structural','executor_connected':True}))
    publish_decision(out,{'at':time.time(),'candidates':[],'outcomes':[]},1)
    account=tmp_path/'evidence'/('account-8453-'+config['owner']);account.mkdir()
    (account/'installation-structural.json').write_text(json.dumps({'contract':'0x'+'22'*20}))
    monkeypatch.setattr(module,'active_lock',lambda _:True)
    state=module.Reader(tmp_path).snapshot()
    assert state['state']=='running' and state['last_data_at'] and '临界借款' in state['activity']
    assert state['contract']=='0x'+'22'*20 and state['executor_connected']

def test_submitted_record_uses_final_fee_not_initial_model(tmp_path):
    candidate={'route_key':'a','gross_surplus':999,'model_fee_raw':1,'tokens':[]}
    d={'at':time.time(),'candidates':[candidate],'outcomes':[{'route_key':'a','status':'submitted','simulated_gross_profit':900,'fee_in_settlement':100}]}
    publish_decision(tmp_path,d,1)
    saved=json.loads((tmp_path/'decision-1.json').read_text())
    assert saved['candidates'][0]['net_after_modelled_cost']==800
    assert saved['candidates'][0]['status']=='submitted'
