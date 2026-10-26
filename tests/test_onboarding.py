import copy
import json
import stat

from eth_account import Account
import pytest

from searcher.core import Reject, load_config, number
from searcher.onboarding import save_wallet, funding_report, setup_executor, submit_setup_step, verify_contract, SetupPending, preflight_setup
from test_execution import market


def test_launch_checks_network_before_unlocking_wallet(monkeypatch, tmp_path):
    import sys
    import searcher.execution as execution
    import searcher.onboarding as onboarding
    from searcher.__main__ import main
    import searcher.service as service
    config = load_config(); config['owner'] = Account.create().address.lower()
    monkeypatch.setattr('searcher.__main__.load_config', lambda path: config)
    monkeypatch.setattr(service, 'ROOT', tmp_path)
    closed = []
    class WrongNetwork:
        def __init__(self, **kwargs): pass
        def verify_chain(self): raise Reject('wrong chain')
        def close(self): closed.append(True)
    def unexpected_unlock(*args):
        pytest.fail('Wallet password must not be requested before network preflight')
    monkeypatch.setattr(execution, 'BroadcastRPC', WrongNetwork)
    monkeypatch.setattr(onboarding, 'unlock_wallet', unexpected_unlock)
    monkeypatch.setattr(sys, 'argv', ['searcher', 'launch'])
    with pytest.raises(Reject, match='wrong chain'):
        main()
    assert closed == [True]


def local_setup_config(config, index):
    config = copy.deepcopy(config)
    pool = next(iter(index.states.values()))
    config['factories'] = [{'address': pool.factory, 'kind': 'v3'}]
    return config


def test_incompatible_execution_context_is_rejected_before_wallet_unlock(monkeypatch, tmp_path):
    import sys
    import searcher.execution as execution
    import searcher.onboarding as onboarding
    import searcher.service as service
    from searcher.__main__ import main
    config = load_config(); config['owner'] = Account.create().address.lower()
    monkeypatch.setattr('searcher.__main__.load_config', lambda path: config)
    monkeypatch.setattr(service, 'ROOT', tmp_path)
    class Reachable:
        def __init__(self, **kwargs): pass
        def verify_chain(self): pass
        def close(self): pass
    def incompatible(rpc): raise Reject('执行节点的 pending 区块校验不兼容')
    def unexpected_unlock(*args): pytest.fail('Incompatible endpoint must be caught before password input')
    monkeypatch.setattr(execution, 'BroadcastRPC', Reachable)
    monkeypatch.setattr(execution, 'verify_execution_context', incompatible)
    monkeypatch.setattr(onboarding, 'unlock_wallet', unexpected_unlock)
    monkeypatch.setattr(sys, 'argv', ['searcher', 'launch'])
    with pytest.raises(Reject, match='区块校验不兼容'): main()


def test_wallet_encrypts_only_matching_owner_and_refuses_overwrite(tmp_path):
    signer = Account.create()
    config = load_config(); config['owner'] = signer.address.lower()
    path = tmp_path/'wallets/owner.json'
    result = save_wallet(config, signer.key, 'a-local-test-password', path)
    assert result['broadcast'] is False
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert signer.key.hex() not in path.read_text()
    assert Account.from_key(Account.decrypt(path.read_text(), 'a-local-test-password')).address == signer.address
    with pytest.raises(FileExistsError):
        save_wallet(config, signer.key, 'a-local-test-password', path)
    with pytest.raises(Reject, match='公开地址不一致'):
        save_wallet(config, Account.create().key, 'a-local-test-password', tmp_path/'wrong.json')
    assert not (tmp_path/'wrong.json').exists()


def test_readonly_funding_check_does_not_send(market, monkeypatch):
    rpc, config, _, _, _, index, _, _, _ = market
    config = local_setup_config(config, index)
    original = rpc.request
    def request(method, params):
        assert method != 'eth_sendRawTransaction'
        return original(method, params)
    monkeypatch.setattr(rpc, 'request', request)
    result = funding_report(rpc, config)
    assert result['broadcast'] is False
    assert result['deployment_fee_budget_wei'] > 0
    assert result['suggested_topup_wei'] == 0


def test_setup_deploys_verifies_unpauses_and_resumes_without_new_transactions(market, monkeypatch):
    rpc, config, _, signer, directory, index, _, _, _ = market
    config = local_setup_config(config, index)
    original = rpc.request
    sent = []
    def request(method, params):
        result = original(method, params)
        if method == 'eth_sendRawTransaction':
            sent.append(result)
            original('evm_mine', [])
        return result
    monkeypatch.setattr(rpc, 'request', request)
    out = directory/'setup'
    first = setup_executor(rpc, config, signer, out, require_validation=False)
    assert first['paused'] is False and first['trading_started'] is False
    assert len(sent) == 2
    nonce = number(rpc.request('eth_getTransactionCount', [signer.address, 'latest']))
    second = setup_executor(rpc, config, signer, out, require_validation=False)
    assert first['contract'] == second['contract']
    assert len(sent) == 2
    assert nonce == number(rpc.request('eth_getTransactionCount', [signer.address, 'latest']))
    wrong = copy.deepcopy(config)
    wrong['settlement_tokens'][0]['max_input_raw'] = '1'
    with pytest.raises(Reject, match='规模上限'):
        verify_contract(rpc, wrong, first['contract'])


def test_saved_receipt_access_is_checked_before_unlock_and_never_resends(market, monkeypatch):
    import sys
    import searcher.__main__ as cli
    import searcher.execution as execution
    import searcher.onboarding as onboarding
    import searcher.service as service
    from searcher.core import RPCAccessDenied
    rpc, config, _, signer, directory, index, _, _, _ = market
    config = local_setup_config(config, index)
    original = rpc.request
    sent, requested = [], []
    access = {'mode': 'available'}
    def request(method, params):
        if method == 'eth_getTransactionReceipt':
            requested.append(params[0])
            if access['mode'] == 'denied': raise RPCAccessDenied('节点访问被拒绝（HTTP 403）。历史交易查询需要个人令牌。')
            if access['mode'] == 'missing': return None
        result = original(method, params)
        if method == 'eth_sendRawTransaction':
            sent.append(result); original('evm_mine', [])
        return result
    monkeypatch.setattr(rpc, 'request', request)
    account_dir = directory/'setup'
    installed = setup_executor(rpc, config, signer, account_dir, require_validation=False)
    before = {p.name: p.read_bytes() for p in account_dir.glob('*.json')}
    requested.clear()
    ready = preflight_setup(rpc, config, account_dir, require_validation=False)
    assert ready['state'] == 'ready' and ready['contract'] == installed['contract']
    assert set(requested) == set(sent) and len(sent) == 2
    access['mode'] = 'missing'
    assert preflight_setup(rpc, config, account_dir, require_validation=False)['state'] == 'setup_pending'
    assert len(sent) == 2
    access['mode'] = 'denied'
    monkeypatch.setattr(cli, 'load_config', lambda path: config)
    monkeypatch.setattr(service, 'ROOT', directory)
    monkeypatch.setattr(execution, 'BroadcastRPC', lambda **kwargs: rpc)
    monkeypatch.setattr(onboarding, 'preflight_setup', lambda r,c: preflight_setup(r,c,account_dir,require_validation=False))
    def never_unlock(*args): pytest.fail('Historical receipt access must be checked before requesting wallet password')
    monkeypatch.setattr(onboarding, 'unlock_wallet', never_unlock)
    monkeypatch.setattr(sys, 'argv', ['searcher', 'launch'])
    with pytest.raises(RPCAccessDenied, match='403'): cli.main()
    assert len(sent) == 2
    assert {p.name: p.read_bytes() for p in account_dir.glob('*.json')} == before


def test_lost_submission_response_recovers_same_deployment_hash(market, monkeypatch):
    from searcher.artifacts import deployment
    rpc, config, _, signer, directory, index, _, _, _ = market
    config = local_setup_config(config, index)
    original = rpc.request
    sent = []
    def request(method, params):
        result = original(method, params)
        if method == 'eth_sendRawTransaction':
            sent.append(result)
            original('evm_mine', [])
            raise TimeoutError('local simulated lost response after acceptance')
        return result
    monkeypatch.setattr(rpc, 'request', request)
    calldata = deployment(config)['creation_data']
    journal = directory/'lost-response.json'
    receipt = submit_setup_step(rpc, config, signer, journal, calldata, wait_seconds=2)
    assert len(sent) == 1 and receipt['transactionHash'] == sent[0]
    again = submit_setup_step(rpc, config, signer, journal, calldata, wait_seconds=2)
    assert again['transactionHash'] == receipt['transactionHash'] and len(sent) == 1
    record = json.loads(journal.read_text()); record['transaction']['value'] = 1
    journal.write_text(json.dumps(record))
    with pytest.raises(Reject, match='交易与签名不一致'):
        submit_setup_step(rpc, config, signer, journal, calldata, wait_seconds=0)
    assert len(sent) == 1


def test_setup_broadcast_permission_denial_preserves_exact_intent_and_surfaces(market, monkeypatch):
    from types import SimpleNamespace
    from searcher.artifacts import deployment
    from searcher.core import RPCAccessDenied
    rpc, config, _, signer, directory, index, _, _, _ = market
    config = local_setup_config(config, index)
    original = rpc.request
    submissions = []
    def denied_send(method, params):
        if method == 'eth_sendRawTransaction':
            submissions.append(params[0])
            raise RPCAccessDenied('节点访问被拒绝（HTTP 403）。')
        return original(method, params)
    monkeypatch.setattr(rpc, 'request', denied_send)
    journal = directory/'denied-send.json'
    calldata = deployment(config)['creation_data']
    nonce = original('eth_getTransactionCount', [signer.address, 'latest'])
    with pytest.raises(RPCAccessDenied, match='403'):
        submit_setup_step(rpc, config, signer, journal, calldata, wait_seconds=0)
    saved = journal.read_bytes()
    # A retry needs only public identity: it reuses the saved signature and nonce.
    with pytest.raises(RPCAccessDenied, match='403'):
        submit_setup_step(rpc, config, SimpleNamespace(address=signer.address), journal, calldata, wait_seconds=0)
    assert len(submissions) == 2 and submissions[0] == submissions[1]
    assert journal.read_bytes() == saved
    assert original('eth_getTransactionCount', [signer.address, 'latest']) == nonce


def test_empty_wallet_and_wrong_signer_never_broadcast(market, monkeypatch):
    from searcher.artifacts import deployment
    rpc, config, _, signer, directory, index, _, _, _ = market
    config = local_setup_config(config, index)
    calldata = deployment(config)['creation_data']
    with pytest.raises(Reject, match='签名账户'):
        submit_setup_step(rpc, config, Account.create(), directory/'bad-owner.json', calldata, wait_seconds=0)
    rpc.request('anvil_setBalance', [signer.address, '0x0'])
    original = rpc.request
    def request(method, params):
        assert method != 'eth_sendRawTransaction'
        return original(method, params)
    monkeypatch.setattr(rpc, 'request', request)
    with pytest.raises(Reject):
        submit_setup_step(rpc, config, signer, directory/'empty-wallet.json', calldata, wait_seconds=0)
    assert not (directory/'empty-wallet.json').exists()


def test_setup_waits_for_receipts_ahead_of_blocks_for_both_transactions(market, monkeypatch):
    """Reproduce Base preconfirmation/endpoint lag without spending real funds."""
    import httpx
    rpc, config, _, signer, directory, index, _, _, _ = market
    config = local_setup_config(config, index)
    original = rpc.request
    sent, polls = [], {}
    active = {'hash': None, 'height': None}
    missing_blocks = []

    def request(method, params):
        result = original(method, params)
        if method == 'eth_sendRawTransaction':
            sent.append(result)
            original('evm_mine', [])
        if method == 'eth_getTransactionReceipt' and result:
            tx_hash = result['transactionHash']
            polls[tx_hash] = polls.get(tx_hash, 0)+1
            active.update(hash=tx_hash, height=number(result['blockNumber']))
            if polls[tx_hash] == 1:
                return {**result, 'blockNumber': None, 'blockHash': None}
            if polls[tx_hash] == 4:
                return {**result, 'blockHash': '0x'+'11'*32}
            if polls[tx_hash] == 5:
                raise httpx.ReadTimeout('local test: temporary receipt read failure')
            if polls[tx_hash] == 6:
                from searcher.core import RPCUnavailable
                raise RPCUnavailable('RPC total request deadline exceeded')
        if method == 'eth_getBlockByNumber' and active['hash']:
            poll = polls[active['hash']]
            if poll == 2 and params[0] == 'latest':
                return {**result, 'number': hex(active['height'])}
            if poll == 3 and params[0] == hex(active['height']):
                missing_blocks.append(active['hash'])
                return None
        return result

    monkeypatch.setattr(rpc, 'request', request)
    out = directory/'delayed-setup'
    result = setup_executor(rpc, config, signer, out, require_validation=False)
    assert result['paused'] is False
    assert len(sent) == 2 and set(missing_blocks) == set(sent)
    assert all(polls[tx_hash] >= 7 for tx_hash in sent)
    for name in ('setup-deploy.json', 'setup-unpause.json'):
        record = json.loads((out/name).read_text())
        assert number(record['receipt']['status']) == 1
        assert record['receipt']['blockHash'] != '0x'+'11'*32


def test_missing_block_deadline_preserves_deployment_and_resumes_without_signing(market, monkeypatch):
    from types import SimpleNamespace
    from searcher.artifacts import deployment
    rpc, config, _, signer, directory, index, _, _, _ = market
    config = local_setup_config(config, index)
    original = rpc.request
    sent = []
    unavailable = {'enabled': False}

    def request(method, params):
        result = original(method, params)
        if method == 'eth_sendRawTransaction':
            sent.append(result)
            original('evm_mine', [])
        if method == 'eth_getBlockByNumber' and params[0].startswith('0x') and unavailable['enabled']:
            return None
        return result

    monkeypatch.setattr(rpc, 'request', request)
    journal = directory/'pending-deploy.json'
    calldata = deployment(config)['creation_data']
    # Persist/broadcast one transaction, then end this invocation before confirmation.
    with pytest.raises(SetupPending):
        submit_setup_step(rpc, config, signer, journal, calldata, wait_seconds=0)
    assert len(sent) == 1
    raw = json.loads(journal.read_text())['raw_transaction']
    unavailable['enabled'] = True
    public_identity = SimpleNamespace(address=signer.address)  # No signing key or sign method.
    with pytest.raises(SetupPending):
        submit_setup_step(rpc, config, public_identity, journal, calldata, wait_seconds=0)
    assert len(sent) == 1 and json.loads(journal.read_text())['raw_transaction'] == raw
    unavailable['enabled'] = False
    receipt = submit_setup_step(rpc, config, public_identity, journal, calldata, wait_seconds=0)
    assert receipt['transactionHash'] == sent[0] and len(sent) == 1


def test_launch_unlocks_deploys_trades_accounts_stops_and_resumes(market, monkeypatch):
    """Exercise the CLI through EVM settlement; use only a synthetic local market."""
    import sqlite3
    import sys
    import time
    import searcher.__main__ as cli
    import searcher.execution as execution
    import searcher.onboarding as onboarding
    import searcher.pipeline as pipeline
    import searcher.runner as runner
    import searcher.service as service
    from searcher.core import RPC, atomic_json
    from searcher.pools import Pool
    from searcher.statefeed import StateFrame
    from test_execution import LocalRPC
    rpc, config, _, signer, directory, index, _, _, _ = market
    config = local_setup_config(config, index)
    config['policy']['universe_refresh_seconds'] = 100
    account_dir = directory/'evidence'/f"account-{config['chain_id']}-{config['owner']}"
    password = 'local-integration-test-password'
    keystore = directory/'wallet/owner.json'
    save_wallet(config, signer.key, password, keystore)
    config_path = directory/'config.json'
    atomic_json(config_path, config)
    sent, closed, prompts, transitions = [], [], [], []

    class LaunchRPC(LocalRPC):
        def __init__(self, **kwargs): super().__init__(rpc.url)
        def request(self, method, params):
            value = super().request(method, params)
            if method == 'eth_sendRawTransaction':
                sent.append(value)
                super().request('evm_mine', [])
            return value
        def close(self):
            super().close(); closed.append(True)

    class LocalFeed:
        def __init__(self, feed_rpc, config, saved):
            assert feed_rpc.url.startswith('http://127.0.0.1:')
            self.rpc, self.pools = feed_rpc, [Pool.from_json(p) for p in saved['pools']]
        def poll(self, **kwargs):
            now = time.monotonic_ns(); h = self.rpc.header()
            return StateFrame(h, h, [p.json() for p in self.pools], now, time.monotonic_ns(),
                              time.time(), number(h['number']), number(h['timestamp']))

    def local_setup(setup_rpc, c, unlocked):
        # Isolate local journals; release-manifest binding is exercised by validate.
        return setup_executor(setup_rpc, c, unlocked, account_dir, require_validation=False)
    def local_preflight(setup_rpc, c):
        return preflight_setup(setup_rpc, c, account_dir, require_validation=False)
    def password_prompt(prompt):
        prompts.append(prompt)
        return password
    def prepare(feed_rpc, c, blocks, previous=None):
        return {'header': feed_rpc.header(), 'pools': [p.json() for p in index.states.values()]}
    def stop_shadow():
        assert json.loads((account_dir/'installation.json').read_text())['paused'] is False
        transitions.append('shadow_stopped_after_executor_ready')
        return {'process_running': False}
    reconcile = execution.Executor.reconcile
    def stop_after_settlement(executor):
        reconcile(executor)
        if executor.db.execute("SELECT 1 FROM attempts WHERE state='success'").fetchone():
            executor.context_stop.set()

    monkeypatch.setattr(cli, 'ROOT', directory)
    monkeypatch.setattr(runner, 'ROOT', directory)
    monkeypatch.setattr(service, 'ROOT', directory)
    monkeypatch.setattr(execution, 'BroadcastRPC', LaunchRPC)
    monkeypatch.setattr(onboarding, 'setup_executor', local_setup)
    monkeypatch.setattr(onboarding, 'preflight_setup', local_preflight)
    monkeypatch.setattr(onboarding, 'hidden_input', password_prompt)
    monkeypatch.setattr(runner, 'RPC', lambda *args, **kwargs: RPC(rpc.url, **kwargs))
    monkeypatch.setattr(runner, 'prepare', prepare)
    monkeypatch.setattr(pipeline, 'StateFeed', LocalFeed)
    monkeypatch.setattr(execution.Executor, 'reconcile', stop_after_settlement)
    monkeypatch.setattr(service, 'runner_status', lambda: {'process_running': not transitions, 'executor_connected': False})
    monkeypatch.setattr(service, 'stop_runner', stop_shadow)
    monkeypatch.setattr(sys, 'argv', ['searcher', '--config', str(config_path), 'launch',
                                    '--keystore', str(keystore), '--seconds', '5'])
    cli.main()
    with sqlite3.connect(account_dir/'transactions.sqlite') as db:
        state, outcome = db.execute('SELECT state,outcome FROM attempts JOIN intents ON attempts.id=intents.attempt').fetchone()
        assert state == 'success' and json.loads(outcome)['net_profit_raw'] > 0
    runtime = json.loads((directory/'evidence/runtime.json').read_text())
    assert runtime['state'] == 'stopped' and runtime['reader_stopped'] and runtime['discovery_stopped']
    assert not runtime['worker_errors'] and runtime['executor_connected'] is False
    assert len(sent) == 3 and transitions == ['shadow_stopped_after_executor_ready']
    # Same CLI entry after shutdown: decrypt, reuse deployment, reconcile and close.
    cli.main()
    assert len(sent) == 3 and len(closed) == 2 and len(prompts) == 2
    assert number(rpc.request('eth_getTransactionCount', [signer.address, 'pending'])) == number(
        rpc.request('eth_getTransactionCount', [signer.address, 'latest']))

def test_cap_migration_preserves_legacy_journals_pauses_old_and_resumes_new(market,monkeypatch):
    from searcher.onboarding import setup_location
    rpc,config,_,signer,directory,index,_,_,_=market
    config=local_setup_config(config,index)
    config['policy'].pop('cap_migration',None)
    config['policy'].pop('executor_migration',None)
    original=rpc.request;sent=[]
    def request(method,params):
        result=original(method,params)
        if method=='eth_sendRawTransaction':sent.append(result);original('evm_mine',[])
        return result
    monkeypatch.setattr(rpc,'request',request)
    out=directory/'migration'
    old=setup_executor(rpc,config,signer,out,require_validation=False)
    journals={p.name:p.read_bytes() for p in out.glob('setup-*.json')}
    newer=copy.deepcopy(config);newer['policy']['cap_migration']='liquidity-v2'
    newer['settlement_tokens'][0]['max_input_raw']=str((1<<255)-1)
    state=preflight_setup(rpc,newer,out,require_validation=False)
    assert state['state']=='needs_cap_migration' and state['previous_contract']==old['contract']
    assert len(sent)==2
    current=setup_executor(rpc,newer,signer,out,require_validation=False)
    assert current['contract']!=old['contract'] and current['previous_contract']==old['contract']
    assert verify_contract(rpc,config,old['contract'])['paused']
    assert not verify_contract(rpc,newer,current['contract'])['paused']
    assert {p.name:p.read_bytes() for p in out.glob('setup-*.json')}==journals
    assert len(sent)==5 # old deploy/start, old pause, new deploy/start
    assert preflight_setup(rpc,newer,out,require_validation=False)['state']=='ready'
    assert setup_executor(rpc,newer,signer,out,require_validation=False)['contract']==current['contract']
    assert len(sent)==5
    disabled=copy.deepcopy(newer);disabled['policy'].pop('cap_migration')
    assert setup_location(disabled,out)[1] is None  # Already installed namespace remains reusable.
    disabled['settlement_tokens'][0]['max_input_raw'] = str(int(disabled['settlement_tokens'][0]['max_input_raw'])-1)
    with pytest.raises(Reject,match='其他配置'):setup_location(disabled,out)


def test_migration_rejects_unreconciled_account_without_new_signature(tmp_path):
    import sqlite3
    from searcher.onboarding import ensure_migration_account_idle
    with sqlite3.connect(tmp_path/'transactions.sqlite') as db:
        db.execute('CREATE TABLE attempts (state TEXT)');db.execute("INSERT INTO attempts VALUES ('submitted')")
    with pytest.raises(Reject,match='未结算'):ensure_migration_account_idle(tmp_path)


@pytest.mark.parametrize('cut_at',[3,4,5])
def test_migration_recovers_after_broadcast_before_successor(market,monkeypatch,cut_at):
    rpc,config,_,signer,directory,index,*_=market
    config=local_setup_config(config,index)
    config['policy'].pop('cap_migration',None)
    config['policy'].pop('executor_migration',None)
    real_request=rpc.request
    sent=[]
    blocked=[False]
    def request(method,params):
        result=real_request(method,params)
        if method=='eth_sendRawTransaction':
            sent.append(result)
            if len(sent)==cut_at and not blocked[0]:
                blocked[0]=True
            else:
                real_request('evm_mine',[])
        return result
    monkeypatch.setattr(rpc,'request',request)
    import searcher.onboarding as onboarding
    real_submit=onboarding.submit_setup_step
    def bounded(*args,**kwargs):
        kwargs['wait_seconds']=2
        return real_submit(*args,**kwargs)
    monkeypatch.setattr(onboarding,'submit_setup_step',bounded)
    account_dir=directory/'migration-audit'
    old=setup_executor(rpc,config,signer,account_dir,require_validation=False)
    saved={p.name:p.read_bytes() for p in account_dir.glob('setup-*.json')}
    new=copy.deepcopy(config)
    new['policy']['cap_migration']='liquidity-v2'
    new['settlement_tokens'][0]['max_input_raw']=str((1<<255)-1)
    with pytest.raises(SetupPending):
        setup_executor(rpc,new,signer,account_dir,require_validation=False)
    assert len(sent)==cut_at
    real_request('evm_mine',[])
    ready=setup_executor(rpc,new,signer,account_dir,require_validation=False)
    assert len(sent)==5 and len(set(sent))==5
    assert verify_contract(rpc,config,old['contract'])['paused']
    assert not verify_contract(rpc,new,ready['contract'])['paused']
    assert saved=={p.name:p.read_bytes() for p in account_dir.glob('setup-*.json')}
    assert preflight_setup(rpc,new,account_dir,require_validation=False)['state']=='ready'
    # A new-deployment attempt must reach Executor reconciliation on restart.
    from searcher.ledger import RiskLedger
    ledger=RiskLedger(account_dir/'transactions.sqlite',10**18,3)
    ledger.reserve('independent-new-deployment-pending',1)
    ledger.submitted('independent-new-deployment-pending','0x'+'11'*32)
    ledger.close()
    assert preflight_setup(rpc,new,account_dir,require_validation=False)['state']=='ready'
    class IdentityOnly:
        address=signer.address
        def sign_transaction(self,*_):
            pytest.fail('restart must not sign again')
    resumed=setup_executor(rpc,new,IdentityOnly(),account_dir,require_validation=False)
    assert resumed['contract']==ready['contract'] and len(sent)==5


def test_executor_version_migration_pauses_verified_legacy_and_reuses_new(market,monkeypatch):
    from searcher.onboarding import setup_location
    rpc,config,_,signer,directory,index,_,_,_=market
    current=local_setup_config(config,index)
    old=copy.deepcopy(current);old['_executor_version']='cl-v1';old['policy'].pop('executor_migration',None)
    original=rpc.request;sent=[]
    def request(method,params):
        answer=original(method,params)
        if method=='eth_sendRawTransaction':sent.append(answer);original('evm_mine',[])
        return answer
    monkeypatch.setattr(rpc,'request',request)
    base=directory/'version-migration'
    prior=setup_executor(rpc,old,signer,base,require_validation=False)
    old_journal=(base/'setup-deploy.json').read_bytes()
    report=preflight_setup(rpc,current,base,require_validation=False)
    assert report['previous_contract']==prior['contract']
    new=setup_executor(rpc,current,signer,base,require_validation=False)
    assert new['contract']!=prior['contract'] and new['previous_contract']==prior['contract']
    assert verify_contract(rpc,old,prior['contract'])['paused']
    assert (base/'setup-deploy.json').read_bytes()==old_journal
    before=len(sent)
    assert setup_executor(rpc,current,signer,base,require_validation=False)['contract']==new['contract']
    assert len(sent)==before
    assert setup_location(current,base)[1] is None


@pytest.mark.parametrize('cut_at',[1,2,3])
def test_version_upgrade_resumes_after_each_broadcast_and_keeps_original_hash(market,monkeypatch,cut_at):
    rpc,c,_,signer,directory,index,_,_,_=market
    current=local_setup_config(c,index)
    previous=copy.deepcopy(current);previous['_executor_version']='cl-v1';previous['policy'].pop('executor_migration',None)
    original=rpc.request;sent=[];cut=[False]
    def request(method,params):
        answer=original(method,params)
        if method=='eth_sendRawTransaction':
            sent.append(answer);original('evm_mine',[])
            if cut[0] and len(sent)==2+cut_at:
                cut[0]=False
                raise TimeoutError('test crash after submission')
        return answer
    monkeypatch.setattr(rpc,'request',request)
    account_dir=directory/'version-recovery'
    old=setup_executor(rpc,previous,signer,account_dir,require_validation=False)
    cut[0]=True
    # A transport uncertainty may be resolved in the same call by checking the
    # durable tx hash; otherwise the next launch must resume that exact hash.
    try:setup_executor(rpc,current,signer,account_dir,require_validation=False)
    except (TimeoutError,SetupPending):pass
    installed=setup_executor(rpc,current,signer,account_dir,require_validation=False)
    assert installed['contract']!=old['contract']
    assert len(set(sent))==5  # legacy deploy+unpause, old pause, new deploy+unpause
    assert number(rpc.request('eth_getTransactionCount',[signer.address,'pending'])) == number(
        rpc.request('eth_getTransactionCount',[signer.address,'latest']))
    assert verify_contract(rpc,previous,old['contract'])['paused']


@pytest.mark.parametrize('stale_pointer',[False,True])
def test_version_migration_pauses_all_authenticated_history_even_if_pointer_is_stale(market,monkeypatch,stale_pointer):
    from searcher.core import data,atomic_json
    rpc,c,_,signer,directory,index,_,_,_=market
    config=local_setup_config(c,index)
    old=copy.deepcopy(config);old['_executor_version']='cl-v1';old['policy'].pop('executor_migration',None)
    original=rpc.request
    def request(method,params):
        result=original(method,params)
        if method=='eth_sendRawTransaction':original('evm_mine',[])
        return result
    monkeypatch.setattr(rpc,'request',request)
    base=directory/'history-migration';installed=[]
    configs=[]
    for scale in (1,2,3):
        version=copy.deepcopy(old);version['settlement_tokens'][0]['max_input_raw']=str(scale*10**20)
        configs.append(version)
        installed.append(setup_executor(rpc,version,signer,base,require_validation=False))
    # Emulate an orphan left by an older software bug; our current cap migration
    # itself must have paused B before creating C.
    assert verify_contract(rpc,configs[1],installed[1]['contract'])['paused']
    submit_setup_step(rpc,config,signer,base/'emulated-old-bug.json',data('setPaused(bool)',['bool'],[False]),installed[1]['contract'])
    if stale_pointer:atomic_json(base/'installation.json',installed[0])
    config['settlement_tokens'][0]['max_input_raw']=str(3*10**20)
    new=setup_executor(rpc,config,signer,base,require_validation=False)
    assert not new['paused']
    for previous,old_config in zip(installed,configs):
        assert verify_contract(rpc,old_config,previous['contract'])['paused']


@pytest.mark.parametrize('message', ['invalid sender', 'insufficient funds', 'max fee per gas less than block base fee', 'unknown transaction'])
def test_explicit_setup_send_rejection_is_not_converted_to_pending(market, monkeypatch, message):
    from types import SimpleNamespace
    from searcher.core import RPCError
    from searcher.artifacts import deployment
    rpc, config, _, signer, directory, index, _, _, _ = market
    config=local_setup_config(config,index);original=rpc.request;sent=[]
    def request(method,params):
        if method=='eth_sendRawTransaction':
            sent.append(params[0]);raise RPCError(method,{'code':-32000,'message':message})
        return original(method,params)
    monkeypatch.setattr(rpc,'request',request)
    journal=directory/'rejected-setup.json';calldata=deployment(config)['creation_data']
    with pytest.raises(RPCError,match=message):submit_setup_step(rpc,config,signer,journal,calldata,wait_seconds=0)
    saved=journal.read_bytes()
    with pytest.raises(RPCError,match=message):
        submit_setup_step(rpc,config,SimpleNamespace(address=signer.address),journal,calldata,wait_seconds=0)
    assert sent[0]==sent[1] and journal.read_bytes()==saved


@pytest.mark.parametrize('phase',['eth_sendRawTransaction','eth_getTransactionReceipt'])
@pytest.mark.parametrize('kind',['unsupported','local_protocol','http400'])
def test_setup_deterministic_transport_errors_do_not_become_pending(market,monkeypatch,phase,kind):
    import httpx
    from searcher.artifacts import deployment
    rpc, config, _, signer, directory, index, _, _, _ = market
    config=local_setup_config(config,index);original=rpc.request
    if kind=='unsupported':error=httpx.UnsupportedProtocol('bad scheme')
    elif kind=='local_protocol':error=httpx.LocalProtocolError('bad request')
    else:
        request=httpx.Request('POST','http://127.0.0.1')
        error=httpx.HTTPStatusError('bad request',request=request,response=httpx.Response(400,request=request))
    def request(method,params):
        if method==phase:raise error
        return original(method,params)
    monkeypatch.setattr(rpc,'request',request)
    journal=directory/'semantic-transport.json'
    with pytest.raises(type(error)):
        submit_setup_step(rpc,config,signer,journal,deployment(config)['creation_data'],wait_seconds=0)
    assert journal.exists()
