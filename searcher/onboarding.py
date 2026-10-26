
from .protocols import kind_id
"""Local wallet setup and resumable deployment; never used by the shadow runner."""
import copy
import sqlite3
import fcntl
import getpass
import json
import os
from pathlib import Path
import stat
import sys
import time
import warnings

import httpx
from eth_abi import decode
from eth_account import Account
from eth_account.typed_transactions import TypedTransaction
from eth_utils import keccak, to_checksum_address
from hexbytes import HexBytes

from .artifacts import deployment, source_manifest
from .core import ROOT, Reject, RPCError, RPCUnavailable, RPCAccessDenied, address, atomic_json, check, data, number, unhex
from .execution import FeeOracle, runtime_matches

INITIAL_ETH_WEI = 4_000_000_000_000_000  # Startup balance suggestion, not a return estimate.
SETUP_STEP_FEE_CAP = 100_000_000_000_000


class SetupPending(Reject):
    """The signed intent is preserved while canonical inclusion is unavailable."""


def wallet_path(config):
    return Path.home()/'.local/share/atomic-searcher/wallets'/f"{config['chain_id']}-{config['owner']}.json"


def account_directory(config):
    return ROOT/'evidence'/f"account-{config['chain_id']}-{config['owner']}"


def hidden_input(prompt):
    check(sys.stdin.isatty(), '请在本机终端打开此入口；不要通过聊天输入钱包资料。')
    with warnings.catch_warnings():
        warnings.simplefilter('error', getpass.GetPassWarning)
        return getpass.getpass(prompt)


def save_wallet(config, private_key, password, path):
    """Only encrypted data reaches disk; refuse a mismatched account or overwrite."""
    check(len(password) >= 12, '本机加密密码至少 12 个字符。')
    try:
        account = Account.from_key(private_key)
    except Exception:
        raise Reject('私钥格式无效。') from None
    check(account.address.lower() == config['owner'], '钱包地址与配置的公开地址不一致；没有保存。')
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    encrypted = Account.encrypt(account.key, password)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as output:
        json.dump(encrypted, output)
        output.flush(); os.fsync(output.fileno())
    return {'owner': account.address, 'keystore': str(path), 'encrypted': True, 'broadcast': False}


def import_wallet(config, path):
    check(not Path(path).exists(), '加密钱包文件已经存在；无需再次导入。')
    print('只导入本项目使用的独立地址。隐藏输入不会显示字符，也不会进入命令历史。')
    key = hidden_input('在本机粘贴该地址的私钥: ')
    password = hidden_input('设置本机加密密码（至少 12 字符）: ')
    check(password == hidden_input('再次输入本机加密密码: '), '两次密码不一致；没有保存。')
    try: return save_wallet(config, key.strip(), password, path)
    finally: del key, password


def unlock_wallet(config, path):
    path = Path(path)
    check(path.is_file() and not path.is_symlink(), '请先运行 python -m searcher wallet-import 设置本机加密钱包。')
    check(stat.S_IMODE(path.stat().st_mode) & 0o077 == 0, '钱包文件只应允许当前用户读取。')
    password = hidden_input('本机钱包加密密码: ')
    try:
        account = Account.from_key(Account.decrypt(path.read_text(), password))
    except Exception:
        raise Reject('无法解锁钱包，请检查密码和加密文件。') from None
    finally: del password
    check(account.address.lower() == config['owner'], '签名钱包与配置的公开地址不一致。')
    return account


def fee_fields(rpc, config):
    head = rpc.header()
    priority = config['policy']['min_priority_fee_wei']
    maximum = 2*number(head['baseFeePerGas']) + priority
    # Setup has no trading profit. Keep its separate unit-price budget.
    ceiling = config['policy'].get('max_setup_gas_price_wei', config['policy'].get('max_gas_price_wei', 100_000_000))
    check(maximum <= ceiling, '当前部署或设置交易的 gas 报价超过上限。')
    return {'maxFeePerGas': maximum, 'maxPriorityFeePerGas': priority}


def funding_report(rpc, config):
    rpc.verify_chain()
    balance, code, confirmed, pending = rpc.batch([
        ('eth_getBalance', [config['owner'], 'latest']),
        ('eth_getCode', [config['owner'], 'latest']),
        ('eth_getTransactionCount', [config['owner'], 'latest']),
        ('eth_getTransactionCount', [config['owner'], 'pending'])])
    check(code == '0x', '当前自动签名入口只支持普通独立账户；该地址有账户合约代码。')
    dep = deployment(config)
    fee = fee_fields(rpc, config)
    tx = {'from': config['owner'], 'value': '0x0', 'data': dep['creation_data'], 'gas': hex(6_000_000)}
    # Balance override only affects this read-only estimate; it never funds the wallet.
    gas = number(rpc.request('eth_estimateGas', [tx, 'latest', {config['owner']: {'balance': hex(10**18)}}]))
    gas_limit = (gas*120 + 99)//100
    check(gas_limit <= 6_000_000, '部署 gas 超过上限。')
    oracle = FeeOracle(rpc, config)
    cost = oracle.maximum_fee(dep['creation_data'], gas_limit, fee['maxFeePerGas'])
    unpause = oracle.maximum_fee(dep['unpause_calldata'], 60_000, fee['maxFeePerGas'])
    _, previous = setup_location(config, account_directory(config))
    pause = oracle.maximum_fee(dep['pause_calldata'], 60_000, fee['maxFeePerGas']) if previous else 0
    check(max(cost, unpause, pause) <= SETUP_STEP_FEE_CAP, '部署或启动费用超过单步上限。')
    return {'checked_at': time.time(), 'chain': 'Base', 'chain_id': config['chain_id'],
            'owner': to_checksum_address(config['owner']), 'balance_wei': number(balance),
            'balance_eth': str(number(balance)/10**18), 'nonce_latest': number(confirmed),
            'nonce_pending': number(pending), 'deployment_gas_limit': gas_limit,
            'deployment_fee_budget_wei': cost, 'unpause_fee_budget_wei': unpause,
            'previous_pause_fee_budget_wei': pause,
            'setup_fee_budget_eth': str((cost+unpause+pause)/10**18),
            'suggested_initial_balance_eth': '0.004',
            'suggested_topup_wei': max(0, INITIAL_ETH_WEI-number(balance)),
            'daily_trading_fee_cap_eth': str(int(config['policy']['max_daily_fee_wei'])/10**18),
            'keystore_file_present': wallet_path(config).is_file(),
            'source_manifest': source_manifest(), 'broadcast': False,
            'scope': 'Live read-only estimate with 20% deployment gas margin and L1/operator fee budgets. Fees refresh before signing. No loan principal deposit required.'}


def verify_contract(rpc, config, contract):
    contract = address(contract)
    from .artifacts import contract_artifact
    artifact = contract_artifact(config)
    check(runtime_matches(rpc.request('eth_getCode', [contract, 'latest']), artifact), '部署合约与当前编译版本不一致。')
    calls = [data('owner()'), data('chainId()'), data('paused()')]
    calls += [data('factoryKind(address)', ['address'], [f['address']]) for f in config['factories']]
    calls += [data('maxInput(address)', ['address'], [t['address']]) for t in config['settlement_tokens']]
    results = rpc.batch([('eth_call', [{'to': contract, 'data': d}, 'latest']) for d in calls])
    check(decode(['address'], unhex(results[0]))[0].lower() == config['owner'], '合约 owner 不一致。')
    check(number(results[1]) == config['chain_id'], '合约链编号不一致。')
    for f, value in zip(config['factories'], results[3:]):
        check(number(value) == (kind_id(f['kind'])), '合约工厂配置不一致。')
    for token, value in zip(config['settlement_tokens'], results[3+len(config['factories']):]):
        check(number(value) == int(token['max_input_raw']), '合约交易规模上限不一致。')
    return {'contract': contract, 'owner': config['owner'], 'chain_id': config['chain_id'], 'paused': bool(number(results[2]))}


def confirmed_setup_receipt(rpc, receipt, tx_hash, owner, target):
    """Flashblock receipts can arrive before their numbered block is readable."""
    if receipt is None:
        return None
    check(receipt['transactionHash'].lower() == tx_hash, '设置交易回执哈希不一致。')
    check(receipt['from'].lower() == owner and (receipt.get('to') or '').lower() == (target or ''),
          '设置交易回执账户或目标不一致。')
    if receipt.get('blockNumber') is None or receipt.get('blockHash') is None:
        return None
    height = number(receipt['blockNumber'])
    head = rpc.request('eth_getBlockByNumber', ['latest', False])
    # Wait for a sealed successor before requesting the receipt's numbered block.
    # A load-balanced endpoint can still return null afterwards; that is pending,
    # not a failed deployment and not grounds to create a replacement transaction.
    if not head or head.get('number') is None or number(head['number']) <= height:
        return None
    canonical = rpc.request('eth_getBlockByNumber', [hex(height), False])
    if not canonical:
        return None
    check(number(canonical['number']) == height, '设置交易回执区块高度不一致。')
    if canonical['hash'] != receipt['blockHash']:
        return None
    return receipt


def verify_setup_record(config, record, calldata, target=None, *, value=0):
    """Validate a saved public signed transaction without unlocking a wallet."""
    target = address(target) if target else None
    identity = {'chain_id': config['chain_id'], 'owner': config['owner'], 'target': target,
                'calldata_hash': '0x'+keccak(unhex(calldata)).hex()}
    if value:identity['value']=value
    check(record['identity'] == identity, '已有部署记录对应其他配置；停止重复部署。')
    tx, raw = record['transaction'], unhex(record['raw_transaction'])
    check(Account.recover_transaction(raw).lower() == config['owner'], '部署签名账户不一致。')
    decoded = TypedTransaction.from_bytes(HexBytes(raw))
    check(decoded.hash() == TypedTransaction.from_dict(tx).hash(), '保存的交易与签名不一致。')
    check(number(tx['chainId']) == config['chain_id'] and tx['value'] == value and
          tx['data'] == calldata and (tx.get('to') or '').lower() == (target or ''), '保存的设置交易目标不一致。')
    check('0x'+keccak(raw).hex() == record['tx_hash'], '设置交易哈希不一致。')


def require_current_validation():
    path = ROOT/'evidence/validation.json'
    check(path.is_file(), '当前版本尚未完成本机验证。')
    validation = json.loads(path.read_text())
    from .artifacts import compiled_manifest
    check(validation.get('passed') and validation.get('source_manifest') == source_manifest(), '当前版本尚未完成本机验证。')
    check(validation.get('artifact_manifest') == compiled_manifest(), '合约编译产物与已验证版本不一致。')


def setup_location(config, directory):
    """Only the explicitly enabled cap migration uses a new journal namespace.

    No old intent is edited. Decode and re-encode its constructor, and require
    exactly the same bytecode, owner, chain, factories and settlement assets.
    """
    directory = Path(directory)
    dep = deployment(config)
    if config.get('_migration_source') is not None:
        source = Path(config['_migration_source'])
        permitted = (directory, directory/'deployments'/dep['creation_data_hash'][2:])
        check(source in permitted, 'invalid previous deployment journal')
        return source, None
    legacy = directory/'setup-deploy.json'
    if not legacy.exists():
        return directory, None
    if config['policy'].get('executor_migration') == 'mixed-v1':
        return versioned_setup_location(config, directory)
    source = directory
    installation = directory/'installation.json'
    if installation.exists():
        value = json.loads(installation.read_text()).get('deployment_journal_directory')
        if value: source = Path(value)
    check(source == directory or source.parent == directory/'deployments', '旧部署记录路径不属于此账户。')
    record = json.loads((source/'setup-deploy.json').read_text())
    if record['identity']['calldata_hash'] == dep['creation_data_hash']:
        check(source in (directory, directory/'deployments'/dep['creation_data_hash'][2:]), '当前部署日志目录不一致。')
        verify_setup_record(config, record, dep['creation_data'])
        return source, None
    check(config['policy'].get('cap_migration') == 'liquidity-v2',
          '已有部署记录对应其他配置；停止重复部署。')
    from .artifacts import contract_artifact
    artifact = contract_artifact(config)
    bytecode = unhex(artifact['bytecode']['object'])
    creation = unhex(record['transaction']['data'])
    check(creation.startswith(bytecode), '旧部署代码不同，不能按额度迁移。')
    types = ['address', 'uint256', 'address[]', 'uint8[]', 'address[]', 'uint256[]']
    owner, chain, factories, kinds, tokens, caps = decode(types, creation[len(bytecode):])
    check(owner.lower() == config['owner'] and chain == config['chain_id'] and
          list(factories) == [f['address'] for f in config['factories']] and
          list(kinds) == [kind_id(f['kind']) for f in config['factories']] and
          list(tokens) == [t['address'] for t in config['settlement_tokens']] and
          len(caps) == len(tokens), '旧部署账户、工厂或结算资产不同，停止迁移。')
    previous = copy.deepcopy(config)
    previous['_migration_source'] = str(source)
    previous['policy'].pop('cap_migration', None)
    for token, cap in zip(previous['settlement_tokens'], caps):
        check(0 < cap <= int(token['max_input_raw']), '仅支持扩大既有额度的迁移。')
        token['max_input_raw'] = str(cap)
    old_dep = deployment(previous)
    check(source in (directory, directory/'deployments'/old_dep['creation_data_hash'][2:]), '旧部署日志目录与签名不一致。')
    verify_setup_record(previous, record, old_dep['creation_data'])
    target = directory/'deployments'/dep['creation_data_hash'][2:]
    return target, previous


def versioned_setup_location(config, directory):
    """Authenticate the installed predecessor before allocating a fresh journal.

    The predecessor's signed constructor and packaged legacy bytecode fix its
    identity. Its runtime and canonical receipt are checked by preflight_setup.
    """
    dep = deployment(config)
    target = directory/'deployments'/dep['creation_data_hash'][2:]
    source = directory
    installation = directory/'installation.json'
    if installation.exists():
        installed = json.loads(installation.read_text())
        raw_path = installed.get('deployment_journal_directory')
        if raw_path: source = Path(raw_path)
    check(source == directory or source.parent == directory/'deployments',
          '旧部署记录路径不属于此账户。')
    record = json.loads((source/'setup-deploy.json').read_text())
    if record['identity']['calldata_hash'] == dep['creation_data_hash']:
        verify_setup_record(config, record, dep['creation_data'])
        check(source in (directory, target), '当前部署日志目录不一致。')
        return source, None
    from .artifacts import contract_artifact
    previous = copy.deepcopy(config)
    previous['_executor_version'] = 'cl-v1'
    previous['_migration_source'] = str(source)
    previous['policy'].pop('executor_migration', None)
    previous['policy'].pop('cap_migration', None)
    previous['factories'] = [f for f in config['factories'] if f['kind'] != 'v2']
    bytecode = unhex(contract_artifact(previous)['bytecode']['object'])
    creation = unhex(record['transaction']['data'])
    check(creation.startswith(bytecode), '旧部署不是此升级支持的已验证合约版本。')
    types = ['address', 'uint256', 'address[]', 'uint8[]', 'address[]', 'uint256[]']
    owner, chain, factories, kinds, tokens, caps = decode(types, creation[len(bytecode):])
    check(owner.lower() == config['owner'] and chain == config['chain_id'] and
          list(factories) == [f['address'] for f in previous['factories']] and
          list(kinds) == [kind_id(f['kind']) for f in previous['factories']] and
          list(tokens) == [t['address'] for t in config['settlement_tokens']] and
          len(caps) == len(tokens), '旧部署账户、工厂或结算资产不同，停止迁移。')
    for token, cap in zip(previous['settlement_tokens'], caps):
        check(0 < cap <= int(token['max_input_raw']), '新版本额度低于旧部署，停止迁移。')
        token['max_input_raw'] = str(cap)
    old_dep = deployment(previous)
    check(source in (directory, directory/'deployments'/old_dep['creation_data_hash'][2:]),
          '旧部署日志目录与其签名不一致。')
    verify_setup_record(previous, record, old_dep['creation_data'])
    return target, previous


def historical_predecessors(rpc, config, directory):
    """Find authenticated old deployments, including a crash before installation.json.

    Signed journals are the inventory; installation.json is only a convenience
    pointer. No journal is edited and no unknown bytecode is trusted.
    """
    from .artifacts import contract_artifact
    directory = Path(directory)
    target_hash = deployment(config)['creation_data_hash']
    paths = [directory/'setup-deploy.json', *sorted((directory/'deployments').glob('*/setup-deploy.json'))]
    predecessors = []
    for path in paths:
        if not path.exists(): continue
        record = json.loads(path.read_text())
        if record['identity']['calldata_hash'] == target_hash: continue
        creation = unhex(record['transaction']['data'])
        previous = copy.deepcopy(config)
        previous['policy'].pop('executor_migration', None)
        previous['policy'].pop('cap_migration', None)
        previous['_migration_source'] = str(path.parent)
        previous['_executor_version'] = 'cl-v1'
        legacy = unhex(contract_artifact(previous)['bytecode']['object'])
        if creation.startswith(legacy):
            bytecode = legacy
            previous['factories'] = [f for f in config['factories'] if f['kind'] != 'v2']
        else:
            previous.pop('_executor_version')
            bytecode = unhex(contract_artifact(previous)['bytecode']['object'])
            check(creation.startswith(bytecode), '历史部署包含未知执行器代码，停止迁移。')
        types = ['address','uint256','address[]','uint8[]','address[]','uint256[]']
        owner,chain,factories,kinds,tokens,caps = decode(types, creation[len(bytecode):])
        check(owner.lower() == config['owner'] and chain == config['chain_id'] and
              list(factories) == [f['address'] for f in previous['factories']] and
              list(kinds) == [kind_id(f['kind']) for f in previous['factories']] and
              list(tokens) == [t['address'] for t in config['settlement_tokens']] and len(caps) == len(tokens),
              '历史部署参数不属于此账户的已验证升级范围。')
        for token,cap in zip(previous['settlement_tokens'],caps):
            check(0 < cap <= int(token['max_input_raw']), '历史部署额度超出升级范围。')
            token['max_input_raw'] = str(cap)
        old_dep = deployment(previous)
        check(path.parent in (directory, directory/'deployments'/old_dep['creation_data_hash'][2:]),
              '历史部署日志目录与签名不一致。')
        verify_setup_record(previous, record, old_dep['creation_data'])
        result = preflight_setup(rpc,previous,directory,require_validation=False)
        check(result['state'] != 'setup_pending', '历史部署仍有待确认设置交易，保留原记录。')
        predecessors.append((previous,result))
    return predecessors


def ensure_migration_account_idle(directory):
    database = Path(directory)/'transactions.sqlite'
    if database.exists():
        with sqlite3.connect(f'file:{database}?mode=ro', uri=True) as db:
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='attempts'").fetchone():
                check(not db.execute("SELECT 1 FROM attempts WHERE state IN ('reserved','submitted')").fetchone(),
                      '仍有未结算交易，须先用旧版本完成原交易核对。')


def preflight_setup(rpc, config, directory=None, require_validation=True):
    """Read the actual saved receipts and contract before any password prompt.

    No journal or installation is modified. Missing/unsealed receipts remain
    pending; access denial propagates and cannot become permission to resend.
    """
    if require_validation: require_current_validation()
    rpc.verify_chain()
    directory = Path(directory) if directory else account_directory(config)
    base_directory = directory
    if config['policy'].get('executor_migration') == 'mixed-v1':
        history = historical_predecessors(rpc,config,base_directory)
        if any(not entry['paused'] for _,entry in history): ensure_migration_account_idle(base_directory)
    directory, previous = setup_location(config, directory)
    if previous is not None:
        prior = preflight_setup(rpc, previous, base_directory, require_validation=False)
        if prior['state'] == 'setup_pending': return prior
        if not prior['paused'] or not (directory/'setup-deploy.json').exists():
            ensure_migration_account_idle(base_directory)
    dep = deployment(config)
    deploy_path = directory/'setup-deploy.json'
    if not deploy_path.exists():
        check(not (directory/'setup-unpause.json').exists(), '缺少与解除暂停记录对应的部署记录。')
        # A new account has no known receipt yet, but the endpoint must expose
        # the receipt API and support the account batch needed during setup.
        rpc.batch([('eth_getTransactionReceipt', ['0x'+'00'*32]),
                   ('eth_getTransactionCount', [config['owner'], 'pending']),
                   ('eth_getBalance', [config['owner'], 'pending'])])
        return {'state': 'needs_cap_migration' if previous else 'needs_setup',
                'previous_contract': prior['contract'] if previous else None, 'broadcast': False}
    record = json.loads(deploy_path.read_text())
    verify_setup_record(config, record, dep['creation_data'])
    receipt = rpc.request('eth_getTransactionReceipt', [record['tx_hash']])
    receipt = confirmed_setup_receipt(rpc, receipt, record['tx_hash'], config['owner'], None)
    if receipt is None: return {'state': 'setup_pending', 'broadcast': False}
    check(number(receipt['status']) == 1, '已有部署交易回滚；已保留记录，不会自动重复付费。')
    contract = address(receipt['contractAddress'])
    unpause_path = directory/'setup-unpause.json'
    if unpause_path.exists():
        record = json.loads(unpause_path.read_text())
        verify_setup_record(config, record, dep['unpause_calldata'], contract)
        unpause = rpc.request('eth_getTransactionReceipt', [record['tx_hash']])
        unpause = confirmed_setup_receipt(rpc, unpause, record['tx_hash'], config['owner'], contract)
        if unpause is None: return {'state': 'setup_pending', 'broadcast': False}
        check(number(unpause['status']) == 1, '已有解除暂停交易回滚；已保留记录，不会自动重复付费。')
    verified = verify_contract(rpc, config, contract)
    return {**verified, 'state': 'needs_unpause' if verified['paused'] else 'ready', 'broadcast': False}


def setup_gas_limit(estimated, target=False, *, capped_deployment=False):
    ceiling=150_000 if target else 6_000_000
    padded=(estimated*120+99)//100
    if padded>ceiling and capped_deployment and not target:
        # This option is used only for fixed, validated strategy constructors.
        # Retain the existing hard cap, require headroom and execute eth_call at
        # this exact limit before the setup transaction can be signed.
        check((estimated*103+99)//100<=ceiling,'部署费用估算缺少执行余量。')
        padded=ceiling
    check(0<estimated and padded<=ceiling,'设置交易 gas 超过上限。')
    return padded


def submit_setup_step(rpc, config, signer, journal, calldata, target=None, wait_seconds=30, *, value=0, capped_deployment=False):
    """Save the exact signed intent before submission and reuse it on every retry."""
    rpc.verify_chain()
    check(signer.address.lower() == config['owner'], '签名账户不一致。')
    target = address(target) if target else None
    identity = {'chain_id': config['chain_id'], 'owner': config['owner'], 'target': target,
                'calldata_hash': '0x'+keccak(unhex(calldata)).hex()}
    if value:identity['value']=value
    if value:
        from .strategy_access import ATLAS
        check(target==ATLAS and 0<value<=int(config.get('structural',{}).get('atlas_bond_wei',0)) and
              calldata==data('depositAndBond(uint256)',['uint256'],[value]),'unsupported setup value transfer')
    journal = Path(journal)
    if journal.exists():
        record = json.loads(journal.read_text())
        check(record['identity'] == identity, '已有部署记录对应其他配置；停止重复部署。')
        tx = record['transaction']
    else:
        nonce, pending, balance = rpc.batch([('eth_getTransactionCount', [config['owner'], 'latest']),
            ('eth_getTransactionCount', [config['owner'], 'pending']), ('eth_getBalance', [config['owner'], 'pending'])])
        check(number(nonce) == number(pending), '钱包有待确认交易，请先等待确认。')
        tx = {'type': 2, 'chainId': config['chain_id'], 'nonce': number(nonce), 'value': value,
              'data': calldata, **fee_fields(rpc, config)}
        if target: tx['to'] = to_checksum_address(target)
        call = {k: hex(v) if isinstance(v, int) else v for k, v in tx.items()}
        call['from'] = signer.address
        gas = number(rpc.request('eth_estimateGas', [call, 'pending']))
        tx['gas'] = setup_gas_limit(gas,bool(target),capped_deployment=capped_deployment)
        maximum = FeeOracle(rpc, config).maximum_fee(calldata, tx['gas'], tx['maxFeePerGas'])
        check(maximum <= SETUP_STEP_FEE_CAP, '设置交易费用超过上限。')
        check(number(balance) >= maximum + value + int(config['policy']['max_total_fee_wei']), 'Base ETH 余额不足以完成设置并预留一次交易费用。')
        call['gas'] = hex(tx['gas'])
        rpc.request('eth_call', [call, 'pending'])
        raw = bytes(signer.sign_transaction(tx).raw_transaction)
        record = {'identity': identity, 'transaction': tx, 'raw_transaction': '0x'+raw.hex(),
                  'tx_hash': '0x'+keccak(raw).hex(), 'fee_budget_wei': maximum, 'prepared_at': time.time()}
        journal.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        atomic_json(journal, record)
        journal.chmod(0o600)
    verify_setup_record(config, record, calldata, target, value=value)
    from .recovery import transient
    deadline = time.monotonic()+wait_seconds
    sent = False
    while True:
        read_ok, receipt, confirmed = False, None, None
        try:
            receipt = rpc.request('eth_getTransactionReceipt', [record['tx_hash']])
            confirmed = confirmed_setup_receipt(rpc, receipt, record['tx_hash'], config['owner'], target)
            read_ok = True
        except Exception as exc:
            if not transient(exc): raise
            # Keep the same signed intent while the connection recovers.
        if confirmed is not None:
            record['receipt'] = confirmed
            atomic_json(journal, record); journal.chmod(0o600)
            check(number(confirmed['status']) == 1, '设置交易回滚；已保留原回执，不会自动重复付费。')
            return confirmed
        elif read_ok and receipt is None and not sent:
            sent = True
            try:
                returned = rpc.request('eth_sendRawTransaction', [record['raw_transaction']])
            except RPCAccessDenied:
                raise  # Preserve the journal, but do not mislabel denial as pending.
            except RPCError as exc:
                # An explicit rejection is different from losing the response.
                # Only an already-known intent proceeds to receipt recovery.
                if exc.rpc_message.strip().lower() not in ('already known', 'known transaction', 'already imported'):
                    raise
                returned = None
            except Exception as exc:
                if not transient(exc): raise
                # A network failure can happen after acceptance; recover this hash only.
                returned = None
            check(returned is None or returned.lower() == record['tx_hash'], '节点返回了不同的交易哈希。')
        if time.monotonic() >= deadline:
            raise SetupPending(f"设置交易仍在等待完整区块确认：{record['tx_hash']}。记录已保存，稍后重新启动会恢复同一笔交易，不会重新部署。")
        time.sleep(.25)


def setup_executor(rpc, config, signer, directory=None, require_validation=True):
    if require_validation:
        require_current_validation()
    directory = Path(directory) if directory else account_directory(config)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory.chmod(0o700)
    with open(directory/'executor.lock', 'a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        base_directory = directory
        directory, previous = setup_location(config, directory)
        dep = deployment(config)
        if previous is not None:
            prior = preflight_setup(rpc, previous, base_directory, require_validation=False)
            check(prior['state'] != 'setup_pending', '旧部署仍待完整确认，保留原记录。')
            # Reconcile a saved pause intent even if its preconfirmation already
            # makes paused() true. Never sign a deployment over a pending pause.
            if not prior['paused'] or not (directory/'setup-deploy.json').exists():
                ensure_migration_account_idle(base_directory)
            pause_journal = directory/'setup-pause-previous.json'
            if not prior['paused'] or pause_journal.exists():
                submit_setup_step(rpc, config, signer, pause_journal, dep['pause_calldata'], prior['contract'])
            check(verify_contract(rpc, previous, prior['contract'])['paused'], '旧合约暂停尚未确认。')
        if config['policy'].get('executor_migration') == 'mixed-v1':
            history = historical_predecessors(rpc,config,base_directory)
            if any(not state['paused'] for _,state in history): ensure_migration_account_idle(base_directory)
            for previous_config,state in history:
                journal = directory/f"setup-pause-history-{state['contract']}.json"
                if not state['paused'] or journal.exists():
                    submit_setup_step(rpc,config,signer,journal,dep['pause_calldata'],state['contract'])
                check(verify_contract(rpc,previous_config,state['contract'])['paused'], '历史合约暂停尚未确认。')
        receipt = submit_setup_step(rpc, config, signer, directory/'setup-deploy.json' , dep['creation_data'])
        contract = address(receipt['contractAddress'])
        print(f'部署已确认，复用合约 {contract}', flush=True)
        verified = verify_contract(rpc, config, contract)
        if verified['paused']:
            print('正在解除合约暂停并等待确认。', flush=True)
            submit_setup_step(rpc, config, signer, directory/'setup-unpause.json', dep['unpause_calldata'], contract)
        verified = verify_contract(rpc, config, contract)
        check(not verified['paused'], '合约仍暂停。')
        verified.update(verified_at=time.time(), deployment_tx=receipt['transactionHash'], trading_started=False)
        verified['deployment_journal_directory'] = str(directory)
        if previous is not None: verified['previous_contract'] = prior['contract']
        atomic_json(base_directory/'installation.json', verified)
        return verified
