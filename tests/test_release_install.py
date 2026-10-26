import json
from pathlib import Path

import pytest

from searcher.upgrade import install,manifest,lifecycle_locks


def release(tmp_path):
    target=tmp_path/'production';bundle=tmp_path/'release'
    (target/'releases').mkdir(parents=True);(target/'config').mkdir()
    for rel in ('config/base.json','foundry.toml','pyproject.toml','requirements.lock'):
        (target/rel).write_text('base')
    (target/'evidence').mkdir();(target/'evidence/validation.json').write_text('{"old":true}')
    (target/'wallets').mkdir();(target/'wallets/do-not-read').write_text('private')
    (bundle/'files/config').mkdir(parents=True)
    for rel in ('config/base.json','foundry.toml','pyproject.toml','requirements.lock'):
        (bundle/'files'/rel).write_text('next' if rel=='config/base.json' else 'base')
    (bundle/'files/searcher').mkdir();(bundle/'files/searcher/new.py').write_text('new')
    (bundle/'base-manifest.json').write_text(json.dumps(manifest(target)))
    (bundle/'validation.json').write_text(json.dumps({'passed':True,'source_unchanged_during_checks':True,'source_manifest':manifest(bundle/'files')}))
    return target,bundle


def test_verified_update_preserves_private_data_and_is_idempotent(tmp_path):
    target,bundle=release(tmp_path)
    result=install(bundle,target)
    assert result['state']=='installed' and not result['trading_started']
    assert (target/'wallets/do-not-read').read_text()=='private'
    assert manifest(target)==manifest(bundle/'files')
    (target/'evidence/validation.json').write_text('{"old":true}')
    assert install(bundle,target)['state']=='already_installed'
    assert json.loads((target/'evidence/validation.json').read_text())['passed']


def test_running_or_changed_source_is_never_overwritten(tmp_path):
    target,bundle=release(tmp_path)
    with lifecycle_locks(target):
        with pytest.raises(BlockingIOError):install(bundle,target)
    (target/'config/base.json').write_text('user edit')
    with pytest.raises(RuntimeError,match='已经改变'):install(bundle,target)
    assert not (target/'searcher/new.py').exists()


def test_interrupted_copy_restores_original_sources_and_validation(tmp_path,monkeypatch):
    import searcher.upgrade as module
    target,bundle=release(tmp_path);original=manifest(target);replace=module.replace_file
    def broken(source,destination):
        if source==bundle/'files/searcher/new.py':raise OSError('simulated disk failure')
        return replace(source,destination)
    monkeypatch.setattr(module,'replace_file',broken)
    with pytest.raises(OSError):install(bundle,target)
    assert manifest(target)==original
    assert json.loads((target/'evidence/validation.json').read_text())=={'old':True}


def test_fixed_name_temp_symlink_cannot_overwrite_private_file(tmp_path):
    target,bundle=release(tmp_path)
    (target/'config/base.json.release-tmp').symlink_to(target/'wallets/do-not-read')
    install(bundle,target)
    assert (target/'wallets/do-not-read').read_text()=='private'
    assert not (target/'config/base.json').is_symlink()


def test_source_symlink_or_target_directory_symlink_is_rejected(tmp_path):
    target,bundle=release(tmp_path)
    source=bundle/'files/searcher/new.py';source.unlink();source.symlink_to(target/'wallets/do-not-read')
    with pytest.raises(RuntimeError,match='符号链接'):install(bundle,target)
    source.unlink();source.write_text('new')
    (target/'config/base.json').unlink();(target/'config').rmdir()
    (target/'config').symlink_to(target/'wallets',target_is_directory=True)
    with pytest.raises(RuntimeError,match='符号链接'):install(bundle,target)
    assert (target/'wallets/do-not-read').read_text()=='private'


def test_termination_signal_during_install_restores_original_tree(tmp_path):
    import subprocess,sys
    from searcher.core import ROOT
    target,bundle=release(tmp_path);original=manifest(target)
    script='''
import os,signal,sys
from pathlib import Path
import searcher.upgrade as module
bundle,target=map(Path,sys.argv[1:])
original=module.replace_file
def interrupted(source,destination):
    if source==bundle/'files/searcher/new.py':os.kill(os.getpid(),signal.SIGTERM)
    return original(source,destination)
module.replace_file=interrupted
module.install(bundle,target)
'''
    result=subprocess.run([sys.executable,'-c',script,str(bundle),str(target)],cwd=ROOT,capture_output=True,text=True,timeout=5)
    assert result.returncode!=0 and manifest(target)==original


def compiled_release(tmp_path):
    import hashlib
    target,bundle=release(tmp_path);artifacts={}
    from searcher.artifacts import COMPILED_FILES
    for rel in COMPILED_FILES:
        for root,content in ((target,'old-bytecode'),(bundle/'files','new-bytecode')):
            (root/rel).parent.mkdir(parents=True,exist_ok=True);(root/rel).write_text(content)
        artifacts[rel]=hashlib.sha256((bundle/'files'/rel).read_bytes()).hexdigest()
    v=json.loads((bundle/'validation.json').read_text());v['artifact_manifest']=artifacts
    (bundle/'validation.json').write_text(json.dumps(v))
    return target,bundle


def test_verified_bytecode_is_installed_and_repaired_if_stale(tmp_path):
    target,bundle=compiled_release(tmp_path)
    assert install(bundle,target)['state']=='installed'
    artifact=target/'out/AtomicCycle.sol/AtomicCycle.json';assert artifact.read_text()=='new-bytecode'
    artifact.write_text('stale')
    assert install(bundle,target)['state']=='installed' and artifact.read_text()=='new-bytecode'
    assert install(bundle,target)['state']=='already_installed'


def test_artifact_copy_failure_restores_source_and_old_bytecode(tmp_path,monkeypatch):
    import searcher.upgrade as module
    target,bundle=compiled_release(tmp_path);before=manifest(target);replace=module.replace_file
    def fail(source,destination):
        if source==bundle/'files/out/StateLens.sol/StateLens.json':raise OSError('artifact copy interrupted')
        return replace(source,destination)
    monkeypatch.setattr(module,'replace_file',fail)
    with pytest.raises(OSError):install(bundle,target)
    assert manifest(target)==before
    assert (target/'out/AtomicCycle.sol/AtomicCycle.json').read_text()=='old-bytecode'
    assert json.loads((target/'evidence/validation.json').read_text())=={'old':True}


def test_contract_change_without_artifacts_and_tampered_bytecode_rejected(tmp_path):
    target,bundle=release(tmp_path)
    (bundle/'files/src').mkdir();(bundle/'files/src/AtomicCycle.sol').write_text('contract Next {}')
    v=json.loads((bundle/'validation.json').read_text());v['source_manifest']=manifest(bundle/'files')
    (bundle/'validation.json').write_text(json.dumps(v))
    with pytest.raises(RuntimeError,match='编译产物'):install(bundle,target)


def test_send_between_preflight_and_stop_keeps_original_recovery_code(tmp_path,monkeypatch):
    import sqlite3
    from types import SimpleNamespace
    from searcher.onboarding import ensure_migration_account_idle
    import searcher.upgrade as module
    target,bundle=compiled_release(tmp_path);before=manifest(target)
    account=target/'evidence/account-8453-test';account.mkdir()
    database=account/'transactions.sqlite'
    with sqlite3.connect(database) as db:db.execute('CREATE TABLE attempts(state TEXT)')
    ensure_migration_account_idle(account)
    def stop(*args,**kwargs):
        with sqlite3.connect(database) as db:db.execute("INSERT INTO attempts VALUES ('submitted')")
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(module.subprocess,'run',stop)
    with pytest.raises(RuntimeError,match='未结算交易'):install(bundle,target,stop_running=True)
    assert manifest(target)==before
    assert (target/'out/AtomicCycle.sol/AtomicCycle.json').read_text()=='old-bytecode'
    assert json.loads((target/'evidence/validation.json').read_text())=={'old':True}
    assert not list((target/'releases').iterdir())
