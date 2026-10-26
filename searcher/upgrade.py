"""Install a tested local release while holding both lifecycle locks.

No wallet access, signing, deployment, or automatic trading occurs here.
"""
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile

from .source_files import source_files


def reject_symlinks(path):
    for part in (path,*path.parents):
        if part.is_symlink():raise RuntimeError('更新路径包含符号链接；未允许跨目录覆盖')


def manifest(root):
    paths=source_files(root)
    for path in paths:reject_symlinks(path)
    return {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}


@contextmanager
def lifecycle_locks(target):
    handles=[]
    try:
        (target/'evidence').mkdir(exist_ok=True)
        for name in ('trading.lock','runtime.lock'):
            handle=(target/'evidence'/name).open('a+');handles.append(handle)
            fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
        yield
    finally:
        for handle in reversed(handles):handle.close()


def replace_file(source,target):
    reject_symlinks(source);reject_symlinks(target)
    target.parent.mkdir(parents=True,exist_ok=True)
    descriptor,name=tempfile.mkstemp(prefix='.'+target.name+'.release-',dir=target.parent)
    temp=Path(name)
    try:
        with os.fdopen(descriptor,'wb') as output,source.open('rb') as input_file:
            shutil.copyfileobj(input_file,output)
            os.fchmod(output.fileno(),source.stat().st_mode & 0o777)
            output.flush();os.fsync(output.fileno())
        os.replace(temp,target)
    finally:
        if temp.exists():temp.unlink()


@contextmanager
def controlled_interruptions():
    recovering=[False]
    previous={}
    def interrupted(signum,frame):
        if not recovering[0]:raise InterruptedError('更新收到停止信号，正在恢复原版本')
    try:
        for sig in (signal.SIGTERM,signal.SIGHUP,signal.SIGINT):
            previous[sig]=signal.signal(sig,interrupted)
        yield recovering
    finally:
        for sig,handler in previous.items():signal.signal(sig,handler)


def install(bundle,target,stop_running=False):
    bundle,target=Path(bundle).resolve(),Path(target).resolve()
    base=json.loads((bundle/'base-manifest.json').read_text())
    validation=json.loads((bundle/'validation.json').read_text())
    expected=validation['source_manifest']
    artifacts=validation.get('artifact_manifest', {})
    permitted={f'out/{name}.sol/{name}.json' for name in ('AtomicCycle','StateLens','CycleProbe','CLQuoter','V4AtomicPair','V4StateLens','StructuralExecutor','MorphoStateLens','ProtocolLens','MidnightLiquidationLens','CycleQuoteLens','CrossedBooksResolver')} | {'out/ProtocolExecutor.sol/ProtocolExecutor.json','out/ProtocolExecutor.sol/BookExecutor.json','out/VenueExecutor.sol/VenueExecutor.json'}
    if artifacts and set(artifacts) != permitted:raise RuntimeError('编译产物清单不完整')
    def hashes(root, entries):
        values={}
        for rel in entries:
            path=root/rel;reject_symlinks(path)
            values[rel]=hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None
        return values
    if any(rel.startswith('src/') and base.get(rel)!=sha for rel,sha in expected.items()) and not artifacts:
        raise RuntimeError('合约更新缺少已验证编译产物')
    if hashes(bundle/'files',artifacts)!=artifacts:raise RuntimeError('更新包编译产物与已验证版本不一致')
    if not validation.get('passed') or not validation.get('source_unchanged_during_checks'):
        raise RuntimeError('更新包未通过完整验证')
    if manifest(bundle/'files')!=expected:raise RuntimeError('更新包文件与已验证版本不一致')
    current=manifest(target)
    if current not in (base,expected):raise RuntimeError('现有程序已经改变，停止覆盖；需要重新核对版本')
    if stop_running:
        completed=subprocess.run([sys.executable,'-m','searcher','stop'],cwd=target,
                                 capture_output=True,text=True,timeout=45)
        if completed.returncode:raise RuntimeError('旧程序未能正常停止；未修改文件')
    with lifecycle_locks(target),controlled_interruptions() as recovering:
        if manifest(target)==expected and hashes(target,artifacts)==artifacts:
            replace_file(bundle/'validation.json',target/'evidence/validation.json')
            return {'state':'already_installed'}
        current=manifest(target)
        if current not in (base,expected):raise RuntimeError('安装前程序发生变化；未修改文件')
        # The running executor may submit after the launcher's first preflight.
        # Recheck durable ledgers only after stopping it and holding both locks,
        # so its original recovery code remains available for unresolved sends.
        from .onboarding import ensure_migration_account_idle
        for account in (target/'evidence').glob('account-*'):
            if account.is_dir():
                reject_symlinks(account)
                ensure_migration_account_idle(account)
        changed=[p for p,h in expected.items() if current.get(p)!=h]
        changed += [p for p,h in artifacts.items() if hashes(target,[p])[p]!=h]
        if set(base)-set(expected):raise RuntimeError('此安装器不支持删除旧版本文件')
        (target/'releases').mkdir(exist_ok=True)
        backup=Path(tempfile.mkdtemp(prefix='before-release-upgrade-',dir=target/'releases'))
        for rel in changed:
            old=target/rel
            if old.exists():replace_file(old,backup/rel)
        validation_path=target/'evidence/validation.json'
        if validation_path.exists():replace_file(validation_path,backup/'validation.json')
        touched=[]
        try:
            for rel in changed:
                touched.append(rel);replace_file(bundle/'files'/rel,target/rel)
            if manifest(target)!=expected or hashes(target,artifacts)!=artifacts:raise RuntimeError('安装后校验失败')
            replace_file(bundle/'validation.json',validation_path)
        except BaseException:
            recovering[0]=True
            for rel in reversed(touched):
                if (backup/rel).exists():replace_file(backup/rel,target/rel)
                elif (target/rel).exists():(target/rel).unlink()
            if (backup/'validation.json').exists():replace_file(backup/'validation.json',validation_path)
            raise
        return {'state':'installed','backup':str(backup),'trading_started':False}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--bundle',required=True,type=Path)
    parser.add_argument('--target',required=True,type=Path)
    parser.add_argument('--stop',action='store_true')
    args=parser.parse_args()
    try:result=install(args.bundle,args.target,args.stop)
    except BlockingIOError:raise SystemExit('机器人仍在运行，未修改程序。请停止后重试。') from None
    except (RuntimeError,OSError,ValueError,KeyError,subprocess.TimeoutExpired) as exc:
        raise SystemExit('更新未完成：'+str(exc)) from None
    print('更新已安装并核对。钱包、交易记录和链上合约保持原样。' if result['state']=='installed' else '此版本已安装。')


if __name__=='__main__':main()
