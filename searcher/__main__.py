import argparse
import json
from pathlib import Path
import httpx

from .core import ROOT, RPC, Reject, atomic_json, load_config


def main():
    parser = argparse.ArgumentParser(description="Base atomic-cycle search; explicit trade command enables signing and submission")
    parser.add_argument("--config", type=Path, default=ROOT/"config/base.json")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("prepare");p.add_argument("--blocks", type=int, default=6);p.add_argument("--out", type=Path, default=ROOT/"evidence/universe.json")
    p = sub.add_parser("scan");p.add_argument("--snapshot", type=Path, default=ROOT/"evidence/universe.json");p.add_argument("--out", type=Path, default=ROOT/"evidence/scan.json")
    p = sub.add_parser("verify");p.add_argument("--snapshot", type=Path, default=ROOT/"evidence/universe.json");p.add_argument("--pools", type=int, default=16)
    p = sub.add_parser("record");p.add_argument("--seconds", type=int, default=3600);p.add_argument("--interval", type=float, default=2);p.add_argument("--out", type=Path, default=ROOT/"evidence/free-feed.sqlite")
    p = sub.add_parser("watch");p.add_argument("--seconds", type=int, default=3600);p.add_argument("--snapshot", type=Path, default=ROOT/"evidence/universe.json");p.add_argument("--out", type=Path, default=ROOT/"evidence/shadow")
    sub.add_parser("artifacts")
    sub.add_parser("funding", help="check Base balance and current setup fees without signing")
    p = sub.add_parser("wallet-import", help="encrypt the dedicated wallet locally using hidden terminal input")
    p.add_argument("--keystore", type=Path)
    p = sub.add_parser("setup-executor", help="deploy and verify the executor using the local wallet")
    p.add_argument("--keystore", type=Path)
    p = sub.add_parser("launch", help="resume setup then run automatic trading using the local wallet")
    p.add_argument("--keystore", type=Path)
    p.add_argument("--seconds", type=int, default=1800)
    for command in ("start", "stop", "status"): sub.add_parser(command)
    p = sub.add_parser("run");p.add_argument("--seconds",type=int,default=1800)
    p = sub.add_parser("trade", help="run the automatic executor with a locally unlocked keystore")
    p.add_argument("--seconds", type=int, default=1800)
    p.add_argument("--executor", required=True)
    p.add_argument("--keystore", type=Path, required=True)
    args = parser.parse_args();config = load_config(args.config)
    if args.command in ("wallet-import", "trade", "setup-executor", "launch"):
        if config['owner'].lower() == '0x' + '1' * 40:
            parser.error("The bundled owner is a public example. Create config/local.json with your own owner and pass --config config/local.json before importing a wallet or trading.")
    if hasattr(args, "seconds") and not 0 <= args.seconds <= 86400:
        parser.error("seconds must be 0 for continuous run, or 1..86400")
    if getattr(args, "seconds", None) == 0 and args.command == "record":
        parser.error("record requires a finite duration; use run for continuous operation")
    if args.command in ("start", "stop", "status"):
        from . import service
        result = service.start(args.config) if args.command == "start" else getattr(service, args.command)()
        print(json.dumps(result))
    elif args.command == "prepare":
        if not 1 <= args.blocks <= 32: parser.error("blocks must be 1..32")
        from .engine import prepare
        from .snapshots import write_universe
        result = prepare(RPC(), config, args.blocks);write_universe(args.out, result)
        print(json.dumps({"snapshot": str(args.out), "scope": result["scope"]}))
    elif args.command == "scan":
        from .pools import Pool
        from .routes import RouteIndex
        from .engine import evaluate
        snap = json.loads(args.snapshot.read_text())
        index = RouteIndex([Pool.from_json(p) for p in snap["pools"]], [t["address"] for t in config["settlement_tokens"]], config["policy"]["max_hops"])
        result = evaluate(index, config, snap["header"]);atomic_json(args.out, result)
        print(json.dumps({"report": str(args.out), "computation_ms": result["computation_ms"], "scope": result["scope"]}))
    elif args.command == "verify":
        if args.pools < 2: parser.error("pools must be at least 2")
        from .verify import verify_snapshot
        result = verify_snapshot(RPC(), args.snapshot, args.pools)
        atomic_json(ROOT/"evidence/native-quote-verification.json", result)
        print(json.dumps({"all_complete_quotes_equal": result["all_complete_quotes_equal"]}))
        if not result["all_complete_quotes_equal"]: raise SystemExit(1)
    elif args.command == "record":
        if args.interval < 0.2: parser.error("public endpoint polling interval must be at least 0.2s")
        from .recorder import Recorder, record
        recorder = Recorder(args.out)
        try: record(RPC(), recorder, args.seconds, args.interval)
        finally:
            result = recorder.summary();atomic_json(args.out.with_name(args.out.stem+"-summary.json"), result);recorder.close()
        print(json.dumps(result))
    elif args.command == "watch":
        from .pipeline import run_pending
        args.out.mkdir(parents=True, exist_ok=True)
        result = run_pending(RPC(), config, json.loads(args.snapshot.read_text()), args.seconds, args.out)
        print(json.dumps(result))
    elif args.command == "artifacts":
        from .artifacts import deployment, readiness
        atomic_json(ROOT/"evidence/deployment-unsigned.json", deployment(config))
        atomic_json(ROOT/"evidence/readiness.json", readiness(config, ROOT/"evidence"))
        print(json.dumps({"prepared": True, "broadcast": False, "mode": "shadow"}))
    elif args.command == "run":
        from .runner import run
        print(json.dumps(run(config,args.seconds)))
    elif args.command == "funding":
        from .onboarding import funding_report
        rpc = RPC()
        try: result = funding_report(rpc, config)
        finally: rpc.close()
        atomic_json(ROOT/"evidence/funding-readiness.json", result)
        print(json.dumps(result, ensure_ascii=False))
    elif args.command == "wallet-import":
        from .onboarding import import_wallet, wallet_path
        print(json.dumps(import_wallet(config, args.keystore or wallet_path(config)), ensure_ascii=False))
    elif args.command in ("trade", "setup-executor", "launch"):
        from .onboarding import unlock_wallet, wallet_path, setup_executor, preflight_setup, verify_contract
        from .execution import BroadcastRPC, Executor, verify_execution_context
        from .runner import run
        from . import service
        from .recovery import retry_connection
        from .onboarding import SetupPending
        def retry_notice(error, delay):
            print(f'连接暂时中断，{delay} 秒后自动重试，当前启动流程会自动继续。', flush=True)
        def connected(operation, pending=()):
            return retry_connection(operation, on_retry=retry_notice, pending=pending)
        with service.trading_session(args.command):
            rpc = BroadcastRPC(timeout=3)
            executor = None
            try:
                # Check transport, proxy support and chain before asking for secrets.
                connected(rpc.verify_chain)
                connected(lambda: verify_execution_context(rpc))
                if args.command in ('setup-executor', 'launch'):
                    print('正在核验执行节点、已有部署回执及合约；通过后再输入钱包密码。', flush=True)
                    connected(lambda: preflight_setup(rpc, config), pending=(SetupPending,))
                else:
                    connected(lambda: verify_contract(rpc, config, args.executor))
                from .v4 import enabled as v4_enabled, verify_manager
                if v4_enabled(config): connected(lambda: verify_manager(rpc))
                from .structural import enabled as structural_enabled
                if structural_enabled(config):
                    from .structural_onboarding import preflight as structural_preflight
                    ready = connected(lambda: structural_preflight(rpc, config))
                    from .core import check
                    check(ready.get("balance_sufficient", True), "余额需覆盖新策略部署与一次交易费用。")
                from .protocol_execution import enabled as suite_enabled
                if suite_enabled(config):
                    from .protocol_onboarding import preflight_all
                    connected(lambda:preflight_all(rpc,config))
                account = unlock_wallet(config, args.keystore or wallet_path(config))
                if args.command in ("setup-executor", "launch"):
                    print("在 Base 上恢复或完成部署，验证后解除暂停；已有交易会沿用原哈希。", flush=True)
                    installation = connected(lambda: setup_executor(rpc, config, account), pending=(SetupPending,))
                    print(json.dumps(installation, ensure_ascii=False), flush=True)
                    contract = installation['contract']
                else: contract = args.executor
                v4_contract = None
                from .v4 import enabled as v4_enabled
                if v4_enabled(config):
                    from .v4_onboarding import setup_executor as setup_v4
                    def resume_and_setup_v4():
                        # Reopen orphan accounting/recover original hashes before
                        # an additional contract can use this account's next nonce.
                        recovery = Executor(rpc, config, contract, account,
                            ROOT/"evidence"/f"account-{config['chain_id']}-{config['owner']}")
                        try: recovery.reconcile()
                        finally: recovery.close()
                        return setup_v4(rpc, config, account)
                    v4_installation = connected(resume_and_setup_v4, pending=(SetupPending,))
                    v4_contract = v4_installation['contract']
                structural_contract = None
                if structural_enabled(config):
                    from .structural_onboarding import setup_executor as setup_structural
                    def resume_and_setup_structural():
                        recovery = Executor(rpc, config, contract, account,
                            ROOT/"evidence"/f"account-{config['chain_id']}-{config['owner']}")
                        try: recovery.reconcile()
                        finally: recovery.close()
                        return setup_structural(rpc, config, account)
                    structural_installation = connected(resume_and_setup_structural, pending=(SetupPending,))
                    structural_contract = structural_installation["contract"]
                protocol_contracts={}
                if suite_enabled(config):
                    from .protocol_onboarding import setup_executor as setup_protocol
                    for name in __import__('searcher.protocol_execution',fromlist=['contract_names']).contract_names(config):
                        def resume_protocol(name=name):
                            recovery=Executor(rpc,config,contract,account,ROOT/'evidence'/f"account-{config['chain_id']}-{config['owner']}")
                            try:recovery.reconcile()
                            finally:recovery.close()
                            return setup_protocol(rpc,config,account,name=name)
                        installed=connected(resume_protocol,pending=(SetupPending,))
                        protocol_contracts[name]=installed['contract']
                if args.command == "setup-executor": return
                executor = connected(lambda: Executor(rpc, config, contract, account,
                                    ROOT/"evidence"/f"account-{config['chain_id']}-{config['owner']}",
                                    **({'v4_contract':v4_contract} if v4_contract else {}),
                                    **({'structural_contract':structural_contract} if structural_contract else {}),
                                    **({'protocol_contracts':protocol_contracts} if protocol_contracts else {})))
                current = service.runner_status()
                if current['process_running'] and not current.get('executor_connected'):
                    stopped = service.stop_runner()
                    if stopped['process_running']: raise RuntimeError('旧观察进程仍在停止中，请稍后重试。')
                print(json.dumps(run(config, args.seconds, executor)))
            finally:
                if executor is not None: executor.close()
                rpc.close()


if __name__ == "__main__":
    try:
        main()
    except Reject as exc:
        raise SystemExit(str(exc)) from None
    except httpx.HTTPStatusError as exc:
        raise SystemExit(f'节点返回 HTTP {exc.response.status_code}，本次运行已停止。已有交易记录保留，恢复后会核对原哈希。') from None
    except httpx.RequestError:
        raise SystemExit('节点连接中断，本次运行已停止。已有交易记录保留，恢复后会核对原哈希。') from None
