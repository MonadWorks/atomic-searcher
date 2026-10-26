
from .protocols import kind_id
import hashlib
import json
import time

from eth_abi import encode
from eth_utils import keccak

from .core import ROOT, atomic_json, data, unhex, check
from .source_files import source_files


COMPILED_FILES = tuple(f"out/{name}.sol/{name}.json" for name in ('AtomicCycle','StateLens','CycleProbe','CLQuoter','V4AtomicPair','V4StateLens','StructuralExecutor','MorphoStateLens','ProtocolLens','MidnightLiquidationLens','CycleQuoteLens','CrossedBooksResolver')) + ('out/ProtocolExecutor.sol/ProtocolExecutor.json','out/ProtocolExecutor.sol/BookExecutor.json','out/VenueExecutor.sol/VenueExecutor.json')


def compiled_manifest(root=ROOT):
    return {rel: hashlib.sha256((root/rel).read_bytes()).hexdigest() for rel in COMPILED_FILES}


def source_manifest():
    return {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in source_files(ROOT)}


LEGACY_ARTIFACT_HASH = "16f9609086b3ecd4ab1a87b0c05b0dca0e518df5f528f604f84329263309668e"

def contract_artifact(config):
    version = config.get('_executor_version', 'mixed-v1')
    check(version in ('mixed-v1', 'cl-v1'), 'unsupported executor version')
    if version == 'cl-v1':
        raw = (ROOT/'config/legacy-atomic-cycle.json').read_bytes()
        check(hashlib.sha256(raw).hexdigest() == LEGACY_ARTIFACT_HASH, 'legacy artifact hash mismatch')
        return json.loads(raw)
    return json.loads((ROOT/'out/AtomicCycle.sol/AtomicCycle.json').read_text())


def deployment(config):
    artifact = contract_artifact(config)
    args = [config["owner"], config["chain_id"], [f["address"] for f in config["factories"]],
            [kind_id(f["kind"]) for f in config["factories"]],
            [t["address"] for t in config["settlement_tokens"]], [int(t["max_input_raw"]) for t in config["settlement_tokens"]]]
    encoded = encode(["address", "uint256", "address[]", "uint8[]", "address[]", "uint256[]"], args)
    creation = unhex(artifact["bytecode"]["object"])+encoded
    return {"chain_id": config["chain_id"], "owner": config["owner"], "creation_data": "0x"+creation.hex(),
            "creation_data_hash": "0x"+keccak(creation).hex(), "initial_state": "paused",
            "from": config["owner"], "value": "0x0", "broadcast": False,
            "funding_authorization": False, "source_manifest": source_manifest(),
            "setup_after_deployment": ["verify deployed runtime, immutable owner and chain", "first execution authenticates new pools against the allowed factories", "unpause only after live latency and submission-path validation"],
            "pause_calldata": data("setPaused(bool)", ["bool"], [True]),
            "unpause_calldata": data("setPaused(bool)", ["bool"], [False]),
            "prepared_at": time.time()}


def readiness(config, evidence):
    def read(name):
        path = evidence/name
        return json.loads(path.read_text()) if path.exists() else None
    native = read("native-quote-verification.json")
    lens = read("lens-native-verification.json")
    deep_lens = read("deep-lens-native-verification.json")
    lens_runtime_hash = "0x"+keccak(unhex(json.loads((ROOT/"out/StateLens.sol/StateLens.json").read_text())["deployedBytecode"]["object"])).hex()
    feed = read("free-feed-summary.json")
    validation = read("validation.json")
    pipeline = read("pipeline-live-check.json")
    installation = read(f"account-{config['chain_id']}-{config['owner']}/installation.json")
    from .service import status
    process = status()
    active = bool(process.get('process_running') and process.get('executor_connected'))

    return {"generated_at": time.time(), "chain": "Base", "owner": config["owner"],
            "current_mode": "automatic" if active else "stopped", "funded_execution_enabled": active,
            "cached_installation": installation, "process":process,
            "automatic_signing_and_submission_implemented": True,
            "execution_evidence_scope": "temporary loopback Anvil with synthetic markets; not mainnet income or historical capture proof",
            "quote_differential_passed": bool(native and native["all_complete_quotes_equal"]),
            "atomic_lens_quote_differential_passed": bool(lens and lens["all_complete_quotes_equal"]),
            "deep_atomic_lens_quote_differential_passed": bool(deep_lens and deep_lens["all_complete_quotes_equal"]
                                                               and deep_lens.get("lens_runtime_hash") == lens_runtime_hash),
            "local_validation_current": bool(validation and validation.get("passed") and validation.get("source_manifest") == source_manifest() and validation.get("artifact_manifest") == compiled_manifest()),
            "legacy_free_feed_evidence": feed, "live_pipeline_evidence": pipeline, "source_manifest": source_manifest(),
            "timing_interpretation": "pending tag and snapshot anchor do not establish preconfirmation height; timing starts before the RPC request",
            "funding_readiness": "pending end-to-end observed-before-winner and canonical inclusion evidence",
            "conditional_execution_evidence": "local synthetic execution tests; historical first-seen timing and market capture must be measured separately",
            "remaining_gates": ["measure live opportunity lifetime against discovery, quote coverage and submission timing",
                                "link independently detected, executable opportunity to competitor order and actual inclusion",
                                "verify current deployment configuration and execution transport during each launch"],
            "operational_limits": ["one sealed successor starts provisional accounting; canonical receipts are rechecked through finality and orphaned outcomes reopened",
                                   "an externally consumed wallet nonce retains its reservation pending account reconciliation",
                                   "bounded cancellation repricing stops if it would exceed the original reserved budget"],
            "frequency_and_income": {"estimate": None, "reason": "insufficient successful own-account settlements for a frequency or income forecast"},
            "prepared_without_funds": ["authenticated pool discovery and cyclic routing", "integer sizing and native quote comparison",
                                       "zero-inventory atomic execution contract", "atomic state reads and automatic route evaluation",
                                       "signed execution, canonical receipt reconciliation and bounded same-nonce recovery tested locally",
                                       "durable observation recorder and connected fee ledger", "unsigned deployment data, paused by default"]}
