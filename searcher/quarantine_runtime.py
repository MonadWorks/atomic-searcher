"""Bind durable transfer scheduling evidence to the installed account/code."""
import hashlib
from pathlib import Path

from .core import ROOT, address
from .quarantine import DirectionQuarantine


def quarantine_path(directory, executor):
    return Path(directory)/f'transfer-quarantine-{address(executor)}.json'


def open_quarantine(config, executor, path, root=ROOT, validate_only=False):
    artifact = ('config/legacy-atomic-cycle.json' if config.get('_executor_version') == 'cl-v1'
                else 'out/AtomicCycle.sol/AtomicCycle.json')
    identity = dict(chain_id=config['chain_id'], owner=config['owner'], executor=executor,
        helper_sha256=hashlib.sha256((root/'out/CycleProbe.sol/CycleProbe.json').read_bytes()).hexdigest(),
        executor_sha256=hashlib.sha256((root/artifact).read_bytes()).hexdigest())
    return DirectionQuarantine(path, identity, validate_only=validate_only)
