"""Persist pool facts, not millions of reproducible address combinations."""
import json
from pathlib import Path
from .core import atomic_json, check


def write_universe(path, snapshot):
    # Route groups are a derived in-memory acceleration. All authenticated pools,
    # exact states, discovery cursors and expansion history remain on disk.
    atomic_json(path, {k:v for k,v in snapshot.items() if k != 'cycle_groups'})


def read_universe(path):
    path = Path(path)
    if path.stat().st_size <= 16*1024**2:
        saved = json.loads(path.read_text())
        saved.pop('cycle_groups', None)
        return saved
    # Migrate the original atomic_json(indent=2) format without loading its
    # enormous derived list. Its unique two-space top-level closing bracket
    # cannot be confused with the six-space group/entry lines in that format.
    kept, dropping, skipped = [], False, False
    with path.open() as source:
        for line in source:
            if not dropping and line == '  "cycle_groups": [\n':
                check(not skipped, 'duplicate legacy cycle cache')
                dropping = skipped = True
                continue
            if dropping:
                if line == '  ],\n': dropping = False
                elif line == '  ]\n':
                    # The writer currently places fields after cycle_groups.
                    raise ValueError('unexpected legacy cache field order')
                continue
            kept.append(line)
    check(not dropping, 'incomplete legacy universe cache')
    saved = json.loads(''.join(kept))
    saved.pop('cycle_groups', None)
    return saved
