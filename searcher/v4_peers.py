"""Find counterpart PoolKeys from authenticated, initialized manager storage.

Candidate configurations are metadata probes, never price estimates. Prices and
depth are discarded here and reread atomically by the normal quote/execution path.
The bounded template set is a discovery accelerator, not an exhaustive V4 index.
"""
import json
from collections import Counter
from eth_abi import decode
from .core import USDC, check, data, number, unhex
from .math import MIN_SQRT, MAX_SQRT
from .v4 import verify_manager
from .v4_market import Key, MANAGER, signed

# Common fixed-fee configurations plus every configuration observed in inventory.
# Existence is established by the pool-id-derived storage slot, not this list.
DEFAULT_TEMPLATES = ((0, 1), (1, 1), (4, 1), (10, 1), (50, 1),
                     (100, 1), (100, 2), (250, 5), (500, 10),
                     (3000, 60), (10000, 200))


def restore_displaced(inventory, common):
    """Restore bounded metadata displaced by observations on an orphaned fork."""
    if not inventory.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='peer_displaced'").fetchone():
        return
    if common is None:
        inventory.db.execute('DELETE FROM peer_displaced')
        return
    rows = inventory.db.execute('SELECT id,key,block,hash FROM peer_displaced '
        'WHERE eviction_block>? AND block<=? ORDER BY eviction_block DESC,id', (common, common)).fetchall()
    for pid, encoded, block, block_hash in rows:
        if pid in inventory.registry.keys or len(inventory.registry.keys) >= inventory.registry.maximum:
            continue
        key = Key(*json.loads(encoded))
        check(key.id == pid, 'V4 displaced key hash mismatch')
        inventory.db.execute('INSERT OR IGNORE INTO pools VALUES(?,?,?,?)', (pid, encoded, block, block_hash))
        inventory.registry.keys[pid] = key
    inventory.db.execute('DELETE FROM peer_displaced WHERE eviction_block>?', (common,))


class PeerFinder:
    def __init__(self):
        self.verified = False
        self.last = {}

    def step(self, inventory, rpc, head, limit=64):
        check(isinstance(limit, int) and 1 <= limit <= 64, 'invalid V4 peer page size')
        known = inventory.registry.keys
        pairs = sorted({(k.currency0, k.currency1) for k in known.values()
                        if USDC in (k.currency0, k.currency1)})
        if not pairs:
            self.last = {'state': 'awaiting_known_pair'}
            return set()
        cursor = inventory.get('peer:pair_cursor', 0)
        pair = pairs[cursor % len(pairs)]
        templates = sorted(set(DEFAULT_TEMPLATES) | {(k.fee, k.spacing) for k in known.values()})
        progress = 'peer:templates:' + ':'.join(pair)
        offset = inventory.get(progress, 0) % len(templates)
        page = [templates[(offset+i) % len(templates)] for i in range(min(limit, len(templates)))]
        keys = [Key(*pair, fee, spacing) for fee, spacing in page]
        keys = [k for k in keys if k.id not in known]
        admitted, removed, deferred, empty = [], [], 0, 0
        funded = set()
        if keys:
            if not self.verified:
                verify_manager(rpc)
                self.verified = True
            height = number(head['number'])
            raw = rpc.request('eth_call', [{'to': MANAGER,
                'data': data('extsload(bytes32[])', ['bytes32[]'],
                             [[slot.to_bytes(32, 'big') for k in keys for slot in (k.slot, k.slot+3)]])}, hex(height)])
            words = decode(['bytes32[]'], unhex(raw))[0]
            check(len(words) == 2*len(keys), 'V4 peer storage response length mismatch')
            found = []
            for key, raw_word, raw_liquidity in zip(keys, words[::2], words[1::2]):
                word = int.from_bytes(raw_word, 'big')
                if not word:
                    continue
                sqrt = word & ((1 << 160)-1)
                tick = signed((word >> 160) & ((1 << 24)-1), 24)
                protocol = (word >> 184) & ((1 << 24)-1)
                fee = (word >> 208) & ((1 << 24)-1)
                check(MIN_SQRT <= sqrt < MAX_SQRT and -887272 <= tick <= 887272
                      and fee == key.fee and (protocol & 4095) <= 1000
                      and (protocol >> 12) <= 1000 and (word >> 232) == 0,
                      'V4 peer storage inconsistent with fixed-fee PoolKey')
                # A zero current-range liquidity can still lead to funded ticks
                # after a price jump. Keep that key for exact quotes, but admit
                # currently funded counterparts first under the capacity bound.
                liquid = bool(int.from_bytes(raw_liquidity, 'big') & ((1 << 128)-1))
                if not liquid:
                    empty += 1
                found.append((key, liquid))
            after = rpc.request('eth_getBlockByNumber', [hex(height), False])
            check(after is not None and number(after['number']) == height
                  and after['hash'] == head['hash'], 'V4 peer observation was reorganized')
            remaining = dict(known)
            counts = Counter((k.currency0, k.currency1) for k in remaining.values())
            ages = dict(inventory.db.execute('SELECT id,block FROM pools'))
            for key, liquid in sorted(found, key=lambda item: not item[1]):
                if len(remaining) >= inventory.registry.maximum:
                    # A verified counterpart may displace an unrelated singleton,
                    # retaining the existing pair which made this probe useful.
                    singletons = [pid for pid, k in remaining.items()
                        if counts[(k.currency0, k.currency1)] == 1
                        and (k.currency0, k.currency1) != pair] if liquid else []
                    if not singletons:
                        deferred += 1
                        continue
                    victim = min(singletons, key=lambda pid: (ages.get(pid, height), pid))
                    old = remaining.pop(victim)
                    counts[(old.currency0, old.currency1)] -= 1
                    removed.append(victim)
                remaining[key.id] = key
                counts[pair] += 1
                admitted.append(key)
                if liquid:
                    funded.add(key.id)
            with inventory.db:
                if removed:
                    inventory.db.execute('CREATE TABLE IF NOT EXISTS peer_displaced('
                        'id TEXT PRIMARY KEY,key TEXT,block INTEGER,hash TEXT,eviction_block INTEGER)')
                for pid in removed:
                    inventory.db.execute('INSERT OR REPLACE INTO peer_displaced '
                        'SELECT id,key,block,hash,? FROM pools WHERE id=?', (height, pid))
                    inventory.db.execute('DELETE FROM pools WHERE id=?', (pid,))
                if removed:
                    inventory.db.execute('DELETE FROM peer_displaced WHERE id NOT IN '
                        '(SELECT id FROM peer_displaced ORDER BY eviction_block DESC,id LIMIT ?)',
                        (inventory.registry.maximum,))
                for key in admitted:
                    inventory.db.execute('INSERT OR IGNORE INTO pools VALUES(?,?,?,?)',
                        (key.id, json.dumps(key.values()), height, head['hash']))
                if admitted:
                    inventory.db.execute('INSERT OR REPLACE INTO checkpoints VALUES(?,?)', (height, head['hash']))
                    inventory.db.execute('DELETE FROM checkpoints WHERE block NOT IN '
                        '(SELECT block FROM checkpoints ORDER BY block DESC LIMIT 32)')
                inventory.put('peer:pair_cursor', cursor+1)
                inventory.put(progress, (offset+len(page)) % len(templates))
            inventory.registry.keys = remaining
            for pid in removed:
                inventory.tentative.pop(pid, None)
        else:
            with inventory.db:
                inventory.put('peer:pair_cursor', cursor+1)
                inventory.put(progress, (offset+len(page)) % len(templates))
        self.last = {'state': 'complete', 'pair': pair, 'probed': len(keys),
                     'admitted': len(admitted), 'capacity_deferred': deferred,
                     'displaced_singletons': len(removed), 'empty_range': empty}
        # Funded discoveries get the urgent lane first. The ordinary page still
        # enumerates every admitted key, including zero-liquidity gap routes.
        return funded or {k.id for k in admitted}
