"""Header request pruning preserves canonical supported-key admission; no network."""
import copy

from eth_abi import encode
from eth_utils import keccak
import pytest

from searcher.core import USDC, Reject
from searcher.v4_market import INITIALIZE, KEY_TYPES, MANAGER, ZERO, Key
from searcher.v4_runtime import Inventory

ASSET = '0xff00000000000000000000000000000000000001'
OTHER = '0x1100000000000000000000000000000000000001'


def header(height):
    return {'number': hex(height), 'hash': '0x'+format(height, '064x')}


def log(height, *, currency0=USDC, fee=500, spacing=10, hooks=ZERO):
    values = [currency0, ASSET, fee, spacing, hooks]
    return {'address': MANAGER, 'blockNumber': hex(height), 'blockHash': header(height)['hash'],
            'topics': [INITIALIZE, '0x'+keccak(encode(KEY_TYPES, values)).hex(),
                       '0x'+encode(['address'], [currency0]).hex(),
                       '0x'+encode(['address'], [ASSET]).hex()],
            'data': '0x'+encode(['uint24', 'int24', 'address', 'uint160', 'int24'],
                               [fee, spacing, hooks, 1 << 96, 0]).hex()}


class RPC:
    def __init__(self, logs):
        self.logs = logs
        self.headers = []
        self.calls = []
        self.short = False

    def request(self, method, params):
        self.calls.append((method, params))
        if method == 'eth_getLogs':
            return copy.deepcopy(self.logs)
        assert method == 'eth_getBlockByNumber'
        height = int(params[0], 16)
        self.headers.append(height)
        return header(height)

    def batch(self, calls):
        assert 0 < len(calls) <= 2
        return [] if self.short else [self.request(*call) for call in calls]


def inventory(tmp_path, *, backward=False):
    inv = Inventory(tmp_path/'inventory.sqlite')
    with inv.db:
        inv.put('cursor', 138 if backward else 74)
        inv.put('backfill', 138 if backward else 0)
        inv.put('scan_turn', 2 if backward else 0)
    return inv


def test_all_unsupported_page_requests_only_endpoint_header(tmp_path):
    logs = [log(80+i, hooks='0x'+'11'*20) for i in range(10)]
    rpc = RPC(logs)
    inv = inventory(tmp_path)
    try:
        assert inv.scan(rpc, header(138)) == set()
        assert rpc.headers == [138]
        assert sum(method == 'eth_getLogs' for method, _ in rpc.calls) == 1
        assert not inv.registry.keys and inv.get('cursor') == 138
        assert inv.db.execute('SELECT * FROM checkpoints').fetchall() == [(138, header(138)['hash'])]
    finally:
        inv.close()


def test_mixed_page_requests_supported_heights_only_and_preserves_keys(tmp_path):
    supported = [log(80, fee=500), log(80, fee=600), log(83, fee=3000)]
    unsupported = [log(81, hooks='0x'+'11'*20), log(82, fee=1 << 23),
                   log(84, currency0=OTHER), dict(log(85), data='0x1234'), dict(log(86), removed=True)]
    rpc = RPC(unsupported+supported)
    inv = inventory(tmp_path)
    expected = {Key(USDC, ASSET, fee, 10).id for fee in (500, 600, 3000)}
    try:
        assert inv.scan(rpc, header(138)) == expected
        assert set(inv.registry.keys) == expected
        assert rpc.headers == [80, 83, 138]
        assert inv.db.execute('SELECT COUNT(*) FROM pools').fetchone() == (3,)
    finally:
        inv.close()


def test_supported_forged_hash_still_rejects_all_metadata_and_progress(tmp_path):
    rpc = RPC([log(80), dict(log(81, fee=600), blockHash='0x'+'ff'*32)])
    inv = inventory(tmp_path)
    try:
        with pytest.raises(Reject, match='not canonical'):
            inv.scan(rpc, header(138))
        assert not inv.registry.keys and inv.get('cursor') == 74
        assert inv.get('scan_turn') == 0
        assert inv.db.execute('SELECT COUNT(*) FROM pools').fetchone() == (0,)
    finally:
        inv.close()


@pytest.mark.parametrize('entry', [log(74, hooks='0x'+'11'*20), log(139, currency0=OTHER)])
def test_unsupported_out_of_range_still_rejects_before_any_commit(tmp_path, entry):
    rpc = RPC([entry])
    inv = inventory(tmp_path)
    try:
        with pytest.raises(Reject, match='outside requested range'):
            inv.scan(rpc, header(138))
        assert inv.get('cursor') == 74 and inv.get('scan_turn') == 0
        assert not inv.registry.keys
    finally:
        inv.close()


@pytest.mark.parametrize('block_hash', [None, '', 'not-a-hash', '0x'+'gg'*32])
def test_unsupported_log_hash_cannot_create_metadata(tmp_path, block_hash):
    rpc = RPC([dict(log(80, hooks='0x'+'11'*20), blockHash=block_hash)])
    inv = inventory(tmp_path)
    try:
        assert inv.scan(rpc, header(138)) == set()
        assert inv.get('cursor') == 138 and not inv.registry.keys
        assert rpc.headers == [138]
        assert inv.db.execute('SELECT COUNT(*) FROM pools').fetchone() == (0,)
    finally:
        inv.close()


def test_supported_and_endpoint_same_height_requests_it_once(tmp_path):
    rpc = RPC([log(138)])
    inv = inventory(tmp_path)
    try:
        assert inv.scan(rpc, header(138)) == {Key(USDC, ASSET, 500, 10).id}
        assert rpc.headers == [138]
    finally:
        inv.close()


def test_unsupported_backfill_keeps_original_cursor_and_advances_one_page(tmp_path):
    rpc = RPC([log(80, currency0=OTHER)])
    inv = inventory(tmp_path, backward=True)
    try:
        assert inv.scan(rpc, header(138)) == set()
        assert rpc.headers == [138]
        assert inv.get('cursor') == 138 and inv.get('backfill') == 74
        assert inv.get('scan_turn') == 3
        assert inv.db.execute('SELECT COUNT(*) FROM checkpoints').fetchone() == (0,)
    finally:
        inv.close()


def test_short_header_response_cannot_advance_empty_backfill(tmp_path):
    rpc = RPC([]);rpc.short = True
    inv = inventory(tmp_path, backward=True)
    try:
        with pytest.raises(Reject, match='header missing'):
            inv.scan(rpc, header(138))
        assert inv.get('backfill') == 138 and inv.get('scan_turn') == 2
    finally:
        inv.close()


@pytest.mark.parametrize('tip', [138, 200])
def test_endpoint_head_hash_check_applies_only_when_heights_match(tmp_path, tip):
    rpc = RPC([log(80)])
    inv = inventory(tmp_path, backward=True)
    observed_head = dict(header(tip), hash='0x'+'ff'*32)
    try:
        if tip == 138:
            with pytest.raises(Reject, match='head changed'):
                inv.scan(rpc, observed_head)
            assert inv.get('backfill') == 138 and not inv.registry.keys
        else:
            assert inv.scan(rpc, observed_head) == {Key(USDC, ASSET, 500, 10).id}
            assert inv.get('backfill') == 74
        assert rpc.headers == [80, 138]
    finally:
        inv.close()
