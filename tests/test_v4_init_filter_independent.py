"""Independent Initialize prefilter boundaries; synthetic RPC, no network."""
import pytest
from eth_abi import encode
from eth_utils import keccak

from searcher.core import USDC, Reject
from searcher.v4_market import KEY_TYPES, Key
from searcher.v4_runtime import Inventory
from test_v4_discovery_independent import ASSET, BatchLimitedRPC, header, initialization, seed_cursor


def unsupported_log(kind, height):
    key = Key(USDC, ASSET, 500, 10)
    log = initialization(key, height)
    values = list(key.values())
    if kind == 'non_usdc':
        values[:2] = ['0x'+'11'*20, '0x'+'22'*20]
    elif kind == 'hook':
        values[4] = '0x'+'33'*20
    elif kind == 'dynamic_fee':
        values[2] = 1 << 23
    else:
        raise AssertionError(kind)
    log['topics'][1] = '0x'+keccak(encode(KEY_TYPES, values)).hex()
    log['topics'][2:] = ['0x'+encode(['address'],[value]).hex() for value in values[:2]]
    log['data'] = '0x'+encode(['uint24','int24','address','uint160','int24'],
                            [values[2],values[3],values[4],1 << 96,0]).hex()
    return log


def requested_headers(rpc):
    return [int(params[0],16) for calls in rpc.batches for method,params in calls
            if method == 'eth_getBlockByNumber']


@pytest.mark.parametrize('kind',['non_usdc','hook','dynamic_fee'])
def test_unsupported_initializations_cost_only_endpoint_header_and_admit_nothing(tmp_path,kind):
    rpc = BatchLimitedRPC()
    rpc.initializations = [unsupported_log(kind, 80+i) for i in range(10)]
    inv = Inventory(tmp_path/'inventory.sqlite')
    try:
        seed_cursor(inv)
        assert inv.scan(rpc,rpc.header()) == set()
        assert requested_headers(rpc) == [138]
        assert not inv.registry.keys
        assert inv.db.execute('SELECT COUNT(*) FROM pools').fetchone()[0] == 0
        assert inv.get('cursor') == 138
        assert inv.db.execute('SELECT hash FROM checkpoints WHERE block=138').fetchone()[0] == header(138)['hash']
    finally:
        inv.close()


def test_mixed_page_preserves_every_supported_key_and_shared_height_authentication(tmp_path):
    rpc = BatchLimitedRPC()
    good = [Key(USDC,ASSET,500+i,10) for i in range(3)]
    rpc.initializations = [unsupported_log('hook',79),initialization(good[0],80),
                          unsupported_log('dynamic_fee',81),initialization(good[1],81),
                          initialization(good[2],81),unsupported_log('non_usdc',82)]
    inv = Inventory(tmp_path/'inventory.sqlite')
    try:
        seed_cursor(inv)
        assert inv.scan(rpc,rpc.header()) == {key.id for key in good}
        assert set(inv.registry.keys) == {key.id for key in good}
        assert requested_headers(rpc) == [80,81,138]
        assert max(map(len,rpc.batches)) <= 2
    finally:
        inv.close()


def test_supported_orphan_cannot_be_prefiltered_or_partially_persisted(tmp_path):
    rpc = BatchLimitedRPC()
    first,orphan = [Key(USDC,ASSET,500+i,10) for i in range(2)]
    bad = initialization(orphan,82)
    bad['blockHash'] = '0x'+'ff'*32
    rpc.initializations = [initialization(first,80),unsupported_log('hook',81),bad]
    inv = Inventory(tmp_path/'inventory.sqlite')
    try:
        seed_cursor(inv)
        with pytest.raises(Reject,match='canonical'):
            inv.scan(rpc,rpc.header())
        assert requested_headers(rpc) == [80,82,138]
        assert not inv.registry.keys
        assert inv.db.execute('SELECT COUNT(*) FROM pools').fetchone()[0] == 0
        assert inv.get('cursor') == 74 and inv.get('scan_turn') == 1
    finally:
        inv.close()


@pytest.mark.parametrize('kind',['non_usdc','hook','dynamic_fee'])
def test_unsupported_out_of_range_log_rejects_before_metadata_progress(tmp_path,kind):
    class UnfilteredProvider(BatchLimitedRPC):
        def request(self,method,params):
            if method == 'eth_getLogs':
                return [unsupported_log(kind,139)]
            return super().request(method,params)
    rpc = UnfilteredProvider()
    inv = Inventory(tmp_path/'inventory.sqlite')
    try:
        seed_cursor(inv)
        with pytest.raises(Reject,match='range'):
            inv.scan(rpc,rpc.header())
        assert not rpc.batches
        assert inv.get('cursor') == 74 and inv.get('scan_turn') == 1
        assert not inv.registry.keys
    finally:
        inv.close()


@pytest.mark.parametrize('malformed',['address','topic'])
def test_wrong_filter_response_cannot_admit_pool_metadata(tmp_path,malformed):
    rpc = BatchLimitedRPC()
    log = unsupported_log('hook',80)
    if malformed == 'address': log['address'] = '0x'+'44'*20
    else: log['topics'][0] = '0x'+'55'*32
    rpc.initializations = [log]
    inv = Inventory(tmp_path/'inventory.sqlite')
    try:
        seed_cursor(inv)
        assert inv.scan(rpc,rpc.header()) == set()
        assert requested_headers(rpc) == [138]
        assert inv.get('cursor') == 138 and not inv.registry.keys
        assert inv.db.execute('SELECT COUNT(*) FROM pools').fetchone()[0] == 0
    finally:
        inv.close()


@pytest.mark.parametrize('historical',[False,True])
def test_missing_endpoint_header_never_advances_live_or_history_cursor(tmp_path,historical):
    class MissingEndpoint(BatchLimitedRPC):
        def batch(self,calls):
            self.batches.append(tuple(calls))
            return []
    rpc = MissingEndpoint()
    rpc.initializations = [unsupported_log('hook',80 if not historical else 60)]
    inv = Inventory(tmp_path/'inventory.sqlite')
    try:
        seed_cursor(inv)
        if historical:
            with inv.db:
                inv.put('cursor',138)
                inv.put('backfill',70)
                inv.put('scan_turn',2)
        before=(inv.get('cursor'),inv.get('backfill'),inv.get('scan_turn'))
        with pytest.raises(Reject,match='header missing'):
            inv.scan(rpc,rpc.header())
        assert (inv.get('cursor'),inv.get('backfill'),inv.get('scan_turn')) == before
        assert inv.db.execute('SELECT 1 FROM checkpoints WHERE block=138').fetchone() is None
        assert not inv.registry.keys
    finally:
        inv.close()


def test_unsupported_only_backfill_advances_history_not_live_cursor(tmp_path):
    rpc = BatchLimitedRPC(tip=138)
    rpc.initializations = [unsupported_log('non_usdc',40),unsupported_log('hook',60)]
    inv = Inventory(tmp_path/'inventory.sqlite')
    try:
        with inv.db:
            inv.put('cursor',138)
            inv.put('backfill',70)
            inv.put('scan_turn',2)
        assert inv.scan(rpc,rpc.header()) == set()
        assert requested_headers(rpc) == [70]
        assert inv.get('cursor') == 138 and inv.get('backfill') == 6
        assert not inv.registry.keys
    finally:
        inv.close()
