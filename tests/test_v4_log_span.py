"""Live provider range rejection: no skipped initialization pages or quota retry."""
import pytest

from searcher.core import RPCError, Reject
from searcher.v4_runtime import Inventory
from test_v4_init_filter import RPC, header, log


class RangeRPC(RPC):
    def __init__(self, logs=(), *, limit=10, error=None, fail_retry=False):
        super().__init__(logs)
        self.limit, self.error, self.fail_retry = limit, error, fail_retry
        self.ranges = []

    def request(self, method, params):
        if method != 'eth_getLogs': return super().request(method, params)
        lo, hi = (int(params[0][key], 16) for key in ('fromBlock', 'toBlock'))
        self.ranges.append((lo, hi))
        if self.error is not None: raise self.error
        if hi-lo+1 > self.limit:
            raise RPCError(method, {'code': -32600, 'message':
                f'You can make eth_getLogs requests with up to a {self.limit} block range. '
                'Based on your parameters, this block range should work: [0x4b, 0x54]'})
        if self.fail_retry: raise RPCError(method, {'code': -32007, 'message': 'request limit'})
        return [item for item in self.logs if lo <= int(item['blockNumber'], 16) <= hi]


def seed(path, *, backward=False):
    inv = Inventory(path)
    with inv.db:
        inv.put('cursor', 138 if backward else 74)
        inv.put('backfill', 138 if backward else 0)
        inv.put('scan_turn', 2 if backward else 0)
    return inv


def test_explicit_live_range_limit_continues_without_gaps_and_survives_restart(tmp_path):
    path=tmp_path/'i.db'; inv=seed(path)
    rpc=RangeRPC([log(n, fee=n) for n in (75,84,85,128,138)])
    inv.scan(rpc,header(138))
    assert rpc.ranges == [(75,138),(75,84)]
    assert inv.get('cursor') == 84
    inv.close(); inv=Inventory(path)
    while inv.get('cursor') < 138: inv.scan(rpc,header(138))
    assert rpc.ranges[2:] == [(85,94),(95,104),(105,114),(115,124),(125,134),(135,138)]
    assert len(inv.registry.keys) == 5
    assert inv.get('initialize_log_span') == 10
    inv.close()


def test_backward_shrink_preserves_upper_edge_and_next_older_page(tmp_path):
    inv=seed(tmp_path/'i.db',backward=True)
    rpc=RangeRPC([log(138),log(128,fee=600)])
    inv.scan(rpc,header(138)); inv.scan(rpc,header(138))
    assert rpc.ranges == [(75,138),(129,138),(119,128)]
    assert inv.get('backfill') == 118 and inv.get('cursor') == 138
    assert len(inv.registry.keys) == 2
    inv.close()


@pytest.mark.parametrize('error',[
    RPCError('eth_getLogs',{'code':-32007,'message':'request limit reached'}),
    RPCError('eth_getLogs',{'code':-32600,'message':'invalid filter'}),
    RPCError('eth_getLogs',{'code':-32600,'message':'up to a 0 block range'}),
    RPCError('eth_getLogs',{'code':-32600,'message':'up to a 64 block range'}),
])
def test_unrelated_errors_or_invalid_limits_do_not_retry_or_advance(tmp_path,error):
    inv=seed(tmp_path/'i.db'); rpc=RangeRPC(error=error)
    with pytest.raises(RPCError): inv.scan(rpc,header(138))
    assert rpc.ranges == [(75,138)] and inv.get('cursor') == 74
    assert inv.get('initialize_log_span') is None
    inv.close()


def test_retry_error_keeps_cursor_and_reorg_resets_learned_hint(tmp_path):
    inv=seed(tmp_path/'i.db'); rpc=RangeRPC(fail_retry=True)
    with pytest.raises(RPCError): inv.scan(rpc,header(138))
    assert rpc.ranges == [(75,138),(75,84)] and inv.get('cursor') == 74
    rpc.fail_retry=False; inv.scan(rpc,header(138))
    assert inv.get('cursor') == 84
    with inv.db: inv.db.execute('UPDATE checkpoints SET hash=?',('wrong',))
    assert inv.check_reorg(rpc)
    assert inv.get('initialize_log_span') is None and inv.get('cursor') is None
    inv.close()


def test_smaller_page_still_requires_canonical_logs(tmp_path):
    inv=seed(tmp_path/'i.db'); rpc=RangeRPC([dict(log(80),blockHash='wrong')])
    with pytest.raises(Reject,match='not canonical'): inv.scan(rpc,header(138))
    assert inv.get('cursor') == 74 and not inv.registry.keys
    inv.close()
