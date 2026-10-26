"""Snapshot age follows the final verified atomic read, not discarded work.

Real RPC batching/parser/timing and synthetic in-memory HTTP; no signer/network.
"""
import copy
import json
import time
from types import SimpleNamespace

import httpx
import pytest
from eth_abi import encode

from searcher.core import Reject, WETH, batch_request_digest, load_config
from searcher.execution import Executor
from searcher.candidate_refresh import refresh_candidate
from searcher.engine import evaluate
from searcher.pipeline import PendingStrategy
from searcher.recorder import Recorder, decode_payload
from searcher.routes import RouteIndex
from searcher.statefeed import (STATE, StateFeed, StateFrame, observation_start_ns,
                                complete_batch_observation_start)
from test_pipeline import ClockRPC, clock_config, saved, header, H0, H1, H2, ZERO
from test_receipt_eligibility import pools as profitable_pools, HEADER
from test_rpc_timing import transport, Limiter, Session


class SnapshotSession:
    def __init__(self, clock, *, pools=None, changed=True, corrupt=None, network_ms=350):
        self.clock, self.pools, self.changed, self.corrupt = clock, pools, changed, corrupt
        self.payloads, self.atomic_reads = [], 0
        self.header_seconds, self.body_seconds = network_ms/1000*3/7, network_ms/1000*4/7

    def stream(self, method, url, json):
        self.payloads.append(json)
        session = self
        class Stream:
            async def __aenter__(self):
                session.clock.advance(session.header_seconds)
                atomic = json[0]['method'] == 'eth_call'
                if atomic: session.atomic_reads += 1
                n = 3 if atomic and session.changed else 2
                h = header(n, H2 if n == 3 else H1, H1 if n == 3 else H0)
                values = []
                for request in json:
                    method, params = request['method'], request['params']
                    if method == 'eth_getBlockByNumber':
                        value = header(n+1,ZERO,h['hash']) if params[0] == 'pending' else h
                    else:
                        assert method == 'eth_call'
                        context = params[3] if params[1] == 'pending' else dict(number=params[1],time='0x68')
                        block,stamp = int(context['number'],16),int(context['time'],16)
                        if session.corrupt == 'clock': stamp += 1
                        ps = session.pools or saved()['pools']
                        if session.pools:
                            rows = []
                            for p in ps:
                                r1 = p['reserve1']
                                if session.atomic_reads >= 2 and p == ps[0]: r1 = 3*10**20
                                rows.append(bytes.fromhex(p['address'][2:])+b'\2'+int(p['reserve0']).to_bytes(14,'big')+
                                    int(r1).to_bytes(14,'big')+(3000).to_bytes(3,'big'))
                            raw = b'\2'+block.to_bytes(32,'big')+stamp.to_bytes(32,'big')+len(rows).to_bytes(2,'big')+b''.join(rows)
                            value = '0x'+encode(['bytes'],[raw]).hex()
                        else:
                            rows = [(p['address'],True,int(p['sqrt_price']),0,10**12,
                                     1000*session.atomic_reads,[-1,0,1],[0,0,0],[]) for p in ps]
                            if session.corrupt == 'missing': rows = rows[:-1]
                            if session.corrupt == 'order': rows.reverse()
                            value = '0x'+encode([STATE+'[]','uint256','uint256'],[rows,block,stamp]).hex()
                    values.append({'id':request['id'],'result':value})
                class Response(httpx.Response):
                    async def aread(self): session.clock.advance(session.body_seconds); return self.content
                return Response(200,json=list(reversed(values)),request=httpx.Request('POST','https://example.invalid'))
            async def __aexit__(self,*args): pass
        return Stream()
    async def aclose(self): pass


def read_frame(transport, *, changed=True, corrupt=None, delay=.6, network_ms=350):
    rpc,clock = transport
    rpc.public_rate_limiter = Limiter(clock,delay=delay)
    rpc.session = SnapshotSession(clock,changed=changed,corrupt=corrupt,network_ms=network_ms)
    return StateFeed(rpc,clock_config(),saved()).poll()


def test_second_complete_read_has_own_age_and_keeps_entire_poll_latency(transport):
    rpc,clock = transport
    frame = read_frame(transport)
    batches = frame.transport['rpc_batches']
    assert frame.transport['clock_attempts'] == 2
    assert frame.received_ns-frame.requested_ns == 2_850_000_000
    assert frame.received_ns-observation_start_ns(frame) == 350_000_000
    assert frame.observation_started_ns == batches[-1]['attempt_timings'][0]['network_started_ns']
    assert frame.transport['poll_public_limiter_wait_ms'] == pytest.approx(1800)
    assert frame.transport['poll_network_total_ms'] == pytest.approx(1050)
    assert [b['role'] for b in batches] == ['clock_bootstrap','atomic_snapshot','atomic_snapshot']
    assert all(p['fee'] == 2000 for p in frame.snapshot_pools), 'Discarded first-attempt states leaked.'
    assert frame.context_block == 4 and frame.header['hash'] == H2
    assert frame.payload()['observation_freshness_origin'] == 'atomic_snapshot_network_send'


@pytest.mark.parametrize('corrupt,reason',[('clock','clock mismatch'),('missing','incomplete'),('order','order mismatch')])
def test_new_timing_does_not_authorize_incomplete_or_wrong_atomic_state(transport,corrupt,reason):
    with pytest.raises(Reject,match=reason): read_frame(transport,corrupt=corrupt)


@pytest.mark.parametrize('mutation', ['digest','method','methods','outcome','batch_start','future',
    'missing_attempt','count','partial','aggregate_last','reverse_attempt','boolean','missing_binding'])
def test_unbound_or_incomplete_last_batch_retains_poll_start(transport,mutation):
    rpc,clock = transport
    batch = rpc.batch
    def tampered(calls):
        result = batch(calls)
        if calls[0][0] != 'eth_call': return result
        row = rpc.measurements[-1]
        if mutation == 'digest': row['batch_request_digest'] = 'wrong'
        elif mutation == 'method': row['method'] = 'eth_call'
        elif mutation == 'methods': row['methods'] = list(reversed(row['methods']))
        elif mutation == 'outcome': row['outcome'] = 'error'
        elif mutation == 'batch_start': row['batch_started_ns'] = 0
        elif mutation == 'future': row['network_finished_ns'] = round(clock.now*1e9)+1
        elif mutation == 'missing_attempt': row['attempt_timings'] = []
        elif mutation == 'count': row['attempts'] += 1
        elif mutation == 'partial': row['attempt_timings'][0]['timing_complete'] = False
        elif mutation == 'aggregate_last': row['network_started_ns'] += 1
        elif mutation == 'reverse_attempt': row['attempt_timings'][0]['network_started_ns'] = row['network_finished_ns']+1
        elif mutation == 'boolean': row['attempt_timings'][0]['network_started_ns'] = True
        elif mutation == 'missing_binding': row.pop('batch_started_ns')
        return result
    rpc.batch = tampered
    frame = read_frame(transport)
    assert frame.observation_started_ns is None
    assert observation_start_ns(frame) == frame.requested_ns
    assert frame.transport['observation_age_at_receipt_ms'] == 2850


def test_reused_measurement_object_is_not_fresh_provenance(transport):
    rpc,clock = transport
    frame = read_frame(transport,changed=False)
    old = rpc.measurements[-1]
    batch = rpc.batch
    def reused(calls):
        value = batch(calls)
        old.update(rpc.measurements[-1])
        rpc.measurements[-1] = old
        return value
    rpc.batch = reused
    feed = StateFeed(rpc,clock_config(),saved())
    feed.anchor,feed.pending = header(2,H1,H0),header(3,ZERO,H1)
    frame = feed.poll()
    assert frame.observation_started_ns is None


def test_sealed_unknown_and_deserialized_frames_do_not_regain_age(transport):
    rpc,clock = transport
    frame = read_frame(transport,changed=False)
    clone = StateFrame(frame.header,frame.latest,frame.snapshot_pools,frame.requested_ns,frame.received_ns,
                       frame.received_utc,frame.context_block,frame.context_time,
                       transport=copy.deepcopy(frame.transport),observation_started_ns=frame.observation_started_ns)
    assert observation_start_ns(clone) == clone.requested_ns
    unknown = SimpleNamespace(**vars(frame))
    assert observation_start_ns(unknown) == unknown.requested_ns
    sealed = copy.deepcopy(frame); sealed.state_tag = '0x2'
    assert observation_start_ns(sealed) == sealed.requested_ns
    clock.advance(3)
    assert time.monotonic_ns()-observation_start_ns(frame) > 3_000_000_000
    feed = StateFeed(rpc,clock_config(),saved())
    historical = feed.poll(block=2)
    assert historical.observation_started_ns is None


def test_legacy_rpc_without_timing_keeps_conservative_age():
    frame = StateFeed(ClockRPC([header(3,H2,H1),header(3,H2,H1)]),clock_config(),saved()).poll()
    assert frame.observation_started_ns is None and observation_start_ns(frame) == frame.requested_ns


def test_complete_fragmented_batch_and_retry_use_earliest_send(transport):
    rpc,clock = transport
    rpc.public_rate_limiter = Limiter(clock,delay=.6)
    rpc.session = Session(clock,statuses=(200,200,200))
    calls = [('eth_call',[i]) for i in range(40)]
    before = time.monotonic_ns()
    rpc.batch(calls)
    after = time.monotonic_ns()
    row = rpc.measurements[-1]
    assert complete_batch_observation_start(row,calls,before,after) == 100_600_000_000
    assert after-row['network_started_ns'] > 1_200_000_000
    # The same full list also covers a future HTTP retry implementation: wait
    # between sends remains part of age; only before-first-send wait is excluded.
    row = copy.deepcopy(row)
    row['attempt_timings'][0]['http_status'] = 503
    assert complete_batch_observation_start(row,calls,before,after) == row['network_started_ns']
    row['attempt_timings'][1].pop('network_started_ns')
    assert complete_batch_observation_start(row,calls,before,after) is None


def test_recorder_preserves_poll_columns_and_raw_observation_without_rebasing(transport,tmp_path):
    frame = read_frame(transport)
    recorder = Recorder(tmp_path/'frames.sqlite')
    try:
        recorder.append(frame.requested_ns,frame.received_ns,'test',frame.payload(),frame.received_utc)
        requested,received,payload = recorder.db.execute('SELECT requested_ns,received_ns,payload FROM frames').fetchone()
        decoded = decode_payload(payload)
        assert (requested,received) == (frame.requested_ns,frame.received_ns)
        assert decoded['observation_started_ns'] == frame.observation_started_ns
        assert decoded['transport']['clock_attempts'] == 2
        assert recorder.summary()['frames'][0]['mean_request_ms'] == 2850
    finally: recorder.close()


def test_refresh_reprices_complete_new_snapshot_then_expires_on_unchanged_deadline(transport):
    rpc,clock = transport
    config = load_config()
    config['policy'].update(max_quote_ms=500,sizing_mode='fixed',cycle_pages=False)
    config['settlement_tokens'] = [dict(config['settlement_tokens'][0],max_input_raw=str(10**18))]
    ps = profitable_pools()[2:]
    index = RouteIndex(ps,[WETH],2)
    previous = evaluate(index,config,HEADER)['candidates'][0]
    route = index.routes[previous['route_key']]
    rpc.public_rate_limiter = Limiter(clock,delay=.6)
    rpc.session = SnapshotSession(clock,pools=[p.json() for p in ps])
    candidate,states,anchor,deadline,evidence = refresh_candidate(rpc,config,route,index.states,previous_candidate=previous)
    assert candidate is not None and candidate['sizing_mode'] == 'previous_amount_repriced'
    assert candidate['amount_out'] == route.quote(states,previous['amount_in'])[0]
    assert evidence['decision']['decision_age_ms'] == 2850
    assert evidence['decision']['observation_age_ms'] == 350
    assert deadline == evidence['observation_started_ns']+2_000_000_000
    assert candidate['amount_out'] != previous['amount_out']
    assert rpc.session.atomic_reads == 2
    clock.now = deadline/1e9+.000001
    executor = Executor.__new__(Executor); executor.config=config; executor.rpc=rpc
    with pytest.raises(Reject,match='observation expired'):
        executor.execute(candidate,route,states,anchor,deadline)
    assert rpc.session.atomic_reads == 2, 'An old frame secretly refreshed its age.'


def test_strategy_reports_both_ages_without_changing_poll_metric(transport):
    rpc,clock = transport
    frame = read_frame(transport)
    strategy = PendingStrategy(rpc,clock_config(),saved())
    report = strategy.on_snapshot(frame)
    assert report['decision_age_ms'] == report['feed_request_ms'] == 2850
    assert report['observation_age_ms'] == 350


# Duration conditions retained from public snapshots in the live audit. The
# state, RPC replies and binding below are newly generated synthetic evidence;
# these tests never fabricate timing hashes for the old recorded frames.
@pytest.mark.parametrize('frame_id,poll_ms,final_ms', [
    (18,2728.892958,424.889375), (29,2663.193459,381.794083),
    (981,4613.863208,412.688), (996,2078.831875,387.05075),
    (1008,2076.354041,351.9465), (2808,3389.881042,355.734458)])
def test_observed_slow_poll_conditions_keep_new_atomic_state_eligible(transport,frame_id,poll_ms,final_ms):
    frame = read_frame(transport,network_ms=final_ms,delay=(poll_ms/3-final_ms)/1000)
    assert (frame.received_ns-frame.requested_ns)/1e6 == pytest.approx(poll_ms,abs=.000002)
    assert (frame.received_ns-observation_start_ns(frame))/1e6 == pytest.approx(final_ms,abs=.000002)
    assert frame.received_ns > frame.requested_ns+2_000_000_000
    assert frame.received_ns < observation_start_ns(frame)+2_000_000_000
