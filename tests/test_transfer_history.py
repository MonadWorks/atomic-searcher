"""Regress the live 60 -> 120 -> quiet gap -> 60 retry reset.

Old evidence may expire without resetting learning, but historical learning
alone must never blacklist a route. A real successful executor call clears it.
"""
from searcher.transfer_feedback import TransferFeedback, TransferSample
from test_transfer_feedback import EXECUTOR, OWNER, sample_result
from test_transfer_retry import registry


def reject(f, route, clock):
    clock[0] = max(clock[0], max(x['next_probe'] for x in f.edges.values())) + .01
    assert f.claim_probe(route)
    return f.rejected(route, {'revert_name':'BadCallback'})


def test_quiet_gap_preserves_history_without_using_stale_evidence_to_block():
    clock,f,sample,route = registry()
    assert reject(f,route,clock) == 60
    assert reject(f,route,clock) == 120
    clock[0] += 301
    assert not f.deferred(route) and not f.edges
    assert f.priority(route) > 0
    assert f.claim_probe(route)
    assert f.rejected(route, {'revert_name':'BadCallback'}) == 0
    clock[0] += f.retry_seconds + .01
    f.record(sample,sample_result(sample,funded=96))
    assert not f.deferred(route)
    assert all(x['repeated_reverts']==2 for x in f.edges.values())
    assert reject(f,route,clock) == 240


def test_unvisited_expired_evidence_does_not_restore_pending_retry_or_expired_clock():
    clock,f,sample,route = registry()
    assert reject(f,route,clock)==60
    clock[0] += 301  # No active_edges call to clean the expired entry.
    f.record(sample,sample_result(sample,funded=96))
    assert all(x['next_probe'] < clock[0] and not x['probe_pending'] for x in f.edges.values())
    assert reject(f,route,clock)==120


def test_success_after_evidence_expiry_clears_all_related_history():
    clock,f,sample,route = registry()
    assert reject(f,route,clock)==60
    clock[0] += 301
    f.passed(route)
    assert not f.history and not f.edges
    clock[0] += 301
    f.record(sample,sample_result(sample,funded=96))
    assert reject(f,route,clock)==60


def test_history_expires_independently_and_helper_samples_cannot_extend_it():
    clock,f,sample,route = registry()
    assert reject(f,route,clock)==60
    until=next(iter(f.history.values()))['until']
    clock[0]=until-10
    f.record(sample,sample_result(sample,funded=96))
    assert next(iter(f.history.values()))['until']==until
    clock[0]=until+301
    f.record(sample,sample_result(sample,funded=96))
    assert not f.history
    assert reject(f,route,clock)==60


def test_history_capacity_is_bounded_and_unknown_errors_do_not_learn():
    clock,f,sample,route = registry()
    f.capacity=2
    for n in range(4):
        s=TransferSample(sample.token,'0x'+f'{n+100:040x}',sample.destination,100)
        r=type(route)(route.tokens,(s.source,s.destination))
        f.record(s,sample_result(s,funded=96))
        assert reject(f,r,clock)==60
        assert len(f.history)<=2
    saved=dict(f.history)
    clock[0]=max(x['next_probe'] for x in f.edges.values())+.01
    assert f.claim_probe(r)
    assert f.rejected(r, {'revert_name':'BadCallback','validation_deadline_reached':True})==0
    assert dict(f.history)==saved
