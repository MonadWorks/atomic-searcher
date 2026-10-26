"""Reproduce the production TTL/retry race without live RPC or signing."""
from searcher.routes import Route
from test_transfer_retry import registry
from test_transfer_feedback import OTHER_POOL, sample_result


def advanced():
    clock,f,sample,route=registry()
    for when, delay in ((31,60),(92,120),(213,240)):
        clock[0]=when
        assert f.claim_probe(route)
        assert f.rejected(route, {'revert_name':'BadCallback'})==delay
    clock[0]=301
    f.record(sample,sample_result(sample,funded=96))
    clock[0]=454
    assert f.claim_probe(route)
    assert f.rejected(route, {'revert_name':'BadCallback'})==300
    return clock,f,sample,route


def test_expiring_helper_does_not_erase_real_retry_schedule():
    clock,f,sample,route=advanced()
    clock[0]=602
    assert f.deferred(route) and not f.edges
    alternate=Route(route.tokens,(sample.source,OTHER_POOL))
    assert f.deferred(alternate)
    assert not f.claim_probe(route) and not f.claim_probe(alternate)
    clock[0]=755
    assert not f.deferred(route)
    assert f.claim_probe(route) and not f.claim_probe(alternate)
    assert f.rejected(route, {'revert_name':'BadCallback'})==0
    clock[0]=786
    assert f.claim_probe(alternate)


def test_helper_refresh_never_steals_due_real_retry_slot():
    clock,f,sample,route=advanced()
    for when in (755,1056,1357,1658):
        clock[0]=when
        f.record(sample,sample_result(sample,funded=96))
        assert f.claim_probe(route)
        assert f.rejected(route,{'revert_name':'BadCallback'})==300


def test_pass_clears_sample_history_and_claims_after_expiry():
    clock,f,sample,route=advanced()
    clock[0]=755
    assert f.claim_probe(route)
    f.passed(route)
    assert not f.history and not f.edges and not f.probe_slots
    assert f.priority(route)==0 and not f.deferred(route)


def test_reverse_direction_remains_available_during_expired_sample_delay():
    clock,f,sample,route=advanced()
    clock[0]=602
    reverse=Route(route.tokens,tuple(reversed(route.pools)))
    assert f.deferred(route)
    assert not f.deferred(reverse) and f.priority(reverse)==0
    assert f.claim_probe(reverse)
