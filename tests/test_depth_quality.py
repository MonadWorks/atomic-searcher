"""Fill unused continuation batch space without stealing rollback capacity."""
import pytest

from searcher.statefeed import DeepReadScheduler


def group(prefix, score=0, **kwargs):
    return {'pools': [prefix+'-a', prefix+'-b'], 'score': score, **kwargs}


def available(*rows):
    return {pool for row in rows for pool in row['pools']}


def test_one_old_continuation_preserved_before_new_strong_signal_in_same_read():
    scheduler = DeepReadScheduler()
    old = group('old', deep=True, since=1, route_keys=['old-direction'])
    new = group('new', score=10**9, route_keys=['new-direction'])
    assert scheduler.resume([old], 1) == []
    scheduler.add([new], 2)
    scheduler.take(available(old, new), 3)
    rows = scheduler.selected_requests
    assert [row['pools'] for row in rows] == [tuple(old['pools']), tuple(new['pools'])]
    assert rows[0]['since'] == 1 and rows[0]['route_keys'] == ['old-direction']
    assert rows[1]['score'] == 10**9 and rows[1]['route_keys'] == ['new-direction']
    assert scheduler.selected_deep_pools == available(old, new)
    # Failure must return both exact requests, not silently drop the newer one.
    assert scheduler.resume(rows, 4) == []
    assert len(scheduler.resuming) == 2


@pytest.mark.parametrize('occupied', [1012, 1020, 1023, 1024])
def test_old_selected_slot_is_reserved_for_its_return_and_not_reused_by_new(occupied):
    scheduler = DeepReadScheduler()
    old = [group(f'old-{i}', deep=True, since=1) for i in range(occupied)]
    new = [group(f'new-{i}', score=10**9-i) for i in range(11)]
    assert scheduler.resume(old, 1) == []
    scheduler.add(new, 2)
    # Only one existing continuation is currently eligible. Other old work
    # still owns its durable slots even while transfer feedback defers it.
    scheduler.take(available(*(old+new)), 3,
        lambda row: row['pools'][0].startswith('old-') and tuple(row['pools']) != tuple(old[0]['pools']))
    selected = scheduler.selected_requests
    expected_new = min(11, 1024-occupied)
    assert selected[0]['pools'] == tuple(old[0]['pools'])
    assert len(selected) == 1+expected_new
    assert len(scheduler.resuming)+len(selected) <= 1024
    assert len(scheduler.selected_deep_pools) <= 24
    assert scheduler.resume(selected, 4) == []
    assert len(scheduler.resuming) == occupied+expected_new
    # A repeated ACK/failure handoff merges, never double counts the new work.
    assert scheduler.resume(selected, 5) == []
    assert len(scheduler.resuming) == occupied+expected_new


def test_old_batch_at_group_or_pool_cap_precedes_every_new_signal():
    for old in ([group(f'old-{i}', deep=True, since=1) for i in range(12)],
                [{'pools': [f'old-{i}-{j}' for j in range(4)], 'deep':True, 'since':1}
                 for i in range(6)]):
        scheduler = DeepReadScheduler()
        new = group('strong', score=10**9)
        scheduler.resume(old, 1)
        scheduler.add([new], 2)
        scheduler.take(available(*(old+[new])), 3)
        assert [row['pools'] for row in scheduler.selected_requests] == [tuple(row['pools']) for row in old]
        assert tuple(new['pools']) in scheduler.pending
        assert len(scheduler.selected_requests) <= 12 and len(scheduler.selected_deep_pools) <= 24


def test_shallow_continuation_stays_shallow_when_new_deep_request_fills_space():
    scheduler = DeepReadScheduler()
    old = group('old', deep=False, since=1)
    new = group('strong', score=10**9)
    scheduler.resume([old], 1)
    scheduler.add([new], 2)
    scheduler.take(available(old, new), 3)
    assert len(scheduler.selected_requests) == 2
    assert not scheduler.selected_requests[0]['deep']
    assert scheduler.selected_deep_pools == set(new['pools'])
    assert tuple(old['pools']) not in scheduler.last
    assert scheduler.last[tuple(new['pools'])] == 3


def test_fill_preserves_existing_fifo_exploration_and_ranked_signal_order():
    scheduler = DeepReadScheduler()
    old = group('old', deep=True, since=0)
    cold = [group('cold-0'), group('cold-1')]
    weak, strong = group('weak', score=1), group('strong', score=10**9)
    scheduler.resume([old], 0)
    scheduler.add(cold, 1)
    scheduler.add([weak, strong], 2)
    scheduler.take(available(old, *cold, weak, strong), 3)
    assert [row['pools'] for row in scheduler.selected_requests] == [
        tuple(row['pools']) for row in [old, *cold, strong, weak]]
