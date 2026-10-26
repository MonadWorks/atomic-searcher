"""Read versions belong to complete groups, including ordinary rotation."""
import copy

from searcher.fastlane import HotRoutes
from test_fastlane import config
from test_pipeline import saved,A,B
from test_hot_selection_independent import parallel_universe


def pair(active=(A,)):
    clock=[100.0]
    selector=HotRoutes(saved(),config(),clock=lambda:clock[0])
    selector.add([(pool,clock[0]) for pool in active])
    return clock,selector


def test_successful_ordinary_group_is_not_read_repeatedly_for_same_event():
    clock,selector=pair()
    selected=selector.take()
    assert selected['groups']==[(A,B)] and selected['_coactive_versions']=={}
    assert selected['_group_versions']=={(A,B):((A,100.0),)}
    selector.acknowledge(selected)
    assert selector.read_versions[(A,B)]==((A,100.0),)
    assert selector.summary()['coactive_read']==0
    for _ in range(5):assert selector.take()['groups']==[]
    clock[0]=100.1
    selector.add([(A,100.0)])
    assert selector.take()['groups']==[]
    selector.add([(B,100.1)])
    assert selector.take()['groups']==[(A,B)]


def test_taking_without_reserving_preserves_existing_selection_rotation():
    _,selector=pair()
    first=selector.take()
    assert selector.take()['_group_versions']==first['_group_versions']
    assert selector.reserved_versions=={} and selector.read_versions=={}


def test_reserve_blocks_duplicate_read_and_failure_restores_same_version():
    _,selector=pair()
    first=selector.take();selector.reserve(first)
    assert selector.take()['groups']==[]
    assert selector.summary()['reserved_groups']==1
    assert not selector.read_versions
    selector.failed(first)
    retry=selector.take()
    assert retry['_group_versions']==first['_group_versions']
    assert selector.reserved_versions=={} and selector.summary()['read_failures']==1
    selector.reserve(retry);selector.acknowledge(retry)
    assert selector.reserved_versions=={} and selector.take()['groups']==[]


def test_new_event_can_be_reserved_while_older_read_is_inflight():
    clock,selector=pair((A,B))
    older=selector.take();selector.reserve(older)
    clock[0]=100.2;selector.add([(A,clock[0])])
    newer=selector.take();selector.reserve(newer)
    assert len(selector.reserved_versions[(A,B)])==2
    assert selector.take()['groups']==[]
    selector.acknowledge(older)
    assert selector.reserved_versions[(A,B)]=={newer['_group_versions'][(A,B)]}
    assert selector.pending[(A,B)]['version']==newer['_coactive_versions'][(A,B)]
    assert selector.take()['groups']==[]
    selector.failed(newer)
    retry=selector.take()
    assert retry['_group_versions']==newer['_group_versions']


def test_late_old_success_never_regresses_new_success_or_rearms_same_event():
    clock,selector=pair()
    older=selector.take();selector.reserve(older)
    clock[0]=100.2;selector.add([(A,clock[0])])
    newer=selector.take();selector.reserve(newer)
    selector.acknowledge(newer)
    assert selector.reserved_versions[(A,B)]=={older['_group_versions'][(A,B)]}
    selector.acknowledge(older)
    assert selector.read_versions[(A,B)]==newer['_group_versions'][(A,B)]
    assert selector.reserved_versions=={} and selector.take()['groups']==[]


def test_late_old_failure_releases_only_old_reservation():
    clock,selector=pair()
    older=selector.take();selector.reserve(older)
    clock[0]=100.2;selector.add([(A,clock[0])])
    newer=selector.take();selector.reserve(newer)
    selector.failed(older)
    assert selector.reserved_versions[(A,B)]=={newer['_group_versions'][(A,B)]}
    assert selector.take()['groups']==[] and selector.read_versions=={}
    selector.release(newer)
    assert selector.take()['groups']==[(A,B)]
    assert selector.summary()['read_failures']==1


def test_delayed_reserve_uses_captured_version_and_does_not_swallow_new_event():
    clock,selector=pair()
    older=selector.take()
    clock[0]=100.2;selector.add([(A,clock[0])])
    selector.reserve(older)
    assert selector.reserved_versions[(A,B)]=={((A,100.0),)}
    assert selector.take()['_group_versions'][(A,B)]==((A,100.2),)


def test_expiring_an_older_trigger_does_not_create_new_read_version():
    clock,selector=pair()
    clock[0]=100.5;selector.add([(B,100.5)])
    read=selector.take();selector.reserve(read);selector.acknowledge(read)
    clock[0]=101.6
    assert selector.take()['groups']==[]
    assert selector._signature((A,B))==((B,100.5),)


def test_reserved_or_successful_groups_do_not_skip_unread_siblings():
    universe,addresses=parallel_universe(12)
    selector=HotRoutes(universe,config(),clock=lambda:100.0)
    selector.add([(addresses[0],100.0)])
    first=selector.take();selector.reserve(first)
    seen=set(first['groups'])
    for _ in range(20):
        selection=selector.take()
        assert not seen.intersection(selection['groups'])
        seen.update(selection['groups'])
        selector.acknowledge(selection)
    expected={tuple(sorted((addresses[0],other))) for other in addresses[1:]}
    assert seen==expected
    selector.failed(first)
    retried=set()
    for _ in range(12):
        selection=selector.take()
        retried.update(selection['groups']);selector.acknowledge(selection)
    assert retried==set(first['groups'])


def test_resume_copies_reservations_without_sharing_mutable_sets():
    _,selector=pair((A,B))
    selected=selector.take();selector.reserve(selected)
    replacement=HotRoutes(saved(),config(),clock=lambda:100.0)
    replacement.resume(selector)
    assert replacement.reserved_versions==selector.reserved_versions
    assert replacement.take()['groups']==[]
    replacement.release(selected)
    assert selector.reserved_versions
    assert replacement.take()['groups']==[(A,B)]
    assert replacement.summary()['read_failures']==0


def test_resume_invalidates_reservations_when_static_metadata_changes():
    _,selector=pair((A,B))
    selected=selector.take();selector.reserve(selected)
    changed=copy.deepcopy(saved())
    for pool in changed['pools']:pool['token1']='0x'+f'{23:040x}'
    replacement=HotRoutes(changed,config(),clock=lambda:100.0)
    replacement.resume(selector)
    assert replacement.reserved_versions=={}
    assert replacement.take()['groups']==[(A,B)]


def test_failed_coactive_read_preserves_priority_statistics_until_success():
    _,selector=pair((A,B))
    selected=selector.take();selector.reserve(selected);selector.failed(selected)
    assert selector.summary()['coactive_read']==0
    retry=selector.take();selector.reserve(retry);selector.acknowledge(retry)
    assert selector.summary()['coactive_read']==1
    assert selector.summary()['read_failures']==1
    assert not selector.pending and not selector.reserved_versions
