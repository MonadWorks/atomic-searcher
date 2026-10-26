"""Generation cursors wake hot reads without replaying consumed deliveries."""
import threading
import time

from searcher.activity import ActivityInbox


def test_bursts_coalesce_and_drain_atomically_returns_the_delivery_cursor():
    inbox=ActivityInbox()
    inbox.publish(['a','b'],1)
    inbox.publish(iter(['b','c','b']),2)
    assert inbox.generation==2
    generation,events=inbox.drain_batch(3)
    assert generation==2 and dict(events)=={'a':1,'b':2,'c':2}
    assert len(events)==3
    assert inbox.drain_batch(3)==(2,[])
    assert inbox.wait(0,after=generation)==generation


def test_legacy_drain_preserves_reception_time_and_five_second_window():
    inbox=ActivityInbox()
    inbox.publish(['too_old'],0)
    inbox.publish(['boundary'],1)
    inbox.publish(['current'],6)
    inbox.publish(['future'],7)
    assert dict(inbox.drain(6))=={'boundary':1,'current':6}
    assert inbox.drain(8)==[]
    # Delivery timestamps keep the existing last-publish semantics; neither
    # a generation nor a wakeup invents a new event or refreshes its age.
    inbox.publish(['same_pool'],10)
    inbox.publish(['same_pool'],9)
    assert inbox.drain(10)==[('same_pool',9)]


def test_empty_publish_does_not_create_new_work_and_wait_does_not_consume():
    inbox=ActivityInbox()
    inbox.publish([],1)
    inbox.publish(iter(()),1)
    assert inbox.generation==0 and inbox.wait(0)==0
    inbox.publish(['a'],1)
    assert inbox.wait(0)==1
    assert inbox.wait(0,after=0)==1
    assert inbox.wait(0,after=1)==1
    assert inbox.drain_batch(1)==(1,[('a',1)])


def test_publish_between_drain_and_wait_is_immediately_visible():
    inbox=ActivityInbox()
    inbox.publish(['a'],1)
    generation,_=inbox.drain_batch(1)
    inbox.publish(['b'],2)
    started=time.monotonic()
    assert inbox.wait(2,after=generation)==generation+1
    assert time.monotonic()-started<.5
    assert inbox.drain_batch(2)==(2,[('b',2)])


def test_new_delivery_during_inflight_read_is_not_consumed_by_older_ack():
    inbox=ActivityInbox()
    inbox.publish(['a'],1)
    read_generation,read_events=inbox.drain_batch(1)
    inbox.publish(['a'],2)
    inbox.publish(['b'],3)
    # A consumer may successfully finish the first read now. It must retain
    # its captured cursor rather than acknowledge inbox.generation here.
    successful_generation=read_generation
    assert inbox.wait(0,after=successful_generation)==3
    assert read_events==[('a',1)]
    assert inbox.drain_batch(3)==(3,[('a',2),('b',3)])


def test_publish_wakes_all_waiters_without_any_waiter_draining_events():
    inbox=ActivityInbox();ready=threading.Barrier(3);results=[]
    def waiter():
        ready.wait(timeout=1)
        results.append(inbox.wait(2,after=0))
    threads=[threading.Thread(target=waiter) for _ in range(2)]
    for thread in threads:thread.start()
    try:
        ready.wait(timeout=1)
        inbox.publish(['a'],1)
        for thread in threads:thread.join(.5)
        assert all(not thread.is_alive() for thread in threads)
        assert results==[1,1]
        assert inbox.drain_batch(1)==(1,[('a',1)])
    finally:
        inbox.publish(['cleanup'],1)
        for thread in threads:thread.join(2)


def test_spurious_notification_does_not_invent_a_generation():
    inbox=ActivityInbox();waiting=threading.Event();result=[]
    def waiter():
        waiting.set()
        result.append(inbox.wait(2,after=0))
    thread=threading.Thread(target=waiter);thread.start()
    try:
        assert waiting.wait(1)
        with inbox.condition:inbox.condition.notify_all()
        thread.join(.02)
        assert thread.is_alive()
        inbox.publish(['a'],1)
        thread.join(1)
        assert result==[1]
    finally:
        inbox.publish(['cleanup'],1);thread.join(2)


def test_stop_already_set_returns_without_waiting_for_an_event():
    inbox=ActivityInbox();stop=threading.Event();stop.set()
    started=time.monotonic()
    assert inbox.wait(10,after=0,stop=stop)==0
    assert time.monotonic()-started<.2


def test_stop_during_idle_wait_exits_promptly_without_a_publisher():
    inbox=ActivityInbox();stop=threading.Event();started=threading.Event();result=[]
    def waiter():
        started.set()
        result.append(inbox.wait(10,after=0,stop=stop))
    thread=threading.Thread(target=waiter);thread.start()
    try:
        assert started.wait(1)
        stop.set()
        thread.join(.5)
        assert not thread.is_alive() and result==[0]
        assert inbox.drain_batch(1)==(0,[])
    finally:
        stop.set();thread.join(1)


def test_bounded_inbox_preserves_last_4096_pool_deliveries_and_one_cursor():
    inbox=ActivityInbox()
    inbox.publish((f'p{i}' for i in range(5000)),1)
    assert len(inbox.pending)==4096
    assert inbox.generation==1
    # Refreshing a retained pool changes recency, so it survives eviction.
    inbox.publish(['p904','new'],2)
    generation,events=inbox.drain_batch(2)
    assert generation==2 and len(events)==4096
    assert dict(events)['p904']==2 and 'p905' not in dict(events)
    assert events[-2:]==[('p904',2),('new',2)]


def test_concurrent_publish_and_drain_preserve_every_unique_pool_and_cursor():
    inbox=ActivityInbox();start=threading.Barrier(4);done=[]
    def publisher(worker):
        start.wait(timeout=1)
        for i in range(100):inbox.publish([f'{worker}:{i}'],1)
        done.append(worker)
    threads=[threading.Thread(target=publisher,args=(worker,)) for worker in range(3)]
    for thread in threads:thread.start()
    seen={};generations=[]
    try:
        start.wait(timeout=1)
        deadline=time.monotonic()+2
        while len(done)<3 and time.monotonic()<deadline:
            generation,events=inbox.drain_batch(2)
            generations.append(generation);seen.update(events)
            inbox.wait(.02,after=generation)
        for thread in threads:thread.join(1)
        generation,events=inbox.drain_batch(2)
        generations.append(generation);seen.update(events)
        assert all(not thread.is_alive() for thread in threads)
        assert set(seen)=={f'{worker}:{i}' for worker in range(3) for i in range(100)}
        assert set(seen.values())=={1}
        assert generations==sorted(generations)
        assert generation==300
    finally:
        for thread in threads:thread.join(1)
