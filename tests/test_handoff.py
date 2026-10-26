"""Reader/strategy handoff must improve coverage without dropping any page."""
import json
import threading
import time
from types import SimpleNamespace

from searcher.universe import PageProgress
from searcher.pipeline import run_pending
from test_runtime import setup_runtime, FakeRPC


def test_handoff_waits_for_actual_consumption_then_advances():
    gate=PageProgress();_,first=gate.begin()
    started=threading.Event();returned=threading.Event();result=[]
    def wait():
        started.set();result.append(gate.begin(.1));returned.set()
    thread=threading.Thread(target=wait);thread.start();assert started.wait(1)
    assert not returned.wait(.01)
    gate.consumed(first);thread.join(1)
    assert result==[(True,first+1)]
    gate.consumed(first)  # delayed duplicate cannot acknowledge the new page
    assert gate.begin()==(False,first+1)


def test_busy_strategy_gets_same_page_after_bounded_wait_and_stop_interrupts():
    gate=PageProgress();_,first=gate.begin()
    start=time.monotonic();assert gate.begin(.03)==(False,first)
    assert .02<=time.monotonic()-start<.3
    stop=threading.Event();stop.set()
    start=time.monotonic();assert gate.begin(.1,stop)==(False,first)
    assert time.monotonic()-start<.05


def run_handoff_case(monkeypatch,tmp_path,wait_ms,quote_delay=.004):
    module,Feed,c=setup_runtime(monkeypatch)
    c['policy'].update(activity_priority=False,universe_refresh_seconds=100,pending_poll_interval_seconds=.001,page_ack_wait_ms=wait_ms)
    polled,consumed=[],[]
    class PagedFeed(Feed):
        def __init__(self,*args):
            super().__init__(*args);self.pages=SimpleNamespace(pools={});self.page=0
        def poll(self,advance_page=True):
            if advance_page:self.page+=1
            polled.append(self.page)
            time.sleep(.02)  # Fixed transport time, separate consumer thread.
            frame=super().poll();frame.transport={};frame.page=self.page
            return frame
    class Strategy:
        def __init__(self,*args):pass
        def on_frame(self,frame):
            time.sleep(quote_delay);consumed.append(frame.page)
            return {'block':1,'decision_age_ms':0,'computation_ms':0,'candidates':[]}
    monkeypatch.setattr(module,'StateFeed',PagedFeed);monkeypatch.setattr(module,'PendingStrategy',Strategy)
    result=run_pending(FakeRPC(),c,{},.65,tmp_path)
    assert result['reader_stopped'] and result['discovery_stopped'] and not result['worker_errors']
    return polled,consumed


def test_reader_avoids_duplicate_reads_without_skipping_strategy_consumption(monkeypatch,tmp_path):
    old,old_done=run_handoff_case(monkeypatch,tmp_path/'old',0)
    new,new_done=run_handoff_case(monkeypatch,tmp_path/'new',100)
    # Avoid a wall-clock throughput ratio that changes under unrelated CPU
    # load; assert the actual redundant-read reduction and complete delivery.
    assert len(new)-len(set(new)) < len(old)-len(set(old))
    assert set(new_done)==set(range(1,max(new_done)+1))
    assert len(new)-len(set(new))<=1
    assert len(old)-len(set(old))>=5


def test_consumer_slower_than_wait_keeps_same_page_fresh_without_skipping(monkeypatch,tmp_path):
    polled,consumed=run_handoff_case(monkeypatch,tmp_path,10,quote_delay=.09)
    assert len(polled)>len(set(polled))
    assert set(consumed)==set(range(1,max(consumed)+1))
