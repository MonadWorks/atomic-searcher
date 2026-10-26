import threading
import time
from contextlib import nullcontext
from types import SimpleNamespace
import pytest
from searcher.core import USDC, load_config
from searcher.dex_screen import Scheduler
from searcher.strategy_suite import Suite
from searcher.stream_usage import StreamSelection
from searcher.morpho_universe import Universe
from test_dex_screen import pool, row, address, settlements
from test_execution import market


def prepared(now):
    s=Scheduler(settlements(),4,lambda:now[0])
    a,b=pool(1000,USDC,address(100)),pool(1001,USDC,address(100))
    others=[pool(10000+i,USDC,address(1000+i)) for i in range(5500)]
    s.sync({p.address:p for p in (*others,a,b)})
    s.record([a,b],[row(a,USDC),row(b,USDC)],1)
    return s,a,b


def test_direct_event_prepares_both_directions_without_graph_search():
    now=[10.];s,a,b=prepared(now)
    s.ranked=lambda:pytest.fail('graph search in event path')
    s.page=lambda:pytest.fail('state page in event path')
    quotes=s.event_quotes([(a.address,now[0])])
    assert {tuple(h[0] for h in p) for p,_,_ in quotes}=={(a.address,b.address),(b.address,a.address)}
    assert len(quotes)<=8 and all(0<n<=int(v['max_input_raw']) for _,n,v in quotes)
    now[0]+=2;assert not s.event_quotes([(a.address,10.)])
    now[0]+=46;assert not s.event_quotes([(a.address,now[0])])


def test_measured_bad_route_cannot_buy_priority_or_bypass_feedback():
    now=[10.];s,a,b=prepared(now)
    s.feedback=SimpleNamespace(allowed=lambda path:False)
    assert not s.stream_priority()
    assert not s.event_quotes([(a.address,10.)])


def test_busy_prepared_pair_is_not_removed_for_message_volume():
    now=[0.];a,b,c='a','b','c';groups=[(10,(a,b))]
    s=StreamSelection(80_000,clock=lambda:now[0],priority_groups=lambda:groups)
    assert s.select({a,b,c})=={a,b,c};s.subscribed({a,b,c})
    for _ in range(100):s.observe(a);s.observe(b);s.observe(c)
    now[0]=10
    assert s.select({a,b,c})=={a,b}
    assert not {a,b}&s.cool_until.keys()
    assert s.details()['priority_exceeds_sustainable_rate']
    # Removed/unprepared routes lose protection. No new quota is created.
    groups.clear();now[0]+=1;s.select({a,b,c})
    assert not s.priority and a in s.cool_until


def test_priority_protection_still_debits_and_obeys_the_shared_usage_cap(tmp_path):
    from searcher.stream_usage import RequestBudget
    b=RequestBudget(tmp_path/'budget.json',80000);b.save=lambda:None
    s=StreamSelection(80000,budget=b,priority_groups=lambda:[(1,('a','b'))])
    s.select({'a','b'});s.subscribed({'a','b'})
    for _ in range(130):b.add(100,'notification');s.observe('a')
    assert b.state['units']==130 and b.pacing_delay()>0
    assert s.select({'a','b'})=={'a','b'}


def test_priority_route_subscribes_first_and_has_independent_filters():
    from searcher.stream_usage import SubscriptionRoster
    pools=set(range(5500));priority={5498,5499}
    roster=SubscriptionRoster(capacity=8192,batch_size=128)
    selected=roster.select(pools,individual=priority,priority=priority)
    assert list(roster.selected)[:2]==sorted(priority) and selected==pools
    selector=StreamSelection(80000,priority_groups=lambda:[(1,priority)])
    selector.select(pools)
    assert priority<=selector.individual_pools()
    # Even an already full, small roster reserves room for the prepared pair.
    small=SubscriptionRoster(capacity=256,batch_size=128)
    small.select(pools)
    assert small.select(pools,individual=priority,priority=priority)==priority


def test_event_takes_native_quote_path_before_any_market_sweep(tmp_path,monkeypatch):
    import searcher.dex_screen as ds
    now=[time.monotonic()];scheduler,a,b=prepared(now)
    u=SimpleNamespace(lock=threading.Lock(),graph=SimpleNamespace(pools={p.address:p for p in (a,b)}))
    suite=Suite(load_config(),u,{},lambda:None,threading.Event(),tmp_path,'unused')
    scheduler.clock=time.monotonic
    suite.dex_scheduler=scheduler;suite.dex_last_background=time.monotonic()
    suite.activity.inbox.publish({a.address},time.monotonic())
    suite.activity.fast_inbox.publish({a.address},time.monotonic())
    calls=[]
    def quote(rpc,quotes,*args,**kwargs):calls.append((quotes,kwargs))
    monkeypatch.setattr(suite,'dex_quote',quote)
    monkeypatch.setattr(ds,'read_markets',lambda *a:pytest.fail('market sweep before native quote'))
    suite.dex(SimpleNamespace(),threading.Event())
    assert len(calls)==1 and calls[0][1]['event_batch']
    assert calls[0][0] and {a.address,b.address}<=set(scheduler.activity)


def test_idle_loop_waits_for_delivery_instead_of_issuing_network_calls(tmp_path,monkeypatch):
    import searcher.dex_screen as ds
    now=[time.monotonic()];scheduler,a,b=prepared(now)
    u=SimpleNamespace(lock=threading.Lock(),graph=SimpleNamespace(pools={p.address:p for p in (a,b)}))
    suite=Suite(load_config(),u,{},lambda:None,threading.Event(),tmp_path,'unused')
    suite.dex_scheduler=scheduler;suite.dex_next_background=time.monotonic()+1
    calls=[];suite.activity.inbox.wait=lambda *a,**kw:calls.append(kw)
    monkeypatch.setattr(ds,'read_markets',lambda *a:pytest.fail('idle read'))
    suite.dex(SimpleNamespace(),threading.Event())
    assert len(calls)==1 and 'after' in calls[0]


def test_complete_candidate_wakes_executor_and_already_queued_job_is_not_lost():
    from searcher.strategy_suite import Mailbox,Job
    mailbox=Mailbox();stop=threading.Event();returned=threading.Event()
    thread=threading.Thread(target=lambda:(mailbox.wait(2,stop),returned.set()))
    thread.start();time.sleep(.02)
    mailbox.offer(Job('dex',{},None,{},None,time.monotonic_ns()+2_000_000_000))
    assert returned.wait(.2);thread.join(1)
    started=time.monotonic();mailbox.wait(1,stop)
    assert time.monotonic()-started<.1


@pytest.mark.parametrize('handoff', ['same_head', 'next_head', 'price_gone'])
def test_prepared_event_native_proof_submission_and_receipt_without_fee_roundtrip(market, handoff):
    from searcher.morpho_universe import Pool as MetaPool
    from searcher.execution import Executor
    from searcher.context import simulation_context
    from searcher.dex_screen import read_markets
    from searcher.core import data
    rpc,c,contract,signer,directory,index,_,_,send=market
    metadata={p.address:MetaPool(p.address,p.token0,p.token1,p.factory,1,p.fee) for p in index.states.values()}
    u=SimpleNamespace(lock=threading.Lock(),graph=SimpleNamespace(pools=metadata))
    executor=Executor(rpc,c,contract,signer,directory/'event-chain')
    suite=Suite(c,u,{'AtomicCycle':contract},lambda:executor.context,threading.Event(),directory,rpc.url)
    suite.report('dex',state='starting');rpc.read_slot=lambda:nullcontext()
    try:
        scheduler=Scheduler(c['settlement_tokens'],4,feedback=suite.dex_feedback);scheduler.sync(metadata)
        _,anchor,clock=suite.current()
        # This fixture's pool exposes swaps but no liquidity getter. Supply
        # the prepared sizing hints; native calls, the whole proof and the
        # signed transaction below still execute real fixture bytecode.
        rows=[(True,p.sqrt_price,p.liquidity,0,0,0,0,p.fee) for p in index.states.values()]
        scheduler.record(list(metadata.values()),rows,int(clock['number'],16));suite.dex_scheduler=scheduler
        suite.dex_last_background=time.monotonic();trigger=next(iter(metadata))
        suite.activity.inbox.publish({trigger},time.monotonic())
        suite.activity.fast_inbox.publish({trigger},time.monotonic())
        calls=[];request=rpc.request;final_batches=[];batch=rpc.batch
        def track(method,params):calls.append((method,params));return request(method,params)
        def track_batch(items):
            if len(items)==5 and items[0][0]=='eth_call' and items[0][1][0]['to'].lower()==contract:
                final_batches.append(items)
            return batch(items)
        rpc.request=track;rpc.batch=track_batch
        suite.dex(rpc,threading.Event())
        assert suite.health['dex']['complete_quotes']>0
        assert not any(m=='eth_call' and p[0].get('data')==data('fee()') for m,p in calls)
        job=suite.mailbox.slots['dex']
        assert job.candidate['allow_execution_anchor_rollover'] is True
        assert job.candidate['execution_gas_source']=='whole_cycle_probe'
        assert job.candidate['execution_gas_estimate']>21000
        original_deadline=job.deadline
        if handoff != 'same_head':
            if handoff == 'price_gone':
                # Local fixture only: PoolCycleMock packs fee/spacing with token1; numerator is slot 3.
                # Both venue rates become 1:1, consuming the former spread.
                rpc.METHODS=rpc.METHODS|{'anvil_setStorageAt'}
                assert int(rpc.request('eth_getStorageAt',[job.route.pools[0],'0x3','latest']),16)==2
                rpc.request('anvil_setStorageAt',[job.route.pools[0],'0x3','0x'+(1).to_bytes(32,'big').hex()])
            rpc.request('evm_mine',[]);executor.refresh_context()
        result=suite.consume(executor)
        assert job.deadline == original_deadline
        assert len(final_batches)==1
        assert not any(m=='eth_estimateGas' or (m=='eth_call' and p[0].get('to','').lower()==contract) for m,p in calls), 'No duplicate gas/execute call before the exact final batch.'
        if handoff == 'price_gone':
            assert result['status']=='execution_rejected'
            assert not result['execution_anchor_rollover']['final_validation_passed']
            assert executor.db.execute('SELECT count(*) FROM attempts').fetchone()[0]==0
            return
        assert result['status']=='submitted'
        if handoff == 'next_head':
            assert result['execution_anchor_rollover']['final_validation_passed']
            assert result['execution_anchor_rollover']['new_block']==int(job.anchor['number'],16)+1
        rpc.request('evm_mine',[]);executor.reconcile()
        assert executor.db.execute('SELECT state FROM attempts').fetchall()==[('success',)]
    finally:executor.close()
