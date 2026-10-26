"""The final slice still reaches a genuine temporary-EVM trade and receipt.

Synthetic empty pools fill the first slice; funded local V2 pools form the last
route. All snapshots and the final execution use real loopback RPC/EVM results.
This proves wiring, never historical capture or real-network speed/profit.
"""
import copy
import json
import time

import pytest

from eth_abi import encode

from searcher.core import USDC, WETH, data, number
from searcher.execution import Executor
from searcher.pipeline import PendingStrategy, acknowledge_prewarm
from searcher.pools import Pool
from searcher.statefeed import StateFeed, DeepReadScheduler, observation_start_ns
from searcher.transfer_feedback import TransferFeedback, measure_sample
from searcher.universe import PageProgress
from test_execution import artifact, market


@pytest.mark.parametrize('prewarmed', [False, True], ids=['ordinary-tail', 'leased-tail-fresh-principal'])
def test_tail_slice_real_pending_read_quote_submission_and_profit_receipt(market, prewarmed):
    rpc,config,_,signer,directory,_,_,_,send = market
    config = copy.deepcopy(config)
    config['policy'].update(cycle_pages=True, max_hops=2, max_pools=300,
                            route_groups_per_page=96, max_quote_ms=1000)
    def deploy(file,name,types=(),values=()):
        code = artifact(file,name)['bytecode']['object']
        return send(calldata=code+encode(types,values).hex())['contractAddress'].lower()

    healthy = []
    for reserve in (2*10**21,10**21):
        factory = deploy('MixedCycle.t.sol','V2FactoryMock')
        pair = deploy('MixedCycle.t.sol','V2PairMock',['address']*3,[factory,WETH,USDC])
        send(factory,data('add(address,address,address)',['address']*3,[WETH,USDC,pair]))
        for token,amount in ((WETH,10**21),(USDC,reserve)):
            send(token,data('mint(address,uint256)',['address','uint256'],[pair,amount]))
        send(pair,data('sync()'))
        healthy.append(Pool(pair,factory,'v2',WETH,USDC,0,3000))
    contract = deploy('AtomicCycle.sol','AtomicCycle',
        ['address','uint256','address[]','uint8[]','address[]','uint256[]'],
        [signer.address,8453,[p.factory for p in healthy],[3,3],[WETH],[10**20]])
    send(contract,data('setPaused(bool)',['bool'],[False]))
    config['factories'] = [dict(address=p.factory,kind='v2',keys=[]) for p in healthy]

    # Real runtime with zero reserves: genuine EVM results reject these empty
    # synthetic pairs. They create bounded coverage work, no fake opportunity.
    empty = []
    for index in range(26):
        address = '0x'+f'{0x10000+index:040x}'
        rpc.request('anvil_setCode',[address,
            artifact('MixedCycle.t.sol','V2PairMock')['deployedBytecode']['object']])
        empty.append(Pool(address,healthy[0].factory,'v2',WETH,USDC,0,3000))
    groups = [tuple(p.address for p in empty[i:i+2]) for i in range(0,len(empty),2)]
    healthy_group = tuple(sorted(p.address for p in healthy))
    groups.append(healthy_group)
    header = rpc.header()
    saved = dict(header=header,pools=[p.json() for p in empty+healthy],cycle_groups=groups)
    feed = StateFeed(rpc,config,saved)
    # Fix only the selected economic page and its order. StateFeed still owns
    # splitting, ABI call, actual state parsing, clocks and final freshness.
    page = feed.pages
    def select(*args,**kwargs):
        page.selected_groups = groups
        page.selection_summary = {'scope':'synthetic local fixed economic page'}
        page.selected_priority = page.selected_coactive = []
        return empty+healthy
    page.take = select
    strategy,gate = PendingStrategy(rpc,config,saved),PageProgress()
    executor = Executor(rpc,config,contract,signer,directory/'tail-slice-executor')
    try:
        advance,identity = gate.begin()
        first = feed.poll(advance_page=advance)
        first.transport['page_id'] = identity
        assert len(first.snapshot_pools) == 24
        assert healthy_group not in first.transport['route_groups']
        first_report = strategy.on_snapshot(first)
        assert not first_report['candidates']
        assert first_report['page_scan_complete'] and first_report['page_advance_ready']
        assert feed.has_pending_slices
        gate.consumed(identity,first_report['continuation_requests'])

        advance,identity = gate.begin()
        assert advance
        tail = feed.poll(advance_page=advance)
        tail.transport['page_id'] = identity
        assert healthy_group in tail.transport['route_groups']
        assert tail.transport['page_slice']['index'] == tail.transport['page_slice']['count'] == 2
        assert not feed.has_pending_slices
        report = strategy.on_snapshot(tail)
        candidate = next(candidate for candidate in report['candidates']
            if tuple(sorted(strategy.index.routes[candidate['route_key']].pools)) == healthy_group)
        if prewarmed:
            # The page tail genuinely exposed this route. Only the initial
            # failure trigger is synthetic scheduling evidence; helper, new
            # snapshot, exact-principal quote and whole execution below use EVM.
            route = strategy.index.routes[candidate['route_key']]
            principal, original_output = candidate['amount_in'], candidate['amount_out']
            now = [1000.]
            feedback = TransferFeedback(contract, signer.address, clock=lambda:now[0])
            failure = dict(revert_name='BadCallback', revert_selector='0x6c25f7f8',
                execution_probe={'stage':'initial_call','state_tag':'pending','started_utc':time.time(),
                                 'unsigned_call':{'from':signer.address.lower(),'to':contract}})
            assert feedback.schedule_revalidation(route,strategy.index.states,principal,[WETH],failure)
            now[0] += 31
            sample = feedback.take()
            assert sample is not None
            diagnostic = measure_sample(rpc,sample,contract,signer.address,
                artifact('CycleProbe.sol','CycleProbe')['deployedBytecode']['object'])
            assert diagnostic['canonical'] and diagnostic['failure_stage'] == 0
            assert diagnostic['funded'] == diagnostic['credited'] == sample.amount
            feedback.record(sample,diagnostic)
            feedback.finish_sample(sample,True)
            scheduler = DeepReadScheduler()
            ready = feedback.revalidation_requests()
            assert len(ready) == 1
            admitted = scheduler.add(ready,now[0])
            assert admitted['accepted'] == [ready[0]['prewarm_lease_id']]
            feedback.revalidation_queued(admitted['accepted'])
            assert scheduler.take({p.address for p in empty+healthy},now[0]) == set(route.pools)
            assert not scheduler.selected_deep_pools
            assert feedback.prewarm_summary()['dispatched'] == 0

            # Change only the second pool's output reserves: the diagnostic's
            # exact intermediate input remains current, while fresh net output
            # must differ. Reusing the preceding tail quote would fail below.
            send(WETH,data('mint(address,uint256)',['address','uint256'],[route.pools[1],10**20]))
            send(route.pools[1],data('sync()'))
            executor.refresh_context()
            gate.consumed(identity,report['continuation_requests'])
            advance,identity = gate.begin()
            assert advance and not feed.has_pending_slices
            tail = feed.poll(advance_page=advance,requested_groups=scheduler.selected_requests)
            tail.transport['page_id'] = identity
            assert tail.transport['requested_groups']
            assert number(tail.header['number']) > number(report['anchor']['number'])
            # Serialized amounts have no authority: only the registry's live
            # lease may select the principal for this fresh observation.
            tail.transport['requested_groups'][0]['prewarm_amount'] = principal+123
            strategy.transfer_feedback = feedback
            report = strategy.on_snapshot(tail)
            candidate = next(c for c in report['candidates'] if c['route_key'] == route.key)
            assert candidate['prewarm_exact_amount'] and candidate['amount_in'] == principal
            assert candidate['amount_out'] != original_output
            assert candidate['amount_out'] == route.quote(strategy.index.states,principal)[0]
            assert report['prewarm_consumed_route_keys'] == [route.key]
            assert feedback.prewarm_summary()['dispatched'] == 0
            acknowledge_prewarm(strategy,tail,report,[WETH])
            assert report['prewarm_read_ack'] == 1
            assert feedback.prewarm_summary()['dispatched'] == 1
            executor.transfer_feedback = feedback
            assert feedback.prewarm_summary()['claimed'] == 0
        before = number(rpc.request('eth_call',[{'to':WETH,
            'data':data('balanceOf(address)',['address'],[signer.address])},'latest']))
        result = executor.execute(candidate,strategy.index.routes[candidate['route_key']],
            strategy.index.states,tail.header,observation_start_ns(tail)+
            int(config['policy']['snapshot_max_age_ms']*1e6))
        assert result['status'] == 'submitted'
        if prewarmed: assert feedback.prewarm_summary()['claimed'] == 1
        rpc.request('evm_mine',[])
        executor.reconcile()
        assert executor.db.execute('SELECT state FROM attempts').fetchall() == [('success',)]
        outcome = json.loads(executor.db.execute('SELECT outcome FROM intents').fetchone()[0])
        after = number(rpc.request('eth_call',[{'to':WETH,
            'data':data('balanceOf(address)',['address'],[signer.address])},'latest']))
        assert after-before == outcome['gross_profit_raw']
        assert outcome['net_profit_raw'] > 0
    finally:
        executor.close()
