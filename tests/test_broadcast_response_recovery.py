"""Accepted local transaction, lost response, then durable receipt recovery.

Only the fixture's temporary loopback Anvil and synthetic account are used.
This is an execution/recovery test, not a live market or income backtest.
"""
import json
import time

import pytest
from eth_abi import decode

from searcher.core import WETH, data, number, unhex
from searcher.execution import CYCLE_EVENT, Executor
from test_execution import LocalRPC, market


def test_accepted_broadcast_lost_response_recovers_original_once_after_restart(market, monkeypatch):
    rpc, config, contract, signer, directory, index, candidate, head, _ = market
    assert isinstance(rpc, LocalRPC) and rpc.url.startswith('http://127.0.0.1:')
    assert not rpc.fail_send  # The old fixture fault fails BEFORE acceptance.
    route = index.routes[candidate['route_key']]
    request = rpc.request
    broadcasts = []

    def balance():
        return decode(['uint256'], rpc.call(WETH, data('balanceOf(address)',
            ['address'], [signer.address]), 'latest'))[0]

    initial_balance = balance()
    initial_nonce = number(request('eth_getTransactionCount', [signer.address, 'latest']))

    def lose_first_accepted_response(method, params):
        # The real local node accepts/mines the transaction before the client
        # sees a transport failure. Never replace this with a pre-send raise.
        result = request(method, params)
        if method == 'eth_sendRawTransaction':
            broadcasts.append(result)
            if len(broadcasts) == 1:
                raise TimeoutError('synthetic response lost after node acceptance')
        return result

    executor = Executor(rpc, config, contract, signer, directory)
    monkeypatch.setattr(rpc, 'request', lose_first_accepted_response)
    try:
        with pytest.raises(TimeoutError, match='after node acceptance'):
            executor.execute(candidate, route, index.states, head,
                             time.monotonic_ns()+2_000_000_000)
        assert len(broadcasts) == 1
        original_hash = broadcasts[0]
        transaction = request('eth_getTransactionByHash', [original_hash])
        receipt = request('eth_getTransactionReceipt', [original_hash])
        assert transaction is not None and transaction['blockNumber'] is not None
        assert number(transaction['nonce']) == initial_nonce
        assert receipt is not None and number(receipt['status']) == 1
        assert executor.db.execute('SELECT id,tx_hash,state,actual FROM attempts').fetchall() == [
            (original_hash, original_hash, 'submitted', None)]
        assert executor.db.execute('SELECT tx_hash,outcome FROM intents').fetchall() == [(original_hash, None)]
        reserved = executor.db.execute('SELECT reserved FROM attempts').fetchone()[0]
        executor.close()
        executor = None

        executor = Executor(rpc, config, contract, signer, directory)
        assert executor.db.execute('SELECT tx_hash,state,reserved,actual FROM attempts').fetchall() == [
            (original_hash, 'submitted', reserved, None)]
        # A mined receipt still needs the configured sealed successor. The
        # first reconcile must retain the reservation, not rebroadcast/cancel.
        executor.reconcile()
        assert broadcasts == [original_hash]
        assert executor.db.execute('SELECT state,actual FROM attempts').fetchall() == [('submitted', None)]
        request('evm_mine', [])
        executor.reconcile()

        outcome = json.loads(executor.db.execute('SELECT outcome FROM intents').fetchone()[0])
        actual_fee = (number(receipt['gasUsed'])*number(receipt['effectiveGasPrice'])
                      + number(receipt['l1Fee']) + number(receipt['operatorFee']))
        assert executor.db.execute('SELECT tx_hash,state,actual FROM attempts').fetchall() == [
            (original_hash, 'success', str(actual_fee))]
        assert actual_fee > 0 and actual_fee <= int(reserved)
        assert outcome['success'] and not outcome['cancelled']
        assert outcome['included_tx_hash'] == original_hash
        assert outcome['actual_network_fee_wei'] == actual_fee
        assert outcome['gross_profit_raw'] == balance()-initial_balance
        assert outcome['net_profit_raw'] == outcome['gross_profit_raw']-actual_fee > 0

        events = [log for log in receipt['logs'] if log['address'].lower() == contract
                  and log.get('topics', [None])[0] == CYCLE_EVENT]
        assert len(events) == 1 and number(events[0]['topics'][1]) == initial_nonce
        amount, gross = decode(['uint256', 'uint256'], unhex(events[0]['data']))
        assert amount == candidate['amount_in'] and gross == outcome['gross_profit_raw']
        # Repeated reconciliation must not duplicate the trade or its account
        # entry. Chain nonce and balance checks are independent of the journal.
        settled = executor.db.execute('SELECT state,actual,finished_at FROM attempts').fetchall()
        paid = balance()
        executor.reconcile()
        executor.reconcile()
        assert broadcasts == [original_hash]
        assert number(request('eth_getTransactionCount', [signer.address, 'latest'])) == initial_nonce+1
        assert balance() == paid
        assert executor.db.execute('SELECT state,actual,finished_at FROM attempts').fetchall() == settled
        assert executor.db.execute('SELECT COUNT(*) FROM intents').fetchone()[0] == 1
    finally:
        if executor is not None:
            executor.close()
