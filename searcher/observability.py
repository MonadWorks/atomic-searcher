"""Match observed closed pool flows to actual local first-seen timestamps.

This reports visibility intervals, never substitutes them for sequencer arrival,
winning bids, canonical finality or realized wallet profit.
"""
import json
import sqlite3
from collections import defaultdict

from eth_abi import decode

from .core import number, unhex
from .events import V3_SWAP, SLIP_SWAP, V2_SWAP
from .recorder import decode_payload


def audit(database, universe, processing_ms):
    pools = {p["address"]:p for p in json.loads(universe.read_text())["pools"]}
    db=sqlite3.connect(database)
    arrivals={tx:(frame,utc) for tx,frame,utc in db.execute("SELECT tx_hash,frame_id,received_utc FROM arrivals")}
    transactions={}
    requests=[]
    for frame,requested,received,payload in db.execute("SELECT id,requested_ns,received_ns,payload FROM frames ORDER BY id"):
        requests.append((received-requested)/1e6)
        for receipt in decode_payload(payload).get("receipts",[]):
            transactions.setdefault(receipt["transactionHash"],receipt)
    db.close()
    # Use transaction-index ordering only to identify preceding observed touches,
    # not to invent reception times between transactions in one frame.
    ordered=sorted(transactions.values(),key=lambda r:(number(r["blockNumber"]),number(r["transactionIndex"])))
    last_touch={};rows=[]
    for receipt in ordered:
        tx=receipt["transactionHash"];swaps=[];complete=True
        for entry in receipt.get("logs",[]):
            if not entry.get("topics") or entry["topics"][0] not in (V3_SWAP,SLIP_SWAP,V2_SWAP):continue
            addr=entry["address"].lower()
            if addr not in pools:complete=False;continue
            if entry['topics'][0] == V2_SWAP:
                i0,i1,o0,o1=decode(['uint256']*4,unhex(entry['data']))
                a0,a1=i0-o0,i1-o1
            else:a0,a1=decode(["int256","int256"],unhex(entry["data"])[:64])
            swaps.append((addr,a0,a1))
        cash=defaultdict(int)
        for addr,a0,a1 in swaps:
            cash[pools[addr]["token0"]]-=a0;cash[pools[addr]["token1"]]-=a1
        positives={asset:amount for asset,amount in cash.items() if amount>0}
        closed=complete and 2<=len(swaps)<=4 and len({x[0] for x in swaps})==len(swaps) and len(positives)==1 and all(x>=0 for x in cash.values())
        if closed:
            preceding=[last_touch[addr] for addr,_,_ in swaps if addr in last_touch]
            trigger=max(preceding,key=lambda r:(number(r["blockNumber"]),number(r["transactionIndex"]))) if preceding else None
            winner_frame,winner_at=arrivals[tx]
            lead=None;trigger_hash=None
            if trigger is not None:
                trigger_hash=trigger["transactionHash"]
                _,seen_at=arrivals[trigger_hash]
                lead=(winner_at-seen_at)*1000
            rows.append({"transaction_hash":tx,"block":number(receipt["blockNumber"]),"pools":[x[0] for x in swaps],
                         "positive_pool_flow_raw":positives,"preceding_touch":trigger_hash,
                         "first_seen_lead_ms":lead,"local_computation_ms":processing_ms,
                         "visible_time_exceeds_computation_only":lead is not None and lead>processing_ms,
                         "network_fee_wei":number(receipt.get("gasUsed",0))*number(receipt.get("effectiveGasPrice",0))+number(receipt.get("l1Fee",0)),
                         "canonical_inclusion_rechecked":False,"wallet_net_profit_proven":False,"could_we_win":None})
        for addr,_,_ in swaps:last_touch[addr]=receipt
    requests.sort()
    return {"scope":"observed closed pool flows in the configured universe; prior touch may not create the opportunity; unknown pools and unobserved receipts excluded",
            "rpc_latency_ms":{"median":requests[len(requests)//2] if requests else None,"p95":requests[min(len(requests)-1,int(len(requests)*.95))] if requests else None},
            "processing_ms_used":processing_ms,"submission_latency_measured":False,"fills":0,"own_realized_profit":0,
            "cases":rows,"conclusion":"Visibility intervals are evidence for selecting infrastructure; they are not a win-rate or income backtest."}
