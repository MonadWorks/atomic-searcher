"""Bounded unsigned auction evidence, written only after the bidding hot path.

An observed head is not the state root of a pending eth_call. Preserve that
distinction. Never accept a solver object or signed simulation request here.
"""
import hashlib
import json
import time
from .core import atomic_json


def unsigned_notice(notice):
    op=notice.get('partial_user_operation',{})
    fields=('chainId','from','to','control','dapp','value','gas','maxFeePerGas',
            'nonce','deadline','userOpHash')
    hints=op.get('hints',{})
    return {**{k:notice[k] for k in ('auction_id','aggregator','price','received_at','markets') if k in notice},
            'partial_user_operation':{**{k:op[k] for k in fields if k in op},
                'hints':{k:hints[k] for k in ('aggregator','medianPrice','forwardData','rawReport') if k in hints}}}


def trial_record(trial):
    plan=trial['plan']
    from .atlas import AP
    from eth_abi import encode
    return {'market':trial['market'],'route_key':trial['route'].key,
            'borrowers':trial['borrowers'],'score_hint_wei':trial['score'],
            'unsigned_plan':'0x'+encode([AP],[plan]).hex()}


def save(output,audit,result,limit=64):
    directory=output/'auction-evidence';directory.mkdir(exist_ok=True)
    identifier=audit.get('notice',{}).get('auction_id','unknown')
    name=hashlib.sha256(identifier.encode()).hexdigest()[:24]+'.json'
    # result is deliberately not stored wholesale: quoted-only responses can
    # contain solver data. Copy only the terminal decision vocabulary.
    terminal={k:result[k] for k in ('status','reason','reasons','at','processing_ms',
        'queue_age_ms','auction_id','bid_wei','retained_minimum_wei','gateway_accepted') if k in result}
    document={**audit,'decision':terminal,'saved_at':time.time()}
    if len(json.dumps(document))>2*1024*1024:
        raise ValueError('unsigned auction evidence exceeded bounded record size')
    atomic_json(directory/name,document)
    files=sorted(directory.glob('*.json'),key=lambda p:p.stat().st_mtime,reverse=True)
    for path in files[limit:]:path.unlink(missing_ok=True)
