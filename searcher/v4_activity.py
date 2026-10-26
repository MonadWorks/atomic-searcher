"""Bounded canonical V4 discovery: independent recent and durable history lanes.

The recent lane may delegate old/overflow work to the contiguous history cursor;
its observed window is never represented as complete when that happens. Both
lanes share one transaction-read budget and identical canonical authentication.
"""
import json
import time
from .core import USDC, check, number
from .v4 import MANAGER, TOPICS
from .v4_market import key_from_input

TABLES=('activity_queue','recent_activity_queue')


def batch(rpc,calls):
    return [value for start in range(0,len(calls),2)
            for value in rpc.batch(calls[start:start+2])]


def _compact(inventory,table):
    representatives={};pending=[];coalesced=known=0
    with inventory.db:
        for row in inventory.db.execute(f'SELECT sequence,txid,pool_id,block,hash FROM {table} ORDER BY sequence').fetchall():
            sequence,txid,pid,_,_=row
            retained=representatives.setdefault(pid,set())
            if pid in inventory.registry.keys or txid in retained or len(retained)>=2:
                inventory.db.execute(f'DELETE FROM {table} WHERE sequence=?',(sequence,))
                if pid in inventory.registry.keys:known+=1
                else:coalesced+=1
            else:retained.add(txid);pending.append(row)
    return representatives,pending,coalesced,known


def _history(inventory,rpc,head,representatives,count):
    tip=number(head['number']);previous=inventory.get('activity_cursor',max(0,tip-2))
    touched=set();canonical={};backpressure=False;coalesced=0
    if previous<tip:
        lo=previous+1;end=min(tip,previous+64)
        logs=rpc.request('eth_getLogs',[{'address':MANAGER,'topics':[list(TOPICS[1:])],
                                      'fromBlock':hex(lo),'toBlock':hex(end)}])
        blocks={}
        for log in logs:
            height=number(log['blockNumber']);check(lo<=height<=end,'V4 activity log outside requested range')
            if log.get('removed'):continue
            topics=log.get('topics',[])
            check(log.get('address','').lower()==MANAGER and len(topics)>=2 and topics[0] in TOPICS[1:],
                  'V4 activity log outside requested filter')
            for value in (topics[1],log.get('blockHash'),log.get('transactionHash')):
                check(isinstance(value,str) and value.startswith('0x') and len(value)==66
                      and all(c in '0123456789abcdefABCDEF' for c in value[2:]),'V4 activity hash malformed')
            blocks.setdefault(height,[]).append(log)
        resume=inventory.get('activity_resume')
        heights=sorted({end}|({resume['block']} if resume else set()))
        headers=batch(rpc,[('eth_getBlockByNumber',[hex(n),False]) for n in heights])
        check(len(headers)==len(heights) and all(h is not None and number(h['number'])==n for h,n in zip(headers,heights)),
              'V4 activity header missing')
        canonical={n:h['hash'] for n,h in zip(heights,headers)}
        if end==tip:check(canonical[end]==head['hash'],'V4 activity head changed')
        inserts=[];last_complete=previous;next_resume=None
        for height in range(lo,end+1):
            events=sorted(enumerate(blocks.get(height,[])),
                          key=lambda item:number(item[1].get('logIndex',item[0])))
            offset=0
            if resume and resume['block']==height:
                check(resume['hash']==canonical.get(height),'V4 activity resume is not canonical')
                offset=resume['next']
                check(offset<=len(events),'V4 activity resume logs incomplete')
            for position,(_,log) in enumerate(events):
                if position<offset:continue
                pid=log['topics'][1].lower();txid=log['transactionHash'].lower()
                if pid in inventory.registry.keys:touched.add(pid);continue
                retained=representatives.setdefault(pid,set())
                if txid in retained or len(retained)>=2:coalesced+=1;continue
                if count>=inventory.activity_capacity:
                    next_resume={'block':height,'next':position};backpressure=True;break
                inserts.append((txid,pid,height,log['blockHash']))
                retained.add(txid);count+=1
            if backpressure:break
            last_complete=height
        if next_resume:
            height=next_resume['block']
            if height not in canonical:
                header=rpc.request('eth_getBlockByNumber',[hex(height),False])
                check(header is not None and number(header['number'])==height,'V4 activity resume header missing')
                canonical[height]=header['hash']
            next_resume['hash']=canonical[height]
        # Do not hold a SQLite write transaction across network reads.
        with inventory.db:
            inventory.db.executemany('INSERT OR IGNORE INTO activity_queue(txid,pool_id,block,hash) VALUES(?,?,?,?)',inserts)
            inventory.put('activity_cursor',last_complete)
            if next_resume:inventory.put('activity_resume',next_resume)
            else:inventory.db.execute("DELETE FROM metadata WHERE key='activity_resume'")
            inventory.db.execute('INSERT OR REPLACE INTO checkpoints VALUES(?,?)',(end,canonical[end]))
            inventory.db.execute('DELETE FROM checkpoints WHERE block NOT IN (SELECT block FROM checkpoints ORDER BY block DESC LIMIT 32)')
            inventory.put('activity_status',{'queued':count,'backpressure':backpressure,'observed_end':end})
    return touched,canonical,coalesced


def _delegate(inventory,height):
    """Preserve overflow in continuous history, even after that lane caught up."""
    prior=inventory.get('activity_cursor',height-1)
    if prior>=height:
        inventory.put('activity_cursor',max(0,height-1))
        inventory.db.execute("DELETE FROM metadata WHERE key='activity_resume'")
    inventory.put('activity_recent_debt_from',min(height,inventory.get('activity_recent_debt_from',height)))


def _recent(inventory,rpc,head):
    tip=number(head['number']);previous=inventory.get('activity_recent_cursor',max(0,tip-64))
    if previous>=tip:return set(),{}
    lo=max(previous+1,max(0,tip-63));end=tip
    logs=rpc.request('eth_getLogs',[{'address':MANAGER,'topics':[list(TOPICS[1:])],
                                  'fromBlock':hex(lo),'toBlock':hex(end)}])
    events=[];touched=set()
    for index,log in enumerate(logs):
        height=number(log['blockNumber']);check(lo<=height<=end,'V4 activity log outside requested range')
        if log.get('removed'):continue
        topics=log.get('topics',[])
        check(log.get('address','').lower()==MANAGER and len(topics)>=2 and topics[0] in TOPICS[1:],
              'V4 activity log outside requested filter')
        for value in (topics[1],log.get('blockHash'),log.get('transactionHash')):
            check(isinstance(value,str) and value.startswith('0x') and len(value)==66
                  and all(c in '0123456789abcdefABCDEF' for c in value[2:]),'V4 activity hash malformed')
        events.append((height,number(log.get('logIndex',index)),log))
    # Logs themselves are only hints. Even queued keys require the transaction's
    # canonical block below. Authenticate the window endpoint before persisting.
    resume=inventory.get('activity_resume')
    resume_in_window=bool(resume and lo<=resume['block']<=end)
    heights=sorted({end}|({resume['block']} if resume_in_window else set()))
    headers=batch(rpc,[('eth_getBlockByNumber',[hex(height),False]) for height in heights])
    check(len(headers)==len(heights) and all(header is not None and number(header['number'])==height
          for header,height in zip(headers,heights)),'V4 activity header missing')
    canonical={height:header['hash'] for height,header in zip(heights,headers)}
    check(canonical[end]==head['hash'],'V4 activity head changed')
    processed=set();history_cursor=inventory.get('activity_cursor',lo-1)
    if resume_in_window:
        check(canonical[resume['block']]==resume['hash'],'V4 activity resume is not canonical')
        block_events=sorted((event for event in events if event[0]==resume['block']),key=lambda event:event[1])
        check(resume['next']<=len(block_events),'V4 activity resume logs incomplete')
        processed={id(event[2]) for event in block_events[:resume['next']]}
    delegated=[];coalesced=0
    with inventory.db:
        if lo>previous+1:delegated.append(previous+1)
        # Most recent records win the small live inbox. Displaced work remains
        # reachable from the historical cursor, whose original FIFO is untouched.
        records=inventory.db.execute('SELECT sequence,txid,pool_id,block,hash FROM recent_activity_queue ORDER BY sequence').fetchall()
        existing={(row[1],row[2],row[4]):row for row in records}
        historical_pending=set(inventory.db.execute('SELECT txid,pool_id,hash FROM activity_queue WHERE block BETWEEN ? AND ?',
                                                    (lo,end)).fetchall())
        new=[]
        for height,index,log in sorted(events,key=lambda item:(-item[0],-item[1])):
            pid=log['topics'][1].lower();txid=log['transactionHash'].lower();h=log['blockHash']
            # Completed history must not revive an undecodable transaction.
            # Still-pending observations remain eligible for live priority even
            # when history has already ingested the entire current block.
            if (height<=history_cursor or id(log) in processed) and (txid,pid,h) not in historical_pending:continue
            if pid in inventory.registry.keys:touched.add(pid);continue
            identity=(txid,pid,h)
            if identity in existing:continue
            new.append((None,txid,pid,height,h));existing[identity]=new[-1]
        # On ties use the latest event order; retain the original retry schedule
        # for existing records rather than resetting it on every overlapping log.
        candidates=sorted(new+records,key=lambda row:-row[3]);retained=[];representatives={}
        for row in candidates:
            _,txid,pid,height,_=row
            txs=representatives.setdefault(pid,set())
            if txid in txs or len(txs)>=2:
                coalesced+=1;delegated.append(height);continue
            txs.add(txid)
            if len(retained)<inventory.recent_activity_capacity:retained.append(row)
            else:delegated.append(height)
        keep={row[0] for row in retained if row[0] is not None}
        for row in records:
            if row[0] not in keep:
                inventory.db.execute('DELETE FROM recent_activity_queue WHERE sequence=?',(row[0],))
        inventory.db.executemany('INSERT OR IGNORE INTO recent_activity_queue(txid,pool_id,block,hash) VALUES(?,?,?,?)',
                                 [row[1:] for row in retained if row[0] is None])
        if delegated:_delegate(inventory,min(delegated))
        inventory.put('activity_recent_cursor',end)
        inventory.put('activity_recent_status',dict(observed_from=lo,observed_end=end,
            window_fully_enqueued=not delegated,delegated_to_history=bool(delegated),
            delegated_from=min(delegated) if delegated else None,coalesced=coalesced))
        inventory.db.execute('INSERT OR REPLACE INTO checkpoints VALUES(?,?)',(end,canonical[end]))
        inventory.db.execute('DELETE FROM checkpoints WHERE block NOT IN (SELECT block FROM checkpoints ORDER BY block DESC LIMIT 32)')
    return touched,canonical


def _eligible(inventory,table,now):
    order='MAX(block) DESC,COUNT(DISTINCT pool_id) DESC,MIN(sequence)' if table=='recent_activity_queue' else 'MIN(sequence)'
    # A duplicate arriving through the other lane must share its transaction's
    # outstanding cooldown. Durable retry records survive recent-inbox eviction.
    return inventory.db.execute(f'SELECT txid,COUNT(DISTINCT pool_id),MIN(sequence) FROM {table} '
        'WHERE txid NOT IN (SELECT txid FROM activity_retries WHERE next_try>?) '
        f'GROUP BY txid HAVING MAX(next_try)<=? ORDER BY {order}',(now,now)).fetchall()


def _status(inventory,tip,coalesced=0,already_known=0,**updates):
    history=inventory.get('activity_cursor',max(0,tip-2));recent=inventory.get('activity_recent_cursor')
    with inventory.db:
        if history>=tip:
            inventory.db.execute("DELETE FROM metadata WHERE key='activity_recent_debt_from'")
        status=inventory.get('activity_status',{})
        status.update(queued=inventory.db.execute('SELECT COUNT(*) FROM activity_queue').fetchone()[0],
            recent_queued=inventory.db.execute('SELECT COUNT(*) FROM recent_activity_queue').fetchone()[0],
            history_cursor=history,history_backlog_blocks=max(0,tip-history),
            recent_observed_cursor=recent,recent_backlog_blocks=None if recent is None else max(0,tip-recent),
            recent_history_debt_from=inventory.get('activity_recent_debt_from'),
            coalesced=coalesced,already_known=already_known,**updates)
        inventory.put('activity_status',status)


def active_keys(inventory,rpc,head,limit=2):
    check(1<=limit<=4096,'invalid V4 activity transaction limit')
    inventory.check_reorg(rpc,head)
    tip=number(head['number']);previous=inventory.get('activity_cursor',max(0,tip-2))
    representatives,pending,coalesced,already_known=_compact(inventory,'activity_queue')
    _,recent_pending,more_coalesced,more_known=_compact(inventory,'recent_activity_queue')
    coalesced+=more_coalesced;already_known+=more_known
    split=bool(inventory.get('activity_split',False) or tip-previous>64 or recent_pending
               or len(pending)>=inventory.activity_capacity
               or inventory.get('activity_status',{}).get('backpressure',False))
    turn=inventory.get('activity_turn',0)
    # Persist before the RPC: failures/restarts cannot repeatedly choose only the
    # recent lane and starve historical intake. No additional getLogs on failure.
    with inventory.db:
        inventory.put('activity_turn',turn+1)
        if split:inventory.put('activity_split',True)
    recent_due=inventory.get('activity_recent_cursor',max(0,tip-64))<tip
    if split and recent_due and (turn%3!=2 or previous>=tip):
        touched,canonical=_recent(inventory,rpc,head)
    else:
        touched,canonical,extra=_history(inventory,rpc,head,representatives,len(pending));coalesced+=extra
    now=time.time()
    with inventory.db:
        inventory.db.execute('DELETE FROM activity_retries WHERE next_try<=?',(now,))
    retry_slots=4096-inventory.db.execute('SELECT COUNT(*) FROM activity_retries').fetchone()[0]
    # Each newly attempted tx may need one durable cooldown slot. Saturation
    # waits for existing bounded deadlines instead of forgetting a live retry.
    limit=min(limit,max(0,retry_slots))
    history=_eligible(inventory,'activity_queue',now);recent=_eligible(inventory,'recent_activity_queue',now)
    txids=[]
    if split:
        # Reserve one oldest historical transaction even under perpetual live
        # arrivals. With limit=1, alternate lanes; a shared tx costs one method.
        order=(history,recent) if limit==1 and turn%2 else (recent,history)
        for lane in order:
            if lane and len(txids)<limit and lane[0][0] not in txids:txids.append(lane[0][0])
        for row in recent+history:
            if len(txids)>=limit:break
            if row[0] not in txids:txids.append(row[0])
    else:
        txids=[row[0] for row in sorted(history,key=lambda row:(-row[1],row[2]))[:limit//2]]
        for txid,_,_ in history:
            if len(txids)>=limit:break
            if txid not in txids:txids.append(txid)
    _status(inventory,tip,coalesced,already_known)
    if not txids:return touched
    rows=[(*row,table) for txid in txids for table in TABLES for row in inventory.db.execute(
        f'SELECT sequence,txid,pool_id,block,hash,attempts FROM {table} WHERE txid=? ORDER BY sequence',(txid,))]
    responses=batch(rpc,[('eth_getTransactionByHash',[tx]) for tx in txids])
    check(len(responses)==len(txids),'V4 activity transaction response incomplete')
    transactions=dict(zip(txids,responses));tx_heights={}
    for txid,tx in transactions.items():
        if tx is None or tx.get('hash','').lower()!=txid:continue
        if tx.get('blockNumber') is not None:tx_heights[txid]=number(tx['blockNumber'])
        else:
            # Compatibility for providers/fixtures omitting blockNumber:
            # ambiguity cannot amplify header reads or authorize a key.
            claimed={row[3] for row in rows if row[1]==txid and row[4]==tx.get('blockHash')}
            if len(claimed)==1:tx_heights[txid]=next(iter(claimed))
    probe_heights=set(tx_heights.values())
    for txid in txids:
        if txid not in tx_heights:
            # A transaction removed by a reorg may never reappear. Verify
            # its oldest claimed block instead of retaining it forever.
            probe_heights.add(next(row[3] for row in rows if row[1]==txid))
    heights=sorted(probe_heights-set(canonical))
    headers=batch(rpc,[('eth_getBlockByNumber',[hex(n),False]) for n in heights])
    check(len(headers)==len(heights),'V4 queued activity header response incomplete')
    canonical.update({n:h['hash'] for n,h in zip(heights,headers) if h is not None and number(h['number'])==n})
    admitted=[];seen=set(inventory.registry.keys);deferred=undecodable=orphaned=0
    with inventory.db:
        for sequence,txid,pid,height,block_hash,attempts,table in rows:
            if pid in seen:
                for lane in TABLES:inventory.db.execute(f'DELETE FROM {lane} WHERE pool_id=?',(pid,))
                continue
            tx=transactions[txid]
            actual_height=tx_heights.get(txid)
            remined=(tx is not None and tx.get('hash','').lower()==txid
                     and actual_height is not None and actual_height!=height
                     and actual_height in canonical and canonical[actual_height]==tx.get('blockHash'))
            if (height in canonical and canonical[height]!=block_hash) or remined:
                inventory.db.execute(f'DELETE FROM {table} WHERE sequence=?',(sequence,))
                rewind=min(height,actual_height) if remined else height
                inventory.put('activity_cursor',min(inventory.get('activity_cursor',rewind-1),rewind-1))
                inventory.db.execute("DELETE FROM metadata WHERE key='activity_resume'")
                inventory.put('activity_recent_cursor',min(inventory.get('activity_recent_cursor',rewind-1),rewind-1))
                inventory.db.execute("DELETE FROM metadata WHERE key='activity_recent_status'")
                orphaned+=1;continue
            retry=(height not in canonical or tx is None or tx.get('hash','').lower()!=txid
                   or tx.get('blockHash')!=block_hash or tx_heights.get(txid)!=height)
            key=None if retry else key_from_input(tx.get('input',''),pid,USDC)
            if key and key.id not in seen and len(seen)>=inventory.registry.maximum:retry=True
            if retry:
                attempts+=1;deferred+=1;next_try=now+min(300,2**min(attempts,9))
                inventory.db.execute('INSERT INTO activity_retries VALUES(?,?,?) ON CONFLICT(txid) DO UPDATE '
                    'SET next_try=MAX(activity_retries.next_try,excluded.next_try),attempts=MAX(activity_retries.attempts,excluded.attempts)',
                    (txid,next_try,attempts))
                for lane in TABLES:
                    inventory.db.execute(f'UPDATE {lane} SET attempts=MAX(attempts,?),next_try=MAX(next_try,?) WHERE txid=?',
                                         (attempts,next_try,txid))
                continue
            if key:
                admitted.append((key,height,block_hash));seen.add(key.id)
                inventory.db.execute('INSERT OR IGNORE INTO pools VALUES(?,?,?,?)',(key.id,json.dumps(key.values()),height,block_hash))
                for lane in TABLES:inventory.db.execute(f'DELETE FROM {lane} WHERE pool_id=?',(pid,))
            else:
                undecodable+=1
                inventory.db.execute(f'DELETE FROM {table} WHERE sequence=?',(sequence,))
    _status(inventory,tip,coalesced,already_known,deferred=deferred,undecodable=undecodable,orphaned=orphaned)

    for key,_,_ in admitted:
        inventory.registry.keys[key.id]=key;inventory.tentative.pop(key.id,None);touched.add(key.id)
    return touched
