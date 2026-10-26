"""Bounded, durable log catch-up; historical logs supply topology, never prices."""
from .core import check


def merge_ranges(ranges, maximum=64):
    merged = []
    for pair in sorted(ranges):
        check(len(pair) == 2 and all(type(n) is int and n >= 0 for n in pair)
              and pair[0] <= pair[1], 'invalid discovery backlog range')
        lo, hi = pair
        if merged and lo <= merged[-1][1] + 1:
            merged[-1][1] = max(hi, merged[-1][1])
        else:
            merged.append([lo, hi])
    # Bound metadata by joining the smallest holes, never by losing work.
    # A joined hole may be read twice; authentication and last_seen are idempotent.
    while len(merged) > maximum:
        i = min(range(len(merged)-1), key=lambda i: merged[i+1][0]-merged[i][1])
        merged[i:i+2] = [[merged[i][0], merged[i+1][1]]]
    return merged


def subtract_range(ranges, covered):
    if covered is None:
        return [list(pair) for pair in ranges]
    lo, hi = covered
    remaining = []
    for start, end in ranges:
        if start < lo:
            remaining.append([start, min(end, lo-1)])
        if end > hi:
            remaining.append([max(start, hi+1), end])
    return merge_ranges(remaining)


def plan_history(previous, height, recent_blocks, catchup_blocks):
    check(type(catchup_blocks) is int and catchup_blocks > 0, 'invalid discovery catch-up size')
    cursor = previous.get('discovery_cursor')
    lower = max(0, height-recent_blocks+1 if cursor is None else min(height, cursor+1))
    bounded = max(0, height-catchup_blocks+1)
    gap = [lower, bounded-1] if lower < bounded else None
    fresh = [max(lower, bounded), height]
    if 'discovery_backlog' in previous:
        ranges = previous['discovery_backlog']
    else:
        # Import a legacy recorded gap once. An explicitly empty new queue wins.
        legacy = previous.get('offline_history_gap')
        ranges = [legacy] if legacy is not None else []
    pending = merge_ranges([*ranges, *([gap] if gap else [])])
    pending = subtract_range(pending, fresh)
    # At most one extra eth_getLogs page per refresh. Live coverage stays first.
    chunk = None
    if pending and pending[0][0] <= height:
        lo, hi = pending[0]
        chunk = [lo, min(hi, height, lo+min(10, catchup_blocks)-1)]
    return fresh, gap, pending, chunk


def pool_hint_queue(previous, known, fresh_observed, history_observed, capacity=4096):
    """Keep unverified addresses across pages; a full queue retains source ranges."""
    queue = {key: dict(value) for key, value in previous.get('pending_discovery_pools', {}).items()
             if key not in known}
    # A full collection of failing contracts must not seal off the entrance for
    # new pools. Failed carried hints can return to their durable source ranges.
    from collections import deque
    replaceable = deque(sorted((key for key, item in queue.items() if item['attempts']),
                               key=lambda key:(queue[key]['last_attempt_block'],queue[key]['first_seen'],key)))
    deferred_ranges = []
    overflow = []
    for lane, observed in enumerate((fresh_observed, history_observed)):
        full = False
        for key, block in observed.items():
            if key in known:
                continue
            if key not in queue:
                if len(queue) >= capacity:
                    # Historical discoveries can replace carried failures too,
                    # but cannot evict a pool observed in the fresh window.
                    eligible = next((key for key in replaceable if lane == 0 or key not in fresh_observed), None)
                    if eligible is not None:
                        replaceable.remove(eligible)
                        old = queue.pop(eligible)
                        # One observed block rediscovers this address; replaying
                        # its entire lifetime would manufacture a huge backlog.
                        deferred_ranges.append([old['last_seen'], old['last_seen']])
                    else:
                        full = True
                        continue
                queue[key] = dict(first_seen=block, last_seen=block, attempts=0,
                                  last_attempt_block=-1, retry_after_block=0)
            else:
                queue[key]['last_seen'] = max(block, queue[key]['last_seen'])
                queue[key]['first_seen'] = min(block, queue[key]['first_seen'])
        overflow.append(full)
    return queue, overflow, deferred_ranges


def select_pool_hints(queue, fresh_activity, height, limit, previous_keys=()):
    ready = {key for key, value in queue.items() if value['retry_after_block'] <= height}
    oldest = sorted(ready, key=lambda key: (max(queue[key]['last_attempt_block'], queue[key]['first_seen']),
                                          queue[key]['first_seen'], key))
    fresh = [key for key, _ in fresh_activity.most_common() if key in ready]
    # Reserve part of the unchanged authentication budget for pending work.
    # A continuous stream of new pools cannot starve earlier hints.
    carried = set(previous_keys)
    retained = [key for key in oldest if key in carried or key not in fresh_activity]
    reserve = min(len(retained), max(1, limit//4))
    selected = fresh[:max(0, limit-reserve)]
    selected += [key for key in retained if key not in selected][:reserve]
    selected += [key for key in oldest if key not in selected][:max(0, limit-len(selected))]
    return selected


def finish_pool_hints(queue, attempted, authenticated, rejections, height):
    accepted = set(authenticated)
    reasons = {row['pool']: row.get('reason', '') for row in rejections}
    transient = {'metadata unavailable', 'CL metadata unavailable', 'factory reverse lookup unavailable'}
    for key in attempted:
        reason = reasons.get(key)
        if key in accepted or (reason is not None and reason not in transient):
            queue.pop(key, None)
        else:
            # Missing metadata or an incomplete validation result needs another
            # attempt; keep it durable while spacing out permanently broken hints.
            item = queue[key]
            item['attempts'] = min(item['attempts']+1, 20)
            item['last_attempt_block'] = height
            item['retry_after_block'] = height + min(1024, 2**item['attempts'])
