"""Pending observations drive route decisions and the optional executor."""
import copy
import json
import queue
import threading
import time
import shutil
from collections import Counter, OrderedDict, deque

from .core import RPC, atomic_json, check, number, safe_error
from .engine import evaluate, probe_cycle
from .events import StateBook
from .feed import PendingFeed, validate_frame
from .pools import Pool, snapshot
from .recorder import Recorder
from .retention import save_opportunity
from .routes import RouteIndex
from .statefeed import StateFeed, StateFrame, DeepReadScheduler, observation_start_ns


def acknowledge_prewarm(strategy, frame, report, settlements):
    """Only a fresh consumed snapshot with terminal exact quotes spends a lease."""
    transfers = strategy.transfer_feedback
    consume = getattr(transfers, 'revalidation_consumed', None)
    if (consume is None or not isinstance(frame, StateFrame) or frame.state_tag != 'pending' or
            (time.monotonic_ns()-observation_start_ns(frame))/1e6 > strategy.config['policy']['snapshot_max_age_ms']):
        return
    ids = set().union(*(DeepReadScheduler.lease_ids(row) for row in frame.transport.get('requested_groups', ())))
    if not ids: return
    report['prewarm_read_ack'] = consume(sorted(ids), report.get('prewarm_consumed_route_keys', ()),
        candidates=report['candidates'], states=strategy.index.states, settlements=settlements,
        quoted_amounts=report.get('prewarm_quoted_amounts', {}))


def candidate_attempt_order(candidates, settlements, start=0):
    """Try alternatives after a rejection, with bounded per-asset work.

    Input candidates are already ranked by modeled net within each asset.
    Round-robin preserves settlement fairness. The shared observation deadline
    and the executor's pending-transaction guard still apply to every attempt.
    """
    priority = settlements[start:]+settlements[:start]
    buckets = {token: [] for token in priority}
    for candidate in sorted(candidates, key=lambda c:(
            max(c.get('transfer_revalidation_priority',0),c.get('execution_revalidation_priority',0)),
            -c.get('net_after_modelled_cost',0))):
        group = buckets[candidate['tokens'][0]]
        if len(group) < 3: group.append(candidate)
    ordered = [buckets[token][i] for i in range(3) for token in priority if len(buckets[token]) > i]
    return sorted(ordered, key=lambda c:max(c.get('transfer_revalidation_priority',0),
                                           c.get('execution_revalidation_priority',0)))


class PendingStrategy:
    def __init__(self, rpc, config, saved):
        self.rpc, self.config = rpc, config
        check(config['policy']['max_quote_ms'] > 0, 'quote budget must be positive')
        self.base = StateBook([Pool.from_json(p) for p in saved["pools"]], saved["header"])
        self.index = RouteIndex(list(self.base.pools.values()),
                                [t["address"] for t in config["settlement_tokens"]], config["policy"]["max_hops"])
        self.last_identity = None
        self.last_height = None
        self.last_base_fee = None
        self.route_visits = OrderedDict()
        self.visit_sequence = 0
        self.snapshot_sequence = 0
        self.route_wait_since = OrderedDict()
        self.last_groups = None
        from .route_feedback import RouteFeedback
        self.execution_feedback = RouteFeedback()
        self.transfer_feedback = None
        self.sizing_progress = OrderedDict()
        self.page_id = None
        self.page_pending = set()
        self.page_passes = 0
        self.page_routes = {}
        self.page_no_progress_passes = 0

    def sync(self, frame):
        target = number(frame.header["number"])-1
        previous = number(self.base.header["number"])
        if previous == target and self.base.header["hash"] == frame.header["parentHash"]:
            return
        head = self.rpc.header(target)
        check(head["hash"] == frame.header["parentHash"], "feed parent changed")
        if previous >= target or target-previous > 16 or self.rpc.header(previous)["hash"] != self.base.header["hash"]:
            pools, _ = snapshot(self.rpc, list(self.base.pools.values()), head, self.config["policy"]["bitmap_radius"])
            self.base = StateBook(pools, head)
            # A partial refresh may change the pool universe.
            self.index = RouteIndex(pools, [t["address"] for t in self.config["settlement_tokens"]], self.config["policy"]["max_hops"])
        else:
            logs = []
            for height in range(previous+1, target+1):
                sealed = head if height == target else self.rpc.header(height)
                receipts = self.rpc.request("eth_getBlockReceipts", [hex(height)])
                check(isinstance(receipts, list) and receipts, "missing sealed receipts")
                validate_frame(sealed, sealed, receipts)
                for receipt in receipts:
                    logs.extend(receipt.get("logs", []))
            self.base.apply_observed_range(logs, head)
        # Refresh time-dependent fees at the sealed anchor. Tick windows that
        # became incomplete are rebuilt rather than permanently losing a pool.
        from eth_abi import decode
        from .core import data
        dynamic = [p for p in self.base.pools.values() if p.kind == "slipstream"]
        for p, raw in zip(dynamic, self.rpc.views([(p.address, data("fee()")) for p in dynamic], target)):
            if raw is not None and len(raw) == 32:
                fee = decode(["uint24"], raw)[0]
                if fee < 1_000_000:
                    p.fee = fee
                    if self.base.invalid_reasons.get(p.address, "").startswith("dynamic fee"):
                        p.valid = True; self.base.invalid_reasons.pop(p.address)
        damaged = [p for p in self.base.pools.values() if
                   ((not p.valid and not self.base.invalid_reasons.get(p.address, "").startswith("dynamic fee"))
                    or (p.kind != 'v2' and (((p.tick//p.spacing) >> 8) not in p.bitmap
                        or (p.bitmap and ((p.tick//p.spacing) >> 8) in (min(p.bitmap), max(p.bitmap))))))]
        if damaged:
            refreshed, _ = snapshot(self.rpc, damaged, head, self.config["policy"]["bitmap_radius"])
            for p in refreshed:
                if p.valid: self.base.invalid_reasons.pop(p.address, None)
        check(self.rpc.header(target)["hash"] == head["hash"], "base reorganized during sync")

    def on_frame(self, frame):
        if hasattr(frame, "snapshot_pools"):
            return self.on_snapshot(frame)
        if self.last_identity == frame.identity and not getattr(self.index, "work_queue", ()):
            return None
        self.sync(frame)
        self.last_identity = frame.identity
        book = copy.deepcopy(self.base)
        # StateBook expects one hash for a complete observation. Use a local
        # identity and retain the real parent separately for transaction anchors.
        header = {**frame.header, "hash": frame.identity}
        book.begin(header)
        logs = [{**log, "blockHash": frame.identity} for r in frame.receipts for log in r.get("logs", [])]
        changed = book.apply_visible_batch(logs)
        for key, reason in list(book.invalid_reasons.items()):
            if reason.startswith("dynamic fee"):
                # The sealed fee is an EXECUTION CEILING, not an assertion
                # about the current fee. Probe and execute enforce maxFee.
                book.pools[key].valid = True
                book.invalid_reasons.pop(key)
                changed.add(key)
        self.index.states = book.pools
        self.index.execution_feedback = self.execution_feedback
        self.index.transfer_feedback = self.transfer_feedback
        height = number(header["number"])
        report = evaluate(self.index, self.config, header, None if height != self.last_height else changed)
        self.last_height = height
        report.update({"observation_id": frame.identity, "received_utc": frame.received_utc,
                       "feed_request_ms": (frame.received_ns-frame.requested_ns)/1e6,
                       "decision_age_ms": (time.monotonic_ns()-frame.requested_ns)/1e6,
                       "observation_age_ms": (time.monotonic_ns()-observation_start_ns(frame))/1e6,
                       "preconfirmed": frame.preconfirmed, "anchor": self.base.header,
                       "fee_model": "sealed fee used as enforced execution ceiling",
                       "decision_source": "complete actually received pending transaction prefix"})
        return report

    def on_snapshot(self, frame):
        self.snapshot_sequence += 1
        self.last_identity = frame.identity
        pools = [Pool.from_json(p) for p in frame.snapshot_pools]
        identity = frame.identity
        for p in pools: p.block, p.block_hash = number(frame.header["number"]), identity
        changed = {p.address for p in pools if p.address not in self.index.states or
                   (p.sqrt_price, p.tick, p.liquidity, p.fee, p.valid, p.bitmap, p.ticks, p.reserve0, p.reserve1) !=
                   tuple(getattr(self.index.states[p.address], k) for k in
                         ("sqrt_price", "tick", "liquidity", "fee", "valid", "bitmap", "ticks", "reserve0", "reserve1"))}
        groups = frame.transport.get('route_groups')
        topology_fields = ('kind', 'factory', 'token0', 'token1', 'spacing')
        topology_changed = any(p.address in self.index.states and
            any(getattr(p, field) != getattr(self.index.states[p.address], field) for field in topology_fields)
            for p in pools)
        if set(self.index.states) != {p.address for p in pools} or groups != self.last_groups or topology_changed:
            self.index = RouteIndex(pools, [t["address"] for t in self.config["settlement_tokens"]],
                                    self.config["policy"]["max_hops"], groups=groups)
            self.index.work_queue = deque(self.index.routes)
            self.index.queued_routes = set(self.index.routes)
            self.last_groups = groups
            changed = None
        else: self.index.states = {p.address: p for p in pools}
        self.index.sizing_progress = self.sizing_progress
        page_id = frame.transport.get('page_id')
        requested = {tuple(sorted(row['pools'])):row for row in frame.transport.get('requested_groups', [])}
        lease_ids = set().union(*(DeepReadScheduler.lease_ids(row) for row in requested.values()))
        live = getattr(self.transfer_feedback, 'revalidation_lease_ids', None)
        if live is not None: lease_ids.intersection_update(live())
        prewarm_keys = {key for row in requested.values() if DeepReadScheduler.lease_ids(row) & lease_ids
                       for key in row.get('route_keys', ())}
        amounts = getattr(self.transfer_feedback, 'revalidation_amounts', None)
        required_amounts = amounts(lease_ids) if amounts is not None and lease_ids else {}
        route_signatures = {key:(route.tokens, tuple((pool, self.index.states[pool].kind,
                            self.index.states[pool].factory, self.index.states[pool].token0,
                            self.index.states[pool].token1, self.index.states[pool].spacing)
                            for pool in route.pools)) for key,route in self.index.routes.items()}
        withdrawn = set()
        if page_id != self.page_id or page_id is None:
            self.page_id, self.page_passes = page_id, 0
            self.page_pending = set(self.index.routes)
            self.page_no_progress_passes = 0
        else:
            withdrawn = self.page_pending - self.index.routes.keys()
            self.page_pending.intersection_update(self.index.routes)
            # A same-page refresh may change topology after universe/admission
            # updates. New directions need ownership too; completed routes only
            # reopen for metadata changes, not ordinary price/fee updates.
            self.page_pending.update(key for key,signature in route_signatures.items()
                                     if self.page_routes.get(key) != signature)
        self.page_routes = route_signatures
        requested_keys = {key for key,route in self.index.routes.items()
            if (tuple(sorted(route.pools)) in requested and
                (requested[tuple(sorted(route.pools))].get('route_keys') is None or
                 key in requested[tuple(sorted(route.pools))]['route_keys']))}
        requested_pending = self.page_pending & requested_keys
        ordinary_pending = self.page_pending - requested_keys
        header = {**frame.header, "hash": identity}
        # Revisit deferred routes even if prices haven't changed.
        base_fee = number(frame.latest["baseFeePerGas"])
        affected = None if base_fee != self.last_base_fee else changed
        urgent = {tuple(g) for g in frame.transport.get('activity_priority_groups', [])}
        if not hasattr(self.index, 'work_queue'):
            self.index.work_queue = deque(self.index.routes)
            self.index.queued_routes = set(self.index.routes)
        changed_keys = {r.key for r in (self.index.affected(affected) if affected is not None else self.index.routes.values())}
        for key, route in self.index.routes.items():
            self.route_wait_since.setdefault(key, self.snapshot_sequence)
            needed = (key in self.page_pending if page_id is not None else
                      key in changed_keys or tuple(sorted(route.pools)) in urgent)
            if needed and key not in self.index.queued_routes:
                self.index.work_queue.append(key); self.index.queued_routes.add(key)
        if page_id is not None:
            # One owner, bounded by this page's topology. Changed prices and
            # candidate retries cannot repeatedly consume already-delivered
            # targets while untouched ordinary directions are still waiting.
            self.index.work_queue = deque(key for key in self.index.work_queue if key in self.page_pending)
            self.index.queued_routes = set(self.index.work_queue)
        # Depth keeps its existing three-pass head start. Afterwards ordinary
        # work must finish before the remaining explicit depth scope can be
        # returned. Ordinary directions never enter the depth continuation queue.
        priority_targets = (requested_pending if self.page_passes < 3 else ordinary_pending) or self.page_pending
        # Events get a bounded two-observation head start. Waiting work ages
        # across page switches, so one expensive hot route cannot starve others.
        self.index.work_queue = deque(sorted(self.index.work_queue, key=lambda k: (
            k not in priority_targets,
            tuple(sorted(self.index.routes[k].pools)) not in requested,
            self.route_wait_since[k] - (2 if tuple(sorted(self.index.routes[k].pools)) in urgent else 0),
            self.route_visits.get(k, 0))))
        self.index.execution_feedback = self.execution_feedback
        self.index.transfer_feedback = self.transfer_feedback
        self.index.current_page_target_keys = set(priority_targets)
        # This page already owns and queues its remaining targets. evaluate's
        # normal changed-pool scheduling would otherwise re-add finished routes.
        changed = set() if page_id is not None else affected
        report = (evaluate(self.index, self.config, header, changed, required_amounts=required_amounts)
                  if required_amounts else evaluate(self.index, self.config, header, changed))
        unstarted = getattr(self.index, 'unstarted_keys', set())
        considered_keys = set(getattr(self.index, 'last_evaluated_keys', ())) - unstarted
        completed = set(report.get('quote_resolved_route_keys', ())) & prewarm_keys
        quoted = report.get('prewarm_quoted_amounts', {})
        completed = {key for key in completed if key in required_amounts and quoted.get(key) == required_amounts[key]}
        if (frame.state_tag != 'pending' or (time.monotonic_ns()-observation_start_ns(frame))/1e6 >
                self.config['policy']['snapshot_max_age_ms']): completed = set()
        report['prewarm_consumed_route_keys'] = sorted(completed)
        delivered = (considered_keys-prewarm_keys) | completed
        newly_considered = self.page_pending & delivered
        self.page_pending.difference_update(delivered)
        requested_pending = self.page_pending & requested_keys
        ordinary_pending = self.page_pending - requested_keys
        self.page_no_progress_passes = 0 if newly_considered else self.page_no_progress_passes+1
        # Persist actual evaluation coverage separately from page selection.
        # "considered" includes concrete early rejections; it never means that
        # a quote was profitable or the whole executor call passed.
        coactive = {tuple(sorted(group)): {'pools':list(sorted(group)),
                    'directions_in_page':0, 'directions_considered':0}
                    for group in frame.transport.get('coactive_priority_groups', ())}
        if coactive:
            for key, route in self.index.routes.items():
                item = coactive.get(tuple(sorted(route.pools)))
                if item is not None:
                    item['directions_in_page'] += 1
                    item['directions_considered'] += int(key in considered_keys)
        report['coactive_route_progress'] = list(coactive.values())
        self.page_passes += 1
        # A target is delivered after an actual bounded quote attempt or a
        # concrete rejection. Global optimum search is separate. Only explicit
        # depth requests may be handed back with an acknowledgement; ordinary
        # unvisited work stays owned by this page until it is actually considered.
        continuation = {}
        if requested_pending and not ordinary_pending and self.page_passes >= 3:
            deep = set(frame.transport.get('deep_pools', ()))
            needs_depth = {key for row in report.get('deep_read_requests', ()) for key in row.get('route_keys', ())}
            for key in sorted(requested_pending):
                group = tuple(sorted(self.index.routes[key].pools))
                row = requested.get(group, {})
                entry = continuation.setdefault(group, {'pools':group, 'since':row.get('since', time.monotonic()),
                                       'score':row.get('score', 0),
                                       'deep':bool(row.get('deep', set(group).issubset(deep))), 'route_keys':[]})
                if key in needs_depth: entry['deep'] = True
                DeepReadScheduler.merge_leases(entry, row)
                if row.get('continuation_lane') == 'prewarm': entry['continuation_lane'] = 'prewarm'
                entry['route_keys'].append(key)
        report.update({'page_scan_complete':not self.page_pending,
                       'page_advance_ready':not self.page_pending or bool(continuation) or page_id is None,
                       'page_unfinished_routes':len(self.page_pending), 'page_passes':self.page_passes,
                       'page_total_routes':len(self.index.routes),
                       'page_considered_routes':len(self.index.routes)-len(self.page_pending),
                       'page_newly_considered_routes':len(newly_considered),
                       'page_unfinished_ordinary_routes':len(ordinary_pending),
                       'page_unfinished_requested_routes':len(requested_pending),
                       'page_requested_complete':not requested_pending,
                       'page_withdrawn_routes':len(withdrawn),
                       'page_no_progress_passes':self.page_no_progress_passes,
                       'page_coverage_scope':'current page ownership' if page_id is not None else 'this unpaged observation only',
                       'continuation_requests':list(continuation.values())})
        self.last_base_fee = base_fee
        for key in getattr(self.index, 'last_evaluated_keys', ()):
            self.visit_sequence += 1
            self.route_visits[key] = self.visit_sequence
            self.route_visits.move_to_end(key)
            self.route_wait_since[key] = self.snapshot_sequence+1
            self.route_wait_since.move_to_end(key)
        while len(self.route_visits) > 500_000: self.route_visits.popitem(last=False)
        while len(self.route_wait_since) > 500_000: self.route_wait_since.popitem(last=False)
        report.update({"observation_id": identity, "received_utc": frame.received_utc,
                       "feed_request_ms": (frame.received_ns-frame.requested_ns)/1e6,
                       "decision_age_ms": (time.monotonic_ns()-frame.requested_ns)/1e6,
                       "observation_age_ms": (time.monotonic_ns()-observation_start_ns(frame))/1e6,
                       "preconfirmed": None, "anchor": frame.latest, "state_tag": "pending",
                       "context_block": frame.context_block, "atomic_state_read": True, "transport": frame.transport,
                       "anchor_block": number(frame.latest["number"]), "state_height_proven": False,
                       "decision_source": "single EVM call; all pool prices, fees and cached ticks read together"})
        return report


def run_pending(rpc, config, saved, seconds, output_dir, executor=None, stop_event=None, universe_path=None):
    output_dir.mkdir(parents=True, exist_ok=True)
    # Validate durable scheduling state before starting any worker. A corrupt
    # account file must not leave a partially started background pipeline.
    diagnostic_executor = getattr(executor, 'contract', None) or config.get('_diagnostic_executor')
    quarantine = None
    if diagnostic_executor:
        from .quarantine_runtime import open_quarantine, quarantine_path
        state_path = getattr(executor, 'transfer_quarantine_path', None)
        if state_path is None:
            state_path = quarantine_path(output_dir, diagnostic_executor)
        quarantine = open_quarantine(config, diagnostic_executor, state_path)
    from .fastlane import LatestFrames, FrameSequence, run_hot_reader
    import sqlite3
    from contextlib import closing
    # Each reader owns its SQLite connection; a common sequence makes decision
    # and opportunity IDs unique while databases remain independently bounded.
    last_ids = [0]
    for name in ('first-seen.sqlite', 'hot-first-seen.sqlite'):
        path = output_dir/name
        if path.exists():
            with closing(sqlite3.connect(f'file:{path}?mode=ro', uri=True)) as db:
                last_ids.append(db.execute('SELECT COALESCE(MAX(id),0) FROM frames').fetchone()[0])
    sequence = FrameSequence(max(last_ids))
    stop, latest = stop_event or threading.Event(), LatestFrames()
    from .context import SharedClockHints
    clock_hints = SharedClockHints(stop=stop)
    def state_rpc(url):
        reader_rpc = RPC(url, rpc.chain_id, timeout=3, cancel_event=stop)
        reader_rpc.clock_hints = clock_hints
        return reader_rpc
    universe = queue.Queue(maxsize=1)
    deep_requests = queue.Queue(maxsize=128)
    from .activity import ActivityInbox, ActivityJournal, CycleSubscriptions, StreamBudget, websocket_url, run_stream
    activity_requests = ActivityInbox()
    hot_requests = ActivityInbox()
    hot_enabled = bool(config['policy'].get('hot_lane_enabled', False) and saved.get('pools') and
                       config['policy'].get('activity_priority', False) and
                       config['policy'].get('feed_mode', 'atomic-state') == 'atomic-state')
    from .fastlane import endpoint_url
    hot_url = (endpoint_url() or rpc.url) if hot_enabled else rpc.url
    hot_health = {'state':'starting' if hot_enabled else 'disabled'}
    universe_lock = threading.Lock()
    hot_universe = [0, saved]
    def current_hot_universe():
        with universe_lock: return tuple(hot_universe)
    def publish_activity(pools, received):
        activity_requests.publish(pools, received)
        if hot_enabled: hot_requests.publish(pools, received)
    def record_hot_health(state, **detail):
        nonlocal hot_health
        hot_health = dict(state=state, **detail)
        atomic_json(output_dir/'hot-feed-health.json', hot_health)
    activity_journal = ActivityJournal(output_dir/'activity-events.jsonl')
    activity_subscriptions = CycleSubscriptions(saved)
    ws_health = {'state':'not_configured'}
    from .universe import PageProgress
    page_progress = PageProgress()
    counts = Counter()
    finish = time.monotonic()+seconds if seconds else float("inf")
    feed_health = {"state":"starting", "last_success_at":None}
    worker_errors = []

    from .v4 import enabled as v4_enabled
    v4_worker = None
    if v4_enabled(config):
        from .v4_runtime import Worker
        import os
        def v4_discovery_rpc():
            endpoint=os.environ.get('BASE_DISCOVERY_RPC_URL') or config['policy'].get('discovery_rpc_url') or rpc.url
            connection=RPC(endpoint,rpc.chain_id,timeout=10,cancel_event=stop)
            connection.minimum_request_interval=max(1.0,float(config['policy'].get('discovery_request_interval_seconds',1.0)))
            return connection
        v4_worker = Worker(lambda: state_rpc(hot_url), config, output_dir, stop, finish,
                           discovery_rpc_factory=v4_discovery_rpc)
        v4_worker.start()

    def receive():
        nonlocal feed_health
        feed_rpc = state_rpc(rpc.url)
        recorder = None
        backoff, verified, deep_retry_at = 0, False, 0
        logical_page_id = None
        published_prewarm = set()
        deep_settlements = tuple(t['address'] for t in config['settlement_tokens'])
        def ordinary_group_allowed(group, pools):
            return not ordinary_strategy.execution_feedback.group_deferred(
                group, pools, deep_settlements, transfers=ordinary_strategy.transfer_feedback)
        def make_atomic_feed(state):
            feed = StateFeed(feed_rpc, config, state)
            # Install the live shared feedback before the first read, including
            # after a discovered-universe refresh. Eligibility is never cached.
            feed.group_allowed = ordinary_group_allowed
            return feed
        try:
            recorder = Recorder(output_dir/"first-seen.sqlite", config["policy"].get("recording_max_mb", 512)*1024**2, id_source=sequence.next)
            feed = make_atomic_feed(saved) if config["policy"].get("feed_mode", "atomic-state") == "atomic-state" else PendingFeed(feed_rpc)
            activity_subscriptions.use_index(saved, getattr(feed, 'pages', None))
            deep = DeepReadScheduler()
            def add_requests(rows):
                receipt = deep.add(rows, time.monotonic())
                transfers = ordinary_strategy.transfer_feedback
                if transfers is not None and hasattr(transfers, 'revalidation_queued'):
                    transfers.revalidation_queued(receipt['accepted'])
                    transfers.revalidation_released(receipt['rejected'], 'queue_rejected')
                    transfers.revalidation_released(receipt['released'], 'queue_evicted')
            def maintain_prewarm(in_flight=()):
                transfers = ordinary_strategy.transfer_feedback
                if transfers is None or not hasattr(transfers, 'revalidation_lease_ids'): return
                live = transfers.revalidation_lease_ids()
                transfers.revalidation_released(deep.prune_prewarm(live), 'queue_evicted')
                held = (set(getattr(feed, 'prewarm_lease_ids', lambda:set())()) |
                        published_prewarm | set(in_flight)) & live
                transfers.revalidation_queued(deep.prewarm_lease_ids() | held)
                transfers.revalidation_held(held)
            while not stop.is_set() and time.monotonic() < finish:
                poll_started = time.monotonic()
                check(shutil.disk_usage(output_dir).free > 1024**3, "less than 1 GiB free; stop evidence recording")
                try:
                    maintain_prewarm()
                    if not verified: feed_rpc.verify_chain(); verified = True
                    paged = getattr(feed, 'pages', None) is not None
                    # Let the strategy acknowledge this page before rereading
                    # it. Usually this takes only a few milliseconds. A busy
                    # strategy still gets fresh same-page data after the bound.
                    wait = min(config['policy'].get('page_ack_wait_ms', 100)/1000, max(0, finish-time.monotonic()))
                    advance, page_id = page_progress.begin(wait, stop) if paged else (True, None)
                    if stop.is_set(): break
                    if paged and advance:
                        unadmitted = deep.resume(page_progress.take_continuation(), time.monotonic())
                        check(not unadmitted, 'bounded depth handoff exceeded scheduler capacity')
                        published_prewarm.clear()
                    # Each read owns one complete slice. Keep the economic
                    # page's queued tail before replacing its universe or
                    # admitting another batch of depth requests.
                    new_page = advance and not getattr(feed, 'has_pending_slices', False)
                    if paged and new_page: logical_page_id = page_id
                    try:
                        if not new_page: raise queue.Empty
                        refreshed = universe.get_nowait()
                        if config["policy"].get("feed_mode", "atomic-state") == "atomic-state":
                            previous_pages = getattr(feed, 'pages', None)
                            feed = make_atomic_feed(refreshed)
                            if feed.pages is not None: feed.pages.resume(previous_pages)
                            activity_subscriptions.use_index(refreshed, getattr(feed, 'pages', None))
                    except queue.Empty: pass
                    if isinstance(feed, StateFeed) and getattr(feed, 'pages', None) is not None:
                        for pool, observed_at in activity_requests.drain(time.monotonic()):
                            feed.pages.prioritize([pool], observed_at)
                    transfers = ordinary_strategy.transfer_feedback
                    if advance and transfers is not None:
                        # A diagnostic only schedules a new complete snapshot;
                        # no old quote or helper result becomes execution state.
                        ready = transfers.revalidation_requests()
                        if ready: add_requests(ready)
                    while True:
                        try: add_requests(deep_requests.get_nowait())
                        except queue.Empty: break
                    if new_page and isinstance(feed, StateFeed) and (deep.pending or deep.resuming or deep.prewarm) and time.monotonic() >= deep_retry_at:
                        available_pools = feed.pages.pools if getattr(feed, 'pages', None) else {p.address:p for p in feed.pools}
                        transfers = ordinary_strategy.transfer_feedback
                        deferred = lambda row:ordinary_strategy.execution_feedback.group_deferred(
                            row['pools'], available_pools, deep_settlements, row.get('route_keys'), transfers)
                        extra = deep.take(set(available_pools), time.monotonic(), deferred)
                        poll_kwargs = (dict(deep_pools=deep.selected_deep_pools, requested_groups=deep.selected_requests)
                                       if paged else dict(deep_pools=extra))
                    else:
                        poll_kwargs = dict(advance_page=advance) if paged else {}
                    reading_ids = set().union(*(DeepReadScheduler.lease_ids(row) for row in
                                               poll_kwargs.get('requested_groups', ())))
                    maintain_prewarm(reading_ids)
                    try:
                        frame = feed.poll(**poll_kwargs)
                    except Exception as exc:
                        if stop.is_set(): break
                        current_deep = (getattr(feed, 'page_deep', set()) if paged else poll_kwargs.get('deep_pools', ()))
                        if not current_deep: raise
                        # Only this failed slice is retried shallow and
                        # returned. Later slices still own their requests and
                        # their reserved continuation slots until delivered.
                        failed_requests = (getattr(feed, 'page_requests', []) if paged else deep.selected_requests)
                        deep_retry_at = time.monotonic()+15
                        check(not deep.resume(failed_requests, time.monotonic()),
                              'failed depth batch exceeded scheduler capacity')
                        recorder.error(exc)
                        frame = feed.poll(advance_page=False, retry_shallow=True) if paged else feed.poll()
                        frame.transport["deep_read_error"] = safe_error(exc)
                    if hasattr(frame, 'transport'):
                        frame.transport['continuation_backlog'] = {
                            'scheduled_groups':len(deep.resuming), 'depth_requests':len(deep.pending),
                            'prewarm_groups':len(deep.prewarm)}
                    if paged:
                        frame.transport['page_id'] = page_id
                        if 'page_slice' in frame.transport:
                            frame.transport['page_slice']['logical_page_id'] = logical_page_id
                    if stop.is_set(): break
                    fid = recorder.append(frame.requested_ns, frame.received_ns,
                                          "atomic-pending-state" if frame.preconfirmed is None else
                                          ("verified-preconfirmed-prefix" if frame.preconfirmed else "sealed-prefix"),
                                          frame.payload(), frame.received_utc)
                    published_prewarm = set().union(*(DeepReadScheduler.lease_ids(row) for row in
                        getattr(frame, 'transport', {}).get('requested_groups', ())))
                    maintain_prewarm(published_prewarm)
                    latest.publish('ordinary', (fid, frame))
                    was_connected = feed_health["state"] == "connected"
                    feed_health = {"state":"connected", "last_success_at":time.time(), "frame_id":fid}
                    if not was_connected: atomic_json(output_dir/"feed-health.json", feed_health)
                    backoff = 0
                except Exception as exc:
                    if stop.is_set(): break
                    recorder.error(exc)
                    feed_health = {**feed_health, "state":"retrying", "error":safe_error(exc), "error_at":time.time()}
                    atomic_json(output_dir/"feed-health.json", feed_health)
                    backoff = min(10, max(1, backoff*2))
                interval = config['policy'].get('pending_poll_interval_seconds', .2)
                if hot_enabled and hot_health.get('state') == 'connected' and time.time()-hot_health.get('at', 0) < 1.5:
                    interval = max(interval, config['policy'].get('hot_lane_ordinary_interval_seconds', 1.0))
                stop.wait(backoff or max(0, interval-(time.monotonic()-poll_started)))
        except Exception as exc:
            worker_errors.append(safe_error(exc)); stop.set()
        finally:
            try:
                transfers = ordinary_strategy.transfer_feedback
                if transfers is not None and hasattr(transfers, 'revalidation_lease_ids'):
                    transfers.revalidation_released(transfers.revalidation_lease_ids(), 'stopped')
                if recorder is not None: atomic_json(output_dir/"feed-summary.json", recorder.summary())
            finally:
                if recorder is not None: recorder.close()
                feed_rpc.close()

    def discover_universe():
        from .engine import prepare
        import os
        from .discovery_reads import DiscoveryReads
        discovery_reads = DiscoveryReads(config, rpc, stop, rpc_factory=RPC)
        current = saved
        try:
            while not stop.wait(config["policy"].get("universe_refresh_seconds", 15)) and time.monotonic() < finish:
                try:
                    refreshed = discovery_reads.prepare(6, previous=current)
                    if stop.is_set(): break
                    check(refreshed.get("pools") and any(p["valid"] for p in refreshed["pools"]), "unusable discovery refresh")
                    from .snapshots import write_universe
                    write_universe(output_dir/"universe-latest.json", refreshed)
                    if universe_path is not None: write_universe(universe_path, refreshed)
                    if stop.is_set(): break
                    current = refreshed
                    # Publish the topology before its existing index can arrive
                    # from receive(); a stale index cannot erase new pool hints.
                    activity_subscriptions.replace(refreshed)
                    with universe_lock:
                        hot_universe[0] += 1
                        hot_universe[1] = refreshed
                    try: universe.get_nowait()
                    except queue.Empty: pass
                    universe.put_nowait(refreshed)
                    atomic_json(output_dir/"discovery-health.json", {
                        "state":"updated", "at":time.time(), "cursor":refreshed["discovery_cursor"],
                        "backlog_blocks":sum(hi-lo+1 for lo,hi in refreshed.get('discovery_backlog', [])),
                        "pending_pool_count":len(refreshed.get('pending_discovery_pools', {})),
                        "recovered_range":refreshed.get('discovery_recovered_range'),
                        "history_error":refreshed.get('discovery_history_error'),
                        "phase_ms":refreshed.get('discovery_phase_ms'),
                        "read_timing":refreshed.get('discovery_read_timing'),
                        "read_route":refreshed.get('discovery_read_route'),
                        "expansion_error":refreshed.get('discovery_expansion_error'),
                        "expansion_progress":refreshed.get('discovery_expansion_progress', {}),
                        "expansion_pending_pairs":len((refreshed.get('token_expansion') or {}).get('remaining_pairs', [])),
                        "expansion_factory_offset":(refreshed.get('token_expansion') or {}).get('factory_offset', 0),
                        "expansion_queued_tokens":len(refreshed.get('pending_expansion_tokens', []))})
                except Exception as exc:
                    if stop.is_set(): break
                    atomic_json(output_dir/"discovery-health.json", {"state": "retrying", "error": safe_error(exc), "at": time.time()})
        except Exception as exc:
            worker_errors.append(safe_error(exc)); stop.set()
        finally: discovery_reads.close()

    def observe_activity():
        """Independent low-rate event hints; errors never stall price reads."""
        from .universe import ActivityHints
        from .events import V3_SWAP, SLIP_SWAP, V2_SYNC, POOL_ACTIVITY_TOPICS
        import os
        activity_rpc = RPC(os.environ.get('BASE_ACTIVITY_RPC_URL') or config['policy'].get('activity_rpc_url') or rpc.url,
                           rpc.chain_id, timeout=3, cancel_event=stop)
        hints, verified, failures = ActivityHints(), False, 0
        try:
            while not stop.is_set() and time.monotonic() < finish:
                started = time.monotonic()
                try:
                    if (ws_health['state'] == 'connected' and time.time()-ws_health.get('at',0) < 3
                            and not ws_health.get('http_only_pools',0)):
                        stop.wait(.25); continue
                    if not verified: activity_rpc.verify_chain(); verified = True
                    logs = activity_rpc.request('eth_getLogs', [{'fromBlock':'latest', 'toBlock':'pending',
                                                                 'topics':[list(POOL_ACTIVITY_TOPICS)]}])
                    accepted = hints.accept_logs(logs)
                    changed = {log['address'].lower() for log in accepted}
                    received = time.monotonic()
                    if changed:
                        publish_activity(changed, received)
                        activity_journal.append(accepted, 'http-pending-logs', time.time())
                    failures = 0
                    atomic_json(output_dir/'activity-health.json', {'state':'connected', 'at':time.time(),
                                'changed_pools':len(changed), 'read_ms':(received-started)*1000,
                                'scope':'recent swap activity hints; pending timing not proven'})
                except Exception as exc:
                    if stop.is_set(): break
                    failures += 1
                    atomic_json(output_dir/'activity-health.json', {'state':'retrying', 'at':time.time(), 'error':safe_error(exc)})
                interval = min(15, 2**min(failures,4)) if failures else config['policy'].get('activity_poll_seconds', 1)
                stop.wait(max(0, interval-(time.monotonic()-started)))
        finally: activity_rpc.close()

    def observe_stream():
        from .universe import ActivityHints
        from .core import ROOT
        hints = ActivityHints()
        def health(state, **detail):
            nonlocal ws_health
            ws_health = dict(state=state,at=time.time(),**detail, **activity_subscriptions.details())
            atomic_json(output_dir/'activity-stream-health.json', dict(ws_health))
        def pools():
            ordinary = activity_subscriptions.snapshot()
            if v4_worker is None: return ordinary
            return list(ordinary)+['v4:init']+['v4:'+p for p in v4_worker.pool_ids]
        def publish(logs, source):
            if v4_worker is not None: v4_worker.publish(logs)
            accepted = hints.accept_logs(logs)
            if accepted:
                received = time.monotonic()
                publish_activity({log['address'].lower() for log in accepted}, received)
                activity_journal.append(accepted, source, time.time())
        try:
            url = websocket_url()
            if url is None:
                health('not_configured', fallback='http-pending-logs'); return
            from urllib.parse import urlsplit
            host = urlsplit(url).hostname or ''
            chainstack = host.endswith('.chainstack.com') or host.endswith('.p2pify.com')
            selector = None
            if chainstack:
                from .stream_usage import RequestBudget, StreamSelection
                limit = config['policy'].get('activity_ws_daily_request_limit',80_000)
                from .discovery_reads import DiscoveryStreamBudget, split_settings
                allocation = split_settings(config, websocket=lambda: url)
                budget = DiscoveryStreamBudget(ROOT/'evidence/activity-stream-requests.json', limit,
                    ROOT/'evidence/discovery-http-requests.json', split_active=allocation is not None)
                limit = budget.limit
                selector = StreamSelection(limit, budget=budget)
            else:
                budget = StreamBudget(ROOT/'evidence/activity-stream-usage.json',
                                      config['policy'].get('activity_ws_hourly_byte_limit',500_000))
            options = {}
            if v4_worker is not None:
                from .v4_runtime import stream_options
                options = stream_options()
            run_stream(url, rpc.chain_id, pools, stop, finish, publish, health, budget, selector=selector, **options)
        except Exception as exc:
            health('unavailable', error=type(exc).__name__, fallback='http-pending-logs')

    strategy = PendingStrategy(rpc, config, saved)
    if executor is not None: executor.execution_feedback = strategy.execution_feedback
    strategy.transfer_feedback = None
    transfer_worker = None
    if diagnostic_executor:
        from .transfer_feedback import TransferFeedback
        strategy.transfer_feedback = TransferFeedback(diagnostic_executor, config['owner'], quarantine=quarantine)
        if executor is not None: executor.transfer_feedback = strategy.transfer_feedback
        transfer_worker = threading.Thread(target=strategy.transfer_feedback.run,
            args=(getattr(getattr(executor, 'rpc', rpc), 'url', rpc.url), rpc.chain_id, stop, finish,
                  output_dir/'transfer-health.json'), name='transfer-diagnostics', daemon=True)
        transfer_worker.start()
    ordinary_strategy = strategy
    hot_strategy = PendingStrategy(rpc, config, saved) if hot_enabled else None
    if hot_strategy is not None:
        hot_strategy.execution_feedback = ordinary_strategy.execution_feedback
        hot_strategy.transfer_feedback = ordinary_strategy.transfer_feedback
    hot_reader = None
    if hot_enabled:
        hot_settlements = tuple(t['address'] for t in config['settlement_tokens'])
        hot_group_allowed = lambda group, pools: not ordinary_strategy.execution_feedback.group_deferred(
            group, pools, hot_settlements, transfers=ordinary_strategy.transfer_feedback)
        hot_reader = threading.Thread(target=run_hot_reader, kwargs={
            'rpc_factory':lambda:state_rpc(hot_url),
            'fallback_factory':(lambda:state_rpc(rpc.url)) if hot_url != rpc.url else None,
            'config':config, 'universe':current_hot_universe, 'inbox':hot_requests,
            'stop':stop, 'finish':finish, 'directory':output_dir, 'sequence':sequence,
            'publish':latest.publish, 'health':record_hot_health, 'feed_factory':StateFeed,
            'group_allowed':hot_group_allowed}, name='hot-event-feed', daemon=True)
        hot_reader.start()
    reader = threading.Thread(target=receive, name="pending-feed", daemon=True)
    reader.start()
    discovery = threading.Thread(target=discover_universe, name="pool-discovery", daemon=True)
    discovery.start()
    activity = streaming = None
    if config['policy'].get('activity_priority', False):
        streaming = threading.Thread(target=observe_stream, name='pending-log-stream', daemon=True)
        streaming.start()
        activity = threading.Thread(target=observe_activity, name='pool-activity', daemon=True)
        activity.start()
    next_settlement = 0
    settlement_order = [t["address"] for t in config["settlement_tokens"]]
    try:
        if executor is not None:
            executor.bind_stop_event(stop)
            executor.start_context_worker()
        while not stop.is_set() and time.monotonic() < finish:
            if executor is not None:
                try: executor.reconcile()
                except Exception as exc:
                    counts["reconciliation_errors"] += 1
                    atomic_json(output_dir/"execution-health.json", {"state": "recovering", "error": safe_error(exc), "at": time.time()})
                    stop.wait(.5)
                    continue
                if stop.is_set(): break
            if v4_worker is not None:
                v4_result = v4_worker.consume(executor)
                if v4_result:
                    counts['v4_decisions'] += 1
                    counts['v4_'+v4_result['status']] += 1
            try: lane, (fid, frame) = latest.get(timeout=min(.2 if v4_worker else 1, max(.01, finish-time.monotonic())))
            except queue.Empty:
                if stop.is_set() or time.monotonic() >= finish: break
                check(reader.is_alive(), "feed stopped unexpectedly")
                atomic_json(output_dir/"health.json", {"state":"waiting_for_data" if feed_health["state"] == "starting" else "degraded",
                            "feed":dict(feed_health), "counts":dict(counts), "executor_connected":executor is not None, "at":time.time()})
                continue
            try:
                strategy = hot_strategy if lane == 'hot' else ordinary_strategy
                consume_started = time.monotonic_ns()
                report = strategy.on_frame(frame)
                if report is None: continue
                if lane == 'ordinary': acknowledge_prewarm(strategy, frame, report, settlement_order)
                if lane == 'ordinary' and report.get('page_advance_ready', True):
                    page_progress.consumed(getattr(frame, 'transport', {}).get('page_id'),
                                           report.get('continuation_requests', ()))
                if stop.is_set(): break
                counts["decisions"] += 1
                counts[lane+"_decisions"] += 1
                report['lane'] = lane
                report['consumer_queue_ms'] = (consume_started-frame.received_ns)/1e6
                report['recording_database'] = 'hot-first-seen.sqlite' if lane == 'hot' else 'first-seen.sqlite'
                counts["atomic_state_decisions" if frame.preconfirmed is None else
                       ("preconfirmed_decisions" if frame.preconfirmed else "sealed_decisions")] += 1
                report["frame_id"] = fid
                if report.get("deep_read_routes"):
                    try: deep_requests.put_nowait(report.get('deep_read_requests', report["deep_read_routes"]))
                    except queue.Full: pass  # Bounded work, revisited on future observations.
                age = report.get("observation_age_ms", report["decision_age_ms"])
                fresh = age <= config["policy"]["snapshot_max_age_ms"]
                report["fresh_for_submission"] = fresh
                if strategy.transfer_feedback is not None:
                    for candidate in report['candidates']:
                        strategy.transfer_feedback.offer(strategy.index.routes[candidate['route_key']],
                            strategy.index.states, candidate['amount_in'], settlement_order)
                selected = candidate_attempt_order(report['candidates'], settlement_order, next_settlement)
                refreshes = 0
                for candidate in selected:
                    if stop.is_set(): break
                    counts["candidates"] += 1
                    observation_deadline = observation_start_ns(frame) + int(config["policy"]["snapshot_max_age_ms"]*1e6)
                    execution_candidate = None
                    try:
                        route = strategy.index.routes[candidate["route_key"]]
                        if strategy.transfer_feedback is not None and strategy.transfer_feedback.deferred(route):
                            candidate['status'] = 'sampled_transfer_backoff'
                            continue
                        execution_candidate, execution_states, execution_anchor = candidate, strategy.index.states, report['anchor']
                        if (executor is not None and refreshes == 0 and
                                (time.monotonic_ns()-observation_start_ns(frame))/1e6 > config['policy']['snapshot_max_age_ms']/2):
                            from .candidate_refresh import refresh_candidate
                            refreshes += 1
                            candidate['execution_recheck_trigger'] = 'observation_age'
                            execution_candidate, execution_states, execution_anchor, observation_deadline, evidence = refresh_candidate(
                                rpc, config, route, strategy.index.states, getattr(executor,'context',None),
                                previous_candidate=candidate, clock_hints=clock_hints)
                            candidate['execution_recheck'] = evidence
                        if time.monotonic_ns() > observation_deadline:
                            candidate["status"] = "expired_before_probe"
                            continue
                        if execution_candidate is None:
                            candidate['status'] = ('recheck_budget_exhausted' if
                                candidate['execution_recheck']['decision'].get('search_time_limited')
                                else 'recheck_below_floor')
                            continue
                        if strategy.transfer_feedback is not None and strategy.transfer_feedback.deferred(route):
                            candidate['status'] = 'sampled_transfer_backoff'
                            continue
                        if executor is not None:
                            from .execution import ExecutionAnchorChanged
                            try:
                                execution = executor.execute(execution_candidate, route, execution_states,
                                                             execution_anchor, observation_deadline)
                            except ExecutionAnchorChanged as exc:
                                candidate['execution_anchor_mismatch'] = exc.anchor_mismatch
                                # The executor raises this before probing or
                                # signing. Never retry arbitrary execution or
                                # send failures, and never refresh twice in a
                                # frame, including an earlier age-based read.
                                if refreshes or stop.is_set(): raise
                                from .candidate_refresh import refresh_candidate
                                refreshes += 1
                                candidate['execution_recheck_trigger'] = 'execution_anchor_changed'
                                execution_candidate, execution_states, execution_anchor, observation_deadline, evidence = refresh_candidate(
                                    rpc, config, route, strategy.index.states, getattr(executor, 'context', None),
                                    previous_candidate=candidate, clock_hints=clock_hints)
                                candidate['execution_recheck'] = evidence
                                if time.monotonic_ns() > observation_deadline:
                                    candidate['status'] = 'expired_before_probe'
                                    continue
                                if execution_candidate is None:
                                    candidate['status'] = ('recheck_budget_exhausted' if
                                        evidence['decision'].get('search_time_limited') else 'recheck_below_floor')
                                    continue
                                if strategy.transfer_feedback is not None and strategy.transfer_feedback.deferred(route):
                                    candidate['status'] = 'sampled_transfer_backoff'
                                    continue
                                if stop.is_set(): break
                                execution = executor.execute(execution_candidate, route, execution_states,
                                                             execution_anchor, observation_deadline)
                            candidate['execution'] = execution
                            candidate["status"] = "submitted"
                            strategy.execution_feedback.passed(candidate['route_key'])
                            if strategy.transfer_feedback is not None: strategy.transfer_feedback.passed(route)
                            next_settlement = (settlement_order.index(candidate["tokens"][0])+1) % len(settlement_order)
                            break
                        else:
                            if strategy.transfer_feedback is not None:
                                check(strategy.transfer_feedback.claim_probe(route), 'sampled transfer retry waiting')
                            candidate["probe"] = probe_cycle(rpc, route, strategy.index.states,
                                                             candidate["amount_in"], candidate["minimum_gross_surplus"],
                                                             anchor=report['anchor'],
                                                             block_context=report.get('transport', {}).get('execution_context'))
                            check(candidate["probe"]["first_transaction_gas_budget"] <= candidate["cost_budget"]["gas_limit"],
                                  "first execution including registration exceeds modeled gas budget")
                            candidate["status"] = "whole_cycle_passed"
                            strategy.execution_feedback.passed(candidate['route_key'])
                            counts["whole_cycle_passed"] += 1
                    except Exception as exc:
                        from .candidate_refresh import CandidateRefreshFailure
                        if isinstance(exc, CandidateRefreshFailure):
                            candidate['status'], candidate['error'] = 'recheck_failed', safe_error(exc)
                            candidate['failure_stage'] = 'candidate_refresh'
                            candidate['execution_recheck_failure'] = exc.evidence
                            counts['candidate_recheck_failures'] += 1
                            continue
                        candidate["status"], candidate["error"] = "execution_rejected", safe_error(exc)
                        from .execution import execution_failure_details
                        candidate['execution_failure'] = execution_failure_details(exc)
                        candidate['retry_after_seconds'] = strategy.execution_feedback.record(
                            candidate['route_key'], candidate['execution_failure'])
                        if strategy.transfer_feedback is not None and execution_candidate is not None:
                            candidate['transfer_revalidation_scheduled'] = strategy.transfer_feedback.schedule_revalidation(
                                route, execution_states, execution_candidate['amount_in'], settlement_order,
                                candidate['execution_failure'], max(candidate['retry_after_seconds'],
                                    candidate['execution_failure'].get('transfer_retry_after_seconds', 0)))
                    finally:
                        # A reprice creates a new candidate dictionary. Preserve
                        # account-sync diagnostics on the decision row on every
                        # exit, including a failed call or an exhausted refresh.
                        if execution_candidate is not None and 'execution_context_sync' in execution_candidate:
                            candidate['execution_context_sync'] = dict(execution_candidate['execution_context_sync'])
                if strategy.transfer_feedback is not None:
                    report['transfer_prewarm'] = strategy.transfer_feedback.prewarm_summary()
                atomic_json(output_dir/f"decision-{fid}.json", report)
                if report["candidates"]:
                    # Keep the exact first-seen snapshot even after routine
                    # observations rotate out of the bounded recorder.
                    save_opportunity(output_dir, fid,
                        {"requested_ns":frame.requested_ns, "received_ns":frame.received_ns,
                         "observation_started_ns":getattr(frame, 'observation_started_ns', None),
                         "received_utc":frame.received_utc, "frame":frame.payload(), "decision":report},
                        config["policy"].get("opportunity_retention_mb", 128)*1024**2,
                        config["policy"].get("opportunity_retention_count", 512))
                if counts["decisions"] % 128 == 0:
                    files = sorted(output_dir.glob("decision-*.json"), key=lambda p:int(p.stem.split("-")[1]))
                    for old in files[:-config["policy"].get("decision_retention", 2048)]: old.unlink()
                atomic_json(output_dir/"health.json", {"state": "running", "last_decision_at": time.time(),
                            "counts": dict(counts), "last_block": report["block"], "feed":dict(feed_health), "hot_feed":dict(hot_health),
                            "executor_connected": executor is not None})
            except Exception as exc:
                counts["recovery_errors"] += 1
                atomic_json(output_dir/"health.json", {"state": "recovering", "error": safe_error(exc), "at": time.time()})
    except Exception as exc:
        worker_errors.append(safe_error(exc))
        raise
    finally:
        stop.set()
        clock_hints.close()
        # RPC retries share the stop event; an in-flight request has a bounded
        # timeout. Do not report stopped while a worker can still publish.
        reader.join(); discovery.join()
        if hot_reader is not None: hot_reader.join()
        if transfer_worker is not None: transfer_worker.join()
        if activity is not None: activity.join()
        if streaming is not None: streaming.join()
        if v4_worker is not None: v4_worker.thread.join()
        result = {"v4_stopped": v4_worker is None or not v4_worker.thread.is_alive(), "state": "failed" if worker_errors else "stopped", "counts": dict(counts), "executor_connected": executor is not None,
                  "stopped_at": time.time(), "reader_stopped": not reader.is_alive(),
                  "hot_reader_stopped": hot_reader is None or not hot_reader.is_alive(),
                  'activity_stopped': activity is None or not activity.is_alive(),
                  'activity_stream_stopped': streaming is None or not streaming.is_alive(),
                  'transfer_diagnostics_stopped': transfer_worker is None or not transfer_worker.is_alive(),
                  "discovery_stopped":not discovery.is_alive(), "worker_errors":worker_errors}
        atomic_json(output_dir/"health.json", result)
    return result
