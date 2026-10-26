"""Durable scheduling isolation for corroborated directed transfer failures.

This is not a token blacklist or transaction authorization. Only repeated real
whole-call failures with matching canonical helper evidence enter isolation.
A new exact helper receipt allows one ordinary whole-executor trial; time alone
never does. The existing diagnostic worker owns all recovery RPCs.
"""
import hashlib
import json
import math
import os
import tempfile
import threading
import time
from pathlib import Path

from .core import address


class QuarantineStateError(ValueError):
    pass


def _integer(value, minimum=0, maximum=2**256-1):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError('invalid quarantine integer')
    return value


def _stamp(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError('invalid quarantine timestamp')
    return float(value)


def _hash(value):
    if not isinstance(value, str) or len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
        raise ValueError('invalid quarantine fingerprint')
    return value


def _edge(value):
    if not isinstance(value, (tuple, list)) or len(value) != 3:
        raise ValueError('invalid quarantine edge')
    return tuple(address(v) for v in value)


def _evidence(result, executor):
    """Explicit whitelist: never persist unsigned calldata, endpoints or errors."""
    fields = {k: address(result[k]) for k in ('token', 'source', 'destination', 'executor')}
    if fields['executor'] != executor or len(set(fields.values())) != 4 or result.get('canonical') is not True:
        raise ValueError('invalid quarantine sample identity')
    fields.update({k: _integer(result[k], 1 if k == 'amount' else 0)
                   for k in ('amount', 'funded', 'debited', 'credited', 'block')})
    fields['failure_stage'] = _integer(result.get('failure_stage', 0), 0, 2)
    block_hash = result['block_hash']
    if not isinstance(block_hash, str) or not block_hash.startswith('0x'):
        raise ValueError('invalid quarantine sample block')
    _hash(block_hash[2:])
    fields.update(block_hash=block_hash, canonical=True)
    return fields


def _bad_edges(sample):
    incoming = (sample['token'], sample['source'], sample['executor'])
    outgoing = (sample['token'], sample['executor'], sample['destination'])
    stage = sample['failure_stage']
    return tuple(edge for edge, bad in (
        (incoming, stage == 1 or (stage != 1 and sample['funded'] < sample['amount'])),
        (outgoing, stage == 2 or (stage == 0 and sample['funded'] > 0 and sample['credited'] < sample['funded']))) if bad)


def _sample_key(sample):
    return sample['token'], sample['source'], sample['destination']


class DirectionQuarantine:
    SCHEMA = 1

    def __init__(self, path, identity, clock=time.time, capacity=2048, retry_seconds=300, witness_seconds=3600, validate_only=False):
        self.path, self.clock = Path(path), clock
        self.identity = self._identity(identity)
        self.capacity = _integer(capacity, 1, 4096)
        self.retry_seconds = _integer(retry_seconds, 1, 86400)
        self.witness_seconds = _integer(witness_seconds, 1, 86400)
        self.lock = threading.RLock()
        self.records, self.in_flight = {}, set()
        self.last_wall, self.generation = 0., 0
        self.persistence_error = None
        self.dirty, self.capacity_exhausted = False, False
        self._next_flush = 0.
        self.validate_only = bool(validate_only)
        self._load()

    def _writable(self):
        if self.validate_only: raise QuarantineStateError('quarantine validation is read-only')

    @staticmethod
    def _identity(identity):
        return {'chain_id': _integer(identity['chain_id'], 1),
                'owner': address(identity['owner']), 'executor': address(identity['executor']),
                'helper_sha256': _hash(identity['helper_sha256']),
                'executor_sha256': _hash(identity['executor_sha256'])}

    def _now(self):
        # Wall-clock rollback cannot reset a persisted recovery delay.
        self.last_wall = max(self.last_wall, _stamp(self.clock()))
        return self.last_wall

    def _load(self):
        if not self.path.exists(): return
        try:
            if self.path.stat().st_size > 16*1024*1024: raise ValueError('oversized state')
            saved = json.loads(self.path.read_text())
            if saved['schema'] != self.SCHEMA: raise ValueError('unsupported schema')
            old_identity = self._identity(saved['identity'])
            if any(old_identity[k] != self.identity[k] for k in ('chain_id', 'owner', 'executor')):
                raise ValueError('account or chain identity changed')
            self.last_wall = _stamp(saved['last_wall'])
            self.generation = _integer(saved['generation'])
            records = saved['records']
            if not isinstance(records, list) or len(records) > self.capacity: raise ValueError('invalid record count')
            changed = old_identity != self.identity
            for raw in records:
                edge = _edge(raw['edge'])
                if edge in self.records: raise ValueError('duplicate edge')
                sample = _evidence(raw['sample'], self.identity['executor'])
                if edge not in _bad_edges(sample): raise ValueError('missing directed anomaly')
                state = raw['state']
                if state not in ('watch', 'quarantined', 'eligible'): raise ValueError('invalid state')
                witnesses = raw['witnesses']
                if not isinstance(witnesses, list) or not 1 <= len(witnesses) <= 2:
                    raise ValueError('invalid witnesses')
                witnesses = [self._saved_witness(w) for w in witnesses]
                if len({w['id'] for w in witnesses}) != len(witnesses): raise ValueError('duplicate witness')
                if state != 'watch' and len(witnesses) < 2: raise ValueError('uncorroborated isolation')
                if state == 'watch' and len(witnesses) != 1: raise ValueError('invalid watch witnesses')
                if changed and state == 'watch': continue
                # A restart never resurrects a helper-based trial certificate.
                record = dict(edge=list(edge), sample=sample, witnesses=witnesses,
                    state='quarantined' if state == 'eligible' else state,
                    updated_at=_stamp(raw['updated_at']), next_check=_stamp(raw['next_check']),
                    eligible_until=0., provenance=raw.get('provenance', 'runtime'))
                record['last_clean_block'] = _integer(raw.get('last_clean_block', 0))
                if record['provenance'] != 'runtime': _hash(record['provenance'])
                self.records[edge] = record
                self.dirty |= state == 'eligible' or changed
            if self.dirty and not self.validate_only: self._persist()
        except Exception as exc:
            raise QuarantineStateError('transfer quarantine state invalid: '+type(exc).__name__) from exc

    @staticmethod
    def _saved_witness(witness):
        stage = witness['stage']
        if stage not in ('initial_call', 'gas_estimate', 'final_call_batch'): raise ValueError('invalid probe stage')
        route_key = witness['route_key']
        if not isinstance(route_key, str) or len(route_key) > 300: raise ValueError('invalid route key')
        parts = route_key.split('/')
        if not 3 <= len(parts) <= 5 or any(address(p) != p for p in parts): raise ValueError('invalid route key')
        name = witness['revert_name']
        if name not in ('BadCallback', 'TransferFailed', 'V2TransferFailed'): raise ValueError('invalid revert kind')
        return dict(id=_hash(witness['id']), route_key=route_key, stage=stage,
                    started_utc=_stamp(witness['started_utc']), revert_name=name)

    def _witness(self, route, failure):
        if failure.get('validation_deadline_reached') or failure.get('validation_outcome'):
            return None
        name = failure.get('revert_name')
        if name != 'BadCallback':
            if (name not in ('TransferFailed', 'V2TransferFailed') or
                    len(set(failure.get('gas_attempts', ()))) < 2 or
                    not failure.get('revert_selector') or
                    failure.get('previous_gas_failure', {}).get('revert_selector') != failure['revert_selector'] or
                    (name == 'V2TransferFailed' and failure.get('previous_gas_failure', {}).get('revert_name') != name)):
                return None
        probe = failure.get('execution_probe', {})
        call = probe.get('unsigned_call', {})
        if (probe.get('stage') not in ('initial_call', 'gas_estimate', 'final_call_batch') or
                probe.get('state_tag') != 'pending' or
                call.get('to', '').lower() != self.identity['executor'] or
                call.get('from', '').lower() != self.identity['owner']):
            return None
        stamp = _stamp(probe['started_utc'])
        # Multiple gas ceilings in a single execute are one witness.
        identifier = hashlib.sha256(json.dumps([route.key, stamp], separators=(',', ':')).encode()).hexdigest()
        return self._saved_witness(dict(id=identifier, route_key=route.key, stage=probe['stage'],
                                       started_utc=stamp, revert_name=name))

    def _persist(self):
        """Keep a failed write dirty and visible; never grant a trial on it."""
        self._writable()
        self.dirty = True
        temporary = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            document = dict(schema=self.SCHEMA, identity=self.identity, generation=self.generation+1,
                            last_wall=self.last_wall, records=list(self.records.values()))
            with tempfile.NamedTemporaryFile(mode='w', dir=self.path.parent, prefix=self.path.name+'.', suffix='.tmp', delete=False) as handle:
                temporary = handle.name
                json.dump(document, handle, sort_keys=True, separators=(',', ':'))
                handle.flush(); os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            temporary = None
            self.generation += 1
            self.dirty, self.persistence_error = False, None
            return True
        except Exception as exc:
            self.persistence_error = type(exc).__name__
            self._next_flush = self._now()+5
            return False
        finally:
            if temporary is not None:
                try: os.unlink(temporary)
                except OSError: pass

    def flush(self):
        self._writable()
        with self.lock:
            if self.dirty and self._now() >= self._next_flush: self._persist()

    def _room(self, edge):
        if edge in self.records or len(self.records) < self.capacity: return True
        watches = [(v['updated_at'], k) for k,v in self.records.items() if v['state'] == 'watch']
        if watches:
            del self.records[min(watches)[1]]
            return True
        self.capacity_exhausted = True
        return False

    def observe_failure(self, edge, result, route, failure, sample_age_seconds):
        """Called only after TransferFeedback consumed a real claimed probe."""
        self._writable()
        with self.lock:
            if not 0 <= sample_age_seconds <= 300: return False
            edge = _edge(edge)
            sample = _evidence(result, self.identity['executor'])
            if edge not in _bad_edges(sample): return False
            route_edges = set()
            for i, pool in enumerate(route.pools):
                route_edges.update(((route.tokens[i], self.identity['executor'], pool),
                                    (route.tokens[i+1], pool, self.identity['executor'])))
            if edge not in route_edges: return False
            witness = self._witness(route, failure)
            if witness is None: return False
            failed_edge = ((sample['token'],sample['source'],sample['executor']) if sample['failure_stage'] == 1 else
                           (sample['token'],sample['executor'],sample['destination']) if sample['failure_stage'] == 2 else None)
            if witness['revert_name'] != 'BadCallback' and edge != failed_edge: return False
            if not self._room(edge): return False
            now = self._now()
            previous = self.records.get(edge)
            witnesses = list(previous['witnesses']) if previous else []
            if previous and previous['state'] == 'watch' and now-previous['updated_at'] > self.witness_seconds:
                witnesses = []
            if witness['id'] in {w['id'] for w in witnesses}: return False
            witnesses = (witnesses+[witness])[-2:]
            state = 'quarantined' if len(witnesses) == 2 else 'watch'
            self.records[edge] = dict(edge=list(edge), sample=sample, witnesses=witnesses, state=state,
                updated_at=now, next_check=now+self.retry_seconds, eligible_until=0.,
                provenance=previous['provenance'] if previous else 'runtime')
            self._persist()
            return state == 'quarantined'

    def seed_reviewed(self, edge, result, route, failures, review_sha256):
        """Explicit migration of independently reviewed historical evidence.

        Unlike runtime admission, this records that the canonical helper sample
        and stored real failures were correlated by a reviewer, not concurrently
        observed. The review hash is mandatory; this method performs no RPC.
        """
        self._writable()
        _hash(review_sha256)
        edge = _edge(edge)
        sample = _evidence(result, self.identity['executor'])
        route_edges = {(route.tokens[i], self.identity['executor'], pool) for i,pool in enumerate(route.pools)}
        route_edges.update((route.tokens[i+1], pool, self.identity['executor']) for i,pool in enumerate(route.pools))
        if edge not in _bad_edges(sample) or edge not in route_edges:
            raise ValueError('review lacks matching directed anomaly')
        if len(failures) < 2: raise ValueError('review needs two actual failures')
        witnesses = [self._witness(route, failure) for failure in failures]
        if any(w is None for w in witnesses) or len({w['id'] for w in witnesses}) < 2:
            raise ValueError('review needs distinct whole-call witnesses')
        failed_edge = ((sample['token'],sample['source'],sample['executor']) if sample['failure_stage'] == 1 else
                       (sample['token'],sample['executor'],sample['destination']) if sample['failure_stage'] == 2 else None)
        if any(w['revert_name'] != 'BadCallback' and edge != failed_edge for w in witnesses):
            raise ValueError('review failed call is a different directed edge')
        with self.lock:
            for failure in failures[-2:]: self.observe_failure(edge, result, route, failure, 0)
            item = self.records.get(edge)
            if not item or item['state'] != 'quarantined': raise ValueError('review did not establish isolation')
            item['provenance'] = review_sha256
            return self._persist()

    def blocked(self, edges):
        with self.lock:
            now = self._now()
            return any((item := self.records.get(tuple(edge))) is not None and item['state'] != 'watch'
                       and (item['state'] != 'eligible' or item['eligible_until'] <= now or self.dirty)
                       for edge in edges)

    def claim(self, edges):
        self._writable()
        with self.lock:
            edges = tuple(edges)
            if self.blocked(edges): return False
            now = self._now()
            trials = [self.records[edge] for edge in edges if edge in self.records and self.records[edge]['state'] == 'eligible']
            for item in trials:
                item.update(state='quarantined', eligible_until=0., next_check=now+self.retry_seconds)
            # Consume durably before the real call. Crash/timeout needs a new
            # background sample, never another automatic full-call attempt.
            return self._persist() if trials else True

    def passed(self, edges):
        self._writable()
        with self.lock:
            changed = False
            for edge in edges: changed |= self.records.pop(tuple(edge), None) is not None
            if changed: self._persist()

    def take_due(self, exclude=()):
        self._writable()
        with self.lock:
            self.flush()
            now = self._now()
            excluded = set(exclude) | self.in_flight
            due = [item for item in self.records.values() if item['state'] != 'watch'
                   and (item['state'] != 'eligible' or item['eligible_until'] <= now)
                   and item['next_check'] <= now and _sample_key(item['sample']) not in excluded]
            if not due: return None
            item = min(due, key=lambda v:(v['next_check'],v['updated_at'],v['edge']))
            sample = dict(item['sample'])
            self.in_flight.add(_sample_key(sample))
            return sample

    def observe_sample(self, result):
        self._writable()
        with self.lock:
            sample = _evidence(result, self.identity['executor'])
            now, recovered = self._now(), []
            exact = sample['failure_stage'] == 0 and all(sample[k] == sample['amount'] for k in ('funded','debited','credited'))
            for edge,item in self.records.items():
                if item['state'] == 'watch' or _sample_key(item['sample']) != _sample_key(sample) or item['sample']['amount'] != sample['amount']:
                    continue
                item['next_check'] = now+self.retry_seconds
                # A historical/replayed sample cannot issue another trial.
                last_block = max(item['sample']['block'], item.get('last_clean_block', 0))
                if exact and sample['block'] > last_block:
                    item.update(state='eligible', eligible_until=now+self.retry_seconds, last_clean_block=sample['block'])
                    recovered.append(edge)
                elif edge in _bad_edges(sample):
                    item.update(state='quarantined', eligible_until=0., sample=sample)
            if not any(item['state'] != 'watch' and _sample_key(item['sample']) == _sample_key(sample) for item in self.records.values()):
                return []
            if not self._persist():
                for edge in recovered: self.records[edge].update(state='quarantined', eligible_until=0.)
                return []
            return recovered

    def finish_sample(self, sample_key):
        self._writable()
        with self.lock:
            key, now = tuple(sample_key), self._now()
            self.in_flight.discard(key)
            changed = False
            for item in self.records.values():
                if item['state'] != 'watch' and _sample_key(item['sample']) == key:
                    item['next_check'] = max(item['next_check'], now+self.retry_seconds)
                    changed = True
            if changed: self._persist()

    def summary(self):
        with self.lock:
            now = self._now()
            return dict(persistent=True, schema=self.SCHEMA, records=len(self.records), validate_only=self.validate_only,
                quarantined=sum(v['state'] != 'watch' and (v['state'] != 'eligible' or v['eligible_until'] <= now or self.dirty) for v in self.records.values()),
                trial_eligible=sum(v['state'] == 'eligible' and v['eligible_until'] > now and not self.dirty for v in self.records.values()),
                watching=sum(v['state'] == 'watch' for v in self.records.values()),
                persistence_error=self.persistence_error, persistence_dirty=self.dirty,
                capacity_exhausted=self.capacity_exhausted,
                directions=[dict(token=edge[0],sender=edge[1],recipient=edge[2],state=item['state'],
                    witnesses=len(item['witnesses']),helper_block=item['sample']['block'],
                    recovery_after_seconds=max(0,item['next_check']-now))
                    for edge,item in list(self.records.items())[:64]],
                directions_truncated=len(self.records)>64,
                scope='directed scheduling evidence; fresh helper grants one full-execution trial, never signing authority')
