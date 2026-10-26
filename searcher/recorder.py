"""First-seen evidence with optional bounded retention for continuous runs."""
import hashlib
import json
import sqlite3
import time
import zlib
from pathlib import Path

from .core import Reject, number, safe_error


def decode_payload(value):
    return json.loads(zlib.decompress(value) if isinstance(value, bytes) else value)


class Recorder:
    def __init__(self, path, max_bytes=None, id_source=None):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS frames (id INTEGER PRIMARY KEY, received_utc REAL NOT NULL, requested_ns INTEGER NOT NULL, received_ns INTEGER NOT NULL, mode TEXT NOT NULL, digest TEXT NOT NULL, payload TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS arrivals (tx_hash TEXT PRIMARY KEY, frame_id INTEGER NOT NULL, received_utc REAL NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS errors (at REAL NOT NULL, message TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS retention (key TEXT PRIMARY KEY, value INTEGER NOT NULL)")
        self.max_bytes = max_bytes
        self.id_source = id_source
        self.payload_bytes = self.db.execute("SELECT COALESCE(SUM(LENGTH(payload)),0) FROM frames").fetchone()[0]
        self.last_id = self.db.execute("SELECT COALESCE(MAX(id),0) FROM frames").fetchone()[0]

    def append(self, requested, received, mode, payload, received_utc=None):
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        now = time.time() if received_utc is None else received_utc
        digest = hashlib.sha256(raw.encode()).hexdigest()
        compressed = zlib.compress(raw.encode(), 3)
        if self.id_source is None:
            self.last_id += 1
        else:
            next_id = self.id_source()
            if next_id <= self.last_id: raise Reject("non-increasing evidence frame ID")
            self.last_id = next_id
        with self.db:
            self.db.execute("INSERT INTO frames(id,received_utc,requested_ns,received_ns,mode,digest,payload) VALUES(?,?,?,?,?,?,?)",
                            (self.last_id, now, requested, received, mode, digest, sqlite3.Binary(compressed)))
            frame = self.last_id
            for receipt in payload.get("receipts", []):
                self.db.execute("INSERT OR IGNORE INTO arrivals VALUES(?,?,?)", (receipt["transactionHash"], frame, now))
        self.payload_bytes += len(compressed)
        if self.max_bytes and self.payload_bytes > self.max_bytes:
            self.trim()
        return frame

    def trim(self):
        """Reuse database pages; record explicitly that older evidence expired."""
        target = self.max_bytes*4//5
        removed, cutoff = 0, None
        for fid, size in self.db.execute("SELECT id,LENGTH(payload) FROM frames WHERE id<? ORDER BY id", (self.last_id,)):
            if self.payload_bytes-removed <= target: break
            cutoff, removed = fid, removed+size
        if cutoff is not None:
            with self.db:
                count = self.db.execute("SELECT COUNT(*) FROM frames WHERE id<=?", (cutoff,)).fetchone()[0]
                self.db.execute("DELETE FROM arrivals WHERE frame_id<=?", (cutoff,))
                self.db.execute("DELETE FROM frames WHERE id<=?", (cutoff,))
                self.db.execute("INSERT INTO retention VALUES('expired_frames',?) ON CONFLICT(key) DO UPDATE SET value=value+excluded.value", (count,))
                self.db.execute("INSERT OR REPLACE INTO retention VALUES('last_expired_frame',?)", (cutoff,))
            self.payload_bytes -= removed
            self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def error(self, exc):
        with self.db:
            self.db.execute("INSERT INTO errors VALUES(?,?)", (time.time(), safe_error(exc)))
            if self.max_bytes:
                self.db.execute("DELETE FROM errors WHERE rowid NOT IN (SELECT rowid FROM errors ORDER BY rowid DESC LIMIT 10000)")

    def summary(self):
        frames = self.db.execute("SELECT mode, COUNT(*), MIN(received_utc), MAX(received_utc), AVG((received_ns-requested_ns)/1000000.0) FROM frames GROUP BY mode").fetchall()
        return {"frames": [{"mode":m,"count":n,"first_utc":a,"last_utc":b,"mean_request_ms":ms} for m,n,a,b,ms in frames],
                "first_seen_transactions": self.db.execute("SELECT COUNT(*) FROM arrivals").fetchone()[0],
                "errors": self.db.execute("SELECT COUNT(*) FROM errors").fetchone()[0],
                "retention": {"max_payload_bytes":self.max_bytes, "retained_payload_bytes":self.payload_bytes,
                              **dict(self.db.execute("SELECT key,value FROM retention"))},
                "purpose": "observation timing evidence; not an executed-trade profit report"}

    def close(self):
        self.db.close()


def record(rpc, recorder, seconds, interval=2, on_frame=None):
    rpc.verify_chain()
    finish = time.monotonic()+seconds
    pending_supported = True
    while time.monotonic() < finish:
        start = time.monotonic_ns()
        try:
            try:
                tag = "pending" if pending_supported else "latest"
                receipts = rpc.request("eth_getBlockReceipts", [tag])
            except Reject as exc:
                if pending_supported and any(s in str(exc).lower() for s in ("unsupported", "invalid argument", "invalid params")):
                    pending_supported = False
                    recorder.error(exc)
                    continue
                raise
            received = time.monotonic_ns()
            received_utc = time.time()
            if not isinstance(receipts, list):
                raise Reject("missing receipts; not an empty opportunity set")
            heights = {number(x["blockNumber"]) for x in receipts}
            if len(heights) > 1:
                raise Reject("mixed receipt block heights")
            mode = "http-pending-poll" if pending_supported else "http-confirmed-poll"
            payload = {"receipts": receipts, "block_numbers": sorted(heights),
                       "publication_boundaries_proven": False,
                       "sequencer_arrival_time_known": False}
            frame = recorder.append(start, received, mode, payload, received_utc)
            if on_frame:
                on_frame(frame, payload, received)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as exc:
            recorder.error(exc)
        time.sleep(min(interval, max(0, finish-time.monotonic())))
