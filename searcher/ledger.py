"""Durable fee reservations. Unresolved attempts survive crashes and keep budget.

No signing or submission is performed here. Future funded transport must reserve
before signing, mark submission, then reconcile the canonical receipt.
"""
import sqlite3
import time
from contextlib import nullcontext
from pathlib import Path

from .core import check


class RiskLedger:
    def __init__(self, path, daily_cap, failure_limit):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS attempts (id TEXT PRIMARY KEY, day INTEGER NOT NULL, reserved TEXT NOT NULL, actual TEXT, state TEXT NOT NULL, tx_hash TEXT UNIQUE, finished_at REAL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS faults (reason TEXT PRIMARY KEY, at REAL NOT NULL)")
        if 'kind' not in {r[1] for r in self.db.execute('PRAGMA table_info(attempts)')}:
            self.db.execute("ALTER TABLE attempts ADD COLUMN kind TEXT NOT NULL DEFAULT 'transaction'")
        self.daily_cap, self.failure_limit = int(daily_cap), failure_limit

    def reserve(self, attempt, maximum_fee, now=None, *, kind="transaction"):
        now = time.time() if now is None else now
        check(kind in ("transaction","auction"),"unknown reservation kind")
        day = int(now//86400)
        check(0 < maximum_fee <= self.daily_cap, "invalid fee reservation")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            check(self.db.execute("SELECT COUNT(*) FROM faults").fetchone()[0] == 0, "persistent fee-accounting fault; operator review required")
            rows = self.db.execute("SELECT day,reserved,actual,state FROM attempts").fetchall()
            check(not any(state in ("reserved", "submitted") for _, _, _, state in rows), "unresolved prior attempt; reconcile before another send")
            spent = sum(int(actual) for d, _, actual, state in rows if d == day and state in ("success", "failed"))
            check(spent+maximum_fee <= self.daily_cap, "daily fee ceiling reached")
            streak = 0
            for state, in self.db.execute("SELECT state FROM attempts WHERE state IN ('success','failed') ORDER BY finished_at DESC"):
                if state == "success":
                    break
                streak += 1
            check(streak < self.failure_limit, "failure circuit breaker active")
            self.db.execute("INSERT INTO attempts(id,day,reserved,actual,state,tx_hash,finished_at,kind) VALUES(?,?,?,NULL,'reserved',NULL,NULL,?)", (attempt, day, str(maximum_fee),kind))
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    def extend(self,attempt,maximum_fee,now=None):
        """Reserve the sum of independent parallel auction liabilities before signing."""
        day=int((time.time() if now is None else now)//86400)
        self.db.execute('BEGIN IMMEDIATE')
        try:
            row=self.db.execute("SELECT reserved,state,kind FROM attempts WHERE id=?",(attempt,)).fetchone()
            check(row and row[1]=='submitted' and row[2]=='auction','auction group not pending')
            check(maximum_fee>int(row[0]),'auction reservation must increase')
            check(not self.db.execute('SELECT 1 FROM faults').fetchone(),'persistent fee-accounting fault')
            spent=sum(int(x[0]) for x in self.db.execute("SELECT actual FROM attempts WHERE day=? AND state IN ('success','failed')",(day,)))
            check(spent+maximum_fee<=self.daily_cap,'daily fee ceiling reached')
            self.db.execute('UPDATE attempts SET reserved=? WHERE id=?',(str(maximum_fee),attempt));self.db.commit()
        except Exception:self.db.rollback();raise

    def submitted(self, attempt, tx_hash):
        with self.db:
            result = self.db.execute("UPDATE attempts SET state='submitted',tx_hash=? WHERE id=? AND state='reserved'", (tx_hash, attempt))
            check(result.rowcount == 1, "attempt is not reserved")

    def cancel_unsent(self, attempt):
        with self.db:
            result = self.db.execute("UPDATE attempts SET state='cancelled',actual='0' WHERE id=? AND state='reserved' AND tx_hash IS NULL", (attempt,))
            check(result.rowcount == 1, "only an unsent reservation can be cancelled")

    def settle(self, attempt, success, actual_fee, canonical_receipt_verified, now=None, *, commit=True):
        check(canonical_receipt_verified, "canonical receipt required")
        check(actual_fee >= 0, "invalid actual fee")
        with self.db if commit else nullcontext():
            row = self.db.execute("SELECT state,reserved FROM attempts WHERE id=?", (attempt,)).fetchone()
            check(row is not None and row[0] == "submitted", "attempt not awaiting receipt")
            # Record a cap overrun too: hiding it would allow further overspend.
            settled_at = time.time() if now is None else now
            self.db.execute("UPDATE attempts SET state=?,actual=?,finished_at=?,day=? WHERE id=?",
                            ("success" if success else "failed", str(actual_fee), settled_at, int(settled_at//86400), attempt))
            if actual_fee > int(row[1]):
                self.db.execute("INSERT OR IGNORE INTO faults VALUES(?,?)", ("actual fee exceeded reserved maximum", settled_at))

    def reopen(self, attempt, *, commit=True):
        """Restore the original fee reservation after a provisional receipt is orphaned.

        The executor uses commit=False inside the same transaction that clears
        its outcome. A crash must never leave a released reservation with no
        outcome, or an orphaned profit attached to a reopened transaction.
        """
        with self.db if commit else nullcontext():
            result = self.db.execute("UPDATE attempts SET state='submitted',actual=NULL,finished_at=NULL "
                                     "WHERE id=? AND (state IN ('success','failed') OR (state='cancelled' AND kind='auction'))", (attempt,))
            check(result.rowcount == 1, "attempt has no provisional settlement")
            overruns = self.db.execute("SELECT actual,reserved FROM attempts WHERE state IN ('success','failed')").fetchall()
            if not any(int(actual) > int(reserved) for actual, reserved in overruns):
                self.db.execute("DELETE FROM faults WHERE reason='actual fee exceeded reserved maximum'")

    def close(self):
        self.db.close()
