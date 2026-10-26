"""A complete visible transaction prefix, with a measured observation time.

Some public RPCs accept getBlockReceipts('pending') but return the sealed
block. Fetch receipts by hashes from the pending header instead. Never label
an observation preconfirmed solely because a request used the pending tag.
"""
from dataclasses import dataclass
import hashlib
import json
import time

from .core import check, number


@dataclass
class Frame:
    header: dict
    latest: dict
    receipts: list
    requested_ns: int
    received_ns: int
    received_utc: float

    @property
    def preconfirmed(self):
        return number(self.header["number"]) > number(self.latest["number"])

    @property
    def identity(self):
        # The pending hash is often zero. Prefix content and parent identify
        # the observation; this is explicitly not a canonical block hash.
        value = [self.header["number"], self.header["parentHash"], self.receipts]
        return "0x" + hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()

    def payload(self):
        return {"header": self.header, "latest_at_receipt_read": self.latest,
                "receipts": self.receipts, "preconfirmed": self.preconfirmed,
                "observation_id": self.identity,
                "publication_boundaries_proven": False,
                "sequencer_arrival_time_known": False}


def validate_frame(header, latest, receipts, after=None):
    check(header and latest, "missing feed header")
    txs = header.get("transactions")
    check(isinstance(txs, list) and all(isinstance(t, str) for t in txs), "hash-only header required")
    check(len(set(txs)) == len(txs), "duplicate transaction in prefix")
    check(len(txs) == len(receipts), "incomplete visible prefix")
    n = number(header["number"])
    check(n <= number(latest["number"]) + 1, "feed skipped parent")
    if n > number(latest["number"]):
        check(header["parentHash"] == latest["hash"], "preconfirmation parent mismatch")
    zero = "0x" + "00"*32
    hashes = {r.get("blockHash") for r in receipts if r and r.get("blockHash") != zero}
    if header.get("hash") != zero:
        hashes.add(header.get("hash"))
    check(None not in hashes and len(hashes) <= 1, "mixed receipt block hashes")
    if after is not None:
        check(after["number"] == header["number"] and after["parentHash"] == header["parentHash"],
              "pending block changed during read")
        check(after["transactions"][:len(txs)] == txs, "pending prefix replaced during read")
    for i, (tx, receipt) in enumerate(zip(txs, receipts)):
        check(receipt is not None, "pending receipt unavailable")
        check(receipt["transactionHash"] == tx and number(receipt["transactionIndex"]) == i,
              "receipt prefix order mismatch")
        check(number(receipt["blockNumber"]) == n, "receipt is from a different block")
        for log in receipt.get("logs", []):
            check(not log.get("removed"), "removed pending log")
            check(log["transactionHash"] == tx and number(log["transactionIndex"]) == i
                  and number(log["blockNumber"]) == n, "receipt/log mismatch")
            check(log["blockHash"] == receipt["blockHash"], "receipt/log hash mismatch")


class PendingFeed:
    def __init__(self, rpc):
        self.rpc = rpc

    def poll(self):
        started = time.monotonic_ns()
        header = self.rpc.header("pending")
        hashes = header.get("transactions", [])
        receipts = []
        # Re-read the full prefix: cached receipts could survive an unseen
        # preconfirmation replacement with the same height and parent.
        for offset in range(0, len(hashes), 126):
            receipts.extend(self.rpc.batch([("eth_getTransactionReceipt", [tx])
                                            for tx in hashes[offset:offset+126]]))
        after, latest = self.rpc.batch([("eth_getBlockByNumber", ["pending", False]),
                                       ("eth_getBlockByNumber", ["latest", False])])
        received, utc = time.monotonic_ns(), time.time()
        validate_frame(header, latest, receipts, after)
        return Frame(header, latest, receipts, started, received, utc)
