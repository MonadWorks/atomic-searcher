"""Bounded raw opportunity archives; financial ledgers are never touched."""
import gzip
import json
import os

from .core import atomic_json


def save_opportunity(directory, frame_id, value, max_bytes, max_count):
    path = directory/f"opportunity-{frame_id}.json.gz"
    temp = path.with_suffix(path.suffix+".tmp")
    with open(temp, "wb") as out:
        out.write(gzip.compress(json.dumps(value).encode(), compresslevel=3))
        out.flush(); os.fsync(out.fileno())
    os.replace(temp, path)
    files = sorted(directory.glob("opportunity-*.json.gz"), key=lambda p:int(p.name.split("-")[1].split(".")[0]))
    sizes = {p:p.stat().st_size for p in files}
    total, expired = sum(sizes.values()), []
    while len(files) > 1 and (len(files) > max_count or total > max_bytes):
        old = files.pop(0); old.unlink(); total -= sizes[old]; expired.append(old.name)
    summary = directory/"opportunity-retention.json"
    previous = json.loads(summary.read_text()) if summary.exists() else {}
    atomic_json(summary, {"retained_count":len(files), "retained_bytes":total, "max_bytes":max_bytes,
                         "max_count":max_count, "expired_count":previous.get("expired_count", 0)+len(expired),
                         "last_expired_file":expired[-1] if expired else previous.get("last_expired_file"),
                         "newest":path.name, "oldest":files[0].name})
