#!/usr/bin/env python3
"""Check the tracked publication boundary without printing sensitive values.

Run after staging the intended release. Gitleaks complements this check by
scanning credentials in both content and history; this is not a replacement.
"""
from pathlib import Path, PurePosixPath
import json
import re
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
PRIVATE_DIRS = {
    "evidence", "releases", "runtime", "private", "wallets", "keystores",
    ".venv", "node_modules", ".vercel", "out", "cache", "__pycache__",
}
PRIVATE_SUFFIXES = (".key", ".pem", ".p12", ".pfx", ".secret", ".sqlite",
                    ".sqlite-wal", ".sqlite-shm", ".db", ".log", ".gz", ".zip")
HOME_PATH = re.compile(r"/(?:Users|home)/[A-Za-z0-9_.-]+/")
PRIVATE_KEY = re.compile(
    r"(?i)[\"']?(?:private_key|privateKey|mnemonic|seed_phrase)[\"']?"
    r"\s*[:=]\s*[\"'](?:0x)?[0-9a-f]{64}[\"']"
)


def check_file(relative):
    path = PurePosixPath(relative)
    errors = []
    if (path.is_absolute() or ".." in path.parts or
        any(part in PRIVATE_DIRS or part.startswith(".private") for part in path.parts) or
        path.name.startswith(".env") and path.name != ".env.example" or
        path.name.endswith(PRIVATE_SUFFIXES) or path.name.endswith(".local.json") or
        relative == "config/local.json" or "keystore" in path.name.lower()):
        errors.append("private/generated file category")
    file = ROOT / relative
    if file.is_symlink():
        return errors + ["symbolic link outside auditable publication boundary"]
    if not file.is_file():
        return errors + ["tracked file missing from working tree"]
    # Audit what will be committed, including a staged file whose working copy
    # has since been changed. Do not trust the unstaged content as a substitute.
    raw = subprocess.run(["git", "show", f":{relative}"], cwd=ROOT,
                         check=True, capture_output=True).stdout
    if len(raw) > 1_048_576:
        errors.append("oversized file requires explicit review")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return errors + ["binary content requires explicit review"]
    if HOME_PATH.search(text):
        errors.append("absolute personal home path")
    if PRIVATE_KEY.search(text):
        errors.append("literal private-key field")
    if file.suffix == ".json":
        try:
            value = json.loads(text)
        except ValueError:
            errors.append("invalid JSON")
        else:
            if isinstance(value, dict) and any(k in value for k in ("crypto", "Crypto")):
                errors.append("possible encrypted keystore")
    return errors


def main():
    result = subprocess.run(["git", "ls-files", "-z"], cwd=ROOT,
                            check=True, capture_output=True)
    paths = [p.decode() for p in result.stdout.split(b"\0") if p]
    if not paths:
        raise SystemExit("No tracked files: stage the intended release first.")
    findings = [(p, error) for p in paths for error in check_file(p)]
    for path, error in findings:
        print(f"{path}: {error}")
    if findings:
        raise SystemExit(1)
    print(f"Publication boundary checked: {len(paths)} tracked files; no flagged private artifacts.")


if __name__ == "__main__":
    main()
