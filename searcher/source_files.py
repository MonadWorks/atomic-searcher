"""Files that define a reproducible source release.

Local credentials, runtime evidence and retired research scripts are deliberately
outside this allowlist. Both validation and installation use the same list.
"""

SOURCE_PATTERNS = (
    "searcher/*.py",
    "src/*.sol",
    "tests/*.py",
    "test/*.sol",
    "tests/fixtures/**/*.json",
    "tools/live-view/*.py",
    "tools/live-view/*.html",
    "tools/live-view/*.css",
    "tools/live-view/*.js",
)
REQUIRED_FILES = (
    "config/base.json",
    "foundry.toml",
    "pyproject.toml",
    "requirements.lock",
)
OPTIONAL_FILES = (
    "config/legacy-atomic-cycle.json",
    "config/market-bootstrap.json",
)


def source_files(root):
    paths = {path for pattern in SOURCE_PATTERNS for path in root.glob(pattern)}
    paths.update(root / name for name in REQUIRED_FILES)
    paths.update(root / name for name in OPTIONAL_FILES if (root / name).exists())
    return sorted(paths)
