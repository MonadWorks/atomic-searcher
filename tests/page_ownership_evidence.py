"""Inspectible public-market fixtures; no encoded runtime records."""
import json
from pathlib import Path


def saved_page(number):
    return json.loads((Path(__file__).parent / "fixtures" / f"page-ownership-{int(number)}.json").read_text())
