#!/usr/bin/env python3
"""Build occ_monitoring_registry.json from monitoring_sheet.tsv."""

from __future__ import annotations

import json
import re
from pathlib import Path

from monitoring_registry import norm, norm_path

SHEET = Path(__file__).resolve().parent / "monitoring_sheet.tsv"
OUT = Path(__file__).resolve().parent / "occ_monitoring_registry.json"

MON_LINE = re.compile(r"^M(\d+)\s+(.+?)\s{2,}(.+)$", re.I)


def parse_sheet(text: str) -> dict:
    internal: dict[str, int] = {}
    sp: dict[str, int] = {}
    kafka: dict[str, int] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.lower().startswith("monitor"):
            continue
        m = re.match(r"^M(\d+)\t(.+?)\t(.+)$", line, re.I)
        if not m:
            m = re.match(r"^M(\d+)\s+(.+?)\s{2,}(.+)$", line, re.I)
        if not m:
            continue
        mid = int(m.group(1))
        category = m.group(2).strip().lower()
        component = m.group(3).strip().strip('"')
        if not component:
            continue
        if "internal service" in category:
            internal[norm_path(component)] = mid
        elif "store procedure" in category:
            sp[norm(component)] = mid
        elif "kafka producer" in category:
            # legacy combined file — skip; use sync_monitoring.py
            pass
        elif "kafka consumer" in category:
            pass
    return {"internal": internal, "sp": sp, "kafka_producer": {}, "kafka_consumer": {}}


def main() -> None:
    if not SHEET.exists():
        raise SystemExit(f"Missing {SHEET} — add tab-separated Monitor ID / category / component rows.")
    data = parse_sheet(SHEET.read_text(encoding="utf-8"))
    OUT.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    print(f"[ok] wrote {OUT}")
    for k, v in data.items():
        print(f"  {k}: {len(v)} entries")


if __name__ == "__main__":
    main()
