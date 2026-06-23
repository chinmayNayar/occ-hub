#!/usr/bin/env python3
"""Import authoritative monitoring sheet from transcript or monitoring_sheet.tsv."""

from __future__ import annotations

import json
import re
from pathlib import Path

from monitoring_registry import norm, norm_path

BASE = Path(__file__).resolve().parent
OUT = BASE / "occ_monitoring_registry.json"
TRANSCRIPT = Path(
    "/Users/shivangnayar/.cursor/projects/Users-shivangnayar-Desktop-occ/agent-transcripts"
    "/43bd40b9-bbba-4b77-985b-b4a838780b75/43bd40b9-bbba-4b77-985b-b4a838780b75.jsonl"
)
SHEET = BASE / "monitoring_sheet.tsv"


def sheet_text() -> str:
    if SHEET.exists():
        return SHEET.read_text(encoding="utf-8")
    if not TRANSCRIPT.exists():
        raise SystemExit("No monitoring_sheet.tsv or transcript found.")
    for line in TRANSCRIPT.read_text().splitlines():
        obj = json.loads(line)
        if obj.get("role") != "user":
            continue
        for part in obj.get("message", {}).get("content", []):
            t = part.get("text", "")
            if "M1\tMicroFrontend" in t or "M1\tMicroFrontend" in t:
                if "<user_query>" in t:
                    return t.split("<user_query>", 1)[1].split("</user_query>", 1)[0]
                return t
    raise SystemExit("Monitoring sheet not found in transcript.")


def add_min(store: dict[str, int], key: str, mid: int) -> None:
    k = norm_path(key) if key.startswith("/") or "api/" in key else norm(key)
    if not k:
        return
    if k not in store or mid < store[k]:
        store[k] = mid


def parse_sheet(text: str) -> dict:
    internal: dict[str, int] = {}
    sp: dict[str, int] = {}
    kafka_prod: dict[str, int] = {}
    kafka_cons: dict[str, int] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line.startswith("M"):
            continue
        parts = re.split(r"\t+", line)
        if len(parts) < 3:
            continue
        m = re.search(r"M(\d+)", parts[0], re.I)
        if not m:
            continue
        mid = int(m.group(1))
        cat = parts[1].strip().lower()
        comp = parts[2].strip().strip('"').replace("\n", " ").strip()
        if "internal service" in cat:
            add_min(internal, comp, mid)
            base = re.sub(r"\s*\([^)]+\)\s*$", "", comp, flags=re.I).strip()
            add_min(internal, base, mid)
        elif "store procedure" in cat:
            add_min(sp, comp, mid)
        elif "kafka producer" in cat:
            kafka_prod[norm(comp.strip())] = mid
        elif "kafka consumer" in cat:
            kafka_cons[norm(comp.strip())] = mid
    return {
        "internal": internal,
        "sp": sp,
        "kafka_producer": kafka_prod,
        "kafka_consumer": kafka_cons,
    }


def main() -> None:
    data = parse_sheet(sheet_text())
    OUT.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    print(f"[ok] wrote {OUT}")
    for k, v in data.items():
        print(f"  {k}: {len(v)}")


if __name__ == "__main__":
    main()
