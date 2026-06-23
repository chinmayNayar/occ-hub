#!/usr/bin/env python3
"""Authoritative OCCHUB monitoring ID registry (M1–M518)."""

from __future__ import annotations

import json
import re
from pathlib import Path

MON_RE = re.compile(r"\bM(\d+)\b", re.I)

# M2–M12 microservices (canonical order from monitoring sheet)
MS_MON: dict[str, int] = {
    "occhub-admin-ms": 2,
    "occhub-flight-ms": 3,
    "occhub-crew-ms": 4,
    "occhub-weather-ms": 5,
    "occhub-dispatch-ms": 6,
    "occhub-externalcomunication-ms": 7,
    "occhub-externalcommunication-ms": 7,
    "occhub-crewresoucedashboard-ms": 8,
    "occhub-crewrdashboard-ms": 8,
    "occhub-rosterautomation-ms": 9,
    "occhub-streamorchestor-ms": 10,
    "occhub-ingesthub-ms": 11,
    "occhub-eventconsumer-service-ms": 12,
    "occhub-eventConsumer-service-ms": 12,
}

DB_MON = {"mongo": 13, "aims": 14}

APP_MON: dict[str, int] = {
    "user adoption": 497,
    "apm details": 498,
    "arrival hold summary": 499,
    "ctot automation": 500,
    "flight & hold fuel monitoring": 501,
    "postponement (petd)": 502,
    "rule engine": 503,
    "metar and crew manifest": 504,
    "met plus": 505,
    "fuel information": 506,
    "roster input files": 507,
    "admins": 508,
    "joc nav aims comparison": 509,
    "ground power unit (gpu)": 510,
    "stratosphere": 511,
    "standby flight movement": 512,
    "aircraft and crew rating alert": 513,
    "base departures": 514,
    "crew connection tracker": 515,
    "indigo gd": 516,
    "crew resource dashboard": 517,
    "slot change request (scr)": 518,
}

EXTERNAL_MON: dict[str, int] = {
    "joc": 41,
    "navitar": 42,
    "ori": 43,
    "navblue": 44,
    "6eaviate": 45,
    "jeppson": 46,
    "proverne": 47,
    "hotac": 48,
    "notification": 49,
    "metar": 50,
    "wingops": 51,
}

SHARED_MON: dict[str, int] = {
    "aims mvt path": 52,
    "joc mvt path": 53,
}

# Job name substrings → monitor ID
JOB_MON: list[tuple[str, int]] = [
    ("gpu-scheduler", 54),
    ("gpu scheduler", 54),
    ("crew-duty-change", 55),
    ("mvt", 56),  # generatemvt / scheduler/process
    ("email/sir", 57),
    ("/scr", 58),
    ("flight-count", 59),
    ("refresh-subscription", 60),
    ("msubscription", 60),
]

MF_MON = 1


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def norm_path(s: str) -> str:
    s = norm(s)
    s = re.sub(r"\s*\((get|post|put|delete|upsert|sync alias)\)\s*$", "", s, flags=re.I)
    if s and not s.startswith("/") and ("api/" in s or s.startswith("flight/") or s.startswith("crew/") or s.startswith("admin/") or s.startswith("weather")):
        s = "/" + s
    return s


def fmt_mon(n: int) -> str:
    return f"M{n}"


def parse_mon(text: str) -> int | None:
    m = MON_RE.search(text or "")
    return int(m.group(1)) if m else None


def load_registry_json(base: Path | None = None) -> dict[str, dict[str, int]]:
    """Load component → M# maps from occ_monitoring_registry.json if present."""
    base = base or Path(__file__).resolve().parent
    path = base / "occ_monitoring_registry.json"
    if not path.exists():
        return {"internal": {}, "sp": {}, "kafka_producer": {}, "kafka_consumer": {}}
    data = json.loads(path.read_text(encoding="utf-8"))
    return {
        "internal": {norm_path(k): v for k, v in (data.get("internal") or {}).items()},
        "sp": {norm(k): v for k, v in (data.get("sp") or {}).items()},
        "kafka_producer": {norm(k): v for k, v in (data.get("kafka_producer") or {}).items()},
        "kafka_consumer": {norm(k): v for k, v in (data.get("kafka_consumer") or {}).items()},
    }


def lookup_job_mon(name: str) -> int | None:
    low = norm(name)
    for key, mid in JOB_MON:
        if key in low:
            # mvt job: prefer scheduler/process over other mvt strings
            if key == "mvt" and "scheduler/process" not in low and "scheduler" not in low:
                continue
            return mid
    if "scr" in low and "slot" not in low:
        return 58
    return None


def lookup_app_mon(name: str) -> int | None:
    return APP_MON.get(norm(name))


def lookup_external_mon(name: str) -> int | None:
    return EXTERNAL_MON.get(norm(name))


def lookup_shared_mon(name: str) -> int | None:
    return SHARED_MON.get(norm(name))


def lookup_ms_mon(service: str) -> int | None:
    key = norm(service).replace(" ", "")
    for k, v in MS_MON.items():
        if norm(k) == key or k.lower() == service.lower():
            return v
    return MS_MON.get(service) or MS_MON.get(service.lower())


def lookup_internal_mon(path: str, registry: dict[str, int]) -> int | None:
    p = norm_path(path)
    if p in registry:
        return registry[p]
    # try without leading slash variants
    alt = p.lstrip("/")
    if alt in registry:
        return registry[alt]
    return None


def lookup_sp_mon(name: str, registry: dict[str, int]) -> int | None:
    n = norm(name)
    if n in registry:
        return registry[n]
    # strip brackets for match
    bare = re.sub(r"[\[\]]", "", n)
    for k, v in registry.items():
        if norm(re.sub(r"[\[\]]", "", k)) == bare:
            return v
    return None


def lookup_kafka_mon(topic: str, registry: dict[str, int]) -> int | None:
    t = norm(topic).strip()
    if t in registry:
        return registry[t]
    return None
