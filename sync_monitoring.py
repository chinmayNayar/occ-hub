#!/usr/bin/env python3
"""Apply authoritative monitoring IDs to occ_data.json and build occ_monitoring_registry.json."""

from __future__ import annotations

import json
import re
from pathlib import Path

from monitoring_registry import (
    APP_MON,
    DB_MON,
    EXTERNAL_MON,
    MF_MON,
    MS_MON,
    SHARED_MON,
    fmt_mon,
    load_registry_json,
    lookup_app_mon,
    lookup_external_mon,
    lookup_internal_mon,
    lookup_job_mon,
    lookup_kafka_mon,
    lookup_ms_mon,
    lookup_shared_mon,
    lookup_sp_mon,
    norm,
    norm_path,
    parse_mon,
)

BASE = Path(__file__).resolve().parent
DATA_PATH = BASE / "occ_data.json"
REGISTRY_PATH = BASE / "occ_monitoring_registry.json"

# Kafka producer M61–M100 (authoritative order)
KAFKA_PRODUCER = [
    "prod.crd.open-sim.v1.json",
    "prod.crd.logistics-summary.v1.json",
    "prod.crd.pending-notifications-base.v1.error.json",
    "prod.crd.communication-summary.v1.error.json",
    "prod.crd.unpaired-legs.v1.json",
    "prod.crd.unpaired-legs-base.v1.error.json",
    "prod.crd.sby-position.v1.json",
    "prod.crd.unpaired-legs.v1.error.json",
    "prod.crd.open-trips-position.v1.json",
    "prod.crd.na-percentage.v1.error.json",
    "prod.crd.logistics-summary.v1.error.json",
    "prod.crd.unfinalized-routes.v1.json",
    "prod.crd.open-trips-main-details.v1.error.json",
    "prod.crd.sby-position.v1.error.json",
    "prod.crd.crew-rating-alerts.v1.error.json",
    "prod.crd.crew-fdtl-summary.v1.error.json",
    "prod.crd.crew-rating-alerts.v1.json",
    "prod.crd.open-trips-position.v1.error.json",
    "prod.crd.na-percentage-position.v1.json",
    "prod.crd.pending-notifications-base.v1.json",
    "prod.crd.communication-summary.v1.json",
    "prod.crd.unfinalized-routes-base.v1.error.json",
    "prod.crd.unfinalized-routes-base.v1.json",
    "prod.crd.sby-main.v1.error.json",
    "prod.crd.open-trips-main.v1.error.json",
    "prod.crd.open-trips-main.v1.json",
    "prod.crd.unpaired-legs-base.v1.json",
    "prod.crd.open-sim.v1.error.json",
    "prod.crd.na-percentage.v1.json",
    "prod.crd.unfinalized-routes.v1.error.json",
    "prod.crd.graph-flights.v1.error.json",
    "prod.crd.open-trips-main-details.v1.json",
    "prod.crd.sby-main.v1.json",
    "prod.crd.graph-flights.v1.json",
    "prod.crd.crew-fdtl-summary.v1.json",
    "prod.crd.na-percentage-position.v1.error.json",
    "prod.crd.pending-notifications.v1.error.json",
    "prod.crd.pending-notifications.v1.json",
    "prod.crd.pending-notifications-main-details.v1.json",
    "prod.crd.pending-notifications-main-details.v1.error.json",
]

# Kafka consumer M101–M158
KAFKA_CONSUMER = [
    "flight.cancel.event.json",
    "flight.equipment.event.json",
    "flight.ops.actual.in-block-time.event.json",
    "flight.ops.actual.off-block-time.event.json",
    "flight.ops.actual.landing-time.event.json",
    "flight.ops.actual.take-off-time.event.json",
    "flight.ops.estimated.in-block-time.event.json",
    "flight.ops.estimated.off-block-time.event.json",
    "flight.ops.estimated.landing-time.event.json",
    "flight.ops.estimated.take-off-time.event.json",
    "flight.delay.event.json",
    "flight.diversion.event.json",
    "flight.return.event.json",
    "flight.gate-return.event.json",
    "flight.new.event.json",
    "prd.cms.roster.update.v1.json",
    "prd.cms.roster.insert.v1.json",
    "prd.cms.roster.delete.v1.json",
    "prod.crd.open-sim.v1.json",
    "prod.crd.logistics-summary.v1.json",
    "prod.crd.pending-notifications-base.v1.error.json",
    "prod.crd.communication-summary.v1.error.json",
    "prod.crd.unpaired-legs.v1.json",
    "prod.crd.unpaired-legs-base.v1.error.json",
    "prod.crd.sby-position.v1.json",
    "prod.crd.unpaired-legs.v1.error.json",
    "prod.crd.open-trips-position.v1.json",
    "prod.crd.na-percentage.v1.error.json",
    "prod.crd.logistics-summary.v1.error.json",
    "prod.crd.unfinalized-routes.v1.json",
    "prod.crd.open-trips-main-details.v1.error.json",
    "prod.crd.sby-position.v1.error.json",
    "prod.crd.crew-rating-alerts.v1.error.json",
    "prod.crd.crew-fdtl-summary.v1.error.json",
    "prod.crd.crew-rating-alerts.v1.json",
    "prod.crd.open-trips-position.v1.error.json",
    "prod.crd.na-percentage-position.v1.json",
    "prod.crd.pending-notifications-base.v1.json",
    "prod.crd.communication-summary.v1.json",
    "prod.crd.unfinalized-routes-base.v1.error.json",
    "prod.crd.unfinalized-routes-base.v1.json",
    "prod.crd.sby-main.v1.error.json",
    "prod.crd.open-trips-main.v1.error.json",
    "prod.crd.open-trips-main.v1.json",
    "prod.crd.unpaired-legs-base.v1.json",
    "prod.crd.open-sim.v1.error.json",
    "prod.crd.na-percentage.v1.json",
    "prod.crd.unfinalized-routes.v1.error.json",
    "prod.crd.graph-flights.v1.error.json",
    "prod.crd.open-trips-main-details.v1.json",
    "prod.crd.sby-main.v1.json",
    "prod.crd.graph-flights.v1.json",
    "prod.crd.crew-fdtl-summary.v1.json",
    "prod.crd.na-percentage-position.v1.error.json",
    "prod.crd.pending-notifications.v1.error.json",
    "prod.crd.pending-notifications.v1.json",
    "prod.crd.pending-notifications-main-details.v1.json",
    "prod.crd.pending-notifications-main-details.v1.error.json",
]

# Stored procedures M15–M40
SP_NAMES = [
    "[CRA].[RosterAutomation_GetCrewEmployment]",
    "[CRA].[RosterAutomation_GetCrewCategories]",
    "[CRA].[RosterAutomation_GetFlightDetailsForSSIM]",
    "[CRA].[RosterAutomation_GetCrewDetails]",
    "[CRA].[RosterAutomation_GetCrewRoster]",
    "[CRA].[RosterAutomation_GetCrewRoutes]",
    "[CRA].[RosterAutomation_GetCrewRulesAndPreferences]",
    "[CRA].[RosterAutomation_GetCrewExpiryDetails]",
    "[CRA].[RosterAutomation_usp_GetCrewSimGroundExpiry]",
    "[CRA].[RosterAutomation_elearningexpirytxt]",
    "[CRA].[RosterAutomation_GetSimAndGroundSlots]",
    "CRD_GetPublishedCrewDutiesWithFlt_For_API",
    "CRD_SP_GetActiveACQualifications",
    "dbo.CRD_SP_CrewDashboardOpenSim2_For_API",
    "dbo.CRD_SP_GetCrewRouteDiscrepancies_V10_For_API",
    "CRD_SP_ValidateCrewComposition_For_API",
    "CRD_SP_GetFlightScheduleByISTTimeRange_For_API",
    "CRD_SP_DashboardPendingNotificationsV3_For_API",
    "dbo.CRD_SP_CrewOpsDashboardSBYAvailableV2_For_API",
    "CRD_SP_GetCrewExpectedCounts_Unfinalised_For_API",
    "CRD_SP_GetFirstFlightStartTime_unfinalizedRoutes_For_API",
    "CRD_SP_GetMissingFlightsUnpaired_For_API",
    "CRD_SP_ValidateCrewCompositionForUnpairedLegs_For_API",
    "CRD_SP_GetCrewExpectedCounts_For_API",
    "CRD_SP_GetCrewExpectedCountsv2_For_API",
    "CRD_SP_GetFlightDescriptionCroutes_For_API",
]


def build_kafka_registry() -> dict[str, dict[str, int]]:
    producer: dict[str, int] = {}
    consumer: dict[str, int] = {}
    for i, topic in enumerate(KAFKA_PRODUCER):
        producer[norm(topic)] = 61 + i
    for i, topic in enumerate(KAFKA_CONSUMER):
        consumer[norm(topic)] = 101 + i
    return {"kafka_producer": producer, "kafka_consumer": consumer}


def build_sp_registry() -> dict[str, int]:
    return {norm(n): 15 + i for i, n in enumerate(SP_NAMES)}


def path_aliases(path: str) -> list[str]:
    p = norm_path(path)
    out = [p, p.lstrip("/")]
    if "/crew/api/" in p:
        out.append(p.replace("/crew/api/", "/crewdashboard/api/"))
    if "/crewdashboard/api/" in p:
        out.append(p.replace("/crewdashboard/api/", "/crew/api/"))
    if "/weatherapi/" in p:
        out.append(p.replace("/weatherapi/", "/weather/api/"))
    if "/weather/api/" in p:
        out.append(p.replace("/weather/api/", "/weatherapi/"))
    return list(dict.fromkeys(out))


def lookup_internal(path: str, registry: dict[str, int]) -> int | None:
    for alias in path_aliases(path):
        if alias in registry:
            return registry[alias]
    return lookup_internal_mon(path, registry)


def build_internal_registry(data: dict) -> dict[str, int]:
    """Build internal path registry from occ_data mon tags (draw.io source) cross-checked with sheet order."""
    reg: dict[str, int] = {}
    for item in data.get("groups", {}).get("internal", []):
        mon = parse_mon(item.get("mon", ""))
        if mon is None:
            continue
        name = item.get("name", "")
        reg[norm_path(name)] = mon
    return reg


def apply_to_data(data: dict, reg: dict) -> dict[str, int]:
    stats = {"matched": 0, "missed": 0}
    groups = data.setdefault("groups", {})

    def set_mon(item: dict, mid: int | None) -> None:
        if mid is not None:
            item["mon"] = fmt_mon(mid)
            stats["matched"] += 1
        else:
            stats["missed"] += 1

    for item in groups.get("sp", []):
        mid = lookup_sp_mon(item.get("name", ""), reg["sp"]) or parse_mon(item.get("mon", ""))
        set_mon(item, mid)

    for item in groups.get("external", []):
        set_mon(item, lookup_external_mon(item.get("name", "")))

    for item in groups.get("shared_path", []):
        set_mon(item, lookup_shared_mon(item.get("name", "")))

    for item in groups.get("jobs", []):
        set_mon(item, lookup_job_mon(item.get("name", "")))

    for item in groups.get("kafka_producer", []):
        set_mon(item, lookup_kafka_mon(item.get("name", ""), reg["kafka_producer"]))

    for item in groups.get("kafka_consumer", []):
        set_mon(item, lookup_kafka_mon(item.get("name", ""), reg["kafka_consumer"]))

    for item in groups.get("redis", []):
        item["mon"] = fmt_mon(159)

    for sid, mid in DB_MON.items():
        for item in groups.get(sid, []):
            item["mon"] = fmt_mon(mid)

    for item in groups.get("internal", []):
        mid = lookup_internal(item.get("name", ""), reg["internal"])
        if mid is None:
            mid = parse_mon(item.get("mon", ""))
        set_mon(item, mid)

    for app in data.get("apps", []):
        mid = lookup_app_mon(app.get("name", ""))
        if mid:
            app["mon"] = fmt_mon(mid)

    # Canonical microservice order (M5 weather before M6 dispatch)
    ms_order = [
        "occhub-admin-ms",
        "occhub-flight-ms",
        "occhub-crew-ms",
        "occhub-weather-ms",
        "occhub-dispatch-ms",
        "occhub-externalcomunication-ms",
        "occhub-crewresoucedashboard-ms",
        "occhub-rosterautomation-ms",
        "occhub-streamorchestor-ms",
        "occhub-ingesthub-ms",
        "occhub-eventConsumer-service-ms",
    ]
    data["microservices"] = ms_order

    return stats


def write_registry_file(reg: dict) -> None:
    out = {
        "internal": reg["internal"],
        "sp": reg["sp"],
        "kafka_producer": reg["kafka_producer"],
        "kafka_consumer": reg["kafka_consumer"],
    }
    REGISTRY_PATH.write_text(json.dumps(out, indent=2, sort_keys=True), encoding="utf-8")


def main() -> None:
    data = json.loads(DATA_PATH.read_text(encoding="utf-8"))
    reg = load_registry_json(BASE)
    if not reg["internal"]:
        reg["internal"] = build_internal_registry(data)
    kafka_regs = build_kafka_registry()
    reg["kafka_producer"] = kafka_regs["kafka_producer"]
    reg["kafka_consumer"] = kafka_regs["kafka_consumer"]
    if not reg["sp"]:
        reg["sp"] = build_sp_registry()
    stats = apply_to_data(data, reg)
    write_registry_file(reg)
    DATA_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"[ok] updated {DATA_PATH}")
    print(f"[ok] wrote {REGISTRY_PATH}")
    print(f"[i]  matched={stats['matched']} missed={stats['missed']}")
    print(f"[i]  internal={len(reg['internal'])} sp={len(reg['sp'])} kafka_prod={len(reg['kafka_producer'])} kafka_cons={len(reg['kafka_consumer'])}")


if __name__ == "__main__":
    main()
