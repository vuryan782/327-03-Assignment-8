# CECS 327 Assignment 8
# Database helper functions for distributed IoT query processing.

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
import psycopg2
import psycopg2.extras


load_dotenv(".env.local")

LOCAL_DATABASE_URL = os.getenv("LOCAL_DATABASE_URL") or os.getenv("DATABASE_URL", "")
PARTNER_DATABASE_URL = os.getenv("PARTNER_DATABASE_URL", "")

HOUSE_A_NAME = os.getenv("HOUSE_A_NAME", "House A")
HOUSE_B_NAME = os.getenv("HOUSE_B_NAME", "House B")

HOUSE_A_TOPICS = [t.strip() for t in os.getenv("HOUSE_A_TOPICS", "").split(",") if t.strip()]
HOUSE_B_TOPICS = [t.strip() for t in os.getenv("HOUSE_B_TOPICS", "").split(",") if t.strip()]

SHARING_START_UTC = os.getenv("SHARING_START_UTC", "")

PST = ZoneInfo("America/Los_Angeles")
UTC = timezone.utc

MOISTURE_KEYS = [
    "Moisture Meter - moisture_level",
    "moisture_level",
    "moisture",
    "humidity",
    "Moisture Meter - smart-fridge-1-moisture-sensor",
    "Moisture Meter - smart-fridge-2-moisture-sensor",
]

CURRENT_KEYS = [
    "current_usage",
    "Current Sensor - current_usage",
    "ammeter",
    "current",
    "smart-dishwasher-ammeter",
    "smart-fridge-1-ammeter",
    "smart-fridge-2-ammeter",
]

WATER_KEYS = [
    "water_usage",
    "water_consumption",
    "Water Consumption - water_usage",
    "Water Consumption Sensor - water_usage",
    "flow_sensor",
    "water_per_cycle",
    "smart-dishwasher-water-sensor",
]


@dataclass
class SensorReading:
    house: str
    source_db: str
    topic: str
    time_utc: datetime
    value: float
    payload: Dict[str, Any]


def _parse_iso_utc(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except ValueError:
        return None


def _now_utc() -> datetime:
    return datetime.now(UTC)


def format_pst(dt: datetime) -> str:
    return dt.astimezone(PST).strftime("%Y-%m-%d %I:%M:%S %p PST")


def connect_db(database_url: Optional[str] = None):
    url = database_url or LOCAL_DATABASE_URL
    if not url:
        raise ValueError(
            "No database URL set. Set LOCAL_DATABASE_URL or DATABASE_URL in your .env.local file."
        )
    return psycopg2.connect(url)


def _payload_to_dict(payload: Any) -> Dict[str, Any]:
    if payload is None:
        return {}
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return {}
    return dict(payload)


def _extract_numeric(payload: Dict[str, Any], keys: Iterable[str]) -> Optional[float]:
    for key in keys:
        if key in payload and payload[key] not in (None, ""):
            try:
                return float(payload[key])
            except (TypeError, ValueError):
                continue
    return None


def _query_raw_rows(
    database_url: str,
    start_utc: datetime,
    end_utc: datetime,
    topics: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    sql = """
        SELECT id, topic, time, payload
        FROM metadata_virtual
        WHERE time >= %s
          AND time < %s
    """
    params: List[Any] = [start_utc, end_utc]

    if topics:
        sql += " AND topic = ANY(%s)"
        params.append(topics)

    sql += " ORDER BY time;"

    conn = connect_db(database_url)
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, tuple(params))
                return list(cur.fetchall())
    finally:
        conn.close()


def _rows_to_readings(
    rows: List[Dict[str, Any]],
    keys: Iterable[str],
    house: str,
    source_db: str,
) -> List[SensorReading]:
    readings: List[SensorReading] = []

    for row in rows:
        payload = _payload_to_dict(row.get("payload"))
        value = _extract_numeric(payload, keys)

        if value is None:
            continue

        row_time = row.get("time")
        if row_time is None:
            continue
        if row_time.tzinfo is None:
            row_time = row_time.replace(tzinfo=UTC)

        readings.append(
            SensorReading(
                house=house,
                source_db=source_db,
                topic=row.get("topic", ""),
                time_utc=row_time.astimezone(UTC),
                value=value,
                payload=payload,
            )
        )

    return readings


def _window(hours: int) -> tuple[datetime, datetime]:
    end_utc = _now_utc()
    start_utc = end_utc - timedelta(hours=hours)
    return start_utc, end_utc


def _sharing_start() -> Optional[datetime]:
    return _parse_iso_utc(SHARING_START_UTC)


def get_query_coverage_note(hours: int) -> str:
    start_utc, _ = _window(hours)
    sharing_start = _sharing_start()

    if not sharing_start:
        return "Sharing start time is not configured; completeness check used available configured sources."

    if start_utc >= sharing_start:
        return "Requested window is after sharing began, so local replicated data should cover shared peer data."

    if PARTNER_DATABASE_URL:
        return "Requested window overlaps pre-sharing time, so partner database is queried for missing historical peer data."

    return "Requested window overlaps pre-sharing time, but PARTNER_DATABASE_URL is not configured; peer history may be incomplete."


def _get_team_readings(keys: Iterable[str], hours: int) -> List[SensorReading]:
    start_utc, end_utc = _window(hours)
    sharing_start = _sharing_start()
    readings: List[SensorReading] = []

    local_a_rows = _query_raw_rows(
        LOCAL_DATABASE_URL,
        start_utc,
        end_utc,
        HOUSE_A_TOPICS or None,
    )
    readings.extend(_rows_to_readings(local_a_rows, keys, HOUSE_A_NAME, "local"))

    if HOUSE_B_TOPICS:
        local_b_rows = _query_raw_rows(
            LOCAL_DATABASE_URL,
            start_utc,
            end_utc,
            HOUSE_B_TOPICS,
        )
        readings.extend(_rows_to_readings(local_b_rows, keys, HOUSE_B_NAME, "local-replicated"))

    if PARTNER_DATABASE_URL:
        if sharing_start and start_utc < sharing_start:
            partner_end = min(end_utc, sharing_start)
            if start_utc < partner_end:
                partner_rows = _query_raw_rows(
                    PARTNER_DATABASE_URL,
                    start_utc,
                    partner_end,
                    HOUSE_B_TOPICS or None,
                )
                readings.extend(_rows_to_readings(partner_rows, keys, HOUSE_B_NAME, "partner-original"))
        elif not HOUSE_B_TOPICS:
            partner_rows = _query_raw_rows(
                PARTNER_DATABASE_URL,
                start_utc,
                end_utc,
                None,
            )
            readings.extend(_rows_to_readings(partner_rows, keys, HOUSE_B_NAME, "partner-original"))

    return readings


def _average(values: Iterable[float]) -> Optional[float]:
    vals = list(values)
    if not vals:
        return None
    return sum(vals) / len(vals)


def get_average_moisture(hours: int) -> Optional[float]:
    readings = _get_team_readings(MOISTURE_KEYS, hours)
    return _average(r.value for r in readings)


def get_average_water_consumption(hours: int) -> Optional[float]:
    readings = _get_team_readings(WATER_KEYS, hours)
    return _average(r.value for r in readings)


def get_house_electricity_totals_24h() -> List[Dict[str, Any]]:
    readings = _get_team_readings(CURRENT_KEYS, 24)

    totals: Dict[str, Dict[str, Any]] = {
        HOUSE_A_NAME: {"house": HOUSE_A_NAME, "total_current_usage": 0.0, "reading_count": 0},
        HOUSE_B_NAME: {"house": HOUSE_B_NAME, "total_current_usage": 0.0, "reading_count": 0},
    }

    for reading in readings:
        totals.setdefault(
            reading.house,
            {"house": reading.house, "total_current_usage": 0.0, "reading_count": 0},
        )
        totals[reading.house]["total_current_usage"] += reading.value
        totals[reading.house]["reading_count"] += 1

    return list(totals.values())


def get_db_time() -> Any:
    conn = connect_db(LOCAL_DATABASE_URL)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT NOW();")
                return cur.fetchone()[0]
    finally:
        conn.close()


def get_total_row_count() -> int:
    conn = connect_db(LOCAL_DATABASE_URL)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM metadata_virtual;")
                return int(cur.fetchone()[0])
    finally:
        conn.close()


if __name__ == "__main__":
    print("DB time:", get_db_time())
    print("Total local rows:", get_total_row_count())
    print("Moisture avg 1h:", get_average_moisture(1))
    print("Water avg 1h:", get_average_water_consumption(1))
    print("Electricity 24h:", get_house_electricity_totals_24h())
    print("Coverage note 24h:", get_query_coverage_note(24))
