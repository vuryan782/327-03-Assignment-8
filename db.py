from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras


# Prefer an environment variable for safety:
# Windows PowerShell example:
#   $env:DATABASE_URL="postgresql://user:password@host/neondb?sslmode=require&channel_binding=require"
#
# Fallback is left blank on purpose so you fill it in if needed.
DATABASE_URL = os.getenv("DATABASE_URL", "")


def connect_db():
    """
    Create and return a PostgreSQL connection.
    """
    if not DATABASE_URL:
        raise ValueError(
            "DATABASE_URL is not set. Add it as an environment variable or "
            "paste your Neon connection string into db.py."
        )

    return psycopg2.connect(DATABASE_URL)


def run_query(query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """
    Execute a SELECT query and return rows as dictionaries.
    """
    conn = connect_db()
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params)
                return list(cur.fetchall())
    finally:
        conn.close()


def run_scalar(query: str, params: Optional[tuple] = None) -> Any:
    """
    Execute a query that returns a single value.
    """
    conn = connect_db()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
                return row[0] if row else None
    finally:
        conn.close()


def get_db_time() -> Any:
    """
    Quick connection test.
    """
    return run_scalar("SELECT NOW();")


def get_total_row_count() -> int:
    """
    Return total number of raw IoT rows.
    """
    query = "SELECT COUNT(*) FROM metadata_virtual;"
    result = run_scalar(query)
    return int(result or 0)


def get_recent_sensor_rows(hours: int = 24) -> List[Dict[str, Any]]:
    """
    Return recent raw rows from metadata_virtual.
    Useful for debugging.
    """
    query = """
        SELECT id, topic, time, payload
        FROM metadata_virtual
        WHERE time >= NOW() - (%s || ' hours')::interval
        ORDER BY time DESC;
    """
    return run_query(query, (hours,))


def get_moisture_readings(hours: int) -> List[Dict[str, Any]]:
    """
    Get moisture readings from the last N hours.
    """
    query = """
        SELECT
            id,
            time,
            topic,
            (payload->>'Moisture Meter - moisture_level')::numeric AS moisture_level
        FROM metadata_virtual
        WHERE time >= NOW() - (%s || ' hours')::interval
          AND payload ? 'Moisture Meter - moisture_level'
        ORDER BY time;
    """
    return run_query(query, (hours,))


def get_temperature_readings(hours: int) -> List[Dict[str, Any]]:
    """
    Get fridge temperature readings from the last N hours.
    """
    query = """
        SELECT
            id,
            time,
            topic,
            (payload->>'fridge_temp')::numeric AS fridge_temp
        FROM metadata_virtual
        WHERE time >= NOW() - (%s || ' hours')::interval
          AND payload ? 'fridge_temp'
        ORDER BY time;
    """
    return run_query(query, (hours,))


def get_current_usage_readings(hours: int) -> List[Dict[str, Any]]:
    """
    Get current/electricity usage readings from the last N hours.
    """
    query = """
        SELECT
            id,
            time,
            topic,
            (payload->>'current_usage')::numeric AS current_usage
        FROM metadata_virtual
        WHERE time >= NOW() - (%s || ' hours')::interval
          AND payload ? 'current_usage'
        ORDER BY time;
    """
    return run_query(query, (hours,))


def get_average_moisture(hours: int) -> Optional[float]:
    """
    Return average fridge moisture for the last N hours.
    """
    query = """
        SELECT
            AVG((payload->>'Moisture Meter - moisture_level')::numeric) AS avg_moisture
        FROM metadata_virtual
        WHERE time >= NOW() - (%s || ' hours')::interval
          AND payload ? 'Moisture Meter - moisture_level';
    """
    result = run_scalar(query, (hours,))
    return float(result) if result is not None else None


def get_average_electricity(hours: int) -> Optional[float]:
    """
    Return average current usage for the last N hours.
    """
    query = """
        SELECT
            AVG((payload->>'current_usage')::numeric) AS avg_current
        FROM metadata_virtual
        WHERE time >= NOW() - (%s || ' hours')::interval
          AND payload ? 'current_usage';
    """
    result = run_scalar(query, (hours,))
    return float(result) if result is not None else None


def get_total_electricity_by_topic(hours: int) -> List[Dict[str, Any]]:
    """
    Group electricity usage by MQTT topic over the last N hours.
    This is a starting point for separating houses/devices.
    """
    query = """
        SELECT
            topic,
            COUNT(*) AS reading_count,
            SUM((payload->>'current_usage')::numeric) AS total_current_usage,
            AVG((payload->>'current_usage')::numeric) AS avg_current_usage
        FROM metadata_virtual
        WHERE time >= NOW() - (%s || ' hours')::interval
          AND payload ? 'current_usage'
        GROUP BY topic
        ORDER BY topic;
    """
    return run_query(query, (hours,))


def get_schema_info(table_name: str = "metadata_virtual") -> List[Dict[str, Any]]:
    """
    Show table schema for debugging/reporting.
    """
    query = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position;
    """
    return run_query(query, (table_name,))


if __name__ == "__main__":
    print("DB time:", get_db_time())
    print("Total rows:", get_total_row_count())
    print("Average moisture (24h):", get_average_moisture(24))
    print("Average electricity (24h):", get_average_electricity(24))
    print("Schema:", get_schema_info())
