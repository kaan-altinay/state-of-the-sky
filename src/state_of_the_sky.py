from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import duckdb

AIRBORNE_EVENTS = ("takeoff", "cruising", "descent", "diverting")
DEFAULT_DATA_GLOB = "data/*.csv"
DEFAULT_AIRCRAFT_JSON = "data/airplane_details.json"
DEFAULT_OUTPUT_DIR = "web/data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build hourly State of the Sky GeoJSON snapshots and stats."
    )
    parser.add_argument(
        "--data-glob",
        default=DEFAULT_DATA_GLOB,
        help="Glob for flight event CSV files (default: data/*.csv)",
    )
    parser.add_argument(
        "--aircraft-json",
        default=DEFAULT_AIRCRAFT_JSON,
        help="Path to airplane details NDJSON file (default: data/airplane_details.json)",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for generated artifacts (default: web/data)",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Single date to process (YYYY-MM-DD). If omitted, uses first date in data.",
    )
    parser.add_argument(
        "--all-days",
        action="store_true",
        help="Generate snapshots for all days in the dataset.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="How many top operators/equipment entries to keep per hour.",
    )
    args = parser.parse_args()

    if args.date and args.all_days:
        raise ValueError("Use either --date or --all-days, not both.")
    if args.top_n <= 0:
        raise ValueError("--top-n must be a positive integer.")

    return args


def resolve_path(project_root: Path, path_like: str) -> Path:
    path = Path(path_like)
    return path if path.is_absolute() else project_root / path


def sql_escape(path_like: Path) -> str:
    return str(path_like).replace("'", "''")


def normalize_date(date_str: str) -> date:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("--date must be in YYYY-MM-DD format") from exc


def fetch_rows(cursor: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    cols = [desc[0] for desc in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def setup_tables(con: duckdb.DuckDBPyConnection, data_glob: Path, aircraft_json: Path) -> None:
    csv_glob = sql_escape(data_glob)
    aircraft_path = sql_escape(aircraft_json)

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE flight_events AS
        SELECT
            try_cast(date AS DATE) AS flight_date,
            try_cast(time AS TIME) AS flight_time,
            try_cast(date || ' ' || time AS TIMESTAMP) AS flight_timestamp,
            try_cast(flight_id AS BIGINT) AS flight_id,
            nullif(trim(flight), '') AS flight,
            nullif(trim(callsign), '') AS callsign,
            nullif(trim(operator), '') AS operator,
            nullif(trim(origin_iata), '') AS origin_iata,
            nullif(trim(origin_icao), '') AS origin_icao,
            nullif(trim(destination_iata), '') AS destination_iata,
            nullif(trim(destination_icao), '') AS destination_icao,
            upper(nullif(trim(equipment), '')) AS equipment,
            nullif(trim(registration), '') AS registration,
            lower(nullif(trim(event), '')) AS event,
            try_cast(latitude AS DOUBLE) AS latitude,
            try_cast(longitude AS DOUBLE) AS longitude
        FROM read_csv_auto(
            '{csv_glob}',
            delim=';',
            union_by_name=true,
            header=true,
            filename=true
        )
        WHERE try_cast(date || ' ' || time AS TIMESTAMP) IS NOT NULL
          AND try_cast(date AS DATE) IS NOT NULL;
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE airplane_details AS
        SELECT
            upper(trim(code_icao)) AS code_icao,
            upper(trim(code_iata)) AS code_iata,
            full_name,
            category,
            payload::DOUBLE AS payload,
            volume::DOUBLE AS volume
        FROM read_json_auto('{aircraft_path}');
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE airplane_details_dedup AS
        SELECT
            code_icao,
            code_iata,
            full_name,
            category,
            payload,
            volume,
            duplicate_variants
        FROM (
            SELECT
                code_icao,
                code_iata,
                full_name,
                category,
                payload,
                volume,
                count(*) OVER (PARTITION BY code_icao) AS duplicate_variants,
                row_number() OVER (
                    PARTITION BY code_icao
                    ORDER BY coalesce(volume, -1) DESC, payload DESC, full_name
                ) AS rn
            FROM airplane_details
        ) t
        WHERE rn = 1;
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE flight_capacity AS
        SELECT
            f.flight_date,
            f.flight_time,
            f.flight_timestamp,
            f.flight_id,
            f.flight,
            f.callsign,
            f.operator,
            f.origin_iata,
            f.origin_icao,
            f.destination_iata,
            f.destination_icao,
            f.equipment,
            f.registration,
            f.event,
            f.latitude,
            f.longitude,
            a.full_name AS aircraft_full_name,
            a.category AS aircraft_category,
            a.payload AS available_capacity_weight,
            a.volume AS available_capacity_volume,
            CASE
                WHEN f.equipment IS NULL OR trim(f.equipment) = '' THEN 'missing_equipment'
                WHEN a.code_icao IS NULL THEN 'no_data_on_equipment'
                WHEN a.volume IS NULL THEN 'matched_no_volume'
                ELSE 'matched_full'
            END AS match_status,
            coalesce(cast(f.flight_id AS VARCHAR),
                coalesce(f.registration, 'UNKREG') || '::' ||
                coalesce(f.callsign, 'UNKCALL') || '::' ||
                cast(f.flight_date AS VARCHAR)
            ) AS aircraft_key
        FROM flight_events f
        LEFT JOIN airplane_details_dedup a
            ON f.equipment = a.code_icao;
        """
    )


def resolve_date_window(con: duckdb.DuckDBPyConnection, args: argparse.Namespace) -> tuple[date, date]:
    available = con.execute(
        """
        SELECT min(flight_date) AS min_date, max(flight_date) AS max_date
        FROM flight_capacity
        """
    ).fetchone()

    if available[0] is None or available[1] is None:
        raise RuntimeError("No valid event rows were loaded from the input CSV files.")

    min_date, max_date = available[0], available[1]

    if args.all_days:
        return min_date, max_date

    if args.date:
        selected = normalize_date(args.date)
    else:
        selected = min_date

    present = con.execute(
        """
        SELECT count(*)
        FROM flight_capacity
        WHERE flight_date = ?
        """,
        [selected],
    ).fetchone()[0]

    if present == 0:
        raise ValueError(
            f"No rows found for {selected}. Available range: {min_date} to {max_date}."
        )

    return selected, selected


def build_hour_sequence(start_day: date, end_day: date) -> list[datetime]:
    current = datetime.combine(start_day, time(hour=0, minute=0, second=0))
    end = datetime.combine(end_day, time(hour=23, minute=0, second=0))
    hours: list[datetime] = []

    while current <= end:
        hours.append(current)
        current += timedelta(hours=1)

    return hours


def create_latest_state_table(con: duckdb.DuckDBPyConnection, cutoff_exclusive: datetime) -> None:
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE latest_state_hour AS
        WITH ranked AS (
            SELECT
                aircraft_key,
                flight_id,
                flight,
                callsign,
                operator,
                origin_iata,
                origin_icao,
                destination_iata,
                destination_icao,
                equipment,
                registration,
                event,
                latitude,
                longitude,
                flight_timestamp,
                available_capacity_weight,
                available_capacity_volume,
                aircraft_full_name,
                aircraft_category,
                match_status,
                row_number() OVER (
                    PARTITION BY aircraft_key
                    ORDER BY flight_timestamp DESC
                ) AS rn
            FROM flight_capacity
            WHERE flight_timestamp < ?
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
        )
        SELECT
            aircraft_key,
            flight_id,
            flight,
            callsign,
            operator,
            origin_iata,
            origin_icao,
            destination_iata,
            destination_icao,
            equipment,
            registration,
            event,
            latitude,
            longitude,
            flight_timestamp,
            available_capacity_weight,
            available_capacity_volume,
            aircraft_full_name,
            aircraft_category,
            match_status
        FROM ranked
        WHERE rn = 1;
        """,
        [cutoff_exclusive],
    )


def rows_to_geojson(rows: list[dict[str, Any]]) -> dict[str, Any]:
    features: list[dict[str, Any]] = []

    for row in rows:
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [row["longitude"], row["latitude"]],
                },
                "properties": {
                    "aircraft_key": row["aircraft_key"],
                    "flight_id": row["flight_id"],
                    "flight": row["flight"],
                    "callsign": row["callsign"],
                    "operator": row["operator"],
                    "origin_iata": row["origin_iata"],
                    "destination_iata": row["destination_iata"],
                    "equipment": row["equipment"],
                    "registration": row["registration"],
                    "event": row["event"],
                    "event_timestamp": row["flight_timestamp"].isoformat()
                    if row["flight_timestamp"]
                    else None,
                    "aircraft_full_name": row["aircraft_full_name"],
                    "aircraft_category": row["aircraft_category"],
                    "available_capacity_weight": row["available_capacity_weight"],
                    "available_capacity_volume": row["available_capacity_volume"],
                    "match_status": row["match_status"],
                },
            }
        )

    return {"type": "FeatureCollection", "features": features}


def compute_hour_stats(
    con: duckdb.DuckDBPyConnection,
    hour_start: datetime,
    hour_end: datetime,
    top_n: int,
    snapshot_rel_path: str,
) -> dict[str, Any]:
    airborne_list_sql = ", ".join([f"'{event}'" for event in AIRBORNE_EVENTS])

    airborne_weight = con.execute(
        f"""
        SELECT coalesce(sum(available_capacity_weight), 0)
        FROM latest_state_hour
        WHERE event IN ({airborne_list_sql})
          AND available_capacity_weight IS NOT NULL;
        """
    ).fetchone()[0]

    takeoffs_this_hour = con.execute(
        """
        SELECT count(*)
        FROM flight_capacity
        WHERE event = 'takeoff'
          AND flight_timestamp >= ?
          AND flight_timestamp < ?;
        """,
        [hour_start, hour_end],
    ).fetchone()[0]

    landings_this_hour = con.execute(
        """
        SELECT count(*)
        FROM flight_capacity
        WHERE event = 'landed'
          AND flight_timestamp >= ?
          AND flight_timestamp < ?;
        """,
        [hour_start, hour_end],
    ).fetchone()[0]

    active_aircraft = con.execute(
        """
        SELECT count(*)
        FROM latest_state_hour;
        """
    ).fetchone()[0]

    event_counts = fetch_rows(
        con.execute(
            """
            SELECT
                event,
                count(*) AS aircraft
            FROM latest_state_hour
            GROUP BY event
            ORDER BY aircraft DESC;
            """
        )
    )

    top_operators = fetch_rows(
        con.execute(
            f"""
            SELECT
                coalesce(operator, 'UNKNOWN') AS operator,
                sum(available_capacity_weight) AS capacity_weight,
                count(*) AS aircraft
            FROM latest_state_hour
            WHERE event IN ({airborne_list_sql})
              AND available_capacity_weight IS NOT NULL
            GROUP BY 1
            ORDER BY capacity_weight DESC
            LIMIT ?;
            """,
            [top_n],
        )
    )

    top_equipment = fetch_rows(
        con.execute(
            f"""
            SELECT
                coalesce(equipment, 'UNKNOWN') AS equipment,
                sum(available_capacity_weight) AS capacity_weight,
                count(*) AS aircraft
            FROM latest_state_hour
            WHERE event IN ({airborne_list_sql})
              AND available_capacity_weight IS NOT NULL
            GROUP BY 1
            ORDER BY capacity_weight DESC
            LIMIT ?;
            """,
            [top_n],
        )
    )

    return {
        "hour_start": hour_start.isoformat(),
        "hour_end_exclusive": hour_end.isoformat(),
        "snapshot": snapshot_rel_path,
        "active_aircraft": int(active_aircraft),
        "airborne_capacity_weight": float(airborne_weight),
        "airborne_capacity_tonnes": round(float(airborne_weight) / 1000.0, 3),
        "takeoffs_this_hour": int(takeoffs_this_hour),
        "landings_this_hour": int(landings_this_hour),
        "event_counts": event_counts,
        "top_operators": top_operators,
        "top_equipment": top_equipment,
    }


def clean_snapshot_dir(snapshot_dir: Path) -> None:
    for file in snapshot_dir.glob("*.geojson"):
        file.unlink()


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parent.parent

    data_glob = resolve_path(project_root, args.data_glob)
    aircraft_json = resolve_path(project_root, args.aircraft_json)
    output_dir = resolve_path(project_root, args.output_dir)
    snapshots_dir = output_dir / "snapshots"

    output_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    clean_snapshot_dir(snapshots_dir)

    con = duckdb.connect()
    setup_tables(con, data_glob, aircraft_json)

    start_day, end_day = resolve_date_window(con, args)
    hours = build_hour_sequence(start_day, end_day)

    hourly_stats: list[dict[str, Any]] = []

    for hour_start in hours:
        hour_end = hour_start + timedelta(hours=1)
        create_latest_state_table(con, hour_end)

        snapshot_name = f"{hour_start.strftime('%Y-%m-%dT%H-%M-%S')}.geojson"
        snapshot_path = snapshots_dir / snapshot_name
        snapshot_rel_path = f"snapshots/{snapshot_name}"

        latest_rows = fetch_rows(
            con.execute(
                """
                SELECT
                    aircraft_key,
                    flight_id,
                    flight,
                    callsign,
                    operator,
                    origin_iata,
                    origin_icao,
                    destination_iata,
                    destination_icao,
                    equipment,
                    registration,
                    event,
                    latitude,
                    longitude,
                    flight_timestamp,
                    aircraft_full_name,
                    aircraft_category,
                    available_capacity_weight,
                    available_capacity_volume,
                    match_status
                FROM latest_state_hour;
                """
            )
        )

        geojson = rows_to_geojson(latest_rows)
        snapshot_path.write_text(json.dumps(geojson, separators=(",", ":")), encoding="utf-8")

        stats_row = compute_hour_stats(
            con,
            hour_start=hour_start,
            hour_end=hour_end,
            top_n=args.top_n,
            snapshot_rel_path=snapshot_rel_path,
        )
        hourly_stats.append(stats_row)

    payload = {
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "date_window": {
            "start": start_day.isoformat(),
            "end": end_day.isoformat(),
        },
        "airborne_events": list(AIRBORNE_EVENTS),
        "hour_count": len(hours),
        "hours": hourly_stats,
    }

    (output_dir / "hourly_stats.json").write_text(
        json.dumps(payload, separators=(",", ":"), indent=2),
        encoding="utf-8",
    )

    print(
        f"Generated {len(hours)} hourly snapshots and hourly_stats.json in {output_dir}"
    )


if __name__ == "__main__":
    main()
