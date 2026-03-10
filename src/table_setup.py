import duckdb

con = duckdb.connect("db/flight_events.duckdb")

# Question 1: Create flight_events table in DuckDB using CSVs.
fe_table_exists = con.execute("""
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = 'main'
      AND table_name = 'flight_events'
""").fetchone()[0] > 0

if not fe_table_exists:
    con.execute("""
    CREATE OR REPLACE TABLE flight_events AS
    SELECT
        address,
        altitude::INTEGER AS altitude,
        callsign,
        date::DATE AS flight_date,
        destination_iata,
        destination_icao,
        equipment,
        event,
        flight,
        flight_id::BIGINT AS flight_id,
        latitude::DOUBLE AS latitude,
        longitude::DOUBLE AS longitude,
        operator,
        origin_iata,
        origin_icao,
        registration,
        time::TIME AS flight_time,
        CAST(date || ' ' || time AS TIMESTAMP) AS flight_timestamp,
        filename
    FROM read_csv_auto(
        'data/*.csv',
        delim=';',
        union_by_name=true,
        filename=true,
        header=true
    );
    """)

flight_table_exists = con.execute("""
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = 'main'
      AND table_name = 'flights'
""").fetchone()[0] > 0

if not flight_table_exists:
    con.execute("""
    CREATE OR REPLACE TABLE flights AS
    SELECT
        flight_id,
        flight_date,
        flight,
        callsign,
        operator,
        origin_iata,
        origin_icao,
        destination_iata,
        destination_icao,
        equipment,
        registration,
        flight_timestamp AS first_seen_timestamp
    FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY flight_id
                ORDER BY flight_timestamp
            ) AS rn
        FROM flight_events
    ) t
    WHERE rn = 1;
    """)

# Build aircraft reference table from NDJSON and dedupe non-unique equipment codes.
con.execute("""
CREATE OR REPLACE TABLE airplane_details AS
SELECT
    upper(trim(code_iata)) AS code_iata,
    upper(trim(code_icao)) AS code_icao,
    full_name,
    category,
    payload::DOUBLE AS payload,
    volume::DOUBLE AS volume
FROM read_json_auto('data/airplane_details.json');
""")

con.execute("""
CREATE OR REPLACE TABLE airplane_details_dedup AS
SELECT
    code_iata,
    code_icao,
    full_name,
    category,
    payload,
    volume,
    duplicate_variants
FROM (
    SELECT
        code_iata,
        code_icao,
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
""")

# Question 2: Create capacity table with explicit match-status classification.
con.execute("""
CREATE OR REPLACE TABLE flight_capacity AS
SELECT
    f.flight_id,
    f.flight_date,
    f.flight,
    f.callsign,
    f.operator,
    f.origin_iata,
    f.origin_icao,
    f.destination_iata,
    f.destination_icao,
    f.equipment,
    f.registration,
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
    a.duplicate_variants AS equipment_reference_variants
FROM flights f
LEFT JOIN airplane_details_dedup a
    ON upper(trim(f.equipment)) = a.code_icao;
""")

con.execute("""
CREATE OR REPLACE TABLE capacity_data_quality_summary AS
WITH totals AS (
    SELECT COUNT(*) AS total_flights
    FROM flight_capacity
),
status_counts AS (
    SELECT
        match_status AS metric,
        COUNT(*) AS flights
    FROM flight_capacity
    GROUP BY match_status
),
metrics AS (
    SELECT 'total_flights' AS metric, total_flights AS flights FROM totals
    UNION ALL
    SELECT metric, flights FROM status_counts
)
SELECT
    m.metric,
    m.flights,
    ROUND(100.0 * m.flights / t.total_flights, 2) AS pct_of_flights
FROM metrics m
CROSS JOIN totals t
ORDER BY
    CASE m.metric
        WHEN 'total_flights' THEN 0
        WHEN 'matched_full' THEN 1
        WHEN 'matched_no_volume' THEN 2
        WHEN 'missing_equipment' THEN 3
        WHEN 'no_data_on_equipment' THEN 4
        ELSE 5
    END;
""")