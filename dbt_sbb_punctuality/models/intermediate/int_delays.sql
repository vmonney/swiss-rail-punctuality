WITH base AS (
    SELECT
        operating_day,
        journey_id,
        operator_id,
        operator_abbreviation,
        operator_name,
        product_id,
        line_id,
        line_name,
        rotation_id,
        transport_type,
        station_uic,
        station_name,
        station_sloid,
        scheduled_arrival_ts,
        actual_arrival_ts,
        arrival_status,
        scheduled_departure_ts,
        TIMESTAMP_DIFF(actual_arrival_ts, scheduled_arrival_ts, MINUTE) AS arrival_delay_min
    FROM {{ ref('stg_stop_events') }}
    WHERE
        arrival_status IN ('REAL', 'ESTIMATED')
        AND scheduled_arrival_ts IS NOT NULL
        AND actual_arrival_ts IS NOT NULL
        AND TIMESTAMP_DIFF(actual_arrival_ts, scheduled_arrival_ts, MINUTE) BETWEEN -60 AND 180
)

SELECT
    operating_day,
    journey_id,
    operator_id,
    operator_abbreviation,
    operator_name,
    product_id,
    line_id,
    line_name,
    rotation_id,
    transport_type,
    station_uic,
    station_name,
    station_sloid,
    scheduled_arrival_ts,
    actual_arrival_ts,
    arrival_status,
    arrival_delay_min,
    arrival_delay_min > 0 AS is_delayed,
    arrival_delay_min >= 3 AS is_significantly_delayed
FROM base
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY operating_day, journey_id, station_uic, scheduled_arrival_ts
    ORDER BY
        CASE arrival_status
            WHEN 'REAL' THEN 1
            ELSE 2
        END,
        COALESCE(scheduled_departure_ts, scheduled_arrival_ts) DESC,
        actual_arrival_ts DESC
) = 1
