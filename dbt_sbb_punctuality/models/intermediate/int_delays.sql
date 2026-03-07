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
    TIMESTAMP_DIFF(actual_arrival_ts, scheduled_arrival_ts, MINUTE) AS arrival_delay_min,
    TIMESTAMP_DIFF(actual_arrival_ts, scheduled_arrival_ts, MINUTE) > 0 AS is_delayed,
    TIMESTAMP_DIFF(actual_arrival_ts, scheduled_arrival_ts, MINUTE) >= 3 AS is_significantly_delayed
FROM {{ ref('stg_stop_events') }}
WHERE
    arrival_status IN ('REAL', 'ESTIMATED')
    AND scheduled_arrival_ts IS NOT NULL
    AND actual_arrival_ts IS NOT NULL
    AND TIMESTAMP_DIFF(actual_arrival_ts, scheduled_arrival_ts, MINUTE) BETWEEN -60 AND 180
