-- Fails when delay values are outside an expected operational range.
SELECT
    operating_day,
    journey_id,
    station_uic,
    arrival_delay_min
FROM {{ ref('int_delays') }}
WHERE
    arrival_delay_min IS NOT NULL
    AND (arrival_delay_min < -60 OR arrival_delay_min > 180)
