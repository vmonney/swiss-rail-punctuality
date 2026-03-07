WITH ranked AS (
    SELECT
        station_uic,
        station_name,
        station_sloid,
        ROW_NUMBER() OVER (
            PARTITION BY station_uic
            ORDER BY
                CASE WHEN station_name = 'UNKNOWN_STATION' THEN 1 ELSE 0 END,
                station_name
        ) AS row_num
    FROM {{ ref('stg_stop_events') }}
    WHERE station_uic IS NOT NULL
)

SELECT
    station_uic,
    station_name,
    station_sloid
FROM ranked
WHERE row_num = 1
