WITH delays AS (
    SELECT
        operating_day,
        station_uic,
        station_name,
        arrival_delay_min,
        is_delayed,
        is_significantly_delayed
    FROM {{ ref('int_delays') }}
),

kpis AS (
    SELECT
        operating_day,
        station_uic,
        ANY_VALUE(station_name) AS station_name,
        COUNT(*) AS total_events,
        ROUND(AVG(arrival_delay_min), 3) AS avg_delay,
        ROUND(
            100 * SAFE_DIVIDE(SUM(CASE WHEN arrival_delay_min <= 0 THEN 1 ELSE 0 END), COUNT(*)), 3
        )
            AS pct_on_time,
        ROUND(
            100 * SAFE_DIVIDE(SUM(CASE WHEN is_significantly_delayed THEN 1 ELSE 0 END), COUNT(*)),
            3
        ) AS pct_delayed_3min
    FROM delays
    GROUP BY 1, 2
),

cancelled AS (
    SELECT
        COALESCE(
            betriebstag_date,
            SAFE.PARSE_DATE('%d.%m.%Y', CAST(BETRIEBSTAG AS STRING)),
            SAFE.PARSE_DATE('%Y-%m-%d', CAST(BETRIEBSTAG AS STRING))
        ) AS operating_day,
        SAFE_CAST(BPUIC AS INT64) AS station_uic,
        COUNT(*) AS total_cancelled
    FROM {{ source('raw', 'ist_daten_raw') }}
    WHERE
        COALESCE(SAFE_CAST(FAELLT_AUS_TF AS BOOL), FALSE)
        AND NOT COALESCE(SAFE_CAST(DURCHFAHRT_TF AS BOOL), FALSE)
    GROUP BY 1, 2
)

SELECT
    kpis.operating_day,
    kpis.station_uic,
    kpis.station_name,
    kpis.total_events,
    kpis.avg_delay,
    kpis.pct_on_time,
    kpis.pct_delayed_3min,
    COALESCE(cancelled.total_cancelled, 0) AS total_cancelled
FROM kpis
LEFT JOIN cancelled
    USING (operating_day, station_uic)
