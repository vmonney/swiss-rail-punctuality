WITH base AS (
    SELECT
        operating_day,
        transport_type,
        arrival_delay_min
    FROM {{ ref('int_delays') }}
),

daily_kpis AS (
    SELECT
        operating_day,
        transport_type,
        COUNT(*) AS total_events,
        SUM(CASE WHEN arrival_delay_min <= 0 THEN 1 ELSE 0 END) AS events_on_time,
        SUM(CASE WHEN arrival_delay_min >= 3 THEN 1 ELSE 0 END) AS events_delayed_3min,
        ROUND(AVG(arrival_delay_min), 3) AS avg_delay
    FROM base
    GROUP BY 1, 2
),

cancelled AS (
    SELECT
        COALESCE(
            betriebstag_date,
            SAFE.PARSE_DATE('%d.%m.%Y', CAST(BETRIEBSTAG AS STRING)),
            SAFE.PARSE_DATE('%Y-%m-%d', CAST(BETRIEBSTAG AS STRING))
        ) AS operating_day,
        CAST(VERKEHRSMITTEL_TEXT AS STRING) AS transport_type,
        COUNT(*) AS total_cancelled
    FROM {{ source('raw', 'ist_daten_raw') }}
    WHERE
        COALESCE(SAFE_CAST(FAELLT_AUS_TF AS BOOL), FALSE)
        AND NOT COALESCE(SAFE_CAST(DURCHFAHRT_TF AS BOOL), FALSE)
    GROUP BY 1, 2
),

dim_transport_types AS (
    SELECT
        transport_type,
        transport_type_label,
        transport_type_description
    FROM {{ ref('dim_transport_types') }}
)

SELECT
    d.operating_day,
    d.transport_type,
    COALESCE(dt.transport_type_label, d.transport_type) AS transport_type_label,
    dt.transport_type_description,
    d.total_events,
    d.events_on_time,
    d.events_delayed_3min,
    d.avg_delay,
    ROUND(100 * SAFE_DIVIDE(d.events_on_time, d.total_events), 3) AS pct_on_time,
    ROUND(100 * SAFE_DIVIDE(d.events_delayed_3min, d.total_events), 3) AS pct_delayed_3min,
    COALESCE(c.total_cancelled, 0) AS total_cancelled
FROM daily_kpis AS d
LEFT JOIN cancelled AS c
    USING (operating_day, transport_type)
LEFT JOIN dim_transport_types AS dt
    USING (transport_type)
