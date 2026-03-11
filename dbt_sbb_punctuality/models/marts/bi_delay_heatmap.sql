WITH base AS (
    SELECT
        operating_day,
        transport_type,
        EXTRACT(HOUR FROM scheduled_arrival_ts) AS hour_of_day,
        EXTRACT(DAYOFWEEK FROM operating_day) AS day_of_week_num,
        arrival_delay_min
    FROM {{ ref('int_delays') }}
),

hourly AS (
    SELECT
        operating_day,
        transport_type,
        hour_of_day,
        day_of_week_num,
        COUNT(*) AS total_events,
        SUM(arrival_delay_min) AS total_delay_minutes,
        SUM(CASE WHEN arrival_delay_min <= 0 THEN 1 ELSE 0 END) AS events_on_time
    FROM base
    GROUP BY 1, 2, 3, 4
),

dim_transport_types AS (
    SELECT
        transport_type,
        transport_type_label
    FROM {{ ref('dim_transport_types') }}
)

SELECT
    h.operating_day,
    h.transport_type,
    COALESCE(dt.transport_type_label, h.transport_type) AS transport_type_label,
    h.hour_of_day,
    h.day_of_week_num,
    CASE day_of_week_num
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END AS day_of_week_name,
    h.total_events,
    h.total_delay_minutes,
    h.events_on_time,
    ROUND(SAFE_DIVIDE(h.total_delay_minutes, h.total_events), 3) AS avg_delay,
    ROUND(100 * SAFE_DIVIDE(h.events_on_time, h.total_events), 3) AS pct_on_time
FROM hourly AS h
LEFT JOIN dim_transport_types AS dt
    USING (transport_type)
