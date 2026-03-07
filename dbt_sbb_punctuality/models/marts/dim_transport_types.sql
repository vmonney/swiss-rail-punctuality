SELECT DISTINCT transport_type
FROM {{ ref('stg_stop_events') }}
WHERE transport_type IS NOT NULL
