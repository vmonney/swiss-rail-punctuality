SELECT DISTINCT
    operator_id,
    operator_abbreviation,
    operator_name
FROM {{ ref('stg_stop_events') }}
WHERE operator_id IS NOT NULL
