WITH observed_transport_types AS (
    SELECT DISTINCT transport_type
    FROM {{ ref('stg_stop_events') }}
    WHERE transport_type IS NOT NULL
),

mapping AS (
    SELECT
        transport_type,
        transport_type_label,
        transport_type_description,
        mapping_source
    FROM {{ ref('transport_type_mapping') }}
)

SELECT
    t.transport_type,
    COALESCE(m.transport_type_label, t.transport_type) AS transport_type_label,
    COALESCE(
        m.transport_type_description,
        'No curated description available for this transport type.'
    ) AS transport_type_description,
    COALESCE(m.mapping_source, 'unmapped') AS mapping_source,
    m.transport_type IS NOT NULL AS is_mapped
FROM observed_transport_types AS t
LEFT JOIN mapping AS m
    USING (transport_type)
