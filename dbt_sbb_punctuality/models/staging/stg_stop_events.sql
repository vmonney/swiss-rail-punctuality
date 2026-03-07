WITH base AS (
    SELECT
        COALESCE(
            betriebstag_date,
            SAFE.PARSE_DATE('%d.%m.%Y', CAST(BETRIEBSTAG AS STRING)),
            SAFE.PARSE_DATE('%Y-%m-%d', CAST(BETRIEBSTAG AS STRING))
        ) AS operating_day,
        CAST(FAHRT_BEZEICHNER AS STRING) AS journey_id,
        CAST(BETREIBER_ID AS STRING) AS operator_id,
        CAST(BETREIBER_ABK AS STRING) AS operator_abbreviation,
        CAST(BETREIBER_NAME AS STRING) AS operator_name,
        CAST(PRODUKT_ID AS STRING) AS product_id,
        SAFE_CAST(LINIEN_ID AS INT64) AS line_id,
        CAST(LINIEN_TEXT AS STRING) AS line_name,
        CAST(UMLAUF_ID AS STRING) AS rotation_id,
        COALESCE(NULLIF(TRIM(CAST(VERKEHRSMITTEL_TEXT AS STRING)), ''), 'UNKNOWN')
            AS transport_type,
        COALESCE(SAFE_CAST(ZUSATZFAHRT_TF AS BOOL), FALSE) AS is_additional_service,
        COALESCE(SAFE_CAST(FAELLT_AUS_TF AS BOOL), FALSE) AS is_cancelled,
        SAFE_CAST(BPUIC AS INT64) AS station_uic,
        COALESCE(NULLIF(TRIM(CAST(HALTESTELLEN_NAME AS STRING)), ''), 'UNKNOWN_STATION')
            AS station_name,
        {{ parse_swiss_timestamp("ANKUNFTSZEIT") }} AS scheduled_arrival_ts,
        {{ parse_swiss_timestamp("AN_PROGNOSE") }} AS actual_arrival_ts,
        CASE
            WHEN UPPER(TRIM(CAST(AN_PROGNOSE_STATUS AS STRING))) IN ('REAL') THEN 'REAL'
            WHEN UPPER(TRIM(CAST(AN_PROGNOSE_STATUS AS STRING))) IN ('ESTIMATED', 'GESCHAETZT')
                THEN 'ESTIMATED'
            WHEN UPPER(TRIM(CAST(AN_PROGNOSE_STATUS AS STRING))) IN ('FORECAST', 'PROGNOSE')
                THEN 'FORECAST'
            WHEN UPPER(TRIM(CAST(AN_PROGNOSE_STATUS AS STRING))) IN ('UNKNOWN', 'UNBEKANNT')
                THEN 'UNKNOWN'
            ELSE 'UNKNOWN'
        END AS arrival_status,
        {{ parse_swiss_timestamp("ABFAHRTSZEIT") }} AS scheduled_departure_ts,
        {{ parse_swiss_timestamp("AB_PROGNOSE") }} AS actual_departure_ts,
        UPPER(TRIM(CAST(AB_PROGNOSE_STATUS AS STRING))) AS departure_status,
        COALESCE(SAFE_CAST(DURCHFAHRT_TF AS BOOL), FALSE) AS is_pass_through,
        CAST(SLOID AS STRING) AS station_sloid
    FROM {{ source('raw', 'ist_daten_raw') }}
),

filtered AS (
    SELECT *
    FROM base
    WHERE
        NOT is_pass_through
        AND NOT is_cancelled
        AND operating_day IS NOT NULL
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
    is_additional_service,
    is_cancelled,
    station_uic,
    station_name,
    scheduled_arrival_ts,
    actual_arrival_ts,
    arrival_status,
    scheduled_departure_ts,
    actual_departure_ts,
    departure_status,
    is_pass_through,
    station_sloid
FROM filtered
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
        operating_day,
        COALESCE(journey_id, '__null_journey__'),
        COALESCE(station_uic, -1),
        COALESCE(scheduled_arrival_ts, TIMESTAMP('1900-01-01 00:00:00+00')),
        COALESCE(scheduled_departure_ts, TIMESTAMP('1900-01-01 00:00:00+00'))
    ORDER BY
        CASE arrival_status
            WHEN 'REAL' THEN 1
            WHEN 'ESTIMATED' THEN 2
            WHEN 'FORECAST' THEN 3
            ELSE 4
        END,
        COALESCE(actual_arrival_ts, scheduled_arrival_ts) DESC
) = 1
