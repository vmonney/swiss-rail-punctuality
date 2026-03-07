{% macro parse_swiss_timestamp(column_name) %}
COALESCE(
    SAFE.PARSE_TIMESTAMP('%d.%m.%Y %H:%M:%S', CAST({{ column_name }} AS STRING)),
    SAFE.PARSE_TIMESTAMP('%d.%m.%Y %H:%M', CAST({{ column_name }} AS STRING)),
    SAFE_CAST({{ column_name }} AS TIMESTAMP)
)
{% endmacro %}
