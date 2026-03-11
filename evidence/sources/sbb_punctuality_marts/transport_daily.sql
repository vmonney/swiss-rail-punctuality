SELECT
  operating_day,
  transport_type,
  transport_type AS transport_type_label,
  CAST(NULL AS STRING) AS transport_type_description,
  total_events,
  events_on_time,
  events_delayed_3min,
  avg_delay,
  pct_on_time,
  pct_delayed_3min,
  total_cancelled
FROM sbb_punctuality_marts.bi_transport_daily
