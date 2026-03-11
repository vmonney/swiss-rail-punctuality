SELECT
  operating_day,
  transport_type,
  transport_type AS transport_type_label,
  hour_of_day,
  day_of_week_num,
  day_of_week_name,
  total_events,
  total_delay_minutes,
  events_on_time,
  avg_delay,
  pct_on_time
FROM sbb_punctuality_marts.bi_delay_heatmap
