## Methodology

This report is powered by dbt marts and is designed for reproducible analytics engineering workflows.

### Source Models

- `bi_transport_daily`: daily KPI table at `operating_day x transport_type`.
- `bi_delay_heatmap`: day-of-week and hour rollup at `operating_day x transport_type x day_of_week_num x hour_of_day`.

### Why These Metrics

- `avg_delay` captures central delay tendency.
- `pct_on_time` supports service-quality benchmarking.
- `pct_delayed_3min` aligns with practical operational thresholds.
- `total_cancelled` keeps cancellation impact visible alongside punctuality.

### Data Quality Controls

- dbt schema tests enforce non-null, accepted ranges, and uniqueness.
- KPI columns are bounded to keep percentages in `0..100`.
- Heatmap dimensions are constrained to `hour_of_day in 0..23` and `day_of_week_num in 1..7`.
