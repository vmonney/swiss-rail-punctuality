## Swiss Rail Punctuality Report

This Evidence report replaces Looker Studio for Phase 8 and is built directly from BigQuery mart models.

### Global Filters

```transport_filter_source
select distinct transport_type_label
from sbb_punctuality_marts.transport_daily
order by 1
```

```report_date_source
select distinct operating_day
from sbb_punctuality_marts.transport_daily
order by 1
```

<DateRange name=report_range dates=operating_day data={report_date_source} />
<Dropdown
  name=transport_filter
  title="Transport Type"
  data={transport_filter_source}
  value=transport_type_label
  multiple=true
/>

```kpis_filtered
select
  sum(total_events) as total_events,
  sum(events_on_time) as events_on_time,
  sum(events_delayed_3min) as events_delayed_3min,
  sum(total_cancelled) as total_cancelled,
  round(avg(avg_delay), 2) as avg_delay_min,
  round(sum(events_on_time) / nullif(sum(total_events), 0), 4) as pct_on_time,
  round(sum(events_delayed_3min) / nullif(sum(total_events), 0), 4) as pct_delayed_3min
from sbb_punctuality_marts.transport_daily
where operating_day between '${inputs.report_range.start}' and '${inputs.report_range.end}'
  and transport_type_label in ${inputs.transport_filter.value}
```

<BigValue data={kpis_filtered} value=avg_delay_min title="Average Delay (min)" />
<BigValue data={kpis_filtered} value=pct_on_time title="On-time %" fmt=pct2 />
<BigValue data={kpis_filtered} value=pct_delayed_3min title="Delayed >= 3 min %" fmt=pct2 />
<BigValue data={kpis_filtered} value=total_cancelled title="Cancelled Events" />

### Delay by Transport Type

```delay_by_transport
select
  transport_type_label,
  round(avg(avg_delay), 2) as avg_delay
from sbb_punctuality_marts.transport_daily
where operating_day between '${inputs.report_range.start}' and '${inputs.report_range.end}'
  and transport_type_label in ${inputs.transport_filter.value}
group by 1
order by 2 desc
```

<BarChart
  data={delay_by_transport}
  x=transport_type_label
  y=avg_delay
  title="Average Arrival Delay (minutes) by Transport Type"
/>

### On-time Trend

```on_time_trend
select
  operating_day,
  transport_type_label,
  round(sum(events_on_time) / nullif(sum(total_events), 0), 4) as pct_on_time
from sbb_punctuality_marts.transport_daily
where operating_day between '${inputs.report_range.start}' and '${inputs.report_range.end}'
  and transport_type_label in ${inputs.transport_filter.value}
group by 1, 2
order by 1, 2
```

<LineChart
  data={on_time_trend}
  x=operating_day
  y=pct_on_time
  series=transport_type_label
  title="On-time Performance by Day and Transport Type"
/>

### Delay Heatmap (Hour x Day of Week)

```delay_heatmap_filtered
select
  day_of_week_name,
  day_of_week_num,
  hour_of_day,
  round(avg(avg_delay), 2) as avg_delay
from sbb_punctuality_marts.delay_heatmap
where operating_day between '${inputs.report_range.start}' and '${inputs.report_range.end}'
  and transport_type_label in ${inputs.transport_filter.value}
group by 1, 2, 3
order by day_of_week_num, hour_of_day
```

<Heatmap
  data={delay_heatmap_filtered}
  x=hour_of_day
  y=day_of_week_name
  value=avg_delay
  title="Delay Intensity by Hour and Day of Week"
/>

### Executive Insights

```system_benchmark
select
  round(sum(events_on_time) / nullif(sum(total_events), 0), 4) as system_on_time_ratio
from sbb_punctuality_marts.transport_daily
where operating_day between '${inputs.report_range.start}' and '${inputs.report_range.end}'
  and transport_type_label in ${inputs.transport_filter.value}
```

<BigValue
  data={system_benchmark}
  value=system_on_time_ratio
  title="System On-time Benchmark"
  fmt=pct2
/>

```transport_reliability_snapshot
select
  transport_type_label,
  round(100.0 * sum(events_on_time) / nullif(sum(total_events), 0), 2) as on_time_pct,
  round(100.0 * sum(total_cancelled) / nullif(sum(total_events), 0), 2) as cancelled_pct
from sbb_punctuality_marts.transport_daily
where operating_day between '${inputs.report_range.start}' and '${inputs.report_range.end}'
  and transport_type_label in ${inputs.transport_filter.value}
group by 1
order by on_time_pct asc
```

<BarChart
  data={transport_reliability_snapshot}
  x=transport_type_label
  y=on_time_pct
  title="On-time % by Transport Type (Lower = Priority)"
/>

```highest_risk_time_windows
select
  concat(day_of_week_name, ' ', cast(hour_of_day as string), ':00') as time_window,
  round(avg(avg_delay), 2) as avg_delay
from sbb_punctuality_marts.delay_heatmap
where operating_day between '${inputs.report_range.start}' and '${inputs.report_range.end}'
  and transport_type_label in ${inputs.transport_filter.value}
group by 1
order by avg_delay desc
limit 10
```

<BarChart
  data={highest_risk_time_windows}
  x=time_window
  y=avg_delay
  title="Top 10 Highest-risk Time Windows (by Average Delay)"
/>

Use these outputs as a portfolio-ready narrative:
- Prioritize operational actions on the top high-delay windows shown above.
- Benchmark each transport type against the system on-time baseline.
- Flag transport types with low on-time and high cancellation ratios for targeted interventions.

### KPI Definitions

- `avg_delay_min`: mean of event-level arrival delay (minutes) in selected range.
- `pct_on_time`: `sum(events_on_time) / sum(total_events)` (formatted as % in the UI).
- `pct_delayed_3min`: `sum(events_delayed_3min) / sum(total_events)` (formatted as % in the UI).
