# Phase 8 Dashboard Build (Evidence)

This runbook implements the Phase 8 dashboard deliverables using Evidence instead of Looker Studio, with production-safe KPI definitions and reproducible setup.

## 1) Prerequisites

- BigQuery marts are built:
  - `sbb_punctuality_marts.bi_transport_daily`
  - `sbb_punctuality_marts.bi_delay_heatmap`
- Local tools:
  - Node.js 20+ (Evidence build is not compatible with Node 18)
  - npm
  - `gcloud` CLI for local ADC auth
- Evidence app scaffold exists in `evidence/`.

## 2) Build and test marts

From repository root:

```bash
set -a && source airflow/.env && set +a
uv run dbt build --project-dir dbt_sbb_punctuality --profiles-dir dbt_sbb_punctuality --select +bi_transport_daily +bi_delay_heatmap
```

Expected result: PASS for both models and tests.

## 3) Verification SQL (before rendering report)

Run in BigQuery SQL editor.

Freshness:

```sql
SELECT MAX(operating_day) AS latest_operating_day
FROM `your-project.sbb_punctuality_marts.bi_transport_daily`;
```

KPI bounds:

```sql
SELECT
  COUNTIF(pct_on_time < 0 OR pct_on_time > 100) AS bad_pct_on_time,
  COUNTIF(pct_delayed_3min < 0 OR pct_delayed_3min > 100) AS bad_pct_delayed_3min,
  COUNTIF(total_events < 0 OR total_cancelled < 0) AS bad_non_negative
FROM `your-project.sbb_punctuality_marts.bi_transport_daily`;
```

Heatmap sanity:

```sql
SELECT
  COUNTIF(hour_of_day < 0 OR hour_of_day > 23) AS bad_hour,
  COUNTIF(day_of_week_num < 1 OR day_of_week_num > 7) AS bad_dow
FROM `your-project.sbb_punctuality_marts.bi_delay_heatmap`;
```

## 4) Configure Evidence datasource

From repo root:

```bash
cd evidence
cp .env.example .env
```

Set `.env` values:

- `EVIDENCE_BQ_PROJECT=your-gcp-project-id`
- `EVIDENCE_BQ_DATASET=sbb_punctuality_marts`

Authenticate locally:

```bash
gcloud auth application-default login
```

## 5) Run Evidence locally

From `evidence/`:

```bash
npm install
npm run sources
npm run dev
```

Open the local URL shown by Evidence (typically `http://localhost:3000`).

The report includes:

- Tile 1 (categorical): average delay by transport type (bar chart)
- Tile 2 (temporal): on-time trend by day and transport type (line chart)
- Bonus tile: delay heatmap by hour and day-of-week
- Global filters: date range and transport type multi-select

## 6) Build/export for portfolio

From `evidence/`:

```bash
npm run build
npm run preview
```

If `npm run build` fails with ESM errors, verify your Node version is 20+.

## 7) Share and evidence

1. Publish/deploy the `evidence/build` output to your preferred static host (GitHub Pages, Netlify, Vercel, Cloud Storage static site, etc.).
2. Set visibility to viewer/public depending on host settings.
3. Copy the public report URL for reviewers.
4. Save screenshot as `images/dashboard_screenshot.png`.

## 8) KPI definitions (for README consistency)

- `avg_delay`: average arrival delay in minutes (`arrival_delay_min`)
- `pct_on_time`: `100 * events_on_time / total_events`
- `pct_delayed_3min`: `100 * events_delayed_3min / total_events`
