## Swiss Rail Punctuality - Phase 1

Phase 1 sets up the local developer environment and provisions base GCP infrastructure:

- 1 raw GCS bucket
- 1 BigQuery dataset
- 1 service account for pipeline workloads

## Prerequisites

- Python 3.13
- `uv`
- Terraform >= 1.5
- Authenticated GCP CLI (`gcloud auth application-default login`)

## Local Setup (uv)

Install dependencies (including dev tools):

```bash
uv sync --all-groups
```

Run the app:

```bash
uv run python main.py
```

## Phase 2: Data Exploration with marimo

Phase 2 replaces Jupyter with a reproducible `marimo` notebook script:
- `notebooks/exploration.py`
- reusable helpers in `src/swiss_rail_punctuality/profiling.py`

### 1) Add a local sample file

Put one small file under `data/raw/sample/`:
- `ist_daten_sample.csv` (semicolon-separated)
- or `ist_daten_sample.parquet`

The app validates required columns:
`BETRIEBSTAG`, `VERKEHRSMITTEL_TEXT`, `AN_PROGNOSE_STATUS`, `ANKUNFTSZEIT`, `AN_PROGNOSE`, `FAELLT_AUS_TF`.

### 2) Launch the marimo app

```bash
uv run marimo edit notebooks/exploration.py
```

Or run in read mode:

```bash
uv run marimo run notebooks/exploration.py
```

### 3) What Phase 2 produces

- Row counts, column types, and null-rate table
- Value distributions for `VERKEHRSMITTEL_TEXT` and `AN_PROGNOSE_STATUS`
- Delay validation: `delay_min = AN_PROGNOSE - ANKUNFTSZEIT` in minutes
- Edge-case slices for null arrivals, non-`REAL`/`ESTIMATED` statuses, and cancellations
- A short decision log you can reuse in interviews/README

## Phase 3: Raw Ingestion Pipeline (Airflow)

Phase 3 adds an orchestrated raw ingestion DAG that runs daily:

1. `download_csv` for the target execution date
2. `convert_to_parquet`
3. `upload_to_gcs` under `raw/ist-daten/YYYY/MM/DD/`
4. `load_to_bigquery` into a partitioned raw table
5. `cleanup_local`

### Files added for Phase 3

- `airflow/docker-compose.yaml`
- `airflow/requirements.txt`
- `airflow/.env.example`
- `airflow/dags/sbb_daily_ingest.py`

### 1) Prepare environment variables

From repo root:

```bash
cp airflow/.env.example airflow/.env
```

Update `airflow/.env` with values from Terraform outputs:

```bash
terraform -chdir=terraform output -raw raw_bucket_name
terraform -chdir=terraform output -raw bigquery_dataset_id
terraform -chdir=terraform output -raw service_account_email
```

Set at least:
- `GCP_PROJECT_ID`
- `RAW_BUCKET`
- `BQ_DATASET`
- `BQ_TABLE`
- `INGEST_START_DATE` (for catchup window)

### 2) Add service account key for local Airflow

Create a key if needed:

```bash
gcloud iam service-accounts keys create airflow/credentials/gcp-key.json \
  --iam-account "$(terraform -chdir=terraform output -raw service_account_email)"
```

### 3) Start Airflow

```bash
docker compose --env-file airflow/.env -f airflow/docker-compose.yaml up airflow-init
docker compose --env-file airflow/.env -f airflow/docker-compose.yaml up -d
```

Open Airflow at [http://localhost:8080](http://localhost:8080) with:
- user: `admin`
- password: `admin`

### 4) Run and verify Phase 3

Trigger one run in UI for `sbb_daily_ingest` (or run a backfill/catchup window using `INGEST_START_DATE`).

By default (scheduled or manual run), the DAG ingests the **latest available day** published on the source page.

Before a manual run, open **Trigger DAG**:
- In newer Airflow UI you should now see parameter fields (`mode`, `run_date`, `backfill_days`) directly.
- You can also still use the config JSON.

Config JSON example:

```json
{
  "mode": "latest",
  "backfill_days": 1
}
```

Manual options:
- `mode: "latest"` -> anchor on the latest published day.
- `mode: "date"` -> anchor on a specific day using `run_date`.
- `backfill_days` -> integer from `1` to `7` (includes anchor day and previous available days).

Example: run latest + previous 6 days (7 total):

```json
{
  "mode": "latest",
  "backfill_days": 7
}
```

Example: run from specific anchor date + previous 2 available days:

```json
{
  "mode": "date",
  "run_date": "2026-03-01",
  "backfill_days": 3
}
```

Validate outputs:
- GCS object path exists: `gs://<RAW_BUCKET>/raw/ist-daten/YYYY/MM/DD/`
- BigQuery raw table exists and is populated for that date:

```sql
SELECT betriebstag_date, COUNT(*) AS rows_loaded
FROM `your-project.your_dataset.ist_daten_raw`
GROUP BY betriebstag_date
ORDER BY betriebstag_date DESC
LIMIT 10;
```

### Data quality scope in Phase 3

Phase 3 keeps quality checks lightweight (required columns, non-empty files, load success).  
Formal model-level quality remains in the dbt phases (Phase 5-6). Soda is intentionally deferred as an optional extra-mile addition after the core pipeline is stable.

## Linters

Python linting with Ruff:

```bash
uv run ruff check .
```

SQL linting with SQLFluff (for future SQL/dbt models):

```bash
uv run sqlfluff lint .
```

## Terraform (Phase 1 Infrastructure)

1. Copy the example variables file:

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
```

2. Edit `terraform/terraform.tfvars` and set:
- `project_id`
- `bucket_suffix` (must make the bucket name globally unique)
- optionally keep `region = "europe-west6"` and `dataset_id = "sbb_punctuality"`

3. Initialize, validate, and plan:

```bash
terraform -chdir=terraform init
terraform -chdir=terraform fmt -recursive
terraform -chdir=terraform validate
terraform -chdir=terraform plan
```

4. Apply when ready:

```bash
terraform -chdir=terraform apply
```

## Optional: Generate a Service Account Key (Local Development)

If you need a JSON key locally for tools that cannot use ADC:

```bash
gcloud iam service-accounts keys create sbb-punctuality-sa-key.json \
  --iam-account "$(terraform -chdir=terraform output -raw service_account_email)"
```

The repository ignores `*.json` and `*.tfvars` to avoid leaking credentials.
