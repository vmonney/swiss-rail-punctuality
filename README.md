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
