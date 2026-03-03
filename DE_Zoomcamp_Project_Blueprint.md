# 🚆 Swiss Rail Punctuality Analytics — DE Zoomcamp Project Blueprint

**Project title suggestion:** *"Swiss Rail Pulse: Analyzing SBB/CFF Train Punctuality"*
**Target:** Maximum score (28/28) + portfolio-ready for Swiss DE roles
**Stack:** Docker · Terraform · Airflow · GCP (GCS + BigQuery) · dbt · Looker Studio

---

## 1. Project Overview

### Business Problem

Switzerland prides itself on having one of the most punctual rail networks in the world. But *how punctual is it really?* Using official SBB/CFF "Ist-Daten" (actual data), this project builds an end-to-end pipeline to answer:

- Which train lines and stations have the worst delays?
- How does punctuality vary by time of day, day of week, and season?
- Are S-Bahn commuter trains less punctual than InterCity long-distance trains?
- Which hub stations (Zürich HB, Bern, Lausanne, Fribourg) act as delay propagation points?

This is a compelling narrative for any Swiss employer — it shows domain awareness, technical skill, and analytical thinking about infrastructure they personally use every day.

### End-to-End Data Flow

```
opentransportdata.swiss (daily CSV ~500MB)
        │
        ▼
┌──────────────────┐
│  Airflow DAG     │──► Download CSV → Convert to Parquet → Upload
│  (Docker)        │
└──────────────────┘
        │
        ▼
┌──────────────────┐
│  GCS Bucket      │   raw/ist-daten/YYYY/MM/DD/istdaten.parquet
│  (Data Lake)     │
└──────────────────┘
        │
        ▼
┌──────────────────┐
│  Airflow DAG     │──► GCS → BigQuery native table load
└──────────────────┘
        │
        ▼
┌──────────────────┐
│  BigQuery        │   raw.ist_daten (partitioned by BETRIEBSTAG)
│  (Raw Layer)     │
└──────────────────┘
        │
        ▼
┌──────────────────┐
│  dbt             │──► staging → intermediate → marts
│  (Transform)     │
└──────────────────┘
        │
        ▼
┌──────────────────┐
│  BigQuery        │   marts.fct_delays, marts.dim_stations, etc.
│  (Serving Layer) │
└──────────────────┘
        │
        ▼
┌──────────────────┐
│  Looker Studio   │   Dashboard with 2+ tiles
│  (Visualization) │
└──────────────────┘
```

### Expected Final Outputs

1. **BigQuery warehouse** with partitioned + clustered fact/dimension tables
2. **dbt project** with 10–15 models across staging/intermediate/mart layers, with tests
3. **Airflow DAGs** (ingestion + dbt trigger), fully Dockerized
4. **Terraform config** provisioning GCS bucket, BigQuery dataset, service accounts
5. **Looker Studio dashboard** with ≥2 tiles (delay distribution + temporal trend)
6. **README.md** with architecture diagram, setup guide, screenshots, and design decisions

---

## 2. Data Assessment

### Primary Dataset: Ist-Daten (Actual Data)

| Attribute | Details |
|-----------|---------|
| **Source** | [opentransportdata.swiss/dataset/istdaten](https://opentransportdata.swiss/en/dataset/istdaten) |
| **Archive** | `https://archive.opentransportdata.swiss/actual_data_archive.htm` |
| **Format** | CSV, semicolon-delimited (`;`), UTF-8 |
| **File size** | ~500–535 MB per weekday, ~340–440 MB on weekends |
| **Row count** | ~2.5 million rows per day |
| **Grain** | One row per **stop event** (a single train arriving at / departing from a station) |
| **Temporal coverage** | Daily files available historically (archive goes back years) |
| **License** | Open data (public domain for Swiss public transport data) |
| **No API key needed** | Direct CSV download via URL pattern |

### Schema (22 columns)

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| `BETRIEBSTAG` | DATE | Operating day | **Partition key** — format `DD.MM.YYYY` |
| `FAHRT_BEZEICHNER` | STRING | Trip/journey identifier | Unique per journey per day |
| `BETREIBER_ID` | STRING | Operator code | e.g. `85:11` for SBB |
| `BETREIBER_ABK` | STRING | Operator abbreviation | e.g. `SBB`, `BLS`, `SOB` |
| `BETREIBER_NAME` | STRING | Operator full name | |
| `PRODUKT_ID` | STRING | Product type | `Zug`, `Bus`, `Tram`, etc. |
| `LINIEN_ID` | INT | Line number | |
| `LINIEN_TEXT` | STRING | Line name | e.g. `IC1`, `S1`, `RE`, `IR13` |
| `UMLAUF_ID` | STRING | Rotation/circuit ID | |
| `VERKEHRSMITTEL_TEXT` | STRING | Transport type | `IC`, `IR`, `RE`, `S`, `R`, `Bus`, etc. — **Cluster key** |
| `ZUSATZFAHRT_TF` | BOOL | Extra/additional service? | `true`/`false` |
| `FAELLT_AUS_TF` | BOOL | Cancelled? | `true`/`false` — important for cancellation analysis |
| `BPUIC` | INT | Station UIC code | Join key for station enrichment |
| `HALTESTELLEN_NAME` | STRING | Station name | e.g. `Zürich HB`, `Bern`, `Fribourg/Freiburg` |
| `ANKUNFTSZEIT` | TIMESTAMP | **Scheduled** arrival time | Can be NULL (first station) |
| `AN_PROGNOSE` | TIMESTAMP | **Actual/forecast** arrival time | |
| `AN_PROGNOSE_STATUS` | STRING | Arrival status | `REAL`, `ESTIMATED`, `FORECAST`, `UNKNOWN` |
| `ABFAHRTSZEIT` | TIMESTAMP | **Scheduled** departure time | Can be NULL (last station) |
| `AB_PROGNOSE` | TIMESTAMP | **Actual/forecast** departure time | |
| `AB_PROGNOSE_STATUS` | STRING | Departure status | `REAL`, `ESTIMATED`, `FORECAST`, `UNKNOWN` |
| `DURCHFAHRT_TF` | BOOL | Pass-through (no stop)? | `true`/`false` |
| `SLOID` | STRING | Swiss Location ID | Newer identifier (added Oct 2024) |

### Data Quality Concerns

| Issue | Severity | Where It Hits | Mitigation |
|-------|----------|---------------|------------|
| **NULL arrival/departure** | Medium | First stop has no arrival; last stop has no departure | Filter or `COALESCE` in staging |
| **FORECAST vs REAL status** | High | `FORECAST` means no actual measurement — it's a prediction | Filter to `REAL` and `ESTIMATED` only for delay calculation |
| **UNKNOWN status** | Medium | No data at all for some stops | Exclude from delay metrics, count separately |
| **Cancelled trains (`FAELLT_AUS_TF`)** | Medium | Included in data but skew delay averages | Separate cancellation analysis; exclude from delay KPIs |
| **Timestamp format** | Low | `DD.MM.YYYY HH:MM` or `DD.MM.YYYY HH:MM:SS` — not ISO 8601 | Parse carefully in staging with a dbt macro |
| **Semicolon delimiter** | Low | Not the default in most tools | Specify `sep=";"` in pandas |
| **Swiss German station names** | Low | Accents: `Zürich`, `Genève`, `Fribourg/Freiburg` | Ensure UTF-8 throughout |
| **Duplicate stops** | Low | Same trip + station appearing twice (pass-through + stop) | Dedup using `DURCHFAHRT_TF` |
| **Data available only T+1** | Design | Previous day's data published the next morning | Aligns perfectly with daily batch pattern |

### How the Data Is Accessed

Daily CSV files are directly downloadable. No API key. URL pattern:

```
# Full dataset (all Swiss operators, ~500MB/day):
https://opentransportdata.swiss/dataset/istdaten/permalink

# Archive for historical data:
https://archive.opentransportdata.swiss/actual_data_archive.htm

# SBB-only subset (~100–150MB/day):
https://data.sbb.ch/api/v2/catalog/datasets/ist-daten-sbb/exports/csv
```

**Recommendation:** Use the full Ist-Daten (all operators) for richer analysis that covers BLS, SOB, and regional operators too.

---

## 3. Architecture & Tool Stack

### Tool Selection Rationale

| Role | Tool | Why |
|------|------|-----|
| **Containerization** | Docker + docker-compose | Industry standard. Packages Airflow + all deps. Required for reproducibility (4/4) |
| **IaC** | Terraform v1.5+ | Course-native, GCP-native, heavily used in Swiss enterprise. Gets Cloud 4/4 |
| **Cloud** | GCP (Free Tier) | Course-native. $300 free credit. BigQuery 1TB/month free queries |
| **Data Lake** | Google Cloud Storage (GCS) | Simple, cheap, native BigQuery integration |
| **Orchestration** | Apache Airflow 2.x | #1 orchestrator in Swiss DE job postings. Strong CV signal for SSD Fribourg and beyond |
| **Data Warehouse** | BigQuery | Course-native, serverless, partition/cluster support, generous free tier |
| **Transformation** | dbt-core + dbt-bigquery | Industry standard analytics engineering. Gets Transformation 4/4 |
| **Data Quality** | dbt tests (built-in) | Schema + custom tests. Optional: add Soda Core for "extra mile" |
| **Dashboard** | Looker Studio (free) | Free, connects to BigQuery natively, shareable URL for peer review |
| **Version Control** | Git + GitHub | Required for submission |
| **CI/CD (optional)** | GitHub Actions | Lint, validate, test — "extra mile" |

### Pipeline Stage Map

```
┌─────────────┬────────────────────────────────────────────┐
│ STAGE       │ TOOLS                                      │
├─────────────┼────────────────────────────────────────────┤
│ Provision   │ Terraform → GCS bucket, BQ dataset, SA     │
│ Ingest      │ Airflow DAG → download CSV, convert, GCS   │
│ Store (Raw) │ GCS (Parquet) → BQ native table            │
│ Transform   │ dbt → staging → intermediate → marts       │
│ Quality     │ dbt tests (not_null, unique, accepted_vals) │
│ Serve       │ BigQuery marts → Looker Studio              │
│ Orchestrate │ Airflow (Docker) — daily schedule           │
│ CI/CD       │ GitHub Actions (optional)                   │
└─────────────┴────────────────────────────────────────────┘
```

### Local vs Cloud

| Component | Local | Cloud |
|-----------|-------|-------|
| Docker Desktop | ✅ | — |
| Airflow (docker-compose) | ✅ | — |
| Terraform CLI | ✅ | — |
| dbt-core + dbt-bigquery | ✅ (pip) | — |
| Python 3.10+ | ✅ | — |
| gcloud CLI | ✅ | — |
| GCP Project | — | ✅ Create + billing |
| GCS Bucket | — | ✅ Terraform |
| BigQuery Dataset | — | ✅ Terraform |
| Service Account | — | ✅ Terraform |
| Looker Studio | — | ✅ Free web app |

### Cloud Cost Estimate: ~$0–5

- GCP Free Tier: $300 credit for 90 days (new accounts)
- BigQuery: 1 TB/month queries free, 10 GB/month storage free
- GCS: ~$0.02/GB/month — 50 GB of Parquet = ~$1/month
- **Recommendation:** Use `europe-west6` (Zürich) — tiny premium but great talking point in Swiss interviews

---

## 4. Step-by-Step Implementation Plan

### Phase 1: Environment Setup (Local + Cloud)

**What:** Set up local dev environment and GCP infrastructure.

**Deliverables:**
- Docker Desktop running
- GCP project with billing enabled
- Service account JSON key
- Terraform config: 1 GCS bucket (`sbb-punctuality-raw-<initials>`), 1 BQ dataset (`sbb_punctuality`), 1 service account
- `terraform apply` succeeds

**Why:** Everything downstream depends on cloud resources existing.

**Key Decisions:**
- **GCP region:** `europe-west6` (Zürich) — worth the tiny cost for Swiss portfolio relevance
- **Bucket naming:** Must be globally unique — use `sbb-punctuality-raw-<initials>`

**Dependencies:** None.

**Definition of Done:**
- `terraform plan` shows 3–4 resources; `terraform apply` succeeds
- GCS bucket and BQ dataset visible in Console

---

### Phase 2: Data Exploration & Profiling

**What:** Download 1–2 days of Ist-Daten locally. Profile in Jupyter.

**Deliverables:**
- `notebooks/exploration.ipynb` covering:
  - Row counts, column types, null rates
  - Value distributions: `VERKEHRSMITTEL_TEXT`, `AN_PROGNOSE_STATUS`
  - Delay calculation logic validated: `delay_min = (AN_PROGNOSE - ANKUNFTSZEIT) in minutes`
  - Edge cases identified (NULL arrivals, FORECAST status, cancellations)

**Why:** You cannot design a good data model without understanding the data.

**Key Decisions:**
- **How many days for the project?** 30–90 days (enough for temporal analysis, manageable size)
- **All operators or SBB only?** All — richer analysis
- **Filter strategy:** Only `REAL` and `ESTIMATED` status for delay metrics

**Dependencies:** Phase 1.

**Definition of Done:** You can articulate the grain, delay logic, and filter strategy.

---

### Phase 3: Data Ingestion (Raw Layer)

**What:** Build Airflow DAG to ingest daily data.

**Deliverables:**
- `dags/sbb_daily_ingest.py` with tasks:
  1. `download_csv` — Download daily CSV (parameterized by `execution_date`)
  2. `convert_to_parquet` — pandas read → Parquet write
  3. `upload_to_gcs` — Upload to `gs://bucket/raw/ist-daten/YYYY/MM/DD/`
  4. `load_to_bigquery` — GCS → BQ raw table (partition by `BETRIEBSTAG`)
  5. `cleanup_local` — Remove temp files
- `docker-compose.yaml` with Airflow webserver, scheduler, postgres

**Why:** End-to-end orchestrated ingestion = 4/4 on Data Ingestion.

**Key Decisions:**
- **Parquet vs raw CSV in GCS:** Parquet — 5–10x smaller, typed, columnar. Strong talking point.
- **Backfill:** `catchup=True` with `start_date` = 30–90 days ago
- **BQ raw table:** Partitioned by `BETRIEBSTAG`, clustered by `VERKEHRSMITTEL_TEXT`
- **Idempotency:** Use `WRITE_TRUNCATE` per partition to make reruns safe

**Dependencies:** Phase 1, Phase 2.

**Definition of Done:**
- `docker-compose up` starts Airflow
- DAG trigger for 1 day succeeds; data appears in GCS and BQ
- Backfill of 30+ days completes

---

### Phase 4: Data Storage Design

**What:** Document the layer strategy and partitioning rationale.

**Deliverables:**
- Layer definitions in README:
  - `raw.ist_daten` — direct load, partitioned + clustered
  - `staging.stg_stop_events` — cleaned, typed, deduped
  - `intermediate.int_delays` — delay calculations
  - `marts.fct_daily_delays`, `fct_station_delays` — aggregated facts
  - `marts.dim_stations`, `dim_operators`, `dim_transport_types` — dimensions

**Why:** Partitioning/clustering explanation = 4/4 on Data Warehouse.

**Key Decisions:**
- **Partition:** `BETRIEBSTAG` — all queries filter by date range
- **Cluster:** `VERKEHRSMITTEL_TEXT` — most analyses slice by train type
- **Why:** Explain in README: "Partitioning by date eliminates scanning irrelevant days. Clustering by transport type co-locates rows queried together."

**Dependencies:** Phase 2, Phase 3.

**Definition of Done:** README section documents rationale; you can explain it in 2 sentences.

---

### Phase 5: Data Transformation (dbt)

**What:** Build the dbt project.

**Deliverables:**

```
dbt_sbb_punctuality/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── sources/_sources.yml
│   ├── staging/
│   │   ├── _stg_models.yml          # schema tests
│   │   └── stg_stop_events.sql
│   ├── intermediate/
│   │   ├── _int_models.yml
│   │   └── int_delays.sql
│   └── marts/
│       ├── _marts_models.yml
│       ├── fct_daily_delays.sql
│       ├── fct_station_delays.sql
│       ├── dim_stations.sql
│       ├── dim_operators.sql
│       └── dim_transport_types.sql
├── macros/
│   └── parse_swiss_timestamp.sql
└── tests/
    └── assert_delay_reasonable.sql
```

**Key Model Logic:**

- **`stg_stop_events.sql`:** Parse Swiss timestamps, rename to English snake_case, filter out pass-throughs and cancellations, dedup
- **`int_delays.sql`:** `arrival_delay_min = TIMESTAMP_DIFF(actual, scheduled, MINUTE)`, `is_delayed = delay > 0`, `is_significantly_delayed = delay >= 3` (SBB's own threshold), filter to REAL/ESTIMATED only
- **`fct_daily_delays.sql`:** GROUP BY day × transport type × line → `avg_delay`, `pct_on_time`, `pct_delayed_3min`, `total_cancelled`
- **`fct_station_delays.sql`:** GROUP BY station × day → same metrics but station-level
- **Dimensions:** Deduped lookups from staging data

**Dependencies:** Phase 3 (raw data in BQ), Phase 4.

**Definition of Done:** `dbt run` + `dbt test` pass; mart tables queryable in BQ.

---

### Phase 6: Data Quality & Testing

**What:** dbt tests across all layers.

**Deliverables:**
- Schema tests: `not_null` on key fields, `unique` on composites, `accepted_values` on statuses
- Custom test: `assert_delay_reasonable` (delay between -60 and +180 min)
- At least 5 tests total

**Dependencies:** Phase 5.

**Definition of Done:** `dbt test` passes with 0 failures.

---

### Phase 7: Orchestration & Scheduling

**What:** Wire ingestion + dbt into a coherent daily pipeline.

**Deliverables:**
- **DAG 1: `sbb_daily_ingest`** (daily at 06:00 UTC) — download → parquet → GCS → BQ → trigger DAG 2
- **DAG 2: `sbb_dbt_transform`** (triggered) — dbt run staging → intermediate → marts → dbt test
- docker-compose with mounted volumes for DAGs, dbt, credentials

**Key Decisions:**
- **Schedule:** 06:00 UTC (07:00 CET) — T+1 data available by then
- **dbt in Airflow:** `BashOperator` with `dbt run` / `dbt test` (simplest approach)
- **Backfill:** `catchup=True` from 30–90 days ago

**Dependencies:** Phase 3, Phase 5.

**Definition of Done:** End-to-end daily run completes; DAGs visible in Airflow graph view.

---

### Phase 8: Dashboard

**What:** Looker Studio dashboard connected to BigQuery marts.

**Deliverables:**
- **Tile 1 (Categorical):** Bar chart — Average delay by transport type (IC, IR, S, RE, etc.)
- **Tile 2 (Temporal):** Line chart — % on-time by day, with lines per transport type
- **Bonus tile:** Heatmap of delays by hour × day-of-week
- **Filters:** Date range picker, transport type multi-select

**Dependencies:** Phase 5 (mart tables populated).

**Definition of Done:** Dashboard accessible via shareable URL; 2+ tiles; screenshot in README.

---

### Phase 9: Documentation & README

**What:** Portfolio-quality README.

**Deliverables:**
- Project title + one-paragraph problem statement
- Architecture diagram (Mermaid or image)
- Technologies list with versions
- Dashboard screenshots
- Step-by-step setup instructions (copy-pasteable)
- Design decisions (partition/cluster rationale, Parquet choice)
- Known limitations + future improvements

**Definition of Done:** Someone can clone → follow README → pipeline running within 1 hour.

---

### Phase 10: Production Readiness Checklist

- [ ] `terraform destroy` + `terraform apply` works from scratch
- [ ] `docker-compose up` starts Airflow cleanly
- [ ] DAG backfill completes for 30+ days
- [ ] `dbt run` + `dbt test` pass with 0 errors
- [ ] Dashboard loads correctly with 2+ tiles
- [ ] README has diagram, setup steps, screenshots
- [ ] No hardcoded credentials (env vars + `.gitignore`)
- [ ] Repo is public with meaningful name (e.g., `swiss-rail-punctuality`)

---

## 5. Data Model Diagram

### Star Schema

```
                    ┌──────────────────┐
                    │  dim_stations    │
                    │─────────────────│
                    │ station_code (PK)│
                    │ station_name     │
                    │ sloid            │
                    └────────┬─────────┘
                             │
┌──────────────────┐         │         ┌──────────────────────┐
│  dim_operators   │         │         │ dim_transport_types   │
│─────────────────│         │         │─────────────────────-│
│ operator_id (PK) │    ┌────┴────┐    │ transport_type (PK)   │
│ operator_abbr    ├────┤  FACTS  ├────┤ product_id            │
│ operator_name    │    └────┬────┘    │ description           │
└──────────────────┘         │         └───────────────────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
    ┌─────────┴──────────┐      ┌───────────┴────────────┐
    │  fct_daily_delays  │      │  fct_station_delays    │
    │───────────────────│      │───────────────────────│
    │ operating_day      │      │ operating_day           │
    │ transport_type     │      │ station_code            │
    │ line_name          │      │ transport_type          │
    │ operator_id        │      │ total_stop_events       │
    │ total_stop_events  │      │ avg_arrival_delay_min   │
    │ avg_arrival_delay  │      │ median_arrival_delay    │
    │ median_delay       │      │ pct_on_time             │
    │ pct_on_time        │      │ pct_delayed_3min        │
    │ pct_delayed_3min   │      │ total_cancelled         │
    │ max_delay          │      └────────────────────────┘
    │ total_cancelled    │
    └────────────────────┘
```

### Table Grains

| Table | Grain | Approx. Rows (90 days) |
|-------|-------|------------------------|
| `raw.ist_daten` | 1 row per stop event per day | ~225M |
| `stg_stop_events` | 1 row per stop event (cleaned) | ~200M |
| `int_delays` | 1 row per stop event with delays | ~180M |
| `fct_daily_delays` | 1 row per line × type × day | ~50K |
| `fct_station_delays` | 1 row per station × day | ~250K |
| `dim_stations` | 1 row per unique station | ~30K |
| `dim_operators` | 1 row per unique operator | ~100 |
| `dim_transport_types` | 1 row per transport type | ~10 |

### Partitioning & Clustering

| Table | Partition By | Cluster By | Rationale |
|-------|-------------|------------|-----------|
| `raw.ist_daten` | `BETRIEBSTAG` (day) | `VERKEHRSMITTEL_TEXT` | All queries filter by date; most filter by type |
| `fct_daily_delays` | `operating_day` | `transport_type` | Dashboard filters by range + type |
| `fct_station_delays` | `operating_day` | `station_code` | Station drilldowns within date range |

---

## 6. Project File Structure

```
swiss-rail-punctuality/
│
├── README.md                          # Comprehensive documentation
├── .gitignore                         # .env, *.json, __pycache__, .dbt/
├── Makefile                           # (optional) make setup, make run, make test
│
├── terraform/
│   ├── main.tf                        # GCS bucket, BQ dataset, service account
│   ├── variables.tf                   # Project ID, region, bucket name
│   ├── outputs.tf                     # Bucket URL, dataset ID
│   └── terraform.tfvars.example       # Template (actual .tfvars gitignored)
│
├── airflow/
│   ├── docker-compose.yaml            # Airflow 2.x (webserver, scheduler, postgres)
│   ├── Dockerfile                     # Custom image: dbt, pandas, pyarrow
│   ├── requirements.txt               # Python deps
│   ├── dags/
│   │   ├── sbb_daily_ingest.py        # DAG 1: download → parquet → GCS → BQ
│   │   └── sbb_dbt_transform.py       # DAG 2: dbt run → dbt test
│   └── plugins/
│
├── dbt_sbb_punctuality/
│   ├── dbt_project.yml
│   ├── profiles.yml                   # BQ connection via env vars
│   ├── packages.yml                   # dbt_utils (optional)
│   ├── models/
│   │   ├── sources/_sources.yml
│   │   ├── staging/
│   │   │   ├── _stg_models.yml
│   │   │   └── stg_stop_events.sql
│   │   ├── intermediate/
│   │   │   ├── _int_models.yml
│   │   │   └── int_delays.sql
│   │   └── marts/
│   │       ├── _marts_models.yml
│   │       ├── fct_daily_delays.sql
│   │       ├── fct_station_delays.sql
│   │       ├── dim_stations.sql
│   │       ├── dim_operators.sql
│   │       └── dim_transport_types.sql
│   ├── macros/
│   │   └── parse_swiss_timestamp.sql
│   └── tests/
│       └── assert_delay_reasonable.sql
│
├── notebooks/
│   └── exploration.ipynb              # Data profiling (not production)
│
├── images/
│   ├── architecture.png               # For README
│   └── dashboard_screenshot.png
│
└── .github/                           # (optional)
    └── workflows/
        └── ci.yml
```

---

## 7. Evaluation Criteria Mapping

### Grading Breakdown (Max 28 Points)

| Criterion | Max | How to Get 4/4 | Your Plan |
|-----------|-----|-----------------|-----------|
| **Problem description** | 4 | Well-described, clear problem | ✅ SBB punctuality narrative in README |
| **Cloud** | 4 | Cloud + IaC tools | ✅ GCP + Terraform |
| **Data ingestion** | 4 | End-to-end pipeline, multi-step DAG, data lake | ✅ Airflow: download → Parquet → GCS → BQ |
| **Data warehouse** | 4 | Partitioned + clustered with explanation | ✅ Partitioned by date, clustered by type, explained |
| **Transformations** | 4 | dbt, Spark, or similar | ✅ dbt: staging → intermediate → marts |
| **Dashboard** | 4 | 2+ tiles | ✅ Bar chart + trend line in Looker Studio |
| **Reproducibility** | 4 | Clear instructions, code works | ✅ docker-compose, step-by-step README |
| **TOTAL** | **28** | | **28/28 targeted** |

### Must-Haves vs Nice-to-Haves

| Must-Have (Graded) | Nice-to-Have (Extra Mile) |
|--------------------|---------------------------|
| Terraform for GCP | Makefile for one-command setup |
| Airflow DAG with 3+ tasks | GitHub Actions CI/CD |
| GCS as data lake | Soda Core quality checks |
| BQ with partition + cluster | dbt docs site |
| dbt staging + marts | Map visualization |
| 2-tile dashboard | Unit tests for DAGs |
| README with setup instructions | |

### Portfolio Differentiators for Swiss Market

1. **Swiss-specific dataset** — instant conversation starter in interviews at SSD Fribourg or any Swiss employer
2. **Clean README with architecture diagram** — shows communication skills
3. **Explained design decisions** — "I partitioned by date because..." shows thinking
4. **CI/CD** — even basic GitHub Actions sets you apart from 95% of submissions
5. **Makefile** — `make setup && make run` signals DevOps maturity

---

## 8. Risks & Gotchas

### Common Pitfalls

| Risk | Why It's Tricky | Mitigation |
|------|----------------|------------|
| **Airflow + Docker OOM** | Processing 500MB CSVs in Docker is memory-hungry | Set Docker memory to 6–8 GB, process in chunks, limit parallelism |
| **CSV parsing edge cases** | Semicolon delimiter, Swiss date `DD.MM.YYYY`, timestamps with/without seconds | Build robust parsing in Phase 2; test with multiple days |
| **Backfill takes hours** | 90 days × 500MB downloads | Start with 7 days for dev, backfill 30+ for submission, run overnight |
| **dbt + BQ auth in Docker** | Service account JSON must be mounted correctly | docker-compose volume mount, `DBT_PROFILES_DIR` env var |
| **Timestamp timezone** | SBB data is CET/CEST, BigQuery defaults UTC | Document clearly; be consistent (keep CET or convert to UTC in staging) |
| **Large downloads fail** | 500MB can timeout | Retry logic in Airflow (3 retries, exponential backoff), `stream=True` in requests |
| **Ist-Daten v1 → v2 transition** | v1 being deprecated ~2026 in favor of v2 | Check if v2 is available at `data.opentransportdata.swiss/dataset/ist-daten-v2`; schema is very similar |
| **Looker Studio caching** | Dashboard shows stale data | Set freshness to "every 1 hour" in data source |

### Cost Traps

| Trap | How to Avoid |
|------|-------------|
| **BQ full table scans** | Always filter by partition column; use `--dry-run` |
| **Cross-region egress** | Keep everything in same region (all `europe-west6`) |
| **Forgetting `terraform destroy`** | Set calendar reminder. Storage cost is tiny but don't leave things running |
| **Don't deploy Airflow to a VM** | Run locally. In README mention "in production → Cloud Composer / Kubernetes" |

### Ambiguities to Resolve Before Starting

1. **Verify archive URL:** Manually download 1 day before building the DAG. The archive at `archive.opentransportdata.swiss` may have changed URL patterns
2. **Ist-Daten v1 vs v2:** v1 sunset is ~early 2026. Check v2 availability — it adds foreign station data and a new SLOID format
3. **SBB-only vs all operators:** Full dataset is richer but 3–5x larger. Decision depends on your machine's Docker memory
4. **Peer reviewer dashboard access:** Looker Studio must be set to "Anyone with link can view"

---

## Appendix: Suggested Timeline

| Week | Focus | Output |
|------|-------|--------|
| **1** | Phase 1–2: Setup + Exploration | GCP live, notebook done |
| **2** | Phase 3–4: Ingestion + Storage Design | Airflow running, data in GCS + BQ |
| **3** | Phase 5–6: dbt + Tests | All models passing |
| **4** | Phase 7–8: Orchestration + Dashboard | End-to-end daily run, dashboard live |
| **5** | Phase 9–10: Polish + README + Submit | Portfolio-ready repo |

---

*Blueprint designed for the DataTalksClub DE Zoomcamp final project, targeting 28/28 and portfolio readiness for Data Engineering roles in Switzerland.*
