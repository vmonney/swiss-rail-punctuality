from __future__ import annotations

import logging
import os
import re
import shutil
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import pendulum
import requests
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

from airflow import DAG

DATASET_PAGE_URL = "https://data.opentransportdata.swiss/en/dataset/istdaten"
DOWNLOAD_LINK_PATTERN = re.compile(
    r'href="(?P<link>[^"]*/download/(?P<date>\d{4}-\d{2}-\d{2})_istdaten\.csv)"',
    re.IGNORECASE,
)
REQUIRED_COLUMNS = {
    "BETRIEBSTAG",
    "VERKEHRSMITTEL_TEXT",
    "AN_PROGNOSE_STATUS",
    "ANKUNFTSZEIT",
    "AN_PROGNOSE",
    "FAELLT_AUS_TF",
}
MAX_BACKFILL_DAYS = 7

TMP_ROOT = Path("/opt/airflow/tmp")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
RAW_BUCKET = os.environ.get("RAW_BUCKET", "")
BQ_DATASET = os.environ.get("BQ_DATASET", "")
BQ_TABLE = os.environ.get("BQ_TABLE", "ist_daten_raw")
INGEST_START_DATE = os.environ.get("INGEST_START_DATE", "2026-03-01")

log = logging.getLogger(__name__)


def _require_env(value: str, key: str) -> str:
    if not value:
        raise AirflowFailException(
            f"Missing required environment variable: {key}. "
            "Set it in airflow/.env before starting services."
        )
    return value


def _resolve_run_date(dag_run_conf: dict[str, object]) -> str | None:
    run_date = dag_run_conf.get("run_date")
    if run_date in (None, ""):
        return None

    if not isinstance(run_date, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", run_date):
        raise AirflowFailException(
            "Invalid dag_run.conf['run_date'] format. Use YYYY-MM-DD."
        )

    try:
        pendulum.from_format(run_date, "YYYY-MM-DD")
    except ValueError as exc:
        raise AirflowFailException(
            "Invalid dag_run.conf['run_date'] value. Use a real calendar date in YYYY-MM-DD."
        ) from exc

    return run_date


def _resolve_backfill_days(dag_run_conf: dict[str, object]) -> int:
    backfill_days = dag_run_conf.get("backfill_days", 1)
    if not isinstance(backfill_days, int):
        raise AirflowFailException("dag_run.conf['backfill_days'] must be an integer.")
    if backfill_days < 1 or backfill_days > MAX_BACKFILL_DAYS:
        raise AirflowFailException(
            f"dag_run.conf['backfill_days'] must be between 1 and {MAX_BACKFILL_DAYS}."
        )
    return backfill_days


def _resolve_mode(dag_run_conf: dict[str, object]) -> str:
    mode = dag_run_conf.get("mode", "latest")
    if not isinstance(mode, str):
        raise AirflowFailException("dag_run.conf['mode'] must be a string.")
    normalized_mode = mode.lower().strip()
    if normalized_mode not in {"latest", "date"}:
        raise AirflowFailException("dag_run.conf['mode'] must be either 'latest' or 'date'.")
    return normalized_mode


def _resolve_target_dates(
    context: dict[str, object],
    available_dates: list[str],
) -> list[str]:
    if not available_dates:
        raise AirflowFailException("No downloadable dates found on dataset page.")

    params_config = context.get("params", {})
    if not isinstance(params_config, dict):
        params_config = {}
    effective_config = dict(params_config)

    dag_run = context.get("dag_run")
    dag_run_conf = getattr(dag_run, "conf", {}) if dag_run else {}
    if isinstance(dag_run_conf, dict):
        effective_config.update(dag_run_conf)

    mode = _resolve_mode(effective_config)
    run_date = _resolve_run_date(effective_config)
    backfill_days = _resolve_backfill_days(effective_config)

    if mode == "date":
        if not run_date:
            raise AirflowFailException(
                "dag_run.conf['run_date'] is required when dag_run.conf['mode'] = 'date'."
            )
        anchor_date = run_date
    else:
        anchor_date = available_dates[-1]

    if anchor_date not in available_dates:
        raise AirflowFailException(
            f"Requested run date {anchor_date} is not available in published archive."
        )

    anchor_idx = available_dates.index(anchor_date)
    start_idx = max(0, anchor_idx - backfill_days + 1)
    target_dates = available_dates[start_idx : anchor_idx + 1]

    log.info(
        "Selected target dates=%s (mode=%s, backfill_days=%s)",
        target_dates,
        mode,
        backfill_days,
    )
    return target_dates


def _betriebstag_date_sql(column_ref: str) -> str:
    return (
        "COALESCE("
        f"SAFE_CAST({column_ref} AS DATE), "
        f"SAFE.PARSE_DATE('%Y-%m-%d', CAST({column_ref} AS STRING)), "
        f"SAFE.PARSE_DATE('%d.%m.%Y', CAST({column_ref} AS STRING))"
        ")"
    )


def _extract_link_for_date(html: str, execution_date: str) -> str:
    links_by_date: dict[str, str] = {}
    for match in DOWNLOAD_LINK_PATTERN.finditer(html):
        date_str = match.group("date")
        rel_or_abs_link = match.group("link")
        links_by_date[date_str] = urljoin(DATASET_PAGE_URL, rel_or_abs_link)

    if execution_date not in links_by_date:
        raise AirflowFailException(
            f"No download URL found for execution_date={execution_date}. "
            "Verify the archive has published this day."
        )
    return links_by_date[execution_date]


def _extract_available_dates(html: str) -> list[str]:
    dates = {match.group("date") for match in DOWNLOAD_LINK_PATTERN.finditer(html)}
    return sorted(dates)


def download_csv(**context: object) -> dict[str, list[str]]:
    page_resp = requests.get(DATASET_PAGE_URL, timeout=60)
    page_resp.raise_for_status()
    available_dates = _extract_available_dates(page_resp.text)
    target_dates = _resolve_target_dates(context, available_dates)
    csv_paths: list[str] = []

    for target_date in target_dates:
        run_dir = TMP_ROOT / target_date.replace("-", "")
        run_dir.mkdir(parents=True, exist_ok=True)
        csv_path = run_dir / f"{target_date}_istdaten.csv"
        download_url = _extract_link_for_date(page_resp.text, target_date)

        log.info("Downloading %s to %s", download_url, csv_path)
        with requests.get(download_url, stream=True, timeout=180) as resp:
            resp.raise_for_status()
            with csv_path.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        if not csv_path.exists() or csv_path.stat().st_size == 0:
            raise AirflowFailException(f"Downloaded file is empty: {csv_path}")
        csv_paths.append(str(csv_path))

    return {"target_dates": target_dates, "csv_paths": csv_paths}


def convert_to_parquet(**context: object) -> dict[str, list[str]]:
    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="download_csv")
    if not isinstance(payload, dict):
        raise AirflowFailException("download_csv output payload is invalid.")
    target_dates = payload.get("target_dates")
    csv_paths = payload.get("csv_paths")
    if not isinstance(target_dates, list) or not isinstance(csv_paths, list):
        raise AirflowFailException("download_csv output payload is missing expected keys.")
    if len(target_dates) != len(csv_paths):
        raise AirflowFailException("download_csv output payload has mismatched list lengths.")

    parquet_paths: list[str] = []

    for csv_path_str in csv_paths:
        csv_path = Path(csv_path_str)
        if not csv_path.exists():
            raise AirflowFailException(f"CSV file does not exist: {csv_path}")

        df = pd.read_csv(csv_path, sep=";", low_memory=False)
        missing_columns = REQUIRED_COLUMNS - set(df.columns)
        if missing_columns:
            raise AirflowFailException(
                f"CSV is missing required columns: {sorted(missing_columns)}"
            )
        if df.empty:
            raise AirflowFailException(f"CSV contains no rows: {csv_path}")

        parquet_path = csv_path.with_suffix(".parquet")
        df.to_parquet(parquet_path, index=False)

        if parquet_path.stat().st_size == 0:
            raise AirflowFailException(f"Parquet file is empty: {parquet_path}")

        parquet_paths.append(str(parquet_path))

    return {"target_dates": target_dates, "parquet_paths": parquet_paths}


def upload_to_gcs(**context: object) -> list[dict[str, str]]:
    project_id = _require_env(PROJECT_ID, "GCP_PROJECT_ID")
    bucket_name = _require_env(RAW_BUCKET, "RAW_BUCKET")

    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="convert_to_parquet")
    if not isinstance(payload, dict):
        raise AirflowFailException("convert_to_parquet output payload is invalid.")
    target_dates = payload.get("target_dates")
    parquet_paths = payload.get("parquet_paths")
    if not isinstance(target_dates, list) or not isinstance(parquet_paths, list):
        raise AirflowFailException(
            "convert_to_parquet output payload is missing expected keys."
        )
    if len(target_dates) != len(parquet_paths):
        raise AirflowFailException(
            "convert_to_parquet output payload has mismatched list lengths."
        )

    uploads: list[dict[str, str]] = []
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)

    for target_date, parquet_path_str in zip(target_dates, parquet_paths, strict=True):
        parquet_path = Path(parquet_path_str)
        if not parquet_path.exists():
            raise AirflowFailException(f"Parquet file does not exist: {parquet_path}")

        year, month, day = target_date.split("-")
        object_name = f"raw/ist-daten/{year}/{month}/{day}/{parquet_path.name}"
        blob = bucket.blob(object_name)
        blob.upload_from_filename(parquet_path)

        gcs_uri = f"gs://{bucket_name}/{object_name}"
        log.info("Uploaded %s", gcs_uri)
        uploads.append({"target_date": target_date, "gcs_uri": gcs_uri})

    return uploads


def load_to_bigquery(**context: object) -> None:
    project_id = _require_env(PROJECT_ID, "GCP_PROJECT_ID")
    dataset = _require_env(BQ_DATASET, "BQ_DATASET")

    ti = context["ti"]
    uploads = ti.xcom_pull(task_ids="upload_to_gcs")
    if not isinstance(uploads, list):
        raise AirflowFailException("upload_to_gcs output payload is invalid.")
    if not uploads:
        raise AirflowFailException("No uploaded files found to load into BigQuery.")

    raw_table_id = f"{project_id}.{dataset}.{BQ_TABLE}"

    bq_client = bigquery.Client(project=project_id)

    load_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        autodetect=True,
    )
    temp_betriebstag_date_sql = _betriebstag_date_sql("t.BETRIEBSTAG")

    for upload in uploads:
        target_date = upload.get("target_date")
        gcs_uri = upload.get("gcs_uri")
        if not isinstance(target_date, str) or not isinstance(gcs_uri, str):
            raise AirflowFailException("upload_to_gcs output entries must contain strings.")

        temp_table_id = (
            f"{project_id}.{dataset}._tmp_{BQ_TABLE}_{target_date.replace('-', '')}"
        )

        bq_client.load_table_from_uri(
            gcs_uri,
            destination=temp_table_id,
            job_config=load_config,
        ).result()

        create_raw_sql = f"""
        CREATE TABLE IF NOT EXISTS `{raw_table_id}`
        PARTITION BY betriebstag_date
        CLUSTER BY VERKEHRSMITTEL_TEXT
        AS
        SELECT
          t.*,
          {temp_betriebstag_date_sql} AS betriebstag_date
        FROM `{temp_table_id}` AS t
        WHERE 1 = 0
        """
        bq_client.query(create_raw_sql).result()
        bq_client.query(
            f"ALTER TABLE `{raw_table_id}` ADD COLUMN IF NOT EXISTS betriebstag_date DATE"
        ).result()

        upsert_partition_sql = f"""
        DELETE FROM `{raw_table_id}`
        WHERE betriebstag_date = DATE('{target_date}');

        INSERT INTO `{raw_table_id}`
        SELECT
          t.*,
          {temp_betriebstag_date_sql} AS betriebstag_date
        FROM `{temp_table_id}` AS t
        WHERE {temp_betriebstag_date_sql} = DATE('{target_date}');
        """
        bq_client.query(upsert_partition_sql).result()

        try:
            bq_client.delete_table(temp_table_id, not_found_ok=False)
        except NotFound:
            log.warning("Temporary table already removed: %s", temp_table_id)


def cleanup_local(**context: object) -> None:
    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="download_csv")
    if not isinstance(payload, dict):
        log.info("No download payload found; skipping local cleanup.")
        return

    target_dates = payload.get("target_dates", [])
    if not isinstance(target_dates, list):
        log.info("download_csv payload has no target_dates list; skipping local cleanup.")
        return

    for target_date in target_dates:
        if not isinstance(target_date, str):
            continue
        run_dir = TMP_ROOT / target_date.replace("-", "")
        if run_dir.exists():
            shutil.rmtree(run_dir)
            log.info("Deleted temp directory %s", run_dir)


with DAG(
    dag_id="sbb_daily_ingest",
    description="Daily Swiss ISTDATEN ingestion: CSV -> Parquet -> GCS -> BigQuery raw.",
    schedule="0 6 * * *",
    start_date=pendulum.parse(INGEST_START_DATE, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    params={
        "mode": Param(
            default="latest",
            type="string",
            enum=["latest", "date"],
            description="latest=latest published day, date=use run_date.",
        ),
        "run_date": Param(
            default=None,
            type=["null", "string"],
            description="Required only when mode=date. Format: YYYY-MM-DD.",
        ),
        "backfill_days": Param(
            default=1,
            type="integer",
            minimum=1,
            maximum=MAX_BACKFILL_DAYS,
            description=(
                "Number of days to process including anchor day (1-7). "
                "Uses previous available days."
            ),
        ),
    },
    tags=["phase3", "ingestion", "raw"],
) as dag:
    download_csv_task = PythonOperator(
        task_id="download_csv",
        python_callable=download_csv,
    )

    convert_to_parquet_task = PythonOperator(
        task_id="convert_to_parquet",
        python_callable=convert_to_parquet,
    )

    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )

    load_to_bigquery_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
    )

    cleanup_local_task = PythonOperator(
        task_id="cleanup_local",
        python_callable=cleanup_local,
        trigger_rule="all_done",
    )

    (
        download_csv_task
        >> convert_to_parquet_task
        >> upload_to_gcs_task
        >> load_to_bigquery_task
        >> cleanup_local_task
    )
