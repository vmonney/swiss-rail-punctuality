from __future__ import annotations

import pendulum
from airflow.operators.bash import BashOperator

from airflow import DAG

DBT_PROJECT_DIR = "/opt/airflow/dbt_sbb_punctuality"
DBT_PROFILES_DIR = "/opt/airflow/dbt_sbb_punctuality"
DBT_RUNTIME_DIR = "/tmp/dbt_sbb_punctuality_runtime"
DBT_LOG_PATH = "/tmp/dbt_sbb_punctuality_logs"
DBT_TARGET_PATH = "/tmp/dbt_sbb_punctuality_target"

dbt_env = {
    "DBT_PROJECT_DIR": DBT_PROJECT_DIR,
    "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
    "DBT_RUNTIME_DIR": DBT_RUNTIME_DIR,
    "DBT_LOG_PATH": DBT_LOG_PATH,
    "DBT_TARGET_PATH": DBT_TARGET_PATH,
    "PYTHONNOUSERSITE": "1",
}

with DAG(
    dag_id="sbb_dbt_transform",
    description="Triggered dbt pipeline: deps -> run -> test for Swiss rail punctuality models.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["phase7", "dbt", "transform"],
) as dag:
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            "rm -rf \"$DBT_RUNTIME_DIR\" "
            "&& mkdir -p \"$DBT_RUNTIME_DIR\" \"$DBT_LOG_PATH\" \"$DBT_TARGET_PATH\" "
            "&& cp -R \"$DBT_PROJECT_DIR\"/. \"$DBT_RUNTIME_DIR\"/ "
            "&& dbt deps --project-dir \"$DBT_RUNTIME_DIR\" --profiles-dir \"$DBT_RUNTIME_DIR\" "
            "--log-path \"$DBT_LOG_PATH\""
        ),
        env=dbt_env,
        append_env=True,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "mkdir -p \"$DBT_LOG_PATH\" \"$DBT_TARGET_PATH\" "
            "&& dbt run --project-dir \"$DBT_RUNTIME_DIR\" --profiles-dir \"$DBT_RUNTIME_DIR\" "
            "--log-path \"$DBT_LOG_PATH\""
        ),
        env=dbt_env,
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "mkdir -p \"$DBT_LOG_PATH\" \"$DBT_TARGET_PATH\" "
            "&& dbt test --project-dir \"$DBT_RUNTIME_DIR\" --profiles-dir \"$DBT_RUNTIME_DIR\" "
            "--log-path \"$DBT_LOG_PATH\""
        ),
        env=dbt_env,
        append_env=True,
    )

    dbt_deps >> dbt_run >> dbt_test
