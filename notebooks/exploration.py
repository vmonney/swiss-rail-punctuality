import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path

    import marimo as mo

    from swiss_rail_punctuality.profiling import (
        REQUIRED_COLUMNS,
        column_types,
        compute_delay_minutes,
        delay_summary,
        edge_case_frames,
        ensure_required_columns,
        null_rates,
        profile_overview,
        read_ist_data,
        value_distribution,
    )

    return (
        Path,
        REQUIRED_COLUMNS,
        column_types,
        compute_delay_minutes,
        delay_summary,
        edge_case_frames,
        ensure_required_columns,
        mo,
        null_rates,
        profile_overview,
        read_ist_data,
        value_distribution,
    )


@app.cell
def _(mo):
    mo.md("""
    # Swiss Rail Punctuality - Phase 2 (marimo)

    This notebook profiles 1-2 days of Ist-Daten and validates delay logic
    before building ingestion/modeling layers.
    """)
    return


@app.cell
def _(Path):
    import os

    default_csv = Path("data/raw/sample/ist_daten_sample_real_sbb.csv")
    default_parquet = Path("data/raw/sample/ist_daten_sample_real_sbb.parquet")
    default_data_path = default_csv if default_csv.exists() else default_parquet
    data_path = Path(os.getenv("MARIMO_DATA_PATH", str(default_data_path)))
    return (data_path,)


@app.cell
def _(data_path, ensure_required_columns, mo, read_ist_data):
    if not data_path.exists():
        mo.stop(
            True,
            mo.md(
                "Data file not found: "
                f"`{data_path}`. Place a sample file under `data/raw/sample/` "
                "or set `MARIMO_DATA_PATH` and rerun."
            ),
        )

    df = read_ist_data(data_path)
    ensure_required_columns(df)
    mo.md(f"Loaded `{data_path}` with `{len(df):,}` rows and `{len(df.columns)}` columns.")
    return (df,)


@app.cell
def _(mo):
    mo.md("""
    ## Profiling Overview
    """)
    return


@app.cell
def _(df, profile_overview):
    profile_overview(df)
    return


@app.cell
def _(column_types, df):
    column_types(df)
    return


@app.cell
def _(df, null_rates):
    null_rates(df)
    return


@app.cell
def _(mo):
    mo.md("""
    ## Value Distributions
    """)
    return


@app.cell
def _(df, value_distribution):
    value_distribution(df, "VERKEHRSMITTEL_TEXT").head(20)
    return


@app.cell
def _(df, value_distribution):
    value_distribution(df, "AN_PROGNOSE_STATUS")
    return


@app.cell
def _(mo):
    mo.md("""
    ## Delay Logic Validation
    """)
    return


@app.cell
def _(compute_delay_minutes, df):
    delay_min = compute_delay_minutes(df)
    delay_min.head(10).rename("delay_min")
    return (delay_min,)


@app.cell
def _(delay_min, delay_summary):
    delay_summary(delay_min)
    return


@app.cell
def _(delay_min):
    delay_outliers = delay_min[(delay_min < -60) | (delay_min > 180)].to_frame(name="delay_min")
    delay_outliers.head(20)
    return


@app.cell
def _(mo):
    mo.md("""
    ## Edge Cases
    """)
    return


@app.cell
def _(df, edge_case_frames):
    edge_cases = edge_case_frames(df)
    {
        "null_arrivals": len(edge_cases["null_arrivals"]),
        "non_delay_status": len(edge_cases["non_delay_status"]),
        "cancelled": len(edge_cases["cancelled"]),
    }
    return (edge_cases,)


@app.cell
def _(edge_cases):
    edge_cases["null_arrivals"].head(10)
    return


@app.cell
def _(edge_cases):
    edge_cases["non_delay_status"].head(10)
    return


@app.cell
def _(edge_cases):
    edge_cases["cancelled"].head(10)
    return


@app.cell
def _(REQUIRED_COLUMNS, mo):
    mo.md(f"""
    ## Decision Log (Portfolio Notes)

    - Grain candidate: one stop event record per train movement update.
    - Delay KPI filter: only `REAL` and `ESTIMATED` statuses.
    - Coverage target: 30-90 days for trends with manageable local backfills.
    - Operator scope: all operators for richer Swiss network analysis.
    - Required raw columns validated in this phase: `{", ".join(REQUIRED_COLUMNS)}`.
    """)
    return


if __name__ == "__main__":
    app.run()
