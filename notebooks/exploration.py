import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
    from pathlib import Path

    import altair as alt
    import marimo as mo
    import pandas as pd

    from swiss_rail_punctuality.profiling import (
        REQUIRED_COLUMNS,
        column_types,
        compute_delay_minutes,
        delay_summary,
        edge_case_frames,
        ensure_required_columns,
        null_rates,
        parse_swiss_timestamp,
        profile_overview,
        read_ist_data,
        value_distribution,
    )

    return (
        Path,
        REQUIRED_COLUMNS,
        alt,
        column_types,
        compute_delay_minutes,
        delay_summary,
        edge_case_frames,
        ensure_required_columns,
        mo,
        null_rates,
        os,
        parse_swiss_timestamp,
        pd,
        profile_overview,
        read_ist_data,
        value_distribution,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Swiss Rail Punctuality - Phase 2 Exploration

    This marimo app explores the latest full-day ISTDATEN file and documents
    the key profiling outputs required for Phase 2:

    - row counts, types, null rates
    - value distributions (`VERKEHRSMITTEL_TEXT`, `AN_PROGNOSE_STATUS`)
    - validated delay logic
    - edge-case identification (`NULL` arrivals, status filtering, cancellations)
    """)
    return


@app.cell
def _(Path, mo, os):
    data_dir = Path("data/raw/full")
    available_files = sorted(data_dir.glob("*_istdaten.csv"), reverse=True)

    env_data_path = os.getenv("MARIMO_DATA_PATH")
    if env_data_path:
        default_data_path = Path(env_data_path)
    elif available_files:
        default_data_path = available_files[0]
    else:
        default_data_path = data_dir / "YYYY-MM-DD_istdaten.csv"

    file_map = {p.name: str(p) for p in available_files[:30]}
    default_name = default_data_path.name
    if file_map and default_name not in file_map:
        default_name = next(iter(file_map))
    picker = mo.ui.dropdown(
        options=list(file_map.keys()),
        value=default_name,
        label="Data file",
    )
    return available_files, data_dir, file_map, picker


@app.cell
def _(available_files, data_dir, mo, picker):
    if not available_files:
        mo.stop(
            True,
            mo.md(
                "No full-day files found under "
                f"`{data_dir}`. Run `uv run python fetch_raw.py --days 2` first."
            ),
        )
    mo.vstack(
        [
            mo.md(
                f"### File selection\nFound `{len(available_files)}` daily files "
                f"under `{data_dir}`. Exploring the latest by default."
            ),
            picker,
        ]
    )
    return


@app.cell
def _(Path, file_map, picker):
    data_path = Path(file_map[picker.value])
    return (data_path,)


@app.cell
def _(data_path, ensure_required_columns, mo, read_ist_data):
    if not data_path.exists():
        mo.stop(True, mo.md(f"Data file not found: `{data_path}`"))

    df = read_ist_data(data_path)
    ensure_required_columns(df)
    mo.md(
        f"Loaded `{data_path}` with `{len(df):,}` rows and `{len(df.columns)}` columns."
    )
    return (df,)


@app.cell
def _(df, edge_case_frames, parse_swiss_timestamp, pd, profile_overview):
    overview = profile_overview(df)

    status = df["AN_PROGNOSE_STATUS"].fillna("").astype(str).str.upper()
    valid_delay_status = status.isin(["REAL", "ESTIMATED"])
    cancelled = (
        df["FAELLT_AUS_TF"].astype("string").str.lower().isin(["true", "1", "t", "yes"])
    )
    arrival_sched = parse_swiss_timestamp(df["ANKUNFTSZEIT"])
    arrival_actual = parse_swiss_timestamp(df["AN_PROGNOSE"])
    computable_delay = (
        valid_delay_status & ~cancelled & arrival_sched.notna() & arrival_actual.notna()
    )

    edge_cases = edge_case_frames(df)
    kpi = pd.DataFrame(
        [
            {"metric": "rows_total", "value": overview["row_count"]},
            {"metric": "columns_total", "value": overview["column_count"]},
            {"metric": "rows_real_or_estimated", "value": int(valid_delay_status.sum())},
            {"metric": "rows_cancelled", "value": int(cancelled.sum())},
            {"metric": "rows_with_computable_arrival_delay", "value": int(computable_delay.sum())},
            {"metric": "rows_null_ankunftszeit", "value": len(edge_cases["null_arrivals"])},
            {"metric": "rows_non_delay_status", "value": len(edge_cases["non_delay_status"])},
        ]
    )
    return cancelled, computable_delay, edge_cases, kpi, valid_delay_status


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Portfolio Visuals
    """)
    return


@app.cell
def _(alt, df, mo, value_distribution):
    transport_dist = value_distribution(df, "VERKEHRSMITTEL_TEXT").head(12)
    transport_chart = (
        alt.Chart(transport_dist)
        .mark_bar()
        .encode(
            x=alt.X("count:Q", title="Rows"),
            y=alt.Y("value:N", sort="-x", title="Transport type"),
            tooltip=["value", "count", "share_pct"],
        )
        .properties(title="Top transport types by row volume", width=700, height=320)
    )
    mo.ui.altair_chart(transport_chart)
    return


@app.cell
def _(alt, df, mo, value_distribution):
    status_dist = value_distribution(df, "AN_PROGNOSE_STATUS")
    status_chart = (
        alt.Chart(status_dist)
        .mark_arc(innerRadius=65)
        .encode(
            theta=alt.Theta("count:Q", title="Rows"),
            color=alt.Color("value:N", title="Status"),
            tooltip=["value", "count", "share_pct"],
        )
        .properties(title="Arrival status composition", width=450, height=300)
    )
    mo.ui.altair_chart(status_chart)
    return


@app.cell
def _(cancelled, delay_min, mo, valid_delay_status):
    delay_filtered = delay_min[valid_delay_status & ~cancelled].dropna()
    delay_hist_df = delay_filtered.to_frame(name="delay_min")

    min_delay = mo.ui.slider(
        start=-120,
        stop=60,
        step=5,
        value=-20,
        label="Min delay (minutes)",
    )
    max_delay = mo.ui.slider(
        start=-30,
        stop=240,
        step=5,
        value=60,
        label="Max delay (minutes)",
    )
    bin_width = mo.ui.slider(
        start=1,
        stop=20,
        step=1,
        value=2,
        label="Bin width (minutes)",
    )
    mo.hstack([min_delay, max_delay, bin_width])
    return bin_width, delay_hist_df, max_delay, min_delay


@app.cell
def _(alt, bin_width, delay_hist_df, max_delay, min_delay, mo):
    if min_delay.value >= max_delay.value:
        mo.stop(
            True,
            mo.md("Set `Min delay` lower than `Max delay` to render the histogram."),
        )

    focused = delay_hist_df[
        (delay_hist_df["delay_min"] >= min_delay.value)
        & (delay_hist_df["delay_min"] <= max_delay.value)
    ]

    delay_chart = (
        alt.Chart(focused)
        .mark_bar()
        .encode(
            x=alt.X(
                "delay_min:Q",
                bin=alt.Bin(step=bin_width.value),
                scale=alt.Scale(domain=[min_delay.value, max_delay.value]),
                title="Arrival delay (minutes, focused range)",
            ),
            y=alt.Y("count():Q", title="Row count"),
            tooltip=[alt.Tooltip("count():Q", title="Rows in bin")],
        )
        .properties(
            title="Arrival delay distribution (REAL + ESTIMATED, focused)",
            width=700,
            height=320,
        )
    )
    mo.vstack(
        [
            mo.md(
                f"Rows in selected range: `{len(focused):,}` / `{len(delay_hist_df):,}`."
            ),
            mo.ui.altair_chart(delay_chart),
        ]
    )
    return


@app.cell
def _(column_types, df):
    column_types(df)
    return


@app.cell
def _(df, null_rates):
    null_rates(df).head(25)
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
def _(compute_delay_minutes, delay_summary, df):
    delay_min = compute_delay_minutes(df)
    summary = delay_summary(delay_min)
    return delay_min, summary


@app.cell
def _(cancelled, delay_min, df, parse_swiss_timestamp, pd, valid_delay_status):
    manual = (
        (
            parse_swiss_timestamp(df["AN_PROGNOSE"]) - parse_swiss_timestamp(df["ANKUNFTSZEIT"])
        ).dt.total_seconds()
        / 60
    ).round(3)
    comparison_mask = valid_delay_status & ~cancelled & manual.notna()
    max_abs_diff = (delay_min[comparison_mask] - manual[comparison_mask]).abs().max()
    validation = pd.DataFrame(
        [
            {"check": "formula_match_max_abs_diff_min", "value": float(max_abs_diff or 0.0)},
            {"check": "validated_rows", "value": int(comparison_mask.sum())},
        ]
    )
    return (validation,)


@app.cell
def _(delay_min):
    delay_min[(delay_min < -60) | (delay_min > 180)].to_frame(name="delay_min").head(20)
    return


@app.cell
def _(edge_cases):
    {
        "null_arrivals": edge_cases["null_arrivals"].head(10),
        "non_delay_status": edge_cases["non_delay_status"].head(10),
        "cancelled": edge_cases["cancelled"].head(10),
    }
    return


@app.cell(hide_code=True)
def _(REQUIRED_COLUMNS, computable_delay, mo, summary, validation):
    if "count" in set(summary["metric"].astype(str)):
        valid_delay_rows = int(summary.loc[summary["metric"] == "count", "value"].iloc[0])
    else:
        valid_delay_rows = 0

    checklist = [
        (
            "- [x] Row counts and column counts are computed "
            f"(`{int(computable_delay.shape[0]):,}` rows checked)."
        ),
        "- [x] Column type profile is displayed for all columns.",
        "- [x] Null rates are profiled and ranked.",
        "- [x] Value distributions are shown for `VERKEHRSMITTEL_TEXT` and `AN_PROGNOSE_STATUS`.",
        (
            "- [x] Delay logic validated; max formula diff = "
            f"`{validation.iloc[0]['value']}` minutes."
        ),
        "- [x] Edge cases identified: NULL arrivals, non-delay statuses, cancellations.",
        (
            "- [x] Delay summary produced (count, mean, median, p95, min, max) "
            f"with `{valid_delay_rows:,}` valid rows."
        ),
    ]

    mo.md(f"""
    ## Phase 2 Compliance Checklist

    {'\n'.join(checklist)}

    ### Decision log for portfolio narrative

    - **Grain:** one row = one journey stop event.
    - **Delay metric scope:** keep `REAL` / `ESTIMATED`; exclude cancellations and
      non-computable rows.
    - **Scale strategy:** local exploration on 1 day, then 30-90 day backfill in ingestion phase.
    - **Coverage choice:** all operators to make the analysis network-wide.
    - **Critical columns validated:** `{", ".join(REQUIRED_COLUMNS)}`.
    """)
    return


@app.cell
def _(data_path, df, kpi, mo, summary, validation, value_distribution):
    rows_total = int(kpi.loc[kpi["metric"] == "rows_total", "value"].iloc[0])
    delay_ready_rows = int(
        kpi.loc[kpi["metric"] == "rows_with_computable_arrival_delay", "value"].iloc[0]
    )
    cancelled_rows = int(kpi.loc[kpi["metric"] == "rows_cancelled", "value"].iloc[0])
    non_delay_status_rows = int(
        kpi.loc[kpi["metric"] == "rows_non_delay_status", "value"].iloc[0]
    )

    mean_delay = float(summary.loc[summary["metric"] == "mean", "value"].iloc[0])
    p95_delay = float(summary.loc[summary["metric"] == "p95", "value"].iloc[0])
    max_diff = float(
        validation.loc[
            validation["check"] == "formula_match_max_abs_diff_min",
            "value",
        ].iloc[0]
    )

    top_transport = value_distribution(df, "VERKEHRSMITTEL_TEXT").head(1)
    top_transport_name = str(top_transport.iloc[0]["value"])
    top_transport_share = float(top_transport.iloc[0]["share_pct"])

    executive_bullets = [
        f"- Dataset profiled: `{data_path.name}` with `{rows_total:,}` stop-event rows.",
        (
            "- Delay-ready coverage: "
            f"`{delay_ready_rows:,}` rows after filtering to `REAL`/`ESTIMATED`, "
            "excluding cancellations and null arrival timestamps."
        ),
        (
            f"- Delay behavior snapshot: mean `{mean_delay:.2f}` min, "
            f"p95 `{p95_delay:.2f}` min."
        ),
        (
            "- Data quality highlights: "
            f"`{cancelled_rows:,}` cancellations and "
            f"`{non_delay_status_rows:,}` non-delay statuses identified."
        ),
        (
            "- Formula validation: "
            f"`delay_min = AN_PROGNOSE - ANKUNFTSZEIT` with max absolute diff "
            f"`{max_diff:.3f}` minutes."
        ),
        (
            "- Network composition: top transport category is "
            f"`{top_transport_name}` at `{top_transport_share:.2f}%` of rows."
        ),
    ]

    mo.md(
        f"""
    ## Executive Summary (README-ready)

    Use these bullets directly in your Phase 2 README or portfolio page:

    {chr(10).join(executive_bullets)}
    """
    )
    return


if __name__ == "__main__":
    app.run()
