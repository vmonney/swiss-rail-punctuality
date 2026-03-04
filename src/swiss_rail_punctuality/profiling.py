from __future__ import annotations

from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = (
    "BETRIEBSTAG",
    "VERKEHRSMITTEL_TEXT",
    "AN_PROGNOSE_STATUS",
    "ANKUNFTSZEIT",
    "AN_PROGNOSE",
    "FAELLT_AUS_TF",
)


def read_ist_data(path: str | Path) -> pd.DataFrame:
    file_path = Path(path)
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(file_path, sep=";", low_memory=False)
    if suffix == ".parquet":
        return pd.read_parquet(file_path)
    raise ValueError(f"Unsupported file extension: {suffix}. Use .csv or .parquet.")


def ensure_required_columns(df: pd.DataFrame, required: tuple[str, ...] = REQUIRED_COLUMNS) -> None:
    missing = [column for column in required if column not in df.columns]
    if missing:
        missing_cols = ", ".join(missing)
        raise ValueError(f"Missing required columns: {missing_cols}")


def profile_overview(df: pd.DataFrame) -> dict[str, int]:
    return {
        "row_count": int(df.shape[0]),
        "column_count": int(df.shape[1]),
    }


def column_types(df: pd.DataFrame) -> pd.DataFrame:
    return (
        pd.DataFrame({"column": df.columns, "dtype": df.dtypes.astype(str).values})
        .sort_values("column")
        .reset_index(drop=True)
    )


def null_rates(df: pd.DataFrame) -> pd.DataFrame:
    total_rows = len(df)
    if total_rows == 0:
        result = pd.DataFrame({"column": df.columns, "null_count": 0, "null_rate_pct": 0.0})
        return result.sort_values("column").reset_index(drop=True)

    null_count = df.isna().sum()
    null_rate_pct = (null_count / total_rows * 100).round(3)

    return (
        pd.DataFrame(
            {
                "column": null_count.index,
                "null_count": null_count.values.astype(int),
                "null_rate_pct": null_rate_pct.values,
            }
        )
        .sort_values(["null_rate_pct", "column"], ascending=[False, True])
        .reset_index(drop=True)
    )


def value_distribution(df: pd.DataFrame, column: str) -> pd.DataFrame:
    counts = df[column].fillna("NULL").astype(str).value_counts(dropna=False)
    total = max(int(counts.sum()), 1)
    dist = pd.DataFrame(
        {
            "value": counts.index,
            "count": counts.values.astype(int),
            "share_pct": (counts.values / total * 100).round(3),
        }
    )
    return dist.reset_index(drop=True)


def parse_swiss_timestamp(series: pd.Series) -> pd.Series:
    values = series.astype("string").str.strip()
    parsed_with_seconds = pd.to_datetime(
        values, format="%d.%m.%Y %H:%M:%S", errors="coerce", dayfirst=True
    )
    parsed_without_seconds = pd.to_datetime(
        values, format="%d.%m.%Y %H:%M", errors="coerce", dayfirst=True
    )
    parsed_iso = pd.to_datetime(values, errors="coerce", utc=False)
    return parsed_with_seconds.fillna(parsed_without_seconds).fillna(parsed_iso)


def compute_delay_minutes(df: pd.DataFrame) -> pd.Series:
    arrival_actual = parse_swiss_timestamp(df["ANKUNFTSZEIT"])
    arrival_forecast = parse_swiss_timestamp(df["AN_PROGNOSE"])
    delta_minutes = (arrival_forecast - arrival_actual).dt.total_seconds() / 60
    return delta_minutes.round(3)


def delay_summary(delay_minutes: pd.Series) -> pd.DataFrame:
    cleaned = delay_minutes.dropna()
    if cleaned.empty:
        return pd.DataFrame(
            {
                "metric": ["count", "mean", "median", "p95", "min", "max"],
                "value": [0, None, None, None, None, None],
            }
        )

    stats = {
        "count": int(cleaned.count()),
        "mean": round(float(cleaned.mean()), 3),
        "median": round(float(cleaned.median()), 3),
        "p95": round(float(cleaned.quantile(0.95)), 3),
        "min": round(float(cleaned.min()), 3),
        "max": round(float(cleaned.max()), 3),
    }
    return pd.DataFrame({"metric": list(stats.keys()), "value": list(stats.values())})


def edge_case_frames(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    status = df["AN_PROGNOSE_STATUS"].fillna("").astype(str).str.upper()
    cancelled = df["FAELLT_AUS_TF"].astype("string").str.lower().isin(["true", "1", "t", "yes"])

    return {
        "null_arrivals": df[df["ANKUNFTSZEIT"].isna()],
        "non_delay_status": df[~status.isin(["REAL", "ESTIMATED"])],
        "cancelled": df[cancelled],
    }

