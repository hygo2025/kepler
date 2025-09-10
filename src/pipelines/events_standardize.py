from __future__ import annotations

from typing import Final, List

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F

from src.utils import to_bigint_tolerant, to_bool_tolerant, trim_or_null

ALL_COLS: Final[List[str]] = [
    "anonymized_user_id",
    "anonymized_anonymous_id",
    "anonymized_session_id",
    "dt",
    "browser_family",
    "os_family",
    "is_bot",
    "name_raw",
    "event_type",
    "collector_timestamp",
    "anonymized_listing_id",
]

ID_COLS: Final[List[str]] = [
    "anonymized_user_id",
    "anonymized_anonymous_id",
    "anonymized_session_id",
]

CATEGORICAL_COLS: Final[List[str]] = [
    "browser_family",
    "os_family",
    "name_raw",
    "event_type",
]

DATE_CANDIDATES: Final[List[str]] = ["collector_timestamp", "dt"]
LISTING_ID_COL: Final[str] = "anonymized_listing_id"


def _parse_event_ts(df: DataFrame) -> DataFrame:
    if "collector_timestamp" in df.columns:
        ct_ms: Column = to_bigint_tolerant(col=F.col("collector_timestamp"))
        df = df.withColumn(colName="event_ts_from_ct", col=(ct_ms / F.lit(1000)).cast("timestamp"))
    else:
        df = df.withColumn(colName="event_ts_from_ct", col=F.lit(None).cast("timestamp"))

    if "dt" in df.columns:
        df = df.withColumn(colName="event_ts_from_dt", col=F.to_timestamp(col=F.col("dt"), format="yyyy-MM-dd"))
    else:
        df = df.withColumn(colName="event_ts_from_dt", col=F.lit(None).cast("timestamp"))

    df = df.withColumn(
        colName="event_ts",
        col=F.coalesce(F.col("event_ts_from_ct"), F.col("event_ts_from_dt"))
    )

    df = df.withColumn(colName="event_date", col=F.to_date(col=F.col("event_ts")))
    df = df.withColumn(colName="month", col=F.date_format(date=F.col("event_date"), format="yyyy-MM"))

    return df.drop("event_ts_from_ct", "event_ts_from_dt")


def coerce_schema(df: DataFrame) -> DataFrame:
    cols: List[str] = [name for name in ALL_COLS if name in df.columns]
    if cols:
        df = df.select(*cols)

    for name in [n for n in (ID_COLS + [LISTING_ID_COL]) if n in df.columns]:
        df = df.withColumn(colName=name, col=trim_or_null(name))

    if "is_bot" in df.columns:
        df = df.withColumn(colName="is_bot", col=to_bool_tolerant(F.col("is_bot")))

    for name in [n for n in CATEGORICAL_COLS if n in df.columns]:
        df = df.withColumn(colName=name, col=trim_or_null(name))

    df = _parse_event_ts(df=df)

    return df


def standardize_events(df_raw: DataFrame) -> DataFrame:
    return coerce_schema(df=df_raw)
