from __future__ import annotations

from typing import Final, List

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F


def trim_or_null(col_name: str) -> Column:
    return F.when(F.trim(F.col(col_name)) == "", None).otherwise(F.trim(F.col(col_name)))


class EventsStandardizer:
    ALL_COLS: Final[List[str]] = [
        "anonymized_user_id", "anonymized_anonymous_id", "anonymized_session_id",
        "dt", "browser_family", "os_family", "is_bot", "name_raw", "event_type",
        "collector_timestamp", "anonymized_listing_id",
    ]
    ID_COLS: Final[List[str]] = [
        "anonymized_user_id", "anonymized_anonymous_id", "anonymized_session_id",
    ]
    CATEGORICAL_COLS: Final[List[str]] = [
        "browser_family", "os_family", "name_raw", "event_type",
    ]
    LISTING_ID_COL: Final[str] = "anonymized_listing_id"

    def __init__(self):
        pass

    def transform(self, df_raw: DataFrame) -> DataFrame:
        return self._coerce_schema(df=df_raw)

    def _coerce_schema(self, df: DataFrame) -> DataFrame:
        cols_to_select: List[str] = [name for name in self.ALL_COLS if name in df.columns]
        if cols_to_select:
            df = df.select(*cols_to_select)

        id_cols_to_trim = [n for n in (self.ID_COLS + [self.LISTING_ID_COL]) if n in df.columns]
        for name in id_cols_to_trim:
            df = df.withColumn(colName=name, col=trim_or_null(name))

        categorical_cols_to_trim = [n for n in self.CATEGORICAL_COLS if n in df.columns]
        for name in categorical_cols_to_trim:
            df = df.withColumn(colName=name, col=trim_or_null(name))

        if "is_bot" in df.columns:
            df = df.withColumn(colName="is_bot", col=F.col("is_bot").cast("boolean"))

        df = self._parse_event_ts(df=df)

        return df

    @staticmethod
    def _parse_event_ts(df: DataFrame) -> DataFrame:
        if "collector_timestamp" in df.columns:
            df = df.withColumn("event_ts_from_ct", (F.col("collector_timestamp").cast("bigint") / F.lit(1000)).cast("timestamp"))
        else:
            df = df.withColumn("event_ts_from_ct", F.lit(None).cast("timestamp"))

        if "dt" in df.columns:
            df = df.withColumn("event_ts_from_dt", F.to_timestamp(F.col("dt"), "yyyy-MM-dd"))
        else:
            df = df.withColumn("event_ts_from_dt", F.lit(None).cast("timestamp"))

        df = df.withColumn(
            "event_ts",
            F.coalesce(F.col("event_ts_from_ct"), F.col("event_ts_from_dt"))
        )

        df = df.withColumn("event_date", F.to_date(F.col("event_ts")))
        df = df.withColumn("month", F.date_format(F.col("event_date"), "yyyy-MM"))

        return df.drop("event_ts_from_ct", "event_ts_from_dt")