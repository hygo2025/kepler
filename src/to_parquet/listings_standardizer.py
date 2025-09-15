from __future__ import annotations

from typing import Final, List

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F


class ListingsStandardizer:
    ALL_COLS: Final[List[str]] = [
        'dt', 'anonymized_listing_id', 'created_at', 'updated_at', 'status', 'price',
        'state', 'city', 'neighborhood', 'usable_areas', 'total_areas', 'bathrooms',
        'bedrooms', 'suites', 'parking_spaces', 'floors', 'ceiling_height', 'amenities'
    ]
    NUM_COLS_FLOAT: Final[List[str]] = ['price', 'usable_areas', 'total_areas', 'ceiling_height']
    NUM_COLS_INT: Final[List[str]] = ['bathrooms', 'bedrooms', 'suites', 'parking_spaces', 'floors']
    DATE_CANDIDATES: Final[List[str]] = ['created_at', 'dt', 'updated_at']

    def __init__(self):
        pass

    def transform(self, df_raw: DataFrame) -> DataFrame:
        cols: List[str] = [c for c in self.ALL_COLS if c in df_raw.columns]
        df: DataFrame = df_raw.select(*cols) if cols else df_raw
        return self._coerce_schema(df=df)

    def _coerce_schema(self, df: DataFrame) -> DataFrame:
        for c in self.NUM_COLS_FLOAT:
            if c in df.columns:
                df = df.withColumn(c, F.regexp_replace(F.col(c), r"[^0-9.]", "").cast("double"))

        for c in self.NUM_COLS_INT:
            if c in df.columns:
                df = df.withColumn(c, F.col(c).cast("integer"))

        for col_name in self.DATE_CANDIDATES:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.to_date(F.col(col_name)))

        month_col: Column = F.coalesce(
            F.date_format(F.col('created_at'), 'yyyy-MM'),
            F.date_format(F.col('dt'), 'yyyy-MM'),
            F.date_format(F.col('updated_at'), 'yyyy-MM')
        )
        df = df.withColumn('month', month_col)

        return df

    @staticmethod
    def explode_amenities(df: DataFrame) -> DataFrame:
        if "amenities" not in df.columns:
            return df

        return (
            df.withColumn("amenity", F.explode(
                F.split(F.regexp_replace(F.col("amenities"), r"[\[\]'\s]+", " "), " ")
            ))
            .filter(F.length(F.col("amenity")) > 0)
        )