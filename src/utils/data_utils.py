from __future__ import annotations

from typing import List, Union
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F


def parse_ts_multi(col: Union[str, Column]) -> Column:
    return F.coalesce(
        F.col(col=col).cast("timestamp"),
        F.to_timestamp(col=F.col(col=col)),
        F.to_timestamp(col=F.col(col=col), format="yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(col=F.col(col=col), format="yyyy-MM-dd HH:mm:ss.SSS"),
        F.to_timestamp(col=F.col(col=col), format="yyyy-MM-dd HH:mm:ss 'UTC'"),
        F.to_timestamp(col=F.col(col=col), format="yyyy-MM-dd'T'HH:mm:ssXXX"),
        F.to_timestamp(col=F.col(col=col), format="yyyy-MM-dd'T'HH:mm:ss'Z'"),
        F.to_timestamp(col=F.to_date(col=F.col(col=col), format="yyyy-MM-dd")),
    )


def parse_dates(df: DataFrame, date_candidates: List[str]) -> DataFrame:
    for c in date_candidates:
        if c in df.columns:
            df = df.withColumn(colName=c, col=parse_ts_multi(col=c))
    return df


def clean_number_str(col: Column) -> Column:
    s: Column = col.cast("string")
    return F.when(
        condition=F.length(F.regexp_replace(str=s, pattern=r"\s+", replacement="")) == 0,
        value=None,
    ).otherwise(
        F.regexp_replace(
            str=F.regexp_replace(str=s, pattern=r"\s+", replacement=""),
            pattern=",",
            replacement=".",
        )
    )


def to_numeric_tolerant(col: Column, target_type: str = "int") -> Column:
    cleaned: Column = clean_number_str(col=col)
    if target_type == "int":
        dbl: Column = F.when(
            condition=cleaned.rlike("^[+-]?\\d*\\.?\\d+$"),
            value=cleaned.cast("double"),
        ).otherwise(F.lit(None).cast("double"))
        return F.when(condition=dbl.isNotNull(), value=F.round(col=dbl).cast("int")).otherwise(F.lit(None).cast("int"))
    elif target_type == "bigint":
        return F.when(
            condition=cleaned.rlike("^[+-]?\\d+$"),
            value=cleaned.cast("bigint"),
        ).otherwise(F.lit(None).cast("bigint"))
    else:
        raise ValueError(f"target_type {target_type} não suportado")


def to_int_tolerant(col: Column) -> Column:
    return to_numeric_tolerant(col=col, target_type="int")


def to_bigint_tolerant(col: Column) -> Column:
    return to_numeric_tolerant(col=col, target_type="bigint")


def to_bool_tolerant(col: Column) -> Column:
    s: Column = col.cast("string")
    s = F.trim(F.lower(s))
    return F.when(
        condition=(s == F.lit("true")) | (s == F.lit("1")),
        value=F.lit(True),
    ).when(
        condition=(s == F.lit("false")) | (s == F.lit("0")),
        value=F.lit(False),
    ).otherwise(F.lit(None).cast("boolean"))

def trim_or_null(c: Union[str, Column]) -> Column:
    col = F.col(c) if not isinstance(c, Column) else c
    s = F.trim(col.cast("string"))
    return F.when(F.length(s) > 0, s).otherwise(F.lit(None).cast("string"))