from __future__ import annotations

from typing import List, Final

from pandas import DataFrame
from pyspark.sql import functions as F, Column

from src.utils import clean_number_str, to_int_tolerant, parse_dates

ALL_COLS: Final[List[str]] = [
    'dt', 'anonymized_listing_id', 'created_at', 'updated_at', 'status',
    'price', 'state', 'city', 'neighborhood', 'usable_areas', 'total_areas',
    'bathrooms', 'bedrooms', 'suites', 'parking_spaces', 'floors',
    'ceiling_height', 'amenities'
]

NUM_COLS_FLOAT: Final[List[str]] = ['price', 'usable_areas', 'total_areas', 'ceiling_height']
NUM_COLS_INT: Final[List[str]] = ['bathrooms', 'bedrooms', 'suites', 'parking_spaces', 'floors']
CAT_COLS: Final[List[str]] = ['status', 'state', 'city', 'neighborhood']

DATE_CANDIDATES: Final[List[str]] = ['created_at', 'dt', 'updated_at']
ID_COL: Final[str] = 'anonymized_listing_id'


def coerce_schema(df):
    for c in NUM_COLS_FLOAT:
        if c in df.columns:
            df = df.withColumn(colName=c, col=clean_number_str(col=F.col(c)).cast("double"))
    for c in NUM_COLS_INT:
        if c in df.columns:
            df = df.withColumn(colName=c, col=to_int_tolerant(col=F.col(c)))

    df = parse_dates(df=df, date_candidates=DATE_CANDIDATES)

    month_col: Column = (
        F.when(condition=F.col('created_at').isNotNull(),
               value=F.date_format(date=F.col('created_at').cast("date"), format='yyyy-MM'))
        .when(condition=F.col('dt').isNotNull(), value=F.date_format(date=F.col('dt').cast("date"), format='yyyy-MM'))
        .otherwise(value=F.date_format(date=F.col('updated_at').cast("date"), format='yyyy-MM'))
    )
    df = df.withColumn(colName='month', col=month_col)

    return df


def amenities_explode(df):
    if 'amenities' not in df.columns:
        return None
    cleaned: DataFrame = (df
                          .withColumn(colName='amenities_norm',
                                      col=F.regexp_replace(str=F.col('amenities'), pattern=r"[\[\]']", replacement=""))
                          .withColumn(colName='amenities_norm',
                                      col=F.regexp_replace(str=F.col('amenities_norm'), pattern=r"\s+",
                                                           replacement=" "))
                          .withColumn(colName='amenities_norm', col=F.trim(F.col('amenities_norm')))
                          )
    exploded = (cleaned
                .withColumn(colName='amenity', col=F.explode(
        F.filter(
            F.split(F.col('amenities_norm'), r"\s+"),
            lambda x: (x.isNotNull()) & (F.length(x) > 0)
        )
    ))
                .where(F.col('amenity').isNotNull() & (F.length('amenity') > 0))
                )
    return exploded.drop('amenities_norm')


def standardize_listings(df_raw: DataFrame) -> DataFrame:
    cols: List[str] = [c for c in ALL_COLS if c in df_raw.columns]
    df: DataFrame = df_raw.select(cols)
    df = coerce_schema(df=df)
    return df
