from typing import List

from pyspark.sql import SparkSession

from src.utils.enviroment import enriched_events_vix_path, enriched_events_path


def prepare_events_vix(spark: SparkSession):
    print(enriched_events_path())
    df = spark.read.option("mergeSchema", "true").parquet(enriched_events_path())
    columns_to_drop: List[str] = [
        "anonymized_session_id",
        "unified_user_id",
        "listing_id",
        "user_id",
        "anonymous_id",
        "portal",
        "browser_family",
        "os_family",
        "collector_timestamp",
        "state",
        "city"
    ]
    df_transformed = df.drop(*columns_to_drop)

    df_transformed = df_transformed \
        .withColumnRenamed("listing_id_numeric", "listing_id") \
        .withColumnRenamed("user_numeric_id", "user_id") \
        .withColumnRenamed("session_numeric_id", "session_id")

    first_columns: List[str] = [
        "listing_id",
        "user_id",
        "session_id",
        "event_type",
        "price",
        "event_ts"
    ]

    remaining_columns: List[str] = [
        col for col in df_transformed.columns if col not in first_columns
    ]

    new_column_order: List[str] = first_columns + remaining_columns

    df_final = df_transformed.select(*new_column_order)

    df_final.coalesce(4).write.mode("overwrite").parquet(enriched_events_vix_path())