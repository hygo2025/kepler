import os

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql import SparkSession

from src.utils import make_spark


def point_in_time_join(
        spark: SparkSession,
        listings_path: str,
        events_path: str,
        num_partitions: int = 512,
) -> DataFrame:
    """
    Combina os dataframes de eventos e listings de forma otimizada para
    evitar data leakage, encontrando o snapshot mais recente para cada evento.
    """

    listings_df = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(listings_path)
        .withColumn("listing_snapshot_date", F.to_date(F.col("dt")))
        .repartition(num_partitions, F.col("anonymized_listing_id"))
    )

    events_df = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(events_path)
        .withColumn("event_date", F.to_date(F.col("event_ts")))
        .repartition(num_partitions, F.col("anonymized_listing_id"))
    )

    listings_aliased = listings_df.select(
        [F.col(c).alias(f"listing_{c}") for c in listings_df.columns]
    )

    joined_df = events_df.join(
        listings_aliased,
        events_df.anonymized_listing_id == listings_aliased.listing_anonymized_listing_id,
        how="inner"
    ).where(F.col("event_date") >= F.col("listing_listing_snapshot_date"))

    event_unique_cols = ["anonymized_user_id", "anonymized_session_id", "event_ts", "anonymized_listing_id"]

    window_spec = Window.partitionBy(*event_unique_cols).orderBy(F.col("listing_listing_snapshot_date").desc())

    point_in_time_df = (
        joined_df.withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") == 1)
        .drop("rank", "listing_anonymized_listing_id")
    )

    return point_in_time_df


if __name__ == '__main__':
    spark = make_spark()
    data_path = os.getenv("DATA_PATH")

    LISTINGS_PATH = f"{data_path}/listings"
    EVENTS_PATH = f"{data_path}/events"
    MERGED_DATA_PATH = f"{data_path}/enriched_events"

    merged_data = point_in_time_join(spark, LISTINGS_PATH, EVENTS_PATH)

    df_to_save = merged_data.withColumn("year", F.year(F.col("event_date"))) \
        .withColumn("month", F.month(F.col("event_date")))

    print(f"Salvando dados consolidados e particionados em: {MERGED_DATA_PATH}")

    (
        df_to_save.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(MERGED_DATA_PATH)
    )

    print("Salvamento concluído.")
    spark.stop()

