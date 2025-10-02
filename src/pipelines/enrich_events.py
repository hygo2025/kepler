from itertools import chain
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def create_and_apply_user_map(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    print("Iniciando mapeamento de 'unified_user_id' para 'user_numeric_id'...")
    id_window = Window.orderBy(F.lit(1))
    distinct_users = df.select("unified_user_id").distinct()
    user_mapping_table = distinct_users.withColumn("user_numeric_id", F.row_number().over(id_window))
    final_df = df.join(user_mapping_table, "unified_user_id", "inner")

    return final_df, user_mapping_table

def fill_user_id(raw_events: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("anonymous_id")
    df_with_filled_id = raw_events.withColumn(
        "session_user_id", F.first(F.col("user_id"), ignorenulls=True).over(window_spec)
    )

    df_with_unified_id = df_with_filled_id.withColumn(
        "unified_user_id",
        F.coalesce(F.col("user_id"), F.col("session_user_id"), F.col("anonymous_id"))
    )

    result = df_with_unified_id.drop("session_user_id")
    return result


def boost(df: DataFrame, boost_period: int = 7, boost: float = 1.3) -> DataFrame:
    df = df.withColumn("days_until_today", F.datediff(F.current_date(), F.col("dt")))
    df = df.withColumn(
        "boost",
        F.when(
            F.col("days_until_today") <= boost_period,
            F.col("weight") * boost,
        ).otherwise(
            F.col("weight")
            * F.exp((-F.col("days_until_today") + boost_period) / 270.0)
        ),
    )
    return df


def to_enriched_event(raw_events: DataFrame) -> DataFrame:
    def create_ratings_map() -> F.Column:
        return F.create_map(
            [
                F.lit(x)
                for x in chain(
                *{
                    "VISIT": 0.1, "LEAD": 1.0, "LEAD_IDENTIFIED": 2.0,
                    "LEAD_INTENTION": 0.25, "FAVORITE": 0.25,
                    "PREVIEW": 0.075, "OTHER": 0.075,
                }.items()
            )
            ]
        )

    raw_events = fill_user_id(raw_events).withColumn(
        "weight", create_ratings_map()[F.col("event_type")]
    )
    raw_events = boost(df=raw_events)
    raw_events = raw_events.withColumn("rating", F.col("boost"))
    return raw_events


def rename_and_drop_columns(
        users: DataFrame, listings: DataFrame, raw_events: DataFrame
) -> tuple:
    users = users.withColumnRenamed("id", "user_user_id").withColumnRenamed(
        "anonymous_id", "user_anonymous_id"
    )
    listings = (
        listings
        .withColumnRenamed("anonymized_listing_id", "listing_id")
        .drop("month", "dt")
    )
    raw_events = (
        raw_events
        .withColumnRenamed("anonymized_user_id", "user_id")
        .withColumnRenamed("anonymized_anonymous_id", "anonymous_id")
        .withColumnRenamed("anonymized_listing_id", "listing_id")
        .withColumn("partition_date", F.col("dt"))
        .drop(
            "anonymized_session_id", "browser_family", "os_family",
            "is_bot", "event_date", "month", "listing_id_numeric"
        )
    )
    return users, listings, raw_events

def enrich_events(spark: SparkSession, paths: dict):
    users_raw = spark.read.option("mergeSchema", "true").parquet(paths["USER_SESSIONS_PATH"])
    listings_raw = spark.read.option("mergeSchema", "true").parquet(paths["LISTINGS_PROCESSED_PATH"])
    raw_events_raw = spark.read.option("mergeSchema", "true").parquet(paths["EVENTS_PROCESSED_PATH"])

    users, listings, raw_events = rename_and_drop_columns(
        users=users_raw,
        listings=listings_raw,
        raw_events=raw_events_raw,
    )

    raw_events = raw_events.drop("dt").withColumn(
        "dt", F.from_unixtime(F.col("collector_timestamp") / 1000).cast("timestamp")
    )
    events_enriched = to_enriched_event(raw_events=raw_events)
    events_enriched = events_enriched.filter(F.col("rating") > 0.001)
    events_enriched = events_enriched.join(listings, on="listing_id", how="inner")

    events_enriched = events_enriched.join(
        users, events_enriched["user_id"] == users["user_anonymous_id"], "left"
    )

    print(events_enriched.show(10))
    final_events_df, user_mapping_table = create_and_apply_user_map(events_enriched)

    final_events_df = (
        final_events_df
        .drop("dt", "weight","days_until_today","boost","created_at","updated_at","user_anonymous_id","user_user_id")
        .withColumnRenamed("partition_date", "dt")
    )

    final_output_path = paths['ENRICHED_EVENTS_PATH']
    user_map_output_path = paths['USER_ID_MAPPING_PATH']

    final_events_persisted = None
    user_map_persisted = None
    try:
        print("Materializando DataFrames em cache...")
        final_events_persisted = final_events_df.persist()
        user_map_persisted = user_mapping_table.persist()

        print(final_events_persisted.printSchema)

        print(f"Salvando base de dados de ratings finais em: {final_output_path}")
        (
            final_events_persisted
            .coalesce(4)
            .write
            .mode("overwrite")
            .partitionBy("dt")
            .option("compression", "snappy")
            .parquet(final_output_path)
        )

        print(f"Salvando novo mapa de usuários unificados em: {user_map_output_path}")
        (
            user_map_persisted
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(user_map_output_path)
        )
    finally:
        if final_events_persisted: final_events_persisted.unpersist()
        if user_map_persisted: user_map_persisted.unpersist()
