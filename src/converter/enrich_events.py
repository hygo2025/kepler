from itertools import chain
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.utils.enviroment import user_sessions_path, listings_processed_path, events_processed_path, \
    enriched_events_path, user_id_mapping_path, session_id_mapping_path


def create_and_apply_user_map(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    print("Iniciando mapeamento de 'unified_user_id' para 'user_numeric_id'...")
    id_window = Window.orderBy(F.lit(1))
    distinct_users = df.select("unified_user_id").distinct()
    user_mapping_table = distinct_users.withColumn("user_numeric_id", F.row_number().over(id_window))
    final_df = df.join(user_mapping_table, "unified_user_id", "inner")

    return final_df, user_mapping_table

def create_and_apply_session_map(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    print("Iniciando mapeamento de 'anonymized_session_id' para 'session_numeric_id'...")
    id_window = Window.orderBy(F.lit(1))
    distinct_sessions = df.select("anonymized_session_id").distinct()
    session_mapping_table = distinct_sessions.withColumn("session_numeric_id", F.row_number().over(id_window))
    final_df = df.join(session_mapping_table, "anonymized_session_id", "inner")

    return final_df, session_mapping_table

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
            "usage_types", "is_bot", "event_date", "month", "listing_id_numeric"
        )
    )
    return users, listings, raw_events

def enrich_events(spark: SparkSession):
    users_raw = spark.read.option("mergeSchema", "true").parquet(user_sessions_path())
    listings_raw = spark.read.option("mergeSchema", "true").parquet(listings_processed_path())
    events_raw = spark.read.option("mergeSchema", "true").parquet(events_processed_path())
    print(events_raw.show(10, truncate=False))
    users, listings, raw_events = rename_and_drop_columns(
        users=users_raw,
        listings=listings_raw,
        raw_events=events_raw,
    )

    raw_events = raw_events.drop("dt").withColumn(
        "dt", F.from_unixtime(F.col("collector_timestamp") / 1000).cast("timestamp")
    )
    events_enriched = fill_user_id(raw_events=raw_events)
    events_enriched = events_enriched.join(listings, on="listing_id", how="inner")

    events_enriched = events_enriched.join(
        users, events_enriched["user_id"] == users["user_anonymous_id"], "left"
    )
    print("Esquema dos eventos enriquecidos:")
    print(events_enriched.printSchema())

    events_with_user_map, user_mapping_table = create_and_apply_user_map(events_enriched)

    print("Esquema dos eventos com mapeamento de usuário aplicado:")
    print(events_with_user_map.printSchema())
    final_events_df, session_mapping_table = create_and_apply_session_map(events_with_user_map)

    print("Esquema dos eventos com mapeamento de sessão aplicado:")
    print(final_events_df.printSchema())

    final_events_df = (
        final_events_df
        .drop("dt","created_at","updated_at","user_anonymous_id","user_user_id")
        .withColumnRenamed("partition_date", "dt")
    )

    final_output_path = enriched_events_path()
    user_map_output_path = user_id_mapping_path()
    session_map_output_path = session_id_mapping_path()

    final_events_persisted = None
    user_map_persisted = None
    session_map_persisted = None
    try:
        print("Materializando DataFrames em cache...")
        final_events_persisted = final_events_df.persist()
        user_map_persisted = user_mapping_table.persist()
        session_map_persisted = session_mapping_table.persist()

        print(final_events_persisted.printSchema())

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

        print(f"Salvando novo mapa de sessões unificadas em: {session_map_output_path}")
        (
            session_map_persisted
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(session_map_output_path)
        )
    finally:
        if final_events_persisted: final_events_persisted.unpersist()
        if user_map_persisted: user_map_persisted.unpersist()
        if session_map_persisted: session_map_persisted.unpersist()