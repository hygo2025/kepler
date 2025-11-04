import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window
from typing import List  # <-- ADICIONADO

from src.utils.enviroment import (
    user_sessions_path, listings_processed_path, events_processed_path,
    enriched_events_path, user_id_mapping_path, session_id_mapping_path
)


def _fill_user_id(raw_events: DataFrame) -> DataFrame:
    """
    Aplica 'Identity Stitching'. Preenche o 'user_id' para eventos
    anônimos se um login for encontrado na mesma partição (anonymous_id).
    """
    window_spec = Window.partitionBy("anonymous_id")
    df_with_filled_id = raw_events.withColumn(
        "session_user_id", F.first(F.col("user_id"), ignorenulls=True).over(window_spec)
    )

    # Coalesce: usa user_id se existir, senão o session_user_id (preenchido),
    # senão o anonymous_id
    df_with_unified_id = df_with_filled_id.withColumn(
        "unified_user_id",
        F.coalesce(F.col("user_id"), F.col("session_user_id"), F.col("anonymous_id"))
    )
    return df_with_unified_id.drop("session_user_id")


def _create_and_apply_user_map(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Cria e aplica um ID numérico para 'unified_user_id'."""
    print("\nIniciando mapeamento de 'unified_user_id' para 'user_numeric_id'...")
    id_window = Window.orderBy(F.lit(1))
    distinct_users = df.select("unified_user_id").distinct()
    user_mapping_table = distinct_users.withColumn("user_numeric_id", F.row_number().over(id_window))
    final_df = df.join(user_mapping_table, "unified_user_id", "inner")
    return final_df, user_mapping_table


def _create_and_apply_session_map(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Cria e aplica um ID numérico para 'anonymized_session_id'."""
    print("\nIniciando mapeamento de 'anonymized_session_id' para 'session_numeric_id'...")
    id_window = Window.orderBy(F.lit(1))
    distinct_sessions = df.select("anonymized_session_id").distinct()
    session_mapping_table = distinct_sessions.withColumn("session_numeric_id", F.row_number().over(id_window))
    final_df = df.join(session_mapping_table, "anonymized_session_id", "inner")
    return final_df, session_mapping_table


def _rename_and_drop_columns(
        users: DataFrame, listings: DataFrame, raw_events: DataFrame
) -> tuple:
    """Prepara os DFs para o join, renomeando e limpando colunas."""
    users = users.withColumnRenamed("id", "user_user_id").withColumnRenamed(
        "anonymous_id", "user_anonymous_id"
    )
    listings = (
        listings
        .withColumnRenamed("anonymized_listing_id", "listing_id")
        .drop("month", "dt")  # Remove colunas antigas de partição/data
    )
    raw_events = (
        raw_events
        .withColumnRenamed("anonymized_user_id", "user_id")
        .withColumnRenamed("anonymized_anonymous_id", "anonymous_id")
        .withColumnRenamed("anonymized_listing_id", "listing_id")
        .withColumn("partition_date", F.col("dt"))  # Salva a data de partição
        .drop(
            "usage_types", "is_bot", "event_date", "month", "listing_id_numeric"
        )
    )
    return users, listings, raw_events


def enrich_events(spark: SparkSession):
    """
    Etapa principal de enriquecimento E padronização:
    1. Carrega dados processados (eventos, listings, sessões).
    2. Aplica 'Identity Stitching' (fill_user_id).
    3. Junta eventos com dados de listings e sessões.
    4. Cria IDs numéricos (surrogate keys) para usuários e sessões.
    5. Padroniza (limpa, renomeia, reordena) o DataFrame final.
    6. Salva os dados enriquecidos/padronizados e os mapas.
    """
    print("\nIniciando enrich_events...")
    # 1. Carregar dados
    users_raw = spark.read.option("mergeSchema", "true").parquet(user_sessions_path())
    listings_raw = spark.read.option("mergeSchema", "true").parquet(listings_processed_path())
    events_raw = spark.read.option("mergeSchema", "true").parquet(events_processed_path())

    users, listings, raw_events = _rename_and_drop_columns(
        users=users_raw, listings=listings_raw, raw_events=events_raw,
    )

    # Garante que 'dt' seja o timestamp do evento
    raw_events = raw_events.drop("dt").withColumn(
        "dt", F.from_unixtime(F.col("collector_timestamp") / 1000).cast("timestamp")
    )

    # 2. Identity Stitching
    events_enriched = _fill_user_id(raw_events=raw_events)

    # 3. Join com Listings (trazendo dados do anúncio)
    events_enriched = events_enriched.join(listings, on="listing_id", how="inner")

    # 4. Join com Sessões (trazendo o user_id principal do anonymous_id)
    events_enriched = events_enriched.join(
        users, events_enriched["user_id"] == users["user_anonymous_id"], "left"
    )

    # 5. Criar e aplicar IDs numéricos
    events_with_user_map, user_mapping_table = _create_and_apply_user_map(events_enriched)
    final_events_df, session_mapping_table = _create_and_apply_session_map(events_with_user_map)

    # 6. Padronização final
    print("\nAplicando padronização final (renomeando e reordenando colunas)...")

    columns_to_drop: List[str] = [
        "anonymized_session_id", "unified_user_id", "listing_id",
        "user_id", "anonymous_id", "portal", "browser_family",
        "os_family", "collector_timestamp",
        "dt", "created_at", "updated_at", "user_anonymous_id", "user_user_id"
    ]

    df_transformed = final_events_df.drop(*columns_to_drop)

    df_transformed = df_transformed \
        .withColumnRenamed("listing_id_numeric", "listing_id") \
        .withColumnRenamed("user_numeric_id", "user_id") \
        .withColumnRenamed("session_numeric_id", "session_id") \
        .withColumnRenamed("partition_date", "dt")

    first_columns: List[str] = [
        "listing_id", "user_id", "session_id",
        "event_type", "price", "event_ts"
    ]
    remaining_columns: List[str] = [
        col for col in df_transformed.columns if col not in first_columns
    ]
    new_column_order: List[str] = first_columns + remaining_columns

    final_events_df_standardized = df_transformed.select(*new_column_order)

    # 7. Salvar resultados
    final_output_path = enriched_events_path()
    user_map_output_path = user_id_mapping_path()
    session_map_output_path = session_id_mapping_path()

    final_events_persisted = None
    user_map_persisted = None
    session_map_persisted = None
    try:
        print("\nMaterializando DataFrames em cache...")
        # Persiste o DataFrame JÁ PADRONIZADO
        final_events_persisted = final_events_df_standardized.persist()
        user_map_persisted = user_mapping_table.persist()
        session_map_persisted = session_mapping_table.persist()

        print(f"\nSalvando base de dados de eventos enriquecidos e padronizados em: {final_output_path}")
        (
            final_events_persisted
            .coalesce(4)
            .write
            .mode("overwrite")
            .partitionBy("dt")
            .option("compression", "snappy")
            .parquet(final_output_path)
        )

        print(f"\nSalvando mapa de usuários unificados em: {user_map_output_path}")
        (
            user_map_persisted
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(user_map_output_path)
        )

        print(f"\nSalvando mapa de sessões unificadas em: {session_map_output_path}")
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

    print("\nenrich_events concluído.")