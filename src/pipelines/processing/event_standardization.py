from typing import List
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from src.utils.enviroment import enriched_events_vix_path, enriched_events_path


def standardize_events(spark: SparkSession):
    """
    Etapa final de padronização.
    Renomeia IDs para o padrão final (ex: listing_id, user_id)
    e reordena as colunas para o formato de consumo.
    """
    print("Iniciando standardize_events...")
    df = spark.read.option("mergeSchema", "true").parquet(enriched_events_path())

    # Remove IDs de string e colunas intermediárias
    columns_to_drop: List[str] = [
        "anonymized_session_id", "unified_user_id", "listing_id",
        "user_id", "anonymous_id", "portal", "browser_family",
        "os_family", "collector_timestamp",
    ]
    df_transformed = df.drop(*columns_to_drop)

    # Renomeia IDs numéricos para o nome final
    df_transformed = df_transformed \
        .withColumnRenamed("listing_id_numeric", "listing_id") \
        .withColumnRenamed("user_numeric_id", "user_id") \
        .withColumnRenamed("session_numeric_id", "session_id")

    # Reordena colunas para o dataset final
    first_columns: List[str] = [
        "listing_id", "user_id", "session_id",
        "event_type", "price", "event_ts"
    ]
    remaining_columns: List[str] = [
        col for col in df_transformed.columns if col not in first_columns
    ]
    new_column_order: List[str] = first_columns + remaining_columns
    df_final = df_transformed.select(*new_column_order)

    # Salva o dataset final (ex: 'vix' path)
    output_path = enriched_events_vix_path()
    print(f"Salvando dataset final padronizado em: {output_path}")
    df_final.coalesce(4).write.mode("overwrite").parquet(output_path)
    print("standardize_events concluído.")