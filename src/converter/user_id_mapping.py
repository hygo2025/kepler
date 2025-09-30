from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def create_user_identity_map(spark: SparkSession, paths: dict) -> None:
    print("Iniciando a criação do mapa de identidade de usuários...")
    events_df = spark.read.parquet(paths["EVENTS_PROCESSED_PATH"])

    identity_data = events_df.filter(F.col("anonymized_user_id").isNotNull()).select(
        "anonymized_anonymous_id",
        "anonymized_user_id"
    )

    identity_map = identity_data.groupBy("anonymized_anonymous_id").agg(
        F.first("anonymized_user_id").alias("mapped_user_id")
    )

    identity_map = identity_map.withColumnRenamed("anonymized_anonymous_id", "anonymous_id")

    output_path = paths["USER_ID_MAPPING_PATH"]
    print(f"Salvando mapa de identidade com {identity_map.count()} entradas em: {output_path}")
    (
        identity_map.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(output_path)
    )
    print("Criação do mapa de identidade concluída.")