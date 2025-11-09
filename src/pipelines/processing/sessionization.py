import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from src.utils.enviroment import user_sessions_path, events_processed_path


def resolve_user_identities(spark: SparkSession) -> None:
    """
    Resolve identidades de usuários anônimos e logados ('Identity Stitching').

    Agrupa eventos por 'anonymous_id', identifica o 'user_id' mais frequente
    associado e descarta casos ambíguos (muitos user_ids distintos).
    Salva um mapeamento final de (anonymous_id -> user_id principal).
    """
    print("\nIniciando resolve_user_identities...")
    num_partitions = 512
    collision_threshold = 7  # Limite de 'user_ids' distintos por 'anonymous_id'
    output_path = user_sessions_path()

    raw_events = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(events_processed_path())
        .repartition(num_partitions, F.col("anonymized_listing_id"))
    )

    # 1. Agrupa todos os user_ids por anonymous_id
    grouped = raw_events.groupBy("anonymized_anonymous_id").agg(
        F.collect_list("anonymized_user_id").alias("user_id_list")
    )

    # 2. Conta a frequência de cada user_id distinto
    counted_grouped = (
        grouped
        .withColumn("distinct_user_ids", F.array_distinct(F.col("user_id_list")))
        .withColumn(
            "user_id_entries",
            F.expr(
                """
                transform(
                    distinct_user_ids,
                    x -> struct(
                        x as key,
                        size(filter(user_id_list, y -> y = x)) as value
                    )
                )
                """
            ),
        )
        .withColumn("user_id", F.map_from_entries(F.col("user_id_entries"))).drop(
            "distinct_user_ids", "user_id_entries", "user_id_list"
        )
    )

    # 3. Filtra "colisões" (anonymous_ids com muitos user_ids)
    filtered_grouped = counted_grouped.filter(
        F.size(F.map_keys(F.col("user_id"))) <= collision_threshold
    )

    # 4. Seleciona o user_id MAIS FREQUENTE como o 'id' principal
    user_sessions_df = filtered_grouped.select(
        F.col("anonymized_anonymous_id"),
        F.expr(
            """
            struct(
                anonymized_anonymous_id AS anonymous_id,
                (
                    sort_array(
                        transform(
                            arrays_zip(map_keys(user_id), map_values(user_id)),
                            x -> struct(x['0'] AS key, -1 * x['1'] AS valNeg)
                        ),
                        /* Ordena pela contagem (negativa, ascendente) */
                        true 
                    )[0]['key']
                ) AS id
            )
            """
        ).alias("user_session")
    )

    result = user_sessions_df.select(
        F.col("user_session.anonymous_id"), F.col("user_session.id")
    )

    # 5. Salva o resultado
    (
        result
        .coalesce(2)
        .write
        .mode("overwrite")
        .parquet(output_path)
    )
    print("\nresolve_user_identities concluído.")