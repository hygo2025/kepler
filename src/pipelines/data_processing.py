from typing import Tuple
from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from typing import Tuple

def debug_data_loss(
        events_df: DataFrame,
        listings_df: DataFrame
):
    """
    Analisa a perda de dados durante o join entre eventos e listings.
    Usa um LEFT JOIN para identificar e quantificar eventos que não encontram
    correspondência no dataset de listings, espelhando a lógica da função principal.
    """
    print("\n--- INICIANDO DIAGNÓSTICO DE PERDA DE DADOS ---")

    initial_events_count = events_df.count()
    print(f"Contagem inicial de eventos (após filtro de data): {initial_events_count}")

    # Renomeia as colunas para o join
    events_renamed = events_df.withColumnRenamed("anonymized_listing_id", "listing_id")
    listings_renamed = listings_df.withColumnRenamed("anonymized_listing_id", "listing_id")

    # Reexecuta o join original para obter a contagem real de sucesso
    final_join_df = perform_point_in_time_join(events_df, listings_df)
    final_events_count = final_join_df.count()

    lost_count = initial_events_count - final_events_count
    loss_percentage = (lost_count / initial_events_count) * 100 if initial_events_count > 0 else 0

    print(f"Contagem final de eventos (após INNER JOIN): {final_events_count}")
    print(f"Total de eventos perdidos no join: {lost_count} ({loss_percentage:.2f}%)")

    if lost_count > 0:
        print("\nInvestigando as causas da perda...")

        # ===== LÓGICA CORRIGIDA AQUI =====
        # 1. Fazemos o LEFT JOIN
        # 2. Adicionamos o .select() com o prefixo, exatamente como na função principal.
        left_join_df = events_renamed.join(
            listings_renamed,
            on=[
                events_renamed.listing_id == listings_renamed.listing_id,
                events_renamed.event_date >= listings_renamed.listing_snapshot_date
            ],
            how="left"
        ).select(
            events_renamed["*"],
            *[listings_renamed[c].alias("listing_" + c) for c in listings_renamed.columns]
        )

        # 3. Agora a coluna 'listing_listing_id' existe e podemos usá-la para filtrar.
        #    Esta coluna será nula para todos os eventos que não encontraram par.
        lost_events_df = left_join_df.filter(F.col("listing_listing_id").isNull()).cache()

        print("\nExemplos de eventos que foram perdidos:")
        lost_events_df.select("listing_id", "event_date", "event_type").show(10, truncate=False)

        # O resto do código para encontrar a causa da perda continua igual
        all_listing_ids_df = listings_renamed.select("listing_id").distinct().cache()

        orphan_ids_df = lost_events_df.select("listing_id").distinct() \
            .join(all_listing_ids_df, on="listing_id", how="left_anti")

        orphan_count = orphan_ids_df.count()
        temporal_mismatch_count = lost_count - orphan_count

        print("\n--- Causa da Perda ---")
        print(f"Eventos com IDs de imóveis que NUNCA aparecem no dataset (órfãos): {orphan_count}")
        print(
            f"Eventos com IDs válidos, mas fora do período de tempo do imóvel (incompat. temporal): {temporal_mismatch_count}")

    print("--- DIAGNÓSTICO CONCLUÍDO ---\n")


def analyze_session_coverage(df: DataFrame, session_col: str, user_col: str):
    """
    Analisa a cobertura e a qualidade da coluna de sessão em um DataFrame de eventos.
    """
    print(f"\n--- INICIANDO ANÁLISE DA COLUNA '{session_col}' ---")

    # Cache para acelerar as múltiplas contagens
    df.cache()

    total_events = df.count()
    print(f"Total de eventos para analisar: {total_events}")

    # 1. Contagem de nulos
    events_with_session = df.filter(F.col(session_col).isNotNull()).count()
    events_without_session = total_events - events_with_session
    percentage_null = (events_without_session / total_events) * 100 if total_events > 0 else 0

    print(f"\nEventos COM '{session_col}': {events_with_session}")
    print(f"Eventos SEM '{session_col}' (nulos): {events_without_session}")
    print(f"Porcentagem de eventos com session_id nulo: {percentage_null:.2f}%")

    # 2. Análise de cardinalidade
    unique_sessions = df.select(session_col).distinct().count()
    unique_users = df.select(user_col).distinct().count()

    # O distinct().count() conta o valor nulo como uma entrada, então subtraímos 1 se houver nulos.
    if events_without_session > 0:
        unique_sessions -= 1

    avg_sessions_per_user = (unique_sessions / unique_users) if unique_users > 0 else 0

    print(f"\nTotal de sessões únicas: {unique_sessions}")
    print(f"Total de usuários únicos: {unique_users}")
    print(f"Média de sessões por usuário (com base nos IDs existentes): {avg_sessions_per_user:.2f}")

    df.unpersist()
    print("--- ANÁLISE CONCLUÍDA ---")


def load_and_prepare_data(
        spark: SparkSession,
        listings_path: str,
        events_path: str,
        num_partitions: int = 512
) -> Tuple[DataFrame, DataFrame]:
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

    return listings_df, events_df


from pyspark.sql import DataFrame, Window, functions as F


def perform_point_in_time_join(
        events_df: DataFrame,
        listings_df: DataFrame
) -> DataFrame:
    """
    Combina os dataframes de eventos e listings de forma otimizada para
    evitar data leakage, encontrando o snapshot mais recente para cada evento.

    :param events_df: Dataframe de eventos preparado.
    :param listings_df: Dataframe de listings preparado.
    :return: Um único dataframe com os eventos enriquecidos com os dados dos imóveis.
    """
    print("Executando o Point-in-Time Join (versão otimizada)...")

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

    print("Point-in-Time Join concluído.")
    return point_in_time_df


def perform_temporal_split(
        df: DataFrame,
        val_start_date: str,
        test_start_date: str
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Divide um DataFrame em conjuntos de treino, validação e teste
    com base em duas datas de corte temporais.

    :param df: DataFrame de entrada.
    :param val_start_date: Data de início do conjunto de validação ('YYYY-MM-DD').
    :param test_start_date: Data de início do conjunto de teste ('YYYY-MM-DD').
    :return: Uma tupla contendo (train_df, validation_df, test_df).
    """
    print(
        f"Dividindo dados em treino (< {val_start_date}), validação (>= {val_start_date} e < {test_start_date}) e teste (>= {test_start_date})")

    # Garante que as datas de corte sejam do tipo Date
    val_start_ts = F.to_date(F.lit(val_start_date))
    test_start_ts = F.to_date(F.lit(test_start_date))

    # Filtra os dados de treino
    train_df = df.filter(F.col("event_date") < val_start_ts)

    # Filtra os dados de validação
    validation_df = df.filter(
        (F.col("event_date") >= val_start_ts) &
        (F.col("event_date") < test_start_ts)
    )

    test_df = df.filter(F.col("event_date") >= test_start_ts)

    return train_df, validation_df, test_df

def create_development_sample(
        train_df: DataFrame,
        fraction: float,
        seed: int = 42
) -> DataFrame:
    """
    Cria uma amostra do conjunto de treino baseada em uma fração de usuários.

    :param train_df: O dataframe de treino completo.
    :param fraction: A fração de usuários a ser amostrada (ex: 0.05 para 5%).
    :param seed: Semente para reprodutibilidade.
    :return: Um dataframe de amostra para desenvolvimento.
    """
    print(f"Criando amostra de desenvolvimento com {fraction * 100}% dos usuários...")
    distinct_users = train_df.select("anonymized_user_id").distinct()
    sampled_users = distinct_users.sample(fraction=fraction, seed=seed)
    train_sample_df = train_df.join(sampled_users, on="anonymized_user_id", how="inner")
    return train_sample_df
