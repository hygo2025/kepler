from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from typing import Tuple
from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


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
