from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def create_sequences(
        df: DataFrame,
        session_col: str = "anonymized_session_id"
) -> DataFrame:
    """
    Transforma um DataFrame de eventos em um DataFrame de sequências.
    Cada linha do resultado representará uma sessão completa, com uma lista
    ordenada de eventos.

    :param df: DataFrame de entrada (ex: train_data).
    :param session_col: A coluna usada para agrupar eventos (ex: ID da sessão).
    :return: DataFrame com uma linha por sessão e uma coluna 'sequence'.
    """
    print(f"Criando sequências agrupadas por '{session_col}'...")

    # Criamos um 'struct' para manter as informações de cada evento juntas.
    # É crucial incluir o event_ts para podermos ordenar a sequência depois.
    event_struct = F.struct(
        F.col("event_ts"),
        F.col("listing_id"),
        F.col("event_type"),
        F.col("listing_price"), # Adicionei o preco pois ele pode mudar com o tempo
    )

    # Agrupamos por sessão e coletamos todos os structs de evento em uma lista.
    # Em seguida, ordenamos essa lista pelo timestamp para garantir a ordem cronológica.
    sequences_df = df.groupBy(session_col).agg(
        F.sort_array(
            F.collect_list(event_struct),
            asc=True  # Ordena do evento mais antigo para o mais novo
        ).alias("sequence")
    )

    sequences_count = sequences_df.count()
    print(f"Número de sequências criadas: {sequences_count}")

    # Filtramos sessões que têm pelo menos 2 interações,
    # pois sequências de 1 item não são úteis para prever o próximo.
    sequences_df = sequences_df.filter(F.size(F.col("sequence")) > 1)

    filtered_count = sequences_df.count()
    print(f"Número de sequências após filtragem (tamanho > 1): {filtered_count}")
    print("Criação de sequências concluída.")
    return sequences_df