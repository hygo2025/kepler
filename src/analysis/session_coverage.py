import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils import make_spark


def analyze_session_coverage(df: DataFrame, session_col: str, user_col: str):
    print(f"\n--- INICIANDO ANÁLISE DA COLUNA '{session_col}' ---")

    df.cache()

    total_events = df.count()
    print(f"Total de eventos para analisar: {total_events}")

    events_with_session = df.filter(F.col(session_col).isNotNull()).count()
    events_without_session = total_events - events_with_session
    percentage_null = (events_without_session / total_events) * 100 if total_events > 0 else 0

    print(f"\nEventos COM '{session_col}': {events_with_session}")
    print(f"Eventos SEM '{session_col}' (nulos): {events_without_session}")
    print(f"Porcentagem de eventos com session_id nulo: {percentage_null:.2f}%")

    unique_sessions = df.select(session_col).distinct().count()
    unique_users = df.select(user_col).distinct().count()

    if events_without_session > 0:
        unique_sessions -= 1

    avg_sessions_per_user = (unique_sessions / unique_users) if unique_users > 0 else 0

    print(f"\nTotal de sessões únicas: {unique_sessions}")
    print(f"Total de usuários únicos: {unique_users}")
    print(f"Média de sessões por usuário (com base nos IDs existentes): {avg_sessions_per_user:.2f}")

    df.unpersist()
    print("--- ANÁLISE CONCLUÍDA ---")

if __name__ == '__main__':
    spark = make_spark(memory_storage_fraction=0.5)
    data_path = os.getenv("DATA_PATH")

    MERGED_DATA_PATH = f"{data_path}/enriched_events"

    merged_data_from_disk = spark.read.parquet(MERGED_DATA_PATH)

    analyze_session_coverage(
        df=merged_data_from_disk,
        session_col="anonymized_session_id",
        user_col="anonymized_user_id"
    )