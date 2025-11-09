from pyspark.sql import SparkSession

from src.pipelines.events_pipeline import EventsPipeline
from src.pipelines.listings_pipeline import ListingsPipeline
from src.pipelines.processing.event_enrichment import enrich_events
from src.pipelines.processing.sessionization import resolve_user_identities


def prepare_data(spark: SparkSession):
    """
    Orquestra a execução de todo o pipeline de dados.
    """
    print("\nIniciando pipeline de preparação de dados...")

    # # Etapa 1: Processa, limpa e cria o mapa de listings
    # ListingsPipeline(spark=spark).run()

    # Etapa 2: Processa, limpa e filtra eventos com base nos listings
    EventsPipeline(spark=spark).run()

    # Etapa 3: Cria a sessão de usuário (ID anônimo -> ID de usuário)
    resolve_user_identities(spark=spark)

    # Etapa 4: Enriquece eventos com dados de listings, sessões e 'identity stitching'
    enrich_events(spark=spark)

    print("\nPipeline de preparação de dados concluído com sucesso!")
