from pyspark.sql import SparkSession

from src.pipelines.events_pipeline import run_events_pipeline
from src.pipelines.listings_pipeline import run_listings_pipeline
from src.pipelines.merge_events import run_merge_events_pipeline


def prepare_data(spark: SparkSession):
    print("\nIniciando pipeline de preparação de dados...")
    #run_listings_pipeline(spark=spark)
    run_events_pipeline(spark=spark)
    run_merge_events_pipeline(spark=spark)


    print("\nPipeline de preparação de dados concluído com sucesso!")
