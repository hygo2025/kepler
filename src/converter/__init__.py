from pyspark.sql import SparkSession, Window

from src.converter.events_pipeline import EventsPipeline
from src.converter.listings_pipeline import ListingsPipeline
from src.converter.user_id_mapping import create_user_identity_map


def convert_to_parquet(spark: SparkSession, paths: dict):
    #ListingsPipeline(spark=spark, paths=paths).run()
    create_user_identity_map(spark=spark, paths=paths)
    #EventsPipeline(spark=spark, paths=paths).run()
