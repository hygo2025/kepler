from pyspark.sql import SparkSession, Window

from src.converter.events_pipeline import EventsPipeline
from src.converter.listings_pipeline import ListingsPipeline
from src.converter.user_session import make_user_session


def convert_to_parquet(spark: SparkSession, paths: dict):
    #ListingsPipeline(spark=spark, paths=paths).run()
    #EventsPipeline(spark=spark, paths=paths).run()
    make_user_session(spark=spark, paths=paths)
