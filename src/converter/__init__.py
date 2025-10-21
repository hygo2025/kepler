from pyspark.sql import SparkSession, Window

from src.converter.enrich_events import enrich_events
from src.converter.events_pipeline import EventsPipeline
from src.converter.listings_pipeline import ListingsPipeline
from src.converter.user_session import make_user_session


def prepare_data(spark: SparkSession):
    ListingsPipeline(spark=spark).run()
    EventsPipeline(spark=spark).run()
    # make_user_session(spark=spark)
    # enrich_events(spark=spark)
