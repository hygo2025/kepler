from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.utils.spark_utils import read_csv_data


class EventsPipeline:
    def __init__(self, spark: SparkSession, paths: dict):
        self.spark = spark
        self.paths = paths
        self.listing_map = self.spark.read.parquet(self.paths["LISTING_ID_MAPPING_PATH"])

    def run(self):
        raw_path = self.paths["EVENTS_RAW_PATH"] + "/*.csv.gz"
        all_raw_events = read_csv_data(self.spark, raw_path, multiline=False)

        cleaned_df = self._clean_data(all_raw_events)
        df = cleaned_df.join(self.listing_map, on="anonymized_listing_id", how="inner")

        df.write.mode("overwrite").partitionBy("dt").parquet(self.paths["EVENTS_PROCESSED_PATH"])


    def _clean_data(self, df: DataFrame) -> DataFrame:
        for name in ["anonymized_user_id", "anonymized_anonymous_id", "anonymized_listing_id"]:
            if name in df.columns:
                df = df.withColumn(name, F.when(F.trim(F.col(name)) == "", None).otherwise(F.trim(F.col(name))))

        if "collector_timestamp" in df.columns:
            df = df.withColumn("event_ts", (F.col("collector_timestamp").cast("bigint") / 1000).cast("timestamp"))
        if "dt" in df.columns:
            df = df.withColumn("dt", F.to_date(F.col("dt")))
            if "event_ts" not in df.columns:
                df = df.withColumn("event_ts", F.to_timestamp(F.col("dt")))

        return df
