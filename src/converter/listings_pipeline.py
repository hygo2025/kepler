
from pyspark.sql import SparkSession, DataFrame, Column, Window
from pyspark.sql import functions as F

from src.utils.spark_utils import read_csv_data

class ListingsPipeline:
    def __init__(self, spark: SparkSession, paths: dict):
        self.spark = spark
        self.paths = paths

    def run(self):
        raw_path = self.paths["LISTINGS_RAW_PATH"] + "/*.csv.gz"
        all_raw_listings = read_csv_data(self.spark, raw_path, multiline=True)

        cleaned_listings = self._clean_data(all_raw_listings)
        final_df, mapping_table = self._deduplicate_and_map_ids(cleaned_listings)
        final_df = final_df.drop("status", "floors", "ceiling_height")
        self._save_results(final_df, mapping_table)

    def _clean_data(self, df: DataFrame) -> DataFrame:
        for c in ['price', 'usable_areas', 'total_areas', 'ceiling_height']:
            if c in df.columns:
                df = df.withColumn(c, F.regexp_replace(F.col(c), r"[^0-9.]", "").cast("double"))

        for c in ['bathrooms', 'bedrooms', 'suites', 'parking_spaces', 'floors']:
            if c in df.columns:
                df = df.withColumn(c, F.col(c).cast("integer"))

        if 'dt' in df.columns:
            df = df.withColumn('dt', F.to_date(F.col('dt')))
        if 'created_at' in df.columns:
            df = df.withColumn('created_at', F.to_timestamp(F.col('created_at')))
        if 'updated_at' in df.columns:
            df = df.withColumn('updated_at', F.to_timestamp(F.col('updated_at')))

        return df

    def _deduplicate_and_map_ids(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        window_spec = Window.partitionBy("anonymized_listing_id").orderBy(F.col("updated_at").desc())
        latest_df = (
            df.withColumn("rank", F.row_number().over(window_spec))
            .filter(F.col("rank") == 1)
            .drop("rank")
        )

        id_window = Window.orderBy(F.lit(1))
        distinct_ids = latest_df.select("anonymized_listing_id").distinct()
        mapping_table = distinct_ids.withColumn("listing_id_numeric", F.row_number().over(id_window))

        enriched_df = latest_df.join(mapping_table, "anonymized_listing_id", "inner")
        return enriched_df, mapping_table

    def _save_results(self, df_final, mapping_table):
        final_path = self.paths["LISTINGS_PROCESSED_PATH"]
        mapping_path = self.paths["LISTING_ID_MAPPING_PATH"]

        df_final_persisted = None
        mapping_table_persisted = None
        try:
            df_final_persisted = df_final.persist()
            mapping_table_persisted = mapping_table.persist()

            df_final_persisted.coalesce(16).write.mode("overwrite").parquet(final_path)
            mapping_table_persisted.write.mode("overwrite").parquet(mapping_path)
        finally:
            if df_final_persisted: df_final_persisted.unpersist()
            if mapping_table_persisted: mapping_table_persisted.unpersist()
