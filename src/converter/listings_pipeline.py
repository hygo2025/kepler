
from pyspark.sql import DataFrame, Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lower, regexp_replace, trim
from pyspark.sql.types import StringType

from src.utils.enviroment import listing_id_mapping_path, listings_raw_path
from src.utils.enviroment import listings_processed_path
from src.utils.spark_utils import read_csv_data
from src.utils.geocode import load_geo_cache, geocode_new_locations


def normalize_spark_column(df, col_name):
    return (
        df.withColumn(col_name, trim(lower(col(col_name))))
        .withColumn(col_name, regexp_replace(col(col_name), "\\s+", " "))
    )


def enrich_listings(spark: SparkSession, df: DataFrame):
    df = df.dropna(subset=["state", "city", "neighborhood"])

    for c in ["state", "city", "neighborhood"]:
        df = normalize_spark_column(df, c)

    cache_df = load_geo_cache()
    cache_keys = set(zip(cache_df.state, cache_df.city, cache_df.neighborhood))

    print("Coletando localidades únicas para geocodificação...")
    unique_locations = df.select("state", "city", "neighborhood").distinct().toPandas()

    new_locs = [
        (r.state, r.city, r.neighborhood)
        for r in unique_locations.itertuples(index=False)
        if (r.state, r.city, r.neighborhood) not in cache_keys
    ]

    updated_cache = geocode_new_locations(new_locs, cache_df)

    print("Fazendo join dos resultados de geocodificação...")
    cache_spark = spark.createDataFrame(
        updated_cache[["state", "city", "neighborhood", "geopoint"]]
    )
    df_final = (
        df.join(cache_spark, on=["state", "city", "neighborhood"], how="left")
        .withColumn("geopoint", col("geopoint").cast(StringType()))
    )

    df_final = df_final.fillna({"geopoint": ""})

    df_final = df_final.filter(col("geopoint") != "")

    return df_final

class ListingsPipeline:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def run(self):
        raw_path = listings_raw_path() + "/*.csv.gz"
        all_raw_listings = read_csv_data(self.spark, raw_path, multiline=True)

        cleaned_listings = self._clean_data(all_raw_listings)
        final_df, mapping_table = self._deduplicate_and_map_ids(cleaned_listings)
        final_df = final_df.drop("status", "floors", "ceiling_height")
        final_df = enrich_listings(self.spark, final_df)
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
        final_path = listings_processed_path()
        mapping_path = listing_id_mapping_path()

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
