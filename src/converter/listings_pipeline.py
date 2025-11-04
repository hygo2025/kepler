from pyspark.sql import DataFrame, Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lower, regexp_replace, trim, lit
from pyspark.sql.types import StringType

from src.utils.enviroment import listing_id_mapping_path, listings_raw_path
from src.utils.enviroment import listings_processed_path
from src.utils.geocode import (
    load_cep_cache, geocode_new_ceps
)
from src.utils.spark_utils import read_csv_data


def normalize_spark_column(df, col_name):
    return (
        df.withColumn(col_name, trim(lower(col(col_name))))
        .withColumn(col_name, regexp_replace(col(col_name), "\\s+", " "))
    )


def enrich_listings(spark: SparkSession, df: DataFrame):
    df = df.dropna(subset=["state", "city", "neighborhood"])
    for c in ["state", "city", "neighborhood"]:
        df = normalize_spark_column(df, c)

    print("Iniciando geocodificação por CEP (zip_code)...")
    if "zip_code" in df.columns:
        df_final = df.withColumn("cep", regexp_replace(col("zip_code"), "[^0-9]", ""))
    else:
        df_final = df.withColumn("cep", lit(None).cast(StringType()))

    cache_df_cep = load_cep_cache()
    cache_keys_cep = set(cache_df_cep.cep)

    unique_ceps = df_final.select("cep").filter(col("cep") != "").distinct().toPandas()

    new_ceps = [
        r.cep
        for r in unique_ceps.itertuples(index=False)
        if r.cep and r.cep not in cache_keys_cep
    ]

    updated_cache_cep = geocode_new_ceps(new_ceps, cache_df_cep)

    cols_to_select = ['cep', 'state', 'city', 'neighborhood', 'street', 'longitude', 'latitude']

    spark_cache_cep = spark.createDataFrame(updated_cache_cep).select(cols_to_select)

    cols_to_drop = ['state', 'city', 'neighborhood', 'zip_code']

    df_merged = df_final.drop(*cols_to_drop).join(
        spark_cache_cep,
        on='cep',
        how='left'
    )

    df_merged.printSchema()

    return df_merged


class ListingsPipeline:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def run(self):
        raw_path = listings_raw_path() + "/*.csv.gz"
        all_raw_listings = read_csv_data(self.spark, raw_path, multiline=True)
        all_raw_listings = all_raw_listings.filter(
            (col("status") != "DRAFT") & (col("status") != "BLOCKED")
        )

        # all_raw_listings = all_raw_listings.filter(
        #     (col("state") == "Espírito Santo") & (col("city") == "Vila Velha")
        # )

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
        df = df.filter(F.col("status") == "ACTIVE")
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

            df_final_persisted.coalesce(1).write.mode("overwrite").parquet(final_path)
            mapping_table_persisted.write.mode("overwrite").parquet(mapping_path)
        finally:
            if df_final_persisted: df_final_persisted.unpersist()
            if mapping_table_persisted: mapping_table_persisted.unpersist()