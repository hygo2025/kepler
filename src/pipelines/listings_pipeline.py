import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, lower, regexp_replace, trim, lit
from pyspark.sql.types import StringType

from src.utils.enviroment import listing_id_mapping_path, listings_raw_path, listings_processed_path
from src.utils.geocode import load_cep_cache, geocode_new_ceps
from src.utils.spark_utils import read_csv_data


def normalize_column(df: DataFrame, col_name: str) -> DataFrame:
    return (
        df.withColumn(col_name, trim(lower(col(col_name))))
          .withColumn(col_name, regexp_replace(col(col_name), "\\s+", " "))
    )


def enrich_listings(spark: SparkSession, df: DataFrame) -> DataFrame:
    df = df.dropna(subset=["state", "city", "neighborhood"])
    for c in ["state", "city", "neighborhood"]:
        df = normalize_column(df, c)

    print("\nIniciando geocodificação por CEP...")
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
    df_merged = df_final.drop(*cols_to_drop).join(spark_cache_cep, on='cep', how='left')
    return df_merged


def clean_data(df: DataFrame) -> DataFrame:
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


def deduplicate_and_map_ids(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df_active = df.filter(F.col("status") == "ACTIVE")

    window_spec = Window.partitionBy("anonymized_listing_id").orderBy(F.col("updated_at").desc())
    latest_df = (
        df_active.withColumn("rank", F.row_number().over(window_spec))
                 .filter(F.col("rank") == 1)
                 .drop("rank")
    )

    id_window = Window.orderBy(F.lit(1))
    distinct_ids = latest_df.select("anonymized_listing_id").distinct()
    mapping_table = distinct_ids.withColumn("listing_id_numeric", F.row_number().over(id_window))

    enriched_df = latest_df.join(mapping_table, "anonymized_listing_id", "inner")
    return enriched_df, mapping_table


def save_results(df_final: DataFrame, mapping_table: DataFrame):
    final_path = listings_processed_path()
    mapping_path = listing_id_mapping_path()

    df_final_persisted = None
    mapping_table_persisted = None
    try:
        df_final_persisted = df_final.persist()
        mapping_table_persisted = mapping_table.persist()

        print(f"\nSalvando listings processados em: {final_path}")
        df_final_persisted.coalesce(1).write.mode("overwrite").parquet(final_path)

        print(f"\nSalvando mapeamento de listings em: {mapping_path}")
        mapping_table_persisted.write.mode("overwrite").parquet(mapping_path)
    finally:
        if df_final_persisted:
            df_final_persisted.unpersist()
        if mapping_table_persisted:
            mapping_table_persisted.unpersist()


def run_listings_pipeline(spark: SparkSession):
    print("Iniciando pipeline de listings...")
    raw_path = listings_raw_path() + "/*.csv.gz"
    all_raw_listings = read_csv_data(spark, raw_path, multiline=True)

    all_raw_listings = all_raw_listings.filter(
        (col("status") != "DRAFT") & (col("status") != "BLOCKED")
    )

    cleaned_listings = clean_data(all_raw_listings)
    final_df, mapping_table = deduplicate_and_map_ids(cleaned_listings)

    final_df = final_df.drop("status", "floors", "ceiling_height")
    final_df = enrich_listings(spark, final_df)

    save_results(final_df, mapping_table)
    print("\nListings pipeline concluído.")
