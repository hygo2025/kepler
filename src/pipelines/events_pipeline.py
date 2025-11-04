import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame

from src.utils.enviroment import listing_id_mapping_path, events_raw_path, events_processed_path
from src.utils.spark_utils import read_csv_data


class EventsPipeline:
    """
    Pipeline para processar eventos brutos, limpá-los e
    filtrá-los usando a tabela de mapeamento de listings.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        # Carrega o mapa de listings válidos (criado pelo ListingsPipeline)
        self.listing_map = self.spark.read.parquet(listing_id_mapping_path())

    def _clean_data(self, df: DataFrame) -> DataFrame:
        """Limpa e padroniza os tipos de dados dos eventos."""
        for name in ["anonymized_user_id", "anonymized_anonymous_id", "anonymized_listing_id"]:
            if name in df.columns:
                df = df.withColumn(name, F.when(F.trim(F.col(name)) == "", None).otherwise(F.trim(F.col(name))))

        if "collector_timestamp" in df.columns:
            # Converte timestamp de milissegundos para timestamp
            df = df.withColumn("event_ts", (F.col("collector_timestamp").cast("bigint") / 1000).cast("timestamp"))

        if "dt" in df.columns:
            df = df.withColumn("dt", F.to_date(F.col("dt")))
            if "event_ts" not in df.columns:
                df = df.withColumn("event_ts", F.to_timestamp(F.col("dt")))
        return df

    def run(self):
        """Ponto de entrada para executar o pipeline de eventos."""
        print("Iniciando EventsPipeline...")
        sale_raw_path = events_raw_path() + "/*.csv.gz"
        all_raw_events = read_csv_data(self.spark, sale_raw_path, multiline=False)
        all_raw_events = all_raw_events.filter(F.col("anonymized_user_id").isNotNull())

        print(f"Contagem de eventos brutos (pré-join): {all_raw_events.count()}")

        # Filtra eventos: mantém apenas eventos de listings que são "ACTIVE" e válidos
        joined_df = all_raw_events.join(self.listing_map, on="anonymized_listing_id", how="inner")

        print(f"Contagem de eventos (pós-join com listings): {joined_df.count()}")

        final_df = self._clean_data(joined_df)

        output_path = events_processed_path()
        print(f"Salvando eventos processados em: {output_path}")
        (
            final_df.write
            .mode("overwrite")
            .partitionBy("dt")
            .parquet(output_path)
        )
        print("EventsPipeline concluído.")