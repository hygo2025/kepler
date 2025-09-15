import os
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from typing import Tuple

from src.merge_data import point_in_time_join
from src.utils import make_spark


def debug_data_loss(
        spark: SparkSession,
        listings_path: str = None,
        events_path: str = None,
):
    print("\n--- INICIANDO DIAGNÓSTICO DE PERDA DE DADOS ---")
    listings_df = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(listings_path)
    )

    events_df = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(events_path)
    )

    initial_events_count = events_df.count()
    print(f"Contagem inicial de eventos (após filtro de data): {initial_events_count}")

    events_renamed = events_df.withColumnRenamed("anonymized_listing_id", "listing_id")
    listings_renamed = listings_df.withColumnRenamed("anonymized_listing_id", "listing_id")

    final_join_df = point_in_time_join(spark, listings_path=listings_path, events_path=events_path)

    final_events_count = final_join_df.count()

    lost_count = initial_events_count - final_events_count
    loss_percentage = (lost_count / initial_events_count) * 100 if initial_events_count > 0 else 0

    print(f"Contagem final de eventos (após INNER JOIN): {final_events_count}")
    print(f"Total de eventos perdidos no join: {lost_count} ({loss_percentage:.2f}%)")

    if lost_count > 0:
        print("\nInvestigando as causas da perda...")

        left_join_df = events_renamed.join(
            listings_renamed,
            on=[
                events_renamed.listing_id == listings_renamed.listing_id,
                events_renamed.event_date >= listings_renamed.listing_snapshot_date
            ],
            how="left"
        ).select(
            events_renamed["*"],
            *[listings_renamed[c].alias("listing_" + c) for c in listings_renamed.columns]
        )

        lost_events_df = left_join_df.filter(F.col("listing_listing_id").isNull()).cache()

        print("\nExemplos de eventos que foram perdidos:")
        lost_events_df.select("listing_id", "event_date", "event_type").show(10, truncate=False)

        all_listing_ids_df = listings_renamed.select("listing_id").distinct().cache()

        orphan_ids_df = lost_events_df.select("listing_id").distinct() \
            .join(all_listing_ids_df, on="listing_id", how="left_anti")

        orphan_count = orphan_ids_df.count()
        temporal_mismatch_count = lost_count - orphan_count

        print("\n--- Causa da Perda ---")
        print(f"Eventos com IDs de imóveis que NUNCA aparecem no dataset (órfãos): {orphan_count}")
        print(
            f"Eventos com IDs válidos, mas fora do período de tempo do imóvel (incompat. temporal): {temporal_mismatch_count}")

    print("--- DIAGNÓSTICO CONCLUÍDO ---\n")


if __name__ == '__main__':
    spark = make_spark()
    data_path = os.getenv("DATA_PATH")

    listings_path = f"{data_path}/listings"
    events_path = f"{data_path}/events"

    debug_data_loss(
        spark=spark,
        listings_path=listings_path,
        events_path=events_path,
    )