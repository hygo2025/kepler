import os

from src.to_parquet.raw_to_parquet_converter import RawToParquetConverter
from src.utils import make_spark

if __name__ == "__main__":
    listings_raw_path = os.getenv("LISTINGS_RAW_PATH")
    events_raw_path = os.getenv("EVENTS_RAW_PATH")
    data_path = os.getenv("DATA_PATH")

    spark = make_spark()

    converter = RawToParquetConverter(spark)

    try:
        converter.run(
            suffix="listings",
            raw_path=listings_raw_path,
            data_path=data_path
        )

        converter.run(
            suffix="events",
            raw_path=events_raw_path,
            data_path=data_path
        )

    finally:
        print("Finalizando a sessão Spark.")
        spark.stop()