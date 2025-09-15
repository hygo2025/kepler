from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from src.to_parquet.events_standardizer import EventsStandardizer
from src.to_parquet.listings_standardizer import ListingsStandardizer


class RawToParquetConverter:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.dataset_configs = {
            "events": {
                "standardizer": EventsStandardizer(),
                "multiline": False
            },
            "listings": {
                "standardizer": ListingsStandardizer(),
                "multiline": True
            }
        }

    def run(self, suffix: str, raw_path: str, data_path: str, glob_pattern: str = "*.csv.gz") -> None:
        if not raw_path or not data_path:
            raise ValueError("Os caminhos 'raw_path' e 'data_path' devem ser fornecidos.")

        config = self.dataset_configs.get(suffix)
        if not config:
            raise ValueError(f"Suffix '{suffix}' não é suportado. Válidos: {list(self.dataset_configs.keys())}")

        dataset_name = suffix
        standardizer = config["standardizer"]
        multiline_default = config["multiline"]

        origin_path = Path(raw_path)
        full_glob_path = str(origin_path / glob_pattern)
        out_base = Path(data_path) / dataset_name
        out_base.mkdir(parents=True, exist_ok=True)

        print(f"[{dataset_name}] Lendo todos os arquivos de: {full_glob_path}")
        df_raw = self._read_csv_gz(full_glob_path, multiline=multiline_default)

        if df_raw.rdd.isEmpty():
            print(f"[{dataset_name}] Aviso: Nenhum arquivo encontrado em {origin_path} com padrão '{glob_pattern}'")
            return

        print(f"[{dataset_name}] Sanando e padronizando os dados...")
        df_clean = self._sanitize_raw_by_suffix(df_raw, suffix=dataset_name)

        df_std = standardizer.transform(df_clean)

        df_final = df_std.withColumn("partition_date", F.regexp_replace(F.col("dt"), "-", "_"))

        print(f"[{dataset_name}] Gravando dataset particionado em Parquet -> {out_base}")
        (
            df_final.write
            .mode("overwrite")
            .partitionBy("partition_date")
            .option("compression", "snappy")
            .parquet(str(out_base))
        )

        self._save_schema(df_final.drop("partition_date"), out_base / "_schema")
        print(f"[{dataset_name}] Conclusão: Dataset convertido com sucesso para {out_base}")

    def _read_csv_gz(self, file_path: str, multiline: bool) -> DataFrame:
        return (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("multiLine", str(multiline).lower())
            .option("quote", '"').option("escape", '"')
            .option("delimiter", ",").option("encoding", "utf-8")
            .option("mode", "PERMISSIVE").option("inferSchema", "false")
            .load(file_path)
        )

    @staticmethod
    def _sanitize_raw_by_suffix(df: DataFrame, suffix: str) -> DataFrame:
        if "listing" in suffix.lower() and "amenities" in df.columns:
            return df.withColumn("amenities", F.trim(F.regexp_replace(F.col("amenities"), r"[\r\n\s]+", " ")))
        return df

    @staticmethod
    def _save_schema(df: DataFrame, schema_base: Path) -> None:
        schema_base.parent.mkdir(parents=True, exist_ok=True)
        schema_base.with_suffix(".schema.json").write_text(df.schema.json(), encoding="utf-8")
        schema_base.with_suffix(".schema.ddl").write_text(df._jdf.schema().toDDL(), encoding="utf-8")
