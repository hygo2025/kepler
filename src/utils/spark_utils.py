from pyspark.sql import DataFrame, SparkSession

def read_csv_data(spark: SparkSession, file_path: str, multiline: bool = True) -> DataFrame:
    print(f"Lendo dados CSV de: {file_path}")
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("multiLine", str(multiline).lower())
        .option("quote", '"').option("escape", '"')
        .option("delimiter", ",").option("encoding", "utf-8")
        .option("mode", "PERMISSIVE").option("inferSchema", "false")
        .load(file_path)
    )