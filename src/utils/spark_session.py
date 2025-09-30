from pyspark.sql import SparkSession


def make_spark(
        memory_storage_fraction: float = 0.2,
) -> SparkSession:
    return (
        SparkSession.builder
        .appName("spark")
        .master("local[*]")
        .config("spark.driver.memory", "112g")
        .config("spark.sql.shuffle.partitions", 200)
        .config("spark.default.parallelism", 200)
        .config("spark.memory.storageFraction", memory_storage_fraction)
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256m")
        .config("spark.sql.files.maxPartitionBytes", "256m")
        .config("spark.sql.files.openCostInBytes", "8m")
        .config("spark.shuffle.manager", "sort")
        .config("spark.sql.autoBroadcastJoinThreshold", "512m")# isso tem de ficar em um valor bem baixo talvez algo proximo a 10m
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError")
    ).getOrCreate()
