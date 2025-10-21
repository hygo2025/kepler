from src.utils.enviroment import enriched_events_path
from src.utils.spark_session import make_spark


if __name__ == '__main__':
    spark = make_spark()
    events = spark.read.option("mergeSchema", "true").parquet(enriched_events_path())
    events.printSchema()
    print(events.count())