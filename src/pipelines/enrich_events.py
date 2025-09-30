from itertools import chain
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def fill_user_id(raw_events: DataFrame) -> DataFrame:
    """Preenche o user_id usando uma Window Function para performance máxima."""
    window_spec = Window.partitionBy("anonymous_id")
    result = raw_events.withColumn(
        "filled_user_id", F.first(F.col("user_id"), ignorenulls=True).over(window_spec)
    )
    result = result.withColumn(
        "user_id",
        F.when(F.col("user_id").isNull(), F.col("filled_user_id")).otherwise(
            F.col("user_id")
        ),
    ).withColumn(
        "user_id", F.coalesce(F.col("user_id"), F.col("anonymous_id"))
    ).drop("filled_user_id")
    return result


def boost(df: DataFrame, boost_period: int = 7, boost: float = 1.3) -> DataFrame:
    df = df.withColumn("days_until_today", F.datediff(F.current_date(), F.col("dt")))
    df = df.withColumn(
        "boost",
        F.when(
            F.col("days_until_today") <= boost_period,
            F.col("weight") * boost,
        ).otherwise(
            F.col("weight")
            * F.exp((-F.col("days_until_today") + boost_period) / 270.0)
        ),
    )
    return df


def to_enriched_event(raw_events: DataFrame) -> DataFrame:
    def create_ratings_map() -> F.Column:
        return F.create_map(
            [
                F.lit(x)
                for x in chain(
                *{
                    "VISIT": 0.1, "LEAD": 1.0, "LEAD_IDENTIFIED": 2.0,
                    "LEAD_INTENTION": 0.25, "FAVORITE": 0.25,
                    "PREVIEW": 0.075, "OTHER": 0.075,
                }.items()
            )
            ]
        )

    raw_events = fill_user_id(raw_events).withColumn(
        "weight", create_ratings_map()[F.col("event_type")]
    )
    raw_events = boost(df=raw_events)
    raw_events = raw_events.withColumn("rating", F.col("boost"))
    return raw_events


def rename_and_drop_columns(
        users: DataFrame, listings: DataFrame, raw_events: DataFrame
) -> tuple:
    users = users.withColumnRenamed("id", "user_user_id").withColumnRenamed(
        "anonymous_id", "user_anonymous_id"
    )
    listings = (
        listings
        .withColumnRenamed("anonymized_listing_id", "listing_id")
        .drop("month", "dt")
    )
    raw_events = (
        raw_events
        .withColumnRenamed("anonymized_user_id", "user_id")
        .withColumnRenamed("anonymized_anonymous_id", "anonymous_id")
        .withColumnRenamed("anonymized_listing_id", "listing_id")
        .drop(
            "anonymized_session_id", "browser_family", "os_family",
            "is_bot", "event_date", "month", "listing_id_numeric"
        )
    )
    return users, listings, raw_events

def enrich_events(spark: SparkSession, paths: dict):
    users_raw = spark.read.option("mergeSchema", "true").parquet(paths["USER_SESSIONS_PATH"])
    listings_raw = spark.read.option("mergeSchema", "true").parquet(paths["LISTINGS_PROCESSED_PATH"])
    raw_events_raw = spark.read.option("mergeSchema", "true").parquet(paths["EVENTS_PROCESSED_PATH"])

    users, listings, raw_events = rename_and_drop_columns(
        users=users_raw,
        listings=listings_raw,
        raw_events=raw_events_raw,
    )

    raw_events = raw_events.drop("dt").withColumn(
        "dt", F.from_unixtime(F.col("collector_timestamp") / 1000).cast("timestamp")
    )
    events_enriched = to_enriched_event(raw_events=raw_events)
    events_enriched = events_enriched.filter(F.col("rating") > 0.001)
    events_enriched = events_enriched.join(listings, on="listing_id", how="inner")

    events_enriched = events_enriched.join(
        users, events_enriched["user_id"] == users["user_anonymous_id"], "left"
    )

    print(f"Salvando base de dados de ratings em: {paths['ENRICHED_EVENTS_PATH']}")

    (
        events_enriched
        .coalesce(32)
        .write
        .mode("overwrite")
        .partitionBy("dt")
        .option("compression", "snappy")
        .parquet(paths["ENRICHED_EVENTS_PATH"])
    )
