from itertools import chain

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


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