"""Analysis jobs: read the full parquet dataset
compute aggregates and save the small results to /flights/results/ as parquet

Conventions:
- a "delayed" flight = ARR_DELAY >= 15 min (US DOT standard)
- cancelled/diverted flights have ARR_DELAY = null and are excluded from
  delay statistics (but they do count towards cancellation statistics)
- aggregates keep both averages and counts (n_*) so the app can correctly
  aggregate further (weighted means)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PARQUET_PATH = "hdfs:///flights/parquet"
RESULTS_PATH = "hdfs:///flights/results"

DELAY_THRESHOLD_MIN = 15

DELAY_CAUSE_COLS = [
    "CARRIER_DELAY",
    "WEATHER_DELAY",
    "NAS_DELAY",
    "SECURITY_DELAY",
    "LATE_AIRCRAFT_DELAY",
]


def is_delayed():
    return (F.col("ARR_DELAY") >= DELAY_THRESHOLD_MIN).cast("int")


def save(df, name):
    df.coalesce(1).write.mode("overwrite").parquet(
        "{}/{}".format(RESULTS_PATH, name)
    )
    print("SAVED {}".format(name))


def carrier_delays(df):
    """1. Delays per airline (per year -> trends)."""
    out = df.groupBy("OP_CARRIER", "year").agg(
        F.count("*").alias("n_flights"),
        F.count("ARR_DELAY").alias("n_with_delay_data"),
        F.avg("DEP_DELAY").alias("avg_dep_delay"),
        F.avg("ARR_DELAY").alias("avg_arr_delay"),
        F.sum(is_delayed()).alias("n_delayed"),
        F.sum(F.col("CANCELLED")).alias("n_cancelled"),
    )
    save(out, "carrier_delays")


def cancellations_time(df):
    """2a. Cancellations per (year, month, day of week)."""
    out = (
        df.withColumn("month", F.month("FL_DATE"))
        .withColumn("day_of_week", F.dayofweek("FL_DATE"))  # 1 = Sunday
        .groupBy("year", "month", "day_of_week")
        .agg(
            F.count("*").alias("n_flights"),
            F.sum(F.col("CANCELLED")).alias("n_cancelled"),
        )
    )
    save(out, "cancellations_time")


def cancellation_causes(df):
    """2b. Cancellation causes (A=carrier, B=weather, C=NAS, D=security)."""
    out = (
        df.filter(F.col("CANCELLED") == 1)
        .groupBy("year", "CANCELLATION_CODE")
        .agg(F.count("*").alias("n_cancelled"))
    )
    save(out, "cancellation_causes")


def airport_taxi_out(df):
    """3. Taxi-out time per airport."""
    out = df.groupBy("ORIGIN", "year").agg(
        F.count("*").alias("n_flights"),
        F.avg("TAXI_OUT").alias("avg_taxi_out"),
        F.avg("TAXI_IN").alias("avg_taxi_in"),
    )
    save(out, "airport_taxi_out")


def airport_delay_causes(df):
    """4. Delay cause structure per airport (total minutes per cause)."""
    sums = [F.sum(c).alias("sum_{}".format(c.lower())) for c in DELAY_CAUSE_COLS]
    out = df.groupBy("ORIGIN", "year").agg(
        F.count("*").alias("n_flights"), *sums
    )
    save(out, "airport_delay_causes")


def distance_carriers(df):
    """5. Short vs long haul per carrier."""
    bucket = (
        F.when(F.col("DISTANCE") < 500, "short (<500 mi)")
        .when(F.col("DISTANCE") < 1500, "medium (500-1500 mi)")
        .otherwise("long (>1500 mi)")
    )
    out = (
        df.withColumn("distance_bucket", bucket)
        .groupBy("OP_CARRIER", "distance_bucket", "year")
        .agg(
            F.count("*").alias("n_flights"),
            F.avg("ARR_DELAY").alias("avg_arr_delay"),
            F.avg("DISTANCE").alias("avg_distance"),
        )
    )
    save(out, "distance_carriers")


def routes(df):
    """6. Busiest routes + their punctuality."""
    out = df.groupBy("ORIGIN", "DEST", "year").agg(
        F.count("*").alias("n_flights"),
        F.avg("ARR_DELAY").alias("avg_arr_delay"),
        F.sum(is_delayed()).alias("n_delayed"),
        F.sum(F.col("CANCELLED")).alias("n_cancelled"),
        F.avg("DISTANCE").alias("avg_distance"),
    )
    save(out, "routes")


def route_carrier_stats(df):
    """7. Recommendation data: stats per (route, carrier)."""
    out = df.groupBy("ORIGIN", "DEST", "OP_CARRIER", "year").agg(
        F.count("*").alias("n_flights"),
        F.count("ARR_DELAY").alias("n_with_delay_data"),
        F.avg("ARR_DELAY").alias("avg_arr_delay"),
        F.sum(is_delayed()).alias("n_delayed"),
        F.sum(F.col("CANCELLED")).alias("n_cancelled"),
    )
    save(out, "route_carrier_stats")


ANALYSES = [
    carrier_delays,
    cancellations_time,
    cancellation_causes,
    airport_taxi_out,
    airport_delay_causes,
    distance_carriers,
    routes,
    route_carrier_stats,
]


def main():
    spark = SparkSession.builder.appName("compute-aggregates").getOrCreate()
    df = spark.read.parquet(PARQUET_PATH)  # `year` column from partitioning

    for analysis in ANALYSES:
        analysis(df)

    spark.stop()
    print("ALL AGGREGATES DONE")


if __name__ == "__main__":
    main()
