import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

COLUMN_ORDER = [
    "FL_DATE",
    "OP_CARRIER",
    "OP_CARRIER_FL_NUM",
    "ORIGIN",
    "DEST",
    "CRS_DEP_TIME",
    "DEP_TIME",
    "DEP_DELAY",
    "TAXI_OUT",
    "WHEELS_OFF",
    "WHEELS_ON",
    "TAXI_IN",
    "CRS_ARR_TIME",
    "ARR_TIME",
    "ARR_DELAY",
    "CANCELLED",
    "CANCELLATION_CODE",
    "DIVERTED",
    "CRS_ELAPSED_TIME",
    "ACTUAL_ELAPSED_TIME",
    "AIR_TIME",
    "DISTANCE",
    "CARRIER_DELAY",
    "WEATHER_DELAY",
    "NAS_DELAY",
    "SECURITY_DELAY",
    "LATE_AIRCRAFT_DELAY",
    "Unnamed: 27",
]

DATE_COLS = {"FL_DATE"}
STRING_COLS = {"OP_CARRIER", "ORIGIN", "DEST", "CANCELLATION_CODE", "Unnamed: 27"}
INT_COLS = {"OP_CARRIER_FL_NUM"}


def column_type(name):
    if name in DATE_COLS:
        return DateType()
    if name in STRING_COLS:
        return StringType()
    if name in INT_COLS:
        return IntegerType()
    return DoubleType()


SCHEMA = StructType(
    [StructField(name, column_type(name), True) for name in COLUMN_ORDER]
)


def main(year):
    spark = SparkSession.builder.appName("ingest-{}".format(year)).getOrCreate()

    df = spark.read.csv(
        "hdfs:///flights/raw/{}.csv".format(year), header=True, schema=SCHEMA
    )

    df = df.drop("Unnamed: 27")

    # Hive-style partitioning: the year=YYYY directory becomes a virtual "year" column when the whole /flights/parquet is read
    df.write.mode("overwrite").parquet(
        "hdfs:///flights/parquet/year={}".format(year)
    )

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1])
