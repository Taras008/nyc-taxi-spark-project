from pyspark.sql import DataFrame, SparkSession
from src.schemas import trip_data_schema, trip_fare_schema


def load_trip_data(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("ignoreLeadingWhiteSpace", True)
        .option("ignoreTrailingWhiteSpace", True)
        .schema(trip_data_schema)
        .csv(path)
    )


def load_trip_fare(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("ignoreLeadingWhiteSpace", True)
        .option("ignoreTrailingWhiteSpace", True)
        .schema(trip_fare_schema)
        .csv(path)
    )