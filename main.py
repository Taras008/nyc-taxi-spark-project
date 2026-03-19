from pyspark.sql import SparkSession
from src.data_loader import load_trip_data, load_trip_fare


def main():
    spark = (
        SparkSession.builder
        .appName("NYC Taxi Extraction Stage")
        .getOrCreate()
    )

    trip_data_path = "/data/nyc_taxi_dataset/trip_data/trip_data_1.csv"
    trip_fare_path = "/data/nyc_taxi_dataset/trip_fare/trip_fare_1.csv"

    trip_data_df = load_trip_data(spark, trip_data_path)
    trip_fare_df = load_trip_fare(spark, trip_fare_path)

    print("=== TRIP DATA SCHEMA ===")
    trip_data_df.printSchema()

    print("=== TRIP DATA SAMPLE ===")
    trip_data_df.show(5, truncate=False)

    print("=== TRIP FARE SCHEMA ===")
    trip_fare_df.printSchema()

    print("=== TRIP FARE SAMPLE ===")
    trip_fare_df.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()