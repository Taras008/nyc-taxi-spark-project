from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("NYC Taxi Test App")
        .getOrCreate()
    )

    data = [
        ("Taras", 1, "NYC"),
        ("Anna", 2, "Brooklyn"),
        ("Oleh", 3, "Queens")
    ]

    columns = ["name", "id", "borough"]

    df = spark.createDataFrame(data, columns)

    print("Тестовий DataFrame:")
    df.show()

    spark.stop()


if __name__ == "__main__":
    main()