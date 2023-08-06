import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def process(spark, flights_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param result_path: путь с результатами преобразований
    """

    # Чтение данных
    flights = spark.read \
        .option("header", "true") \
        .parquet(flights_path)

    # Преобразование данных
    result_flights = flights \
        .groupby(flights['ORIGIN_AIRPORT']) \
        .agg(F.avg(flights['DEPARTURE_DELAY']).alias('avg_delay'),
             F.min(flights['DEPARTURE_DELAY']).alias('min_delay'),
             F.max(flights['DEPARTURE_DELAY']).alias('max_delay'),
             F.corr(flights['DEPARTURE_DELAY'], flights['DAY_OF_WEEK']).alias('corr_delay2day_of_week')) \
        .select(F.col('ORIGIN_AIRPORT'),
               F.col('avg_delay'),
               F.col('min_delay'),
               F.col('max_delay'),
               F.col('corr_delay2day_of_week')) \
        .where(F.col('max_delay') > 1000)

    # Запись данных
    result_flights.write.mode("overwrite").parquet(result_path)


def main(flights_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob3').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)
