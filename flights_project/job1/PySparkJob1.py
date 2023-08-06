import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# Схема датасета
flights_schema = StructType([
    StructField('YEAR', IntegerType()),
    StructField('MONTH', IntegerType()),
    StructField('DAY', IntegerType()),
    StructField('DAY_OF_WEEK', IntegerType()),
    StructField('AIRLINE', StringType()),
    StructField('FLIGHT_NUMBER', StringType()),
    StructField('TAIL_NUMBER', StringType()),
    StructField('ORIGIN_AIRPORT', StringType()),
    StructField('DESTINATION_AIRPORT', StringType()),
    StructField('SCHEDULED_DEPARTURE', IntegerType()),
    StructField('DEPARTURE_TIME', IntegerType()),
    StructField('DEPARTURE_DELAY', IntegerType()),
    StructField('TAXI_OUT', IntegerType()),
    StructField('WHEELS_OFF', IntegerType()),
    StructField('SCHEDULED_TIME', IntegerType()),
    StructField('ELAPSED_TIME', IntegerType()),
    StructField('AIR_TIME', IntegerType()),
    StructField('DISTANCE', IntegerType()),
    StructField('WHEELS_ON', IntegerType()),
    StructField('TAXI_IN', IntegerType()),
    StructField('SCHEDULED_ARRIVAL', IntegerType()),
    StructField('ARRIVAL_TIME', IntegerType()),
    StructField('ARRIVAL_DELAY', IntegerType()),
    StructField('DIVERTED', IntegerType()),
    StructField('CANCELLED', IntegerType()),
    StructField('CANCELLATION_REASON', StringType()),
    StructField('AIR_SYSTEM_DELAY', IntegerType()),
    StructField('SECURITY_DELAY', IntegerType()),
    StructField('AIRLINE_DELAY', IntegerType()),
    StructField('LATE_AIRCRAFT_DELAY', IntegerType()),
    StructField('WEATHER_DELAY', IntegerType())
])


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
        .schema(flights_schema) \
        .parquet(flights_path)

    # Преобразование данных
    result_flights = flights \
        .where(flights['TAIL_NUMBER'].isNotNull()) \
        .groupby(flights['TAIL_NUMBER']) \
        .agg(F.count('*').alias('count')) \
        .select(F.col('TAIL_NUMBER'),
               F.col('count')) \
        .orderBy(F.col('count').desc()) \
        .limit(10)

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
    return SparkSession.builder.appName('PySparkJob1').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)