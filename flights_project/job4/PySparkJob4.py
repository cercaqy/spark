import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def read_data(spark, path):
    """
     Функция для чтения данных из Parquet файла.

     :param spark: SparkSession
     :param path: путь к файлу
     :return: DataFrame с данными
     """

    data = spark.read \
        .option("header", "true") \
        .parquet(path)
    return data


def write_data(data, result_path):
    """
    Функция для записи данных в Parquet файл.

    :param data: DataFrame с данными
    :param result_path: путь для сохранения результатов
    """

    data.write.mode("overwrite").parquet(result_path)


def process(flights, airlines, airports):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights: датасет c рейсами
    :param airlines: датасет c авиалиниями
    :param airlines: датасет c аэропортами
    """

    datamart = flights \
        .join(airlines, flights.AIRLINE == airlines.IATA_CODE) \
        .join(airports.alias('origin_airports'), flights.ORIGIN_AIRPORT == origin_airports.IATA_CODE) \
        .join(airports.alias('destination_airports'), flights.DESTINATION_AIRPORT == F.col('destination_airports.IATA_CODE')) \
        .select(F.col('airlines.AIRLINE').alias('AIRLINE_NAME'),
                flights.TAIL_NUMBER.alias('TAIL_NUMBER'),
                F.col('origin_airports.COUNTRY').alias('ORIGIN_COUNTRY'),
                F.col('origin_airports.AIRPORT').alias('ORIGIN_AIRPORT_NAME'),
                F.col('origin_airports.LATITUDE').alias('ORIGIN_LATITUDE'),
                F.col('origin_airports.LONGITUDE').alias('ORIGIN_LONGITUDE'),
                F.col('destination_airports.COUNTRY').alias('DESTINATION_COUNTRY'),
                F.col('destination_airports.AIRPORT').alias('DESTINATION_AIRPORT_NAME'),
                F.col('destination_airports.LATITUDE').alias('DESTINATION_LATITUDE'),
                F.col('destination_airports.LONGITUDE').alias('DESTINATION_LONGITUDE'))

    return datamart


def main(flights_path, airlines_path, airports_path, result_path):
    spark = _spark_session()
    flights = read_data(spark, flights_path)
    airlines = read_data(spark, airlines_path)
    airports = read_data(spark, airports_path)
    datamart = process(flights, airlines, airports)
    write_data(datamart, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob4').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='airlines.parquet', help='Please set airlines datasets path.')
    parser.add_argument('--airports_path', type=str, default='airports.parquet', help='Please set airports datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    airports_path = args.airports_path
    result_path = args.result_path
    main(flights_path, airlines_path, airports_path, result_path)