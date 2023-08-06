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


def process(flights, airlines):
    """
    Основной процесс задачи.

    :param flights: датасет c рейсами
    :param airlines: датасет c авиалиниями
    """

    datamart = flights \
        .join(airlines, flights.AIRLINE == airlines.IATA_CODE) \
        .groupby(airlines.AIRLINE) \
        .agg(F.count(flights.TAIL_NUMBER).alias('correct_count'),
             F.count(F.when(flights.DEPARTURE_DELAY > 0, 1)).alias('diverted_count'),
             F.count(F.when(flights.CANCELLED == 1, 1)).alias('cancelled_count'),
             F.avg(flights.DISTANCE).alias('avg_distance'),
             F.avg(flights.AIR_TIME).alias('avg_air_time'),
             F.count(F.when(flights.CANCELLED == 'A', 1)).alias('airline_issue_count'),
             F.count(F.when(flights.CANCELLED == 'B', 1)).alias('weather_issue_count'),
             F.count(F.when(flights.CANCELLED == 'C', 1)).alias('nas_issue_count'),
             F.count(F.when(flights.CANCELLED == 'D', 1)).alias('security_issue_count'),
             ) \
        .select(airlines.AIRLINE.alias('AIRLINE_NAME'),
                F.col('correct_count'),
                F.col('diverted_count'),
                F.col('cancelled_count'),
                F.col('avg_distance'),
                F.col('avg_air_time'),
                F.col('airline_issue_count'),
                F.col('weather_issue_count'),
                F.col('nas_issue_count'),
                F.col('security_issue_count'))

    return datamart



def main(flights_path, airlines_path, result_path):
    spark = _spark_session()
    flights = read_data(spark, flights_path)
    airlines = read_data(spark, airlines_path)
    datamart = process(flights, airlines)
    write_data(datamart, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob5').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='airlines.parquet', help='Please set airlines datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    result_path = args.result_path
    main(flights_path, airlines_path, result_path)