## Задание
Для дашборда с отображением выполненных рейсов требуется собрать таблицу.
Никакой дополнительной фильтрации данных не требуется.

Требуемые поля:
| Поле                       | Описание                                              |
|----------------------------|-------------------------------------------------------|
| AIRLINE_NAME               | Название авиалинии (airlines.AIRLINE)                 |
| TAIL_NUMBER                | Номер рейса (flights.TAIL_NUMBER)                     |
| ORIGIN_COUNTRY             | Страна отправления (airports.COUNTRY)                 |
| ORIGIN_AIRPORT_NAME        | Полное название аэропорта отправления (airports.AIRPORT) |
| ORIGIN_LATITUDE            | Широта аэропорта отправления (airports.LATITUDE)     |
| ORIGIN_LONGITUDE           | Долгота аэропорта отправления (airports.LONGITUDE)    |
| DESTINATION_COUNTRY        | Страна прибытия (airports.COUNTRY)                    |
| DESTINATION_AIRPORT_NAME   | Полное название аэропорта прибытия (airports.AIRPORT)    |
| DESTINATION_LATITUDE       | Широта аэропорта прибытия (airports.LATITUDE)        |
| DESTINATION_LONGITUDE      | Долгота аэропорта прибытия (airports.LONGITUDE)       |

 Пример вывода:
 | AIRLINE_NAME      | TAIL_NUMBER | ORIGIN_COUNTRY | ORIGIN_AIRPORT_NAME                | ORIGIN_LATITUDE | ORIGIN_LONGITUDE | DESTINATION_COUNTRY | DESTINATION_AIRPORT_NAME           | DESTINATION_LATITUDE | DESTINATION_LONGITUDE |
|-------------------|-------------|----------------|------------------------------------|------------------|------------------|---------------------|-------------------------------------|----------------------|-----------------------|
| American Airlines Inc. | N787AA      | USA            | John F. Kennedy International Airport (New York) | 40.63975         | -73.77893        | USA                 | Los Angeles International Airport    | 33.94254             | -118.40807            |
| American Airlines Inc. | N795AA      | USA            | Los Angeles International Airport | 33.94254         | -118.40807       | USA                 | John F. Kennedy International Airport (New York) | 40.63975             | -73.77893             |
| American Airlines Inc. | N798AA      | USA            | John F. Kennedy International Airport (New York) | 40.63975         | -73.77893        | USA                 | Los Angeles International Airport    | 33.94254             | -118.40807            |
| American Airlines Inc. | N799AA      | USA            | USA                                | 33.94254         | -118.40807       | USA                 | John F. Kennedy International Airport (New York) | 40.63975             | -73.77893             |
| American Airlines Inc. | N376AA      | USA            | Los Angeles International Airport | 32.89595         | -97.03720        | USA                 | Honolulu International Airport       | 21.31869             | -157.92241            |
