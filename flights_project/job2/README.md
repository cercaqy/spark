## Задание
Найдите топ 10 авиамаршрутов (ORIGIN_AIRPORT, DESTINATION_AIRPORT) по наибольшему числу рейсов, а так же посчитайте среднее время в полете (AIR_TIME).

Требуемые поля:
| Колонка             | Описание                               |
|---------------------|----------------------------------------|
| ORIGIN_AIRPORT      | Аэропорт вылета                        |
| DESTINATION_AIRPORT | Аэропорт прибытия                      |
| tail_count          | Число рейсов по маршруту (TAIL_NUMBER) |
| avg_air_time        | среднее время в небе по маршруту       |

Пример вывода:
| ORIGIN_AIRPORT | DESTINATION_AIRPORT | tail_count | avg_air time |
|----------------|---------------------|------------|--------------|
| SEO            | LAX                 | 6771       | 56.071861    |
| LAX            | SFO                 | 6759       | 54.985734    |