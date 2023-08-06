## Задание
Отдел аналитики интересует статистика по компаниям о возникших проблемах. Пришла задача построить сводную таблицу о всех авиакомпаниях содержащую следующие данные:

| Колонка                 | Описание                                            |
|-------------------------|-----------------------------------------------------|
| AIRLINE_NAME            | Название авиалинии [airlines.AIRLINE]               |
| correct_count           | Число выполненных рейсов                            |
| diverted_count          | Число рейсов выполненных с задержкой                |
| cancelled_count         | Число отмененных рейсов                             |
| avg_distance            | Средняя дистанция рейсов                            |
| avg_air_time            | Среднее время в небе                                |
| airline_issue_count     | Число задержек из-за проблем с самолетом [CANCELLATION_REASON]    |
| weather_issue_count     | Число задержек из-за погодных условий [CANCELLATION_REASON]    |
| nas_issue_count         | Число задержек из-за проблем NAS [CANCELLATION_REASON]           |
| security_issue_count    | Число задержек из-за службы безопасности [CANCELLATION_REASON]     |

Пример вывода:
| AIRLINE_NAME            | correct_count | diverted_count | cancelled_count | avg_distance | avg_air_time | airline_issue_count | weather_issue_count | nas_issue_count | security_issue_count |
|-------------------------|---------------|----------------|-----------------|--------------|--------------|---------------------|---------------------|-----------------|----------------------|
| Alaska Airlines Inc.    | 85805         | 217            | 1201            | 1201.221108  | 158.330843   | 175                 | 157                 | 11              | 0                    |
| American Airlines Inc.  | 355623        | 343            | 1047            | 5427.104234  | 139.916971   | 1432                | 3637                | 357             | 0                    |

