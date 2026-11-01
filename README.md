# Japan Travel Data Engine

Портфолио-проект по Data Engineering: собираю данные о погоде, туризме, ценах и сакуре в Японии, чтобы в будущем отвечать на вопрос «когда и куда лучше ехать?». Развиваю проект поэтапно: сначала исторические данные, затем текущие предложения и рекомендации.

## Целевая архитектура

Цель — система анализа и рекомендаций для поездок по Японии: «когда и куда лучше ехать?» и, после подключения текущих предложений, «пора ли ехать прямо сейчас?». Архитектура сочетает объектное хранилище сырых batch-файлов MinIO, планируемый брокер событий Kafka и PostgreSQL для очищенных данных и витрин.

**Пакетный путь (batch).** Python получает историческую погоду Open-Meteo, статистику ночёвок Japan Tourism Agency, региональные индексы цен e-Stat и даты цветения сакуры JMA. Исходные JSON, CSV и Excel сохраняются в MinIO. PySpark очищает данные и загружает каждый источник в отдельные таблицы `dds_schema`: погода по дням, туризм по месяцам, цены и сакура по годам. Общим справочником служат восемь выбранных регионов. Здесь DDS означает слой очищенных подробных данных.

**Аналитика batch в dbt.** dbt будет преобразовывать очищенные данные внутри PostgreSQL в `marts_schema`, отделяя бизнес-расчёты от очистки raw. Принят вариант A: сначала собрать показатели и рассчитать оценку каждого конкретного месяца на зерне «регион + год + месяц», затем усреднить оценки одинаковых месяцев разных лет и ранжировать месяцы внутри региона. Сезонный рейтинг имеет зерно «регион + номер месяца», без года; описательный профиль сезона используется для пояснений. Запланированы подробная витрина `mart_monthly_index`, сезонная витрина и рекомендации по регионам. Текущие витрины будут полностью пересчитываться после обновления DDS, а история опубликованных рейтингов — сохраняться отдельно. [Решение о расчёте сезонного рейтинга](../docs/seasonal-rating-design.md) описывает зерно, снимки и вопросы, которые ещё нужно согласовать, включая формулы, веса и пропуски. Небольшой справочник пиковых периодов New Year, Golden Week и Obon планируется хранить как dbt seed — версионируемый CSV для календарных пометок.

**Потоковый путь (stream).** По плану отдельные Python producers будут получать цены авиабилетов из Amadeus, горящие туры из Level.travel и курсы валют из CBR. Они отправят события в Kafka-топики `flight_prices`, `hot_tours` и `currency_rates`. Spark Structured Streaming обработает сообщения и сохранит очищенные потоковые данные в отдельные таблицы `dds_schema`. dbt построит на их основе витрины `mart_hot_offers` и `mart_current_prices`.

**Объединение и интерфейс.** Batch- и stream-данные останутся раздельными в DDS и соединятся на уровне витрин через dbt-модель `mart_smart_recommendations`: исторический индекс сезона будет сопоставляться с текущими ценами и курсами. Streamlit покажет карту Японии, динамику индекса, горящие предложения и рекомендации. Airflow будет управлять конечными batch-заданиями загрузки, Spark ETL и dbt; длительно работающий Spark Structured Streaming планируется запускать отдельно.


## Что готово сейчас

- Написаны batch-загрузчики четырёх источников: Open-Meteo, статистики туризма, индекса цен по регионам и данных о сакуре. Они сохраняют исходные файлы в MinIO.
- Погодный загрузчик ведёт прогресс по каждому региону в JSON-state и при повторе перезаписывает файл с той же начальной датой диапазона.
- Подготовлены PostgreSQL-таблицы для регионов, ежедневной погоды и месячной статистики туризма, а также staging и таблицы учёта обработанных файлов для обоих источников.
- Реализован Spark-загрузчик справочника регионов. Код погодного ETL выбирает необработанные raw-файлы, проверяет наблюдения и переносит их в DDS через staging; этот этап покрыт тестами, но сквозной запуск с PostgreSQL ещё не проверен.
- Реализован туристический ETL: читает актуальные публикации из MinIO state, разбирает JTA Excel, отбирает восемь регионов и загружает месячные показатели в DDS с приоритетом финальных публикаций. Для каждой публикации сохраняет последнюю успешно обработанную версию файла.
- Туристический ETL проверен на 10 реальных JTA Excel: 256 строк прошли через настоящий Spark JDBC во временную PostgreSQL 18. Чтение объектов выполнялось через локальный HTTP-сервер, имитирующий ответы S3. Проверены повторный запуск и смена версий A → B → A; полный запуск Docker Compose с настоящим MinIO и PostgreSQL 16 пока не проверен.
- Spark пока запускается локально через `local[2]`. Развёртывание кластера отложено до завершения текущего проекта.
- Этапы запускаются вручную. dbt, Kafka, потоковая обработка, Airflow и Streamlit пока не реализованы. Для Airflow, Kafka producer и Streamlit есть только Dockerfile-заготовки.

## Структура

| Путь | Назначение |
|---|---|
| `ingestion/batch/` | Загрузка погоды, статистики туризма, индекса цен и данных о сакуре в MinIO |
| `storage/` | Общие операции с MinIO для ingestion и processing |
| `processing/spark/load_regions.py` | Загрузка справочника регионов в PostgreSQL |
| `processing/spark/weather_etl.py` | Выбор новых погодных файлов, проверка данных и загрузка через staging в DDS |
| `processing/spark/tourism_etl.py` | Обработка актуальных версий JTA-публикаций и загрузка месячной статистики в DDS |
| `processing/spark/jta_excel_parser.py` | Проверка JTA Excel, отбор восьми префектур и сопоставление с кодами регионов |
| `infrastructure/docker/` | Docker Compose и образы сервисов |
| `infrastructure/config/postgres/init/` | Схемы и таблицы PostgreSQL |
| `tests/` | Тесты ingestion, Spark и JDBC |

В погодном ETL Spark сравнивает ключи raw-файлов с таблицей `dds_schema.dds_weather_processed_files` и выбирает только новые. Данные из staging переносятся в `dds_schema.dds_weather_daily` в одной транзакции с записью ключей в журнал.

Сейчас каждый пайплайн рассчитан на один запуск за раз: ingestion должен успешно завершиться перед ETL, а два запуска одного ETL не должны выполняться одновременно. Погодный ingestion сначала сохраняет raw-объект и только после успешной записи обновляет state. Если запись state не удалась, повтор снова запросит неподтверждённый диапазон и перезапишет объект с тем же ключом. Уже обработанный погодный ключ raw-файла повторно не читается: если заменить содержимое такого объекта, нужна отдельная процедура исправления данных.

Spark записывает погодные строки в staging до транзакции переноса в DDS. После сбоя staging может содержать незавершённые данные; следующий одиночный запуск очищает его и повторяет запись. Перенос строк в DDS и регистрация обработанных ключей выполняются в одной транзакции PostgreSQL.

Туристический ingestion сохраняет Excel по ключам с SHA-256 содержимого и обновляет `state/tourism_statistics.json`. Логический ключ определяет публикацию, например `final:2025` или `second_preliminary:2026-08`; `object_key` определяет конкретную версию её файла. ETL читает только файлы, указанные в state, и пропускает ключи, уже сохранённые как последние обработанные версии.

Парсер проверяет все 47 префектур каждого месячного листа и возвращает восемь из `ingestion/batch/config/regions.json`: 8 строк для месячной публикации и 96 для годовой. Excel разбирается в памяти Python-драйвера; Spark создаёт DataFrame и присоединяет `region_id` из PostgreSQL. Это допустимо для текущего небольшого объёма данных.

Зерно `dds_schema.dds_tourism_monthly` — `(region_id, year, month)`. При совпадении периода `final` имеет приоритет над `second_preliminary`; публикация того же типа обновляет показатели, если они изменились. Новая месячная версия не заменяет уже загруженные финальные показатели.

`dds_schema.dds_tourism_processed_files` хранит одну строку на `logical_key`: последнюю успешно обработанную версию `object_key` и время её обработки. При смене версии запись обновляется, поэтому ETL поддерживает возврат A → B → A. Это учёт обработанных публикаций, а не полная история версий и не указатель на источник текущих показателей DDS: месячный файл регистрируется даже тогда, когда его показатели уступили финальным по приоритету.

Перед записью туристический ETL очищает staging, загружает строки и сверяет их количество. Перенос показателей в DDS, обновление обработанных публикаций и очистка staging выполняются одной транзакцией; JDBC-запись staging происходит до неё. Результат содержит `processed_files` — число обработанных файлов и `tourism_rows` — число входных строк staging до выбора приоритетной публикации, а не число изменённых строк DDS. Если новых версий нет, оба значения равны нулю.

## Идея дальнейшего развития

После завершения исходной архитектуры MinIO raw + PostgreSQL DDS и витрины можно исследовать переход к lakehouse на Apache Iceberg с публикацией готовых витрин в PostgreSQL. [Заметка о развитии проекта](../docs/lakehouse-development-idea.md) описывает изменения, порядок пилотного внедрения и проверки. Это отдельный будущий этап; текущий план реализации сохраняется.

## Запуск

Команды выполняются из корня репозитория. Нужны Docker Desktop, Python и доступ к Open-Meteo и сайту Japan Tourism Agency. Примеры команд используют PowerShell; в macOS/Linux создайте окружение командой `python3 -m venv .venv` и используйте `.venv/bin/python` вместо пути к интерпретатору Windows.

1. Скопируйте `infrastructure/env/.env.example` в `infrastructure/env/.env`. Задайте свои `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `POSTGRES_USER` и `POSTGRES_PASSWORD`. Файл `.env` не добавляйте в Git.
2. Установите зависимости для локального ingestion:

   ```powershell
   py -m venv .venv
   .\.venv\Scripts\python.exe -m pip install -r requirements.txt
   ```

3. Соберите и запустите сервисы:

   ```powershell
   docker compose --env-file infrastructure/env/.env -f infrastructure/docker/docker-compose.yml up -d --build
   ```

   Compose собирает MinIO и клиент `mc` из закреплённых тегов официальных GitHub-репозиториев, затем создаёт raw-бакет и поднимает PostgreSQL и Spark. Для первой сборки нужен доступ к GitHub и Go-модулям. SQL-скрипты из `infrastructure/config/postgres/init/` выполняются **только при первом создании** PostgreSQL volume. Если volume уже существовал, перед ETL проверьте наличие `dds_weather_daily`, `dds_weather_processed_files` и `dds_staging_weather_daily` в схеме `dds_schema`. Примените только недостающие скрипты `03`–`05` через `psql`: перезапуск контейнера их не выполнит. Не удаляйте volume ради обновления схемы.

   ```powershell
   docker compose --env-file infrastructure/env/.env -f infrastructure/docker/docker-compose.yml exec -T postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\dt dds_schema.*"'
   ```

   Например, если на существующем volume отсутствуют таблицы журнала и staging, выполните:

   ```powershell
   docker compose --env-file infrastructure/env/.env -f infrastructure/docker/docker-compose.yml exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/04-create-weather-processed-files.sql'
   docker compose --env-file infrastructure/env/.env -f infrastructure/docker/docker-compose.yml exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/05-create-staging-weather-daily.sql'
   ```

   Если нет и `dds_weather_daily`, сначала тем же способом примените `03-create-dds-weather-daily.sql`.

   Для туристического ETL также нужны таблицы `dds_tourism_monthly`, `dds_tourism_processed_files` и `dds_staging_tourism_monthly`. Если их ещё нет, последовательно примените скрипты `06`–`08`:

   ```powershell
   docker compose --env-file infrastructure/env/.env -f infrastructure/docker/docker-compose.yml exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/06-create-dds-tourism-monthly.sql'
   docker compose --env-file infrastructure/env/.env -f infrastructure/docker/docker-compose.yml exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/07-create-tourism-processed-files.sql'
   docker compose --env-file infrastructure/env/.env -f infrastructure/docker/docker-compose.yml exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/08-create-staging-tourism-monthly.sql'
   ```

   Если туристические таблицы уже созданы по предыдущей версии схемы, повторный `CREATE TABLE` их не обновит. Нужна отдельная миграция: `logical_key` в staging и первичный ключ по `logical_key` в таблице обработанных публикаций с сохранением последней обработанной версии каждой публикации. Готового скрипта такой миграции пока нет.

4. Загрузите погоду в MinIO и справочник регионов в PostgreSQL:

   ```powershell
   .\.venv\Scripts\python.exe -m ingestion.batch.openmeteo_weather
   docker compose --env-file infrastructure/env/.env -f infrastructure/docker/docker-compose.yml exec -T spark spark-submit --master 'local[2]' /opt/project/processing/spark/load_regions.py
   ```

5. После **успешного** ingestion запустите погодный ETL:

   ```powershell
   docker compose --env-file infrastructure/env/.env -f infrastructure/docker/docker-compose.yml exec -T spark spark-submit --master 'local[2]' /opt/project/processing/spark/weather_etl.py
   ```

6. Для статистики туризма выполните ingestion, а после его **успешного** завершения — туристический ETL. Справочник регионов должен быть уже загружен:

   ```powershell
   .\.venv\Scripts\python.exe -m ingestion.batch.tourism_statistics
   docker compose --env-file infrastructure/env/.env -f infrastructure/docker/docker-compose.yml exec -T spark spark-submit --master 'local[2]' /opt/project/processing/spark/tourism_etl.py
   ```

Индекс цен и данные о сакуре загружаются отдельными модулями `ingestion.batch.region_price_index` и `ingestion.batch.jma_sakura`. Spark ETL для этих двух источников пока не реализованы.

## Тесты

Полный набор тестов можно запустить в Spark-контейнере, где уже установлены Java и PySpark:

```sh
docker compose --env-file infrastructure/env/.env -f infrastructure/docker/docker-compose.yml exec -T spark python3 -m unittest discover -s tests -v
```

Для локального запуска установите зависимости ниже и Java 17+:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pip install -r processing/spark/requirements.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Тестам с настоящими преобразованиями Spark требуется Java 17 или новее; при более старой Java они пропускаются. Обычный набор тестов не обращается к внешним источникам и не записывает данные в MinIO или PostgreSQL. Отдельная проверка туристического ETL во временной PostgreSQL, описанная выше, в `unittest discover` не входит.
