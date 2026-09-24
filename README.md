# Japan Travel Data Engine

Портфолио-проект по Data Engineering: собираю данные о погоде, туризме, ценах и сакуре в Японии, чтобы в будущем отвечать на вопрос «когда и куда лучше ехать?». Развиваю проект поэтапно: сначала исторические данные, затем текущие предложения и рекомендации.

## Целевая архитектура

Цель — система анализа и рекомендаций для поездок по Японии: «когда и куда лучше ехать?» и, после подключения текущих предложений, «пора ли ехать прямо сейчас?». Архитектура сочетает объектное хранилище сырых batch-файлов MinIO, планируемый брокер событий Kafka и PostgreSQL для очищенных данных и витрин.

**Пакетный путь (batch).** Python получает историческую погоду Open-Meteo, статистику ночёвок Japan Tourism Agency, региональные индексы цен e-Stat и даты цветения сакуры JMA. Исходные JSON, CSV и Excel сохраняются в MinIO. PySpark очищает данные и загружает каждый источник в отдельные таблицы `dds_schema`: погода по дням, туризм по месяцам, цены и сакура по годам. Общим справочником служат восемь выбранных регионов. Здесь DDS означает слой очищенных подробных данных.

**Аналитика batch в dbt.** dbt будет преобразовывать очищенные данные внутри PostgreSQL в `marts_schema`, отделяя бизнес-расчёты от очистки raw. Месячная модель объединит показатели по зерну «регион + месяц» и рассчитает индекс привлекательности поездки от 0 до 10 по погоде, туристической загруженности, относительным региональным ценам и сезонности. Запланированы `mart_monthly_index` для сравнения месяцев и `mart_recommendations` с рекомендациями по регионам. Небольшой справочник пиковых периодов New Year, Golden Week и Obon планируется хранить как dbt seed — версионируемый CSV для календарных пометок.

**Потоковый путь (stream).** По плану отдельные Python producers будут получать цены авиабилетов из Amadeus, горящие туры из Level.travel и курсы валют из CBR. Они отправят события в Kafka-топики `flight_prices`, `hot_tours` и `currency_rates`. Spark Structured Streaming обработает сообщения и сохранит очищенные потоковые данные в отдельные таблицы `dds_schema`. dbt построит на их основе витрины `mart_hot_offers` и `mart_current_prices`.

**Объединение и интерфейс.** Batch- и stream-данные останутся раздельными в DDS и соединятся на уровне витрин через dbt-модель `mart_smart_recommendations`: исторический индекс сезона будет сопоставляться с текущими ценами и курсами. Streamlit покажет карту Японии, динамику индекса, горящие предложения и рекомендации. Airflow будет управлять конечными batch-заданиями загрузки, Spark ETL и dbt; длительно работающий Spark Structured Streaming планируется запускать отдельно.


## Что готово сейчас

- Написаны batch-загрузчики четырёх источников: Open-Meteo, статистики туризма, индекса цен по регионам и данных о сакуре. Они сохраняют исходные файлы в MinIO.
- Погодный загрузчик ведёт прогресс по каждому региону в JSON-state и при повторе перезаписывает файл с той же начальной датой диапазона.
- Подготовлены PostgreSQL-таблицы для регионов, ежедневной погоды, staging и журнала обработанных погодных файлов.
- Реализован Spark-загрузчик справочника регионов. Код погодного ETL выбирает необработанные raw-файлы, проверяет наблюдения и переносит их в DDS через staging; этот этап покрыт тестами, но сквозной запуск с PostgreSQL ещё не проверен.
- Этапы запускаются вручную. dbt, Kafka, потоковая обработка, Airflow и Streamlit пока не реализованы. Для Airflow, Kafka producer и Streamlit есть только Dockerfile-заготовки.

## Структура

| Путь | Назначение |
|---|---|
| `ingestion/batch/` | Загрузка погоды, статистики туризма, индекса цен и данных о сакуре в MinIO |
| `ingestion/storage/` | Работа с MinIO |
| `processing/spark/load_regions.py` | Загрузка справочника регионов в PostgreSQL |
| `processing/spark/weather_etl.py` | Выбор новых погодных файлов, проверка данных и загрузка через staging в DDS |
| `infrastructure/docker/` | Docker Compose и образы сервисов |
| `infrastructure/config/postgres/init/` | Схемы и таблицы PostgreSQL |
| `tests/` | Тесты ingestion, Spark и JDBC |
| `docs/weather-ingestion-recovery.md` | Правила повтора погодной загрузки после ошибок |

В погодном ETL Spark сравнивает ключи raw-файлов с таблицей `dds_schema.dds_weather_processed_files` и выбирает только новые. Данные из staging переносятся в `dds_schema.dds_weather_daily` в одной транзакции с записью ключей в журнал.

Сейчас ETL рассчитан на один запуск за раз. Уже обработанный ключ raw-файла повторно не читается: если заменить содержимое такого объекта, нужна отдельная процедура исправления данных. Для погоды правила повторов и ограничения описаны в [документации восстановления](docs/weather-ingestion-recovery.md).

## Запуск

Команды выполняются из корня репозитория в PowerShell. Нужны Docker Desktop, Python и доступ к Open-Meteo.

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

   Compose запускает MinIO, создаёт raw-бакет, поднимает PostgreSQL и Spark. SQL-скрипты из `infrastructure/config/postgres/init/` выполняются **только при первом создании** PostgreSQL volume. Если volume уже существовал, перед ETL проверьте наличие `dds_weather_daily`, `dds_weather_processed_files` и `dds_staging_weather_daily` в схеме `dds_schema`. Примените только недостающие скрипты `03`–`05` через `psql`: перезапуск контейнера их не выполнит. Не удаляйте volume ради обновления схемы.

   ```powershell
   docker compose --env-file infrastructure/env/.env -f infrastructure/docker/docker-compose.yml exec -T postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\dt dds_schema.*"'
   ```

   Например, если на существующем volume отсутствуют таблицы журнала и staging, выполните:

   ```powershell
   docker compose --env-file infrastructure/env/.env -f infrastructure/docker/docker-compose.yml exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/04-create-weather-processed-files.sql'
   docker compose --env-file infrastructure/env/.env -f infrastructure/docker/docker-compose.yml exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/05-create-staging-weather-daily.sql'
   ```

   Если нет и `dds_weather_daily`, сначала тем же способом примените `03-create-dds-weather-daily.sql`.

4. Загрузите погоду в MinIO и справочник регионов в PostgreSQL:

   ```powershell
   .\.venv\Scripts\python.exe -m ingestion.batch.openmeteo_weather
   docker compose --env-file infrastructure/env/.env -f infrastructure/docker/docker-compose.yml exec -T spark spark-submit --master 'local[2]' /opt/project/processing/spark/load_regions.py
   ```

5. После **успешного** ingestion запустите погодный ETL:

   ```powershell
   docker compose --env-file infrastructure/env/.env -f infrastructure/docker/docker-compose.yml exec -T spark spark-submit --master 'local[2]' /opt/project/processing/spark/weather_etl.py
   ```

Другие источники запускаются отдельными модулями: `ingestion.batch.tourism_statistics`, `ingestion.batch.region_price_index` и `ingestion.batch.jma_sakura`. Погодный Spark ETL их не обрабатывает. Подробности восстановления после сбоя — в [документации погодного ingestion](docs/weather-ingestion-recovery.md).

## Тесты

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pip install -r processing/spark/requirements.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Тестам с настоящими преобразованиями Spark требуется Java 17 или новее; при более старой Java они пропускаются. Тесты не записывают данные в MinIO или PostgreSQL.
