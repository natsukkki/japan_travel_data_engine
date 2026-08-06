# Japan Travel Data Engine

## Локальный запуск

1. Скопируйте `infrastructure/env/.env.example` в
   `infrastructure/env/.env` и задайте учётные данные MinIO.
2. Запустите MinIO из корня проекта:

```powershell
docker compose --env-file infrastructure/env/.env -f infrastructure/docker/docker-compose.yml up -d
```

3. Из корня проекта запустите нужный ingestion:

```powershell
python -m ingestion.batch.openmeteo_weather
python -m ingestion.batch.tourism_statistics
```

Запуск через `-m` сохраняет корректные пакетные импорты и совпадает с тем,
как ingestion-функция будет импортироваться будущим DAG Airflow.

Локальные тесты не обращаются к Open-Meteo или MinIO:

```powershell
python -m unittest discover -s tests -v
```
