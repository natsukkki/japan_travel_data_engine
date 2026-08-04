"""Incremental batch ingestion of Open-Meteo historical weather data."""

import json
import logging
from dataclasses import dataclass
from json import JSONDecodeError
from pathlib import Path
from typing import Any

import boto3
import pendulum
import requests
from botocore.exceptions import ClientError, EndpointConnectionError

from ingestion.config import Settings, load_settings


LOGGER = logging.getLogger(__name__)

OPENMETEO_URL = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_METRICS = (
    "temperature_2m_mean",
    "precipitation_sum",
    "sunshine_duration",
    "wind_speed_10m_mean",
    "relative_humidity_2m_mean",
)
WEATHER_MODEL = "ecmwf_ifs"
WEATHER_TIMEZONE = "Asia/Tokyo"

SOURCE_NAME = "openmeteo_weather"
STATE_VERSION = 1
STATE_KEY = "state/openmeteo_weather.json"
INITIAL_START_DATE = pendulum.date(2024, 1, 1)
REGIONS_PATH = Path(__file__).resolve().parent / "config" / "regions.json"

REQUIRED_REGION_FIELDS = {
    "region_code",
    "prefecture_name",
    "city_name",
    "latitude",
    "longitude",
}


@dataclass(frozen=True)
class Region:
    region_code: str
    prefecture_name: str
    city_name: str
    latitude: float
    longitude: float

    def __post_init__(self) -> None:
        """Validate region configuration values."""

        string_fields = {
            "region_code": self.region_code,
            "prefecture_name": self.prefecture_name,
            "city_name": self.city_name,
        }
        numeric_fields = {
            "latitude": self.latitude,
            "longitude": self.longitude,
        }

        for field_name, value in string_fields.items():
            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"Поле '{field_name}' должно быть непустой строкой"
                )

        for field_name, value in numeric_fields.items():
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"Поле {field_name} должно быть числом")

        if not -90 <= self.latitude <= 90:
            raise ValueError(
                f"Широта должна быть от -90 до 90, получено: {self.latitude}"
            )
        if not -180 <= self.longitude <= 180:
            raise ValueError(
                f"Долгота должна быть от -180 до 180, получено: {self.longitude}"
            )


def create_minio_client(settings: Settings):
    """Create an S3-compatible client for MinIO."""

    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
    )


def load_regions(path: Path = REGIONS_PATH) -> list[Region]:
    """Read and validate the region configuration."""

    try:
        with path.open(mode="r", encoding="utf-8") as file:
            raw_regions = json.load(file)
    except FileNotFoundError as error:
        raise FileNotFoundError(f"Файл с регионами не найден: {path}") from error
    except JSONDecodeError as error:
        raise ValueError(f"В файле {path} некорректный JSON: {error}") from error

    if not isinstance(raw_regions, list):
        raise ValueError(
            f"Ожидался список регионов, получен {type(raw_regions).__name__}"
        )
    if not raw_regions:
        raise ValueError("Файл regions.json содержит пустой список")

    regions: list[Region] = []
    seen_region_codes: set[str] = set()

    for index, raw_region in enumerate(raw_regions, start=1):
        if not isinstance(raw_region, dict):
            raise ValueError(f"Регион №{index} должен быть объектом JSON")

        missing_fields = REQUIRED_REGION_FIELDS - raw_region.keys()
        if missing_fields:
            missing = ", ".join(sorted(missing_fields))
            raise ValueError(
                f"У региона №{index} отсутствуют поля: {missing}"
            )

        try:
            region = Region(
                region_code=raw_region["region_code"],
                prefecture_name=raw_region["prefecture_name"],
                city_name=raw_region["city_name"],
                latitude=raw_region["latitude"],
                longitude=raw_region["longitude"],
            )
        except ValueError as error:
            raise ValueError(f"Ошибка в регионе №{index}: {error}") from error

        if region.region_code in seen_region_codes:
            raise ValueError(
                f"Обнаружен повторяющийся region_code: {region.region_code}"
            )
        seen_region_codes.add(region.region_code)
        regions.append(region)

    return regions


def validate_state(state: Any) -> dict[str, Any]:
    """Validate the stable part of the state schema."""

    if not isinstance(state, dict):
        raise ValueError("Корневой объект state должен быть словарём")
    if state.get("version") != STATE_VERSION:
        raise ValueError("Некорректная или неподдерживаемая версия state")
    if state.get("source") != SOURCE_NAME:
        raise ValueError("Некорректное значение source в state")
    if state.get("model") != WEATHER_MODEL:
        raise ValueError("Модель в state не совпадает с WEATHER_MODEL")
    if not isinstance(state.get("regions"), dict):
        raise ValueError("Поле regions отсутствует или не является словарём")
    return state


def get_state(client, bucket: str) -> dict[str, Any]:
    """Read the ingestion state from MinIO or initialize it."""

    try:
        response = client.get_object(Bucket=bucket, Key=STATE_KEY)
        with response["Body"] as stream:
            raw_state = stream.read()
    except EndpointConnectionError as error:
        raise RuntimeError("Не удалось подключиться к MinIO") from error
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code", "Unknown")
        if error_code in {"NoSuchKey", "NoSuchObject", "404"}:
            return {
                    "version": STATE_VERSION,
                    "source": SOURCE_NAME,
                    "model": WEATHER_MODEL,
                    "regions": {},
                }
        raise RuntimeError(
            f"Ошибка MinIO/S3 при чтении state: {error_code}"
        ) from error

    try:
        state = json.loads(raw_state)
    except (JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("State содержит некорректный JSON") from error

    return validate_state(state)


def determine_start_date(
    state: dict[str, Any],
    region_code: str,
) -> pendulum.Date:
    """Determine the first date not yet ingested for a region."""

    region_state = state["regions"].get(region_code)
    if region_state is None:
        return INITIAL_START_DATE
    if not isinstance(region_state, dict):
        raise ValueError(f"Состояние региона {region_code} должно быть словарём")

    raw_last_successful_date = region_state.get("last_successful_date")
    if (
        not isinstance(raw_last_successful_date, str)
        or not raw_last_successful_date.strip()
    ):
        raise ValueError(
            f"Для региона {region_code} отсутствует last_successful_date"
        )

    try:
        last_successful_date = pendulum.from_format(
            raw_last_successful_date,
            "YYYY-MM-DD",
        ).date()
    except ValueError as error:
        raise ValueError(
            f"Некорректная дата для региона {region_code}: "
            f"{raw_last_successful_date}"
        ) from error

    return last_successful_date.add(days=1)


def split_date_range_by_month(
    start_date: pendulum.Date,
    end_date: pendulum.Date,
) -> list[tuple[pendulum.Date, pendulum.Date]]:
    """Split an inclusive date range into calendar-month chunks."""

    intervals: list[tuple[pendulum.Date, pendulum.Date]] = []
    current_date = start_date

    while current_date <= end_date:
        chunk_end = min(current_date.end_of("month"), end_date)
        intervals.append((current_date, chunk_end))
        current_date = chunk_end.add(days=1)

    return intervals


def fetch_weather(
    session: requests.Session,
    region: Region,
    start_date: pendulum.Date,
    end_date: pendulum.Date,
) -> dict[str, Any]:
    """Fetch one monthly weather chunk from Open-Meteo."""

    parameters = {
        "latitude": region.latitude,
        "longitude": region.longitude,
        "start_date": start_date.to_date_string(),
        "end_date": end_date.to_date_string(),
        "daily": WEATHER_METRICS,
        "timezone": WEATHER_TIMEZONE,
        "models": WEATHER_MODEL,
        "wind_speed_unit": "ms",
    }

    try:
        response = session.get(
            url=OPENMETEO_URL,
            params=parameters,
            timeout=(3.05, 27),
        )
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as error:
        raise RuntimeError(
            f"Ошибка запроса Open-Meteo для {region.region_code}, "
            f"{start_date} — {end_date}"
        ) from error

    if not isinstance(data, dict):
        raise ValueError("Ответ Open-Meteo должен быть JSON-объектом")
    return data


def validate_weather_response(
    data: dict[str, Any],
    start_date: pendulum.Date,
    end_date: pendulum.Date,
) -> None:
    """Validate dates and metric arrays in an Open-Meteo response."""

    daily_metrics = data.get("daily")
    if not isinstance(daily_metrics, dict):
        raise ValueError("Поле daily отсутствует или не является словарём")

    dates = daily_metrics.get("time")
    if not isinstance(dates, list) or not dates:
        raise ValueError("Поле daily.time должно быть непустым списком")

    expected_dates: list[str] = []
    current_date = start_date
    while current_date <= end_date:
        expected_dates.append(current_date.to_date_string())
        current_date = current_date.add(days=1)

    if dates != expected_dates:
        raise ValueError(
            "Даты ответа не соответствуют запросу. "
            f"Ожидались: {expected_dates}, получены: {dates}"
        )

    expected_length = len(dates)
    for metric in WEATHER_METRICS:
        values = daily_metrics.get(metric)
        if not isinstance(values, list):
            raise ValueError(
                f"Метрика {metric} отсутствует или не является списком"
            )
        if len(values) != expected_length:
            raise ValueError(
                f"Количество значений {metric} не совпадает с количеством "
                f"дат: {len(values)} != {expected_length}"
            )
        if any(value is None for value in values):
            raise ValueError(f"Метрика {metric} содержит null")


def upload_json_to_minio(
    client,
    bucket: str,
    object_key: str,
    data: dict[str, Any],
) -> None:
    """Serialize a dictionary and upload it as a JSON object."""

    if not object_key:
        raise ValueError("Ключ MinIO пуст")

    try:
        byte_data = json.dumps(data, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"Не удалось сериализовать объект {object_key} в JSON"
        ) from error

    try:
        client.put_object(
            Bucket=bucket,
            Key=object_key,
            Body=byte_data,
            ContentType="application/json",
        )
    except EndpointConnectionError as error:
        raise RuntimeError("Не удалось подключиться к MinIO") from error
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code", "Unknown")
        raise RuntimeError(
            f"Ошибка MinIO/S3 при записи {object_key}: {error_code}"
        ) from error


def update_region_state(
    state: dict[str, Any],
    region_code: str,
    last_successful_date: pendulum.Date,
    object_key: str,
) -> None:
    """Update one region after its raw object has been stored."""

    state["regions"][region_code] = {
        "last_successful_date": last_successful_date.to_date_string(),
        "last_object_key": object_key,
        "updated_at": pendulum.now("UTC").to_iso8601_string(),
    }


def run_weather_ingestion(
    settings: Settings | None = None,
) -> dict[str, int]:
    """Run the complete incremental weather ingestion workflow."""

    runtime_settings = settings or load_settings()
    target_end_date = pendulum.today(WEATHER_TIMEZONE).subtract(days=1).date()
    regions = load_regions()
    client = create_minio_client(runtime_settings)
    state = get_state(client, runtime_settings.raw_bucket)
    uploaded_objects = 0

    with requests.Session() as session:
        for region in regions:
            start_date = determine_start_date(state, region.region_code)
            intervals = split_date_range_by_month(start_date, target_end_date)

            if not intervals:
                LOGGER.info("Для %s нет новых данных", region.region_code)
                continue

            for from_date, to_date in intervals:
                LOGGER.info(
                    "Загрузка %s: %s — %s",
                    region.region_code,
                    from_date,
                    to_date,
                )
                weather_data = fetch_weather(
                    session,
                    region,
                    from_date,
                    to_date,
                )
                validate_weather_response(weather_data, from_date, to_date)

                object_key = (
                    f"weather/{region.region_code}/{from_date.year}/"
                    f"{from_date.month:02d}/"
                    f"{from_date.to_date_string()}-"
                    f"{to_date.to_date_string()}.json"
                )
                upload_json_to_minio(
                    client,
                    runtime_settings.raw_bucket,
                    object_key,
                    weather_data,
                )

                update_region_state(
                    state,
                    region.region_code,
                    to_date,
                    object_key,
                )
                upload_json_to_minio(
                    client,
                    runtime_settings.raw_bucket,
                    STATE_KEY,
                    state,
                )
                uploaded_objects += 1

    return {
        "uploaded_objects": uploaded_objects,
        "regions": len(regions),
    }


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    summary = run_weather_ingestion()
    LOGGER.info("Ingestion завершён: %s", summary)


if __name__ == "__main__":
    main()
