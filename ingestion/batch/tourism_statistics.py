"""Incremental ingestion of Japan Tourism Agency accommodation statistics."""

import hashlib
import json
import logging
import re
import time
import unicodedata
from dataclasses import dataclass
from json import JSONDecodeError
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
import pendulum
from bs4 import BeautifulSoup
from botocore.exceptions import EndpointConnectionError, ClientError

from ingestion.config import Settings, load_settings
from ingestion.storage.minio import create_minio_client, upload_bytes_to_minio, upload_json_to_minio

LOGGER = logging.getLogger(__name__)

XLSX_CONTENT_TYPE = (
    "application/vnd.openxmlformats-officedocument."
    "spreadsheetml.sheet"
)

TOURISM_STATISTICS_URL = 'https://www.mlit.go.jp/kankocho/tokei_hakusyo/shukuhakutokei.html'

ARTIFACT_PREFIX = 'tourism/jta_accommodation'

STATE_KEY = 'state/tourism_statistics.json'
STATE_VERSION = 1
STATE_SOURCE = "jta_accommodation_statistics"

@dataclass(frozen=True)
class Artifact:
    logical_key: str
    release_type: str
    year: int
    month: int | None
    source_url: str


def validate_state(state: Any) -> dict[str, Any]:
    """Validate the tourism ingestion state."""

    if not isinstance(state, dict):
         raise ValueError("Корневой объект state должен быть словарём")
    if state.get('version')!=STATE_VERSION:
         raise ValueError("Некорректная или неподдерживаемая версия state")
    if state.get('source')!=STATE_SOURCE:
         raise ValueError("Некорректное значение source в state")
    
    last_checked_at = state.get('last_checked_at')
    if last_checked_at is not None and (not isinstance(last_checked_at, str) or not last_checked_at.strip()):
         raise ValueError("Поле last_checked_at должно быть null или непустой строкой")
    
    if not isinstance(state.get('artifacts'), dict):
         raise ValueError("Поле artifacts отсутствует или не является словарём")
    return state
    

def get_state(client, bucket: str) -> dict[str, Any]:
    """Read state from MinIO or create state for the first run."""

    try:
        response = client.get_object(Bucket=bucket, Key=STATE_KEY)
        with response['Body'] as stream:
            raw_state = stream.read()
    except EndpointConnectionError as error:
            raise RuntimeError("Не удалось подключиться к MinIO") from error
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code", "Unknown")
        if error_code in {"NoSuchKey", "NoSuchObject", "404"}:
             return {
                    "version": STATE_VERSION,
                    "source": STATE_SOURCE,
                    "last_checked_at": None,
                    "artifacts": {}
                    }
        raise RuntimeError(
            f"Ошибка MinIO/S3 при чтении state: {error_code}"
        ) from error

    try:
        state = json.loads(raw_state)
    except (JSONDecodeError, UnicodeDecodeError) as error:
            raise ValueError("State содержит некорректный JSON") from error

    return validate_state(state)


def fetch_source_page(session: requests.Session) -> str:
    """Download the official statistics HTML page."""

    try:
        response = session.get(url=TOURISM_STATISTICS_URL, timeout=(3.05, 27))
        response.raise_for_status()
    except requests.RequestException as error:
        raise RuntimeError("Не удалось загрузить страницу туристической статистики") from error

    content_type = response.headers.get('Content-Type', '').lower()
    if 'text/html' not in content_type:
         raise ValueError("Страница туристической статистики вернула данные не в формате HTML")

    try:
        text = response.content.decode('utf-8')
    except UnicodeDecodeError as error:
        raise ValueError("Не удалось декодировать HTML страницы в UTF-8") from error

    if not text.strip():
         raise ValueError("Страница туристической статистики вернула пустой HTML")
    
    return text


def find_all_links(html: str) -> list[tuple[str,str]]:
    """Extract link text and absolute URL from the HTML page."""

    soup =  BeautifulSoup(html, 'html.parser')
    links: list[tuple[str, str]] = []

    for tag in soup.find_all(name='a', href=True):
        text = tag.get_text(separator=' ', strip=True)
        href = tag['href'].strip()

        if not href: 
             continue
        
        absolute_url = urljoin(base=TOURISM_STATISTICS_URL, url=href)
        links.append((text, absolute_url))

    return links

    
def discover_artifacts(html: str) -> list[Artifact]:
    """Find final and second preliminary XLSX publications."""

    discovered: dict[str, Artifact] = {}

    for raw_text, source_url in find_all_links(html):
        text = unicodedata.normalize('NFKC', raw_text)
        url_path = urlparse(source_url).path.lower()

        if not url_path.endswith('.xlsx'):
             continue
        if "集計結果" not in text:
            continue

        if "第2次速報" in text:
            release_type = "second_preliminary"
        elif "確定値" in text:
            release_type = "final"
        else:
            continue

        year_match = re.search(r"(20\d{2})年", text)
        if year_match is None: 
            raise ValueError(f"Не удалось определить год публикации: {raw_text}")
        
        year = int(year_match.group(1))
        if year < 2024: 
            continue

        month: int | None = None

        if release_type == 'second_preliminary':
            month_match = re.search(r"(\d{1,2})月", text)
            if month_match is None:
                 raise ValueError(f"Не удалось определить месяц second preliminary публикации: {raw_text}")
            
            month = int(month_match.group(1))
            if not 1<=month<=12:
                raise  ValueError(f"Некорректный месяц публикации: {month}")
            
            logical_key = f'second_preliminary:{year}-{month:02d}'
        else:
            logical_key = f'final:{year}'

        artifact = Artifact(logical_key=logical_key, 
                            release_type=release_type, 
                            year=year, 
                            month=month, 
                            source_url=source_url
                            )
        
        existing_artifact = discovered.get(logical_key)
        if (existing_artifact is not None and existing_artifact.source_url != source_url):
            raise ValueError(f"Для одной публикации обнаружены разные Excel-файлы: {logical_key}")
        discovered[logical_key] = artifact

    if not discovered:
        raise ValueError("На странице не найдены подходящие Excel-файлы туристической статистики")

    return sorted(discovered.values(), 
                  key=lambda artifact: (artifact.year, artifact.month or 0, artifact.release_type))


def download_artifact(
    session: requests.Session,
    artifact: Artifact,
) -> bytes:
    """Download one XLSX artifact with retries."""

    attempts = 3

    for attempt in range(1, attempts + 1):
        try:
            LOGGER.info(
                "Скачивание %s, попытка %s из %s",
                artifact.logical_key,
                attempt,
                attempts,
            )

            response = session.get(
                url=artifact.source_url,
                timeout=(10, 120),
            )
            response.raise_for_status()

            content = response.content

            if not content:
                raise ValueError(
                    f"Скачанный Excel-файл пуст: "
                    f"{artifact.logical_key}"
                )

            if not content.startswith(b"PK"):
                raise ValueError(
                    f"Скачанный файл не является XLSX: "
                    f"{artifact.logical_key}"
                )

            return content

        except requests.RequestException as error:
            if attempt == attempts:
                raise RuntimeError(
                    f"Не удалось скачать Excel-файл "
                    f"{artifact.logical_key} после {attempts} попыток"
                ) from error

            LOGGER.warning(
                "Не удалось скачать %s: %s. Повторная попытка",
                artifact.logical_key,
                error,
            )
            time.sleep(5 * attempt)

    raise RuntimeError(
        f"Не удалось скачать Excel-файл {artifact.logical_key}"
    )

def check_artifacts_upload(state: dict[str, Any], artifact: Artifact, sha256: str) -> bool:
    """Check whether an artifact is new or has changed."""

    saved_artifact = state['artifacts'].get(artifact.logical_key)

    if saved_artifact is None: return True

    if not isinstance(saved_artifact, dict):
                raise ValueError(f"Некорректный state для {artifact.logical_key}")
   
    saved_sha256 = saved_artifact.get("sha256")

    if not isinstance(saved_sha256, str) or not saved_sha256:
        raise ValueError(f"В state отсутствует sha256 для {artifact.logical_key}")

    return sha256 != saved_artifact['sha256']


def get_artifact_key(artifact: Artifact, sha256: str) -> str:
    """Build a content-addressed MinIO object key."""

    if artifact.release_type == 'final':
        return f"{ARTIFACT_PREFIX}/{artifact.release_type}/{artifact.year}/{sha256}.xlsx"

    if artifact.month is None:
        raise ValueError("Для second preliminary отсутствует месяц")
    
    return f"{ARTIFACT_PREFIX}/{artifact.release_type}/{artifact.year}/{artifact.month:02d}/{sha256}.xlsx"


def update_artifact_state(state: dict[str, Any], 
                          artifact: Artifact, 
                          artifact_key: str, 
                          sha256: str, 
                          ) -> None:
    """Update state after successful XLSX upload."""

    state['artifacts'][artifact.logical_key] = {
        'sha256': sha256,
        'source_url': artifact.source_url,
        'object_key': artifact_key,
        'updated_at': pendulum.now('UTC').to_iso8601_string()
    }


def run_tourism_statistics_ingestion(settings: Settings | None = None,) -> dict[str, int]:
    """Run tourism statistics discovery and ingestion."""

    runtime_settings = settings or load_settings()
    client = create_minio_client(runtime_settings)

    state = get_state(client, runtime_settings.raw_bucket)

    uploaded_artefacts = 0
    skipped_artefacts = 0

    with requests.Session() as session:
        session.headers.update({
            "User-Agent": ("Mozilla/5.0 JapanTravelDataEngine/1.0")
        })

        text = fetch_source_page(session)
        artifacts = discover_artifacts(text)

        LOGGER.info("На странице найдено публикаций: %s", len(artifacts))

        for artifact in artifacts:
            LOGGER.info("Проверка публикации %s", artifact.logical_key,)

            content_bytes = download_artifact(session, artifact)
            sha256 = hashlib.sha256(content_bytes).hexdigest()

            if not check_artifacts_upload(state, artifact, sha256):
                LOGGER.info("Публикация %s не изменилась", artifact.logical_key)
                skipped_artefacts+=1
                continue

            artifact_key = get_artifact_key(artifact, sha256)

            upload_bytes_to_minio(client, runtime_settings.raw_bucket, artifact_key, content_bytes, XLSX_CONTENT_TYPE)

            update_artifact_state(state, artifact, artifact_key, sha256,)

            upload_json_to_minio(client, runtime_settings.raw_bucket, STATE_KEY, state)

            LOGGER.info("Публикация %s загружена в %s", artifact.logical_key, artifact_key)
            uploaded_artefacts += 1

    state["last_checked_at"] = pendulum.now("UTC").to_iso8601_string()
    upload_json_to_minio(client, runtime_settings.raw_bucket, STATE_KEY, state)

    return {
         "discovered_objects": len(artifacts),
        "uploaded_objects": uploaded_artefacts,
        "skipped_objects": skipped_artefacts,
    }


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s: %(message)s"
        ),
    )
    summary = run_tourism_statistics_ingestion()
    LOGGER.info("Ingestion завершён: %s", summary)


if __name__ == "__main__":
    main()