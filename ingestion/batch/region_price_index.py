"""Incremental ingestion of e-Stat regional consumer price index files."""

import hashlib
import logging
import time
import unicodedata
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

import pendulum
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from ingestion.config import Settings, load_settings
from ingestion.storage.minio import create_minio_client, upload_json_to_minio, upload_bytes_to_minio, load_json_from_minio

LOGGER = logging.getLogger(__name__)

XLS_MAGIC = bytes.fromhex("D0CF11E0A1B11AE1")
XLS_CONTENT_TYPE = "application/vnd.ms-excel"

REGION_PRICE_INDEX_URL = "https://www.e-stat.go.jp/en/stat-search/files?cycle=7&layout=datalist&page=1&tclass1val=0&toukei=00200571&tstat=000001067253"

TARGET_TABLE_NAME = (
    "Regional Difference Index of Consumer Prices by Ten Major Groups "
    "(All Japan = 100) - Japan, Districts, Prefectures, Capital cities "
    "and ordinance-designated cities"
)

ARTIFACT_PREFIX = "prices/regional_price_index"

STATE_KEY = "state/regional_price_index.json"
STATE_VERSION = 1
STATE_SOURCE = "estat_regional_price_index"

INITIAL_YEAR = 2024


@dataclass(frozen=True)
class YearPage:
    year: int
    source_url: str


@dataclass(frozen=True)
class Artifact:
    year: int
    source_url: str

    @property
    def logical_key(self) -> str:
        return str(self.year)
    

def validate_state(state: Any) -> dict[str, Any]:
    """Validate the regional price index ingestion state."""

    if not isinstance(state, dict):
        raise ValueError("Корневой объект state должен быть словарём")
    if state.get("version") != STATE_VERSION:
        raise ValueError("Некорректная или неподдерживаемая версия state")
    if state.get("source") != STATE_SOURCE:
        raise ValueError("Некорректное значение source в state")
    
    last_checked_at = state.get("last_checked_at")
    if last_checked_at is not None and (not isinstance(last_checked_at, str) or not last_checked_at.strip()):
        raise ValueError("Поле last_checked_at должно быть null или непустой строкой")

    if not isinstance(state.get("artifacts"), dict):
        raise ValueError("Поле artifacts отсутствует или не является словарём")

    return state


def get_state(client, bucket: str) -> dict[str, Any]:
    """Read state from MinIO or return state for the first run."""

    state = load_json_from_minio(client=client, bucket=bucket, object_key=STATE_KEY,)
    if state is None:
        return {
                "version": STATE_VERSION,
                "source": STATE_SOURCE,
                "last_checked_at": None,
                "artifacts": {}
                }
    
    return validate_state(state)


def fetch_source_page(session: requests.Session, url: str) -> str:
    """Download and validate an e-Stat HTML page."""
    
    try:
        response = session.get(url=url, timeout=(3.05, 27))
        response.raise_for_status()
    except requests.RequestException as error:
        raise RuntimeError(f"Не удалось загрузить страницу e-Stat: {url}") from error
    
    content_type = response.headers.get("Content-Type", "").lower()
    if "text/html" not in content_type:
        raise ValueError(f"Страница e-Stat вернула данные не в формате HTML: {url}")
    
    try:
        text = response.content.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError( "Не удалось декодировать HTML e-Stat в UTF-8") from error
    
    if not text.strip():
        raise ValueError("Страница e-Stat вернула пустой HTML")
    
    return text


def discover_year_pages(html: str) -> list[YearPage]:
    """Discover annual publication pages from the e-Stat catalogue."""

    soup = BeautifulSoup(html, "html.parser")
    discovered: dict[int, str] = {}

    for tag in soup.find_all(name="a", href=True):
        href = tag["href"].strip()

        if not href:
            continue

        absolute_url = urljoin(base=REGION_PRICE_INDEX_URL, url=href)
        parsed_url = urlparse(absolute_url)
        query = parse_qs(parsed_url.query)
        year_values = query.get("year", [])

        if len(year_values) != 1:
            continue

        encoded_year = year_values[0]

        if (len(encoded_year) != 5 or not encoded_year.isdigit() or not encoded_year.endswith("0")):
            continue

        year = int(encoded_year[:4])

        if year < INITIAL_YEAR:
            continue

        if query.get("layout") != ["datalist"]:
            continue

        if query.get("toukei") != ["00200571"]:
            continue

        if query.get("tstat") != ["000001067253"]:
            continue

        existing_url = discovered.get(year)
        if existing_url is not None and existing_url != absolute_url:
            raise ValueError(f"Для {year} года найдены разные страницы e-Stat")

        discovered[year] = absolute_url

    if not discovered:
        raise ValueError(f"В каталоге e-Stat не найдены публикации с {INITIAL_YEAR} года")

    return [YearPage(year=year,source_url=source_url,) for year, source_url in sorted(discovered.items())]


def discover_artifact(html: str, year_page: YearPage) -> Artifact:
    """Find the required e-Stat XLS link on one annual results page."""

    soup = BeautifulSoup(html, "html.parser")
    target_table_tags: list[Tag] = []

    for tag in soup.find_all(name="a", href=True):
        raw_text = tag.get_text(separator=" ",strip=True)
        raw_text = unicodedata.normalize("NFKC", raw_text)
        text = " ".join(raw_text.split())

        if text == TARGET_TABLE_NAME:
            target_table_tags.append(tag)

    if len(target_table_tags) != 1:
        raise ValueError(
            f"Для {year_page.year} года ожидалась одна таблица Regional Difference Index, " 
            f"найдено: {len(target_table_tags)}"
        )
    
    target_table_tag = target_table_tags[0]

    container = target_table_tag.find_parent("article")
    if container is None:
        raise ValueError(f"Не найдена карточка набора e-Stat для {year_page.year} года")

    download_tags = container.select("a[href][data-file_type=\"EXCEL_Report\"]")
    if len(download_tags) != 1:
        raise ValueError(f"Для {year_page.year} года ожидалась одна ссылка EXCEL Report, найдено: {len(download_tags)}")

    download_tag = download_tags[0]

    source_url = urljoin(base=year_page.source_url, url=download_tag["href"].strip())

    return Artifact(year=year_page.year, source_url=source_url)


def download_artifact(session: requests.Session, artifact: Artifact) -> bytes:
    """Download and validate one e-Stat XLS artifact."""

    attempts = 3

    for attempt in range(1, attempts + 1):
        try:
            LOGGER.info(
                "Скачивание price index за %s год, попытка %s из %s",
                artifact.year,
                attempt,
                attempts,
            )
            response = session.get(url=artifact.source_url, timeout=(10, 120))
            response.raise_for_status()
        except requests.RequestException as error:
            if attempt == attempts:
                raise RuntimeError(f"Не удалось скачать XLS за {artifact.year} год после {attempts} попыток") from error

            LOGGER.warning(
                "Не удалось скачать XLS за %s год: %s. "
                "Будет выполнена повторная попытка",
                artifact.year,
                error,
            )

            time.sleep(5 * attempt)
            continue
    
        content = response.content
        if not content:
            raise ValueError(f"Файл за {artifact.year} год пуст")
        if not content.startswith(XLS_MAGIC):
            raise ValueError(f"Файл за {artifact.year} год не является XLS")

        return content
    
    raise RuntimeError(f"Не удалось скачать XLS за {artifact.year} год")


def check_artifact_upload(state: dict[str, Any], artifact: Artifact, sha256: str) -> bool:
    """Return whether an artifact is new or has changed."""

    logical_key = artifact.logical_key
    saved_artifact = state["artifacts"].get(logical_key)

    if saved_artifact is None: 
        return True

    if not isinstance(saved_artifact, dict):
        raise ValueError(f"Некорректный state для {logical_key}")
    
    saved_sha256 = saved_artifact.get("sha256")

    if not isinstance(saved_sha256, str) or not saved_sha256.strip():
        raise ValueError(f"В state отсутствует sha256 для {logical_key}")
    
    return saved_sha256 != sha256


def update_artifact_state(
    state: dict[str, Any], 
    artifact: Artifact, 
    object_key: str,
    sha256: str, 
) -> None:
    """Update state after a successful artifact upload."""

    state["artifacts"][artifact.logical_key] = {
        "sha256": sha256,
        "source_url": artifact.source_url,
        "object_key": object_key,
        "updated_at": pendulum.now("UTC").to_iso8601_string()
    }


def run_region_price_index_ingestion(settings: Settings | None = None,) -> dict[str, int]:
    """Discover and ingest annual regional price index files."""

    runtime_settings = settings or load_settings()
    client = create_minio_client(runtime_settings)

    state = get_state(client=client, bucket=runtime_settings.raw_bucket,)

    uploaded_artifacts = 0
    skipped_artifacts = 0

    with requests.Session() as session:
        session.headers.update({
            "User-Agent": ("Mozilla/5.0 ""JapanTravelDataEngine/1.0")
        })

        primary_html = fetch_source_page(session=session, url=REGION_PRICE_INDEX_URL)
        year_pages = discover_year_pages(primary_html)

        LOGGER.info("В каталоге e-Stat найдено годовых публикаций: %s",len(year_pages),)

        for year_page in year_pages:
            LOGGER.info("Проверка страницы с публикацией за %s год", year_page.year,)

            year_html = fetch_source_page(session=session, url=year_page.source_url)
            artifact = discover_artifact(html=year_html, year_page=year_page)

            LOGGER.info("Проверка файла за %s год", artifact.year,)

            content_bytes = download_artifact(session=session, artifact=artifact)
            sha256 = hashlib.sha256(content_bytes).hexdigest()

            if not check_artifact_upload(state=state, artifact=artifact, sha256=sha256):
                LOGGER.info("Файл за %s год не изменился", artifact.year)
                skipped_artifacts += 1
                continue

            object_key = f"{ARTIFACT_PREFIX}/{artifact.logical_key}/{sha256}.xls"

            upload_bytes_to_minio(
                client=client, 
                bucket=runtime_settings.raw_bucket, 
                object_key=object_key,
                data=content_bytes,
                content_type=XLS_CONTENT_TYPE,
            )
            update_artifact_state(
                state=state, 
                artifact=artifact, 
                sha256=sha256, 
                object_key=object_key
            )
            upload_json_to_minio(
                client=client, 
                bucket=runtime_settings.raw_bucket, 
                object_key=STATE_KEY, 
                data=state
            )

            LOGGER.info("Файл за %s год загружен в %s", artifact.year, object_key)
            uploaded_artifacts += 1

    state["last_checked_at"] = pendulum.now("UTC").to_iso8601_string()

    upload_json_to_minio(
        client=client,
        bucket=runtime_settings.raw_bucket,
        object_key=STATE_KEY,
        data=state,
    )
    return {
        "discovered_objects": len(year_pages),
        "uploaded_objects": uploaded_artifacts,
        "skipped_objects": skipped_artifacts,
    }


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
             "%(asctime)s %(levelname)s "
            "%(name)s: %(message)s"
        ),
    )
    summary = run_region_price_index_ingestion()
    LOGGER.info("Ingestion завершён: %s", summary)


if __name__ == "__main__":
    main()
