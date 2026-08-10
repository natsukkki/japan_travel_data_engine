"""Incremental ingestion of JMA cherry blossom observations."""

import csv
import hashlib
import io
import logging
from dataclasses import dataclass
from typing import Any

import pendulum
import requests

from ingestion.config import Settings, load_settings
from ingestion.storage.minio import create_minio_client, load_json_from_minio, upload_bytes_to_minio, upload_json_to_minio


LOGGER = logging.getLogger(__name__)

ARTIFACT_PREFIX = "events/sakura/jma"

STATE_KEY = "state/jma_sakura.json"
STATE_VERSION = 1
STATE_SOURCE = "jma_sakura"


@dataclass(frozen=True)
class Artifact:
    logical_key: str
    expected_title: str
    source_url: str


ARTIFACTS: tuple[Artifact, Artifact] = (
    Artifact(
        logical_key="flowering",
        expected_title="さくらの開花",
        source_url="https://www.data.jma.go.jp/sakura/data/ruinenchi/004.csv",
    ),
    Artifact(
        logical_key="full_bloom",
        expected_title="さくらの満開",
        source_url="https://www.data.jma.go.jp/sakura/data/ruinenchi/005.csv",
    ),
)


def validate_state(state: Any) -> dict[str, Any]:
    """Validate the JMA sakura ingestion state."""

    if not isinstance(state, dict):
        raise ValueError("Корневой объект state JMA Sakura должен быть словарём")
    if state.get("version") != STATE_VERSION:
        raise ValueError("Некорректная или неподдерживаемая версия state JMA Sakura")
    if state.get("source") != STATE_SOURCE:
        raise ValueError("Некорректное значение source в state JMA Sakura")
    
    last_checked_at = state.get("last_checked_at")
    if last_checked_at is not None and (not isinstance(last_checked_at, str) or not last_checked_at.strip()):
        raise ValueError("Поле last_checked_at должно быть null или непустой строкой")
    
    if not isinstance(state.get("artifacts"), dict):
        raise ValueError("Поле artifacts отсутствует или не является словарём")

    return state


def get_state(client, bucket: str) -> dict[str, Any]:
    """Read state from MinIO or initialize it for the first run."""

    state = load_json_from_minio(client=client, bucket=bucket, object_key=STATE_KEY)

    if state is None:
        return {
            "version": STATE_VERSION,
            "source": STATE_SOURCE,
            "last_checked_at": None,
            "artifacts": {}
        }
    
    return validate_state(state)


def download_artifact(session: requests.Session, url: str) -> bytes:
    """Download and validate one JMA CSV artifact."""

    try:
        response = session.get(url=url, timeout=(3.05, 27))
        response.raise_for_status()
    except requests.RequestException as error:
        raise RuntimeError(f"Не удалось скачать CSV с данными о цветении сакуры: {url}") from error
    
    content_type = response.headers.get("Content-Type", "").lower()
    if "csv" not in content_type:
        raise ValueError(f"Источник вернул данные не в формате CSV: "
            f"{content_type or 'Content-Type не указан'}"
        )
    
    content = response.content
    if not content.strip():
        raise ValueError("Источник вернул пустой CSV с данными о цветении сакуры")

    return content


def validate_artifact(artifact: Artifact, content: bytes) -> None:
    """Validate the minimal structure of one JMA sakura CSV."""

    try:
        text = content.decode("shift_jis")
    except UnicodeDecodeError as error:
        raise ValueError("CSV с данными о цветении сакуры не декодируется в кодировке Shift_JIS") from error

    rows = [
        row
        for row in csv.reader(io.StringIO(text, newline=""))
        if any(value.strip() for value in row)
    ]
    if len(rows) < 2:
        raise ValueError(
            f"Артефакт {artifact.logical_key} "
            "не содержит название показателя и заголовок"
        )

    data_rows = rows[2:]
    if not data_rows:
        raise ValueError(
            f"Артефакт {artifact.logical_key} "
            "не содержит строк с наблюдениями"
        )   

    title_row = rows[0]
    actual_title = title_row[1].strip() if len(title_row) > 1 else None
    if actual_title != artifact.expected_title:
        raise ValueError(
            f"В артефакте {artifact.logical_key} ожидался показатель "
            f"{artifact.expected_title!r}, получен {actual_title!r}"
        ) 

    header = [value.strip() for value in rows[1]]
    if header[:2] != ["番号", "地点名"]:
        raise ValueError(
            f"Артефакт {artifact.logical_key} "
            "не содержит ожидаемые столбцы '番号' и '地点名'"
        )

    has_year_column = any(
        len(column) == 4 and column.isdigit()
        for column in header[2:]
    )
    if not has_year_column:
        raise ValueError(
            f"В артефакте {artifact.logical_key} "
            "не найдены столбцы с годами"
        )


def check_artifact_upload(state: dict[str, Any], artifact: Artifact, sha256: str) -> bool:
    """Return whether a JMA artifact is new or has changed."""

    logical_key = artifact.logical_key
    saved_artifact = state["artifacts"].get(logical_key)

    if saved_artifact is None:
        return True
    
    if not isinstance(saved_artifact, dict):
        raise ValueError(
            f"Некорректный state для артефакта "
            f"{artifact.logical_key}"
        )

    saved_sha256 = saved_artifact.get("sha256")
    if not isinstance(saved_sha256, str) or not saved_sha256.strip():
        raise ValueError(
            f"В state отсутствует sha256 для артефакта "
            f"{artifact.logical_key}"
        )

    return sha256 != saved_sha256


def update_artifact_state(
    state: dict[str, Any], 
    artifact: Artifact,
    object_key: str,
    sha256: str
) -> None:
    """Update state after a successful artifact upload."""

    state["artifacts"][artifact.logical_key] = {
        "sha256": sha256,
        "source_url": artifact.source_url,
        "object_key": object_key,
        "updated_at": pendulum.now("UTC").to_iso8601_string(),
    }



def run_jma_sakura_ingestion(settings: Settings | None = None) -> dict[str, int]:
    """Download changed JMA sakura CSV files and persist ingestion state."""

    runtime_settings = settings or load_settings()
    client = create_minio_client(settings=runtime_settings)

    state = get_state(client=client, bucket=runtime_settings.raw_bucket)

    uploaded_artifacts = 0
    skipped_artifacts = 0

    with requests.Session() as session:
        for artifact in ARTIFACTS:
            LOGGER.info("Проверка артефакта JMA Sakura: %s", artifact.logical_key,)

            content = download_artifact(session=session, url=artifact.source_url)
            validate_artifact(artifact=artifact, content=content)
            
            sha256 = hashlib.sha256(content).hexdigest()
            if not check_artifact_upload(state=state, artifact=artifact, sha256=sha256):
                LOGGER.info("Артефакт %s не изменился", artifact.logical_key,)
                skipped_artifacts += 1
                continue

            object_key = f"{ARTIFACT_PREFIX}/{artifact.logical_key}/{sha256}.csv"

            upload_bytes_to_minio(
                client=client, 
                bucket=runtime_settings.raw_bucket,
                object_key=object_key,
                data=content,
                content_type="text/csv; charset=shift_jis",
            )
            update_artifact_state(
                state=state,
                artifact=artifact,
                object_key=object_key,
                sha256=sha256
            )
            upload_json_to_minio(
                client=client, 
                bucket=runtime_settings.raw_bucket,
                object_key=STATE_KEY,
                data=state
            )
            LOGGER.info("Артефакт %s загружен в %s", artifact.logical_key, object_key,)
            uploaded_artifacts += 1

    state["last_checked_at"] = pendulum.now("UTC").to_iso8601_string()
    upload_json_to_minio(
        client=client,
        bucket=runtime_settings.raw_bucket,
        object_key=STATE_KEY,
        data=state,
    )

    return {
        "discovered_objects": len(ARTIFACTS),
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
    summary = run_jma_sakura_ingestion()
    LOGGER.info("Ingestion завершён: %s", summary)


if __name__ == "__main__":
    main()
