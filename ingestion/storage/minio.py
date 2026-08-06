import json
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError

from ingestion.config import Settings

def create_minio_client(settings: Settings):
    """Create an S3-compatible client for MinIO."""

    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
    )


def upload_bytes_to_minio(
    client, 
    bucket: str, 
    object_key: str, 
    data: bytes, 
    content_type: str,
) -> None:
    """Upload bytes to MinIO."""

    if not bucket.strip():
        raise ValueError("Имя бакета MinIO пусто")
    
    if not object_key.strip():
        raise ValueError("Ключ MinIO пуст")
    
    if not isinstance(data, bytes) or not data:
        raise ValueError(f"Объект {object_key} не содержит данные")
    
    if not content_type.strip():
        raise ValueError(f"Для объекта {object_key} не указан Content-Type")
    
    try:
        client.put_object(
            Bucket=bucket, 
            Key=object_key, 
            Body=data, 
            ContentType=content_type,
        )
    except BotoCoreError as error:
        raise RuntimeError(f"Ошибка MinIO/S3 при записи {object_key}: {error}") from error


def upload_json_to_minio(
    client,
    bucket: str,
    object_key: str,
    data: Any,
) -> None:
    """Serialize an object and upload it as JSON."""

    try:
        byte_data = json.dumps(data, allow_nan=False, ensure_ascii=False,).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"Не удалось сериализовать объект {object_key} в JSON"
        ) from error

    upload_bytes_to_minio(
        client=client, 
        bucket=bucket, 
        object_key=object_key, 
        data=byte_data, 
        content_type="application/json"
    )