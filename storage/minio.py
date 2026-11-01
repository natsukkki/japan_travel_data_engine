"""Shared MinIO operations for ingestion and processing jobs."""

import json
from json import JSONDecodeError
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, EndpointConnectionError, ClientError


def create_minio_client(
    endpoint: str,
    access_key: str,
    secret_key: str,
):
    """Create an S3-compatible client for MinIO."""

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def load_bytes_from_minio(client, bucket: str, object_key: str) -> bytes:
    """Read an object's bytes from MinIO and fail if it is unavailable."""

    try:
        response = client.get_object(Bucket=bucket, Key=object_key)
        with response["Body"] as stream:
            return stream.read()
    except EndpointConnectionError as error:
        raise RuntimeError("Не удалось подключиться к MinIO") from error
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code", "Unknown")
        raise RuntimeError(
            f"Ошибка чтения объекта {object_key} из MinIO: {error_code}"
        ) from error
    except BotoCoreError as error:
        raise RuntimeError(
            f"Не удалось прочитать объект {object_key} из MinIO"
        ) from error


def load_json_from_minio(client, bucket: str, object_key: str) -> Any:
    """Load and deserialize a JSON object from MinIO."""

    try:
        response = client.get_object(Bucket=bucket, Key=object_key)
        with response["Body"] as stream:
            raw_json = stream.read()
    except EndpointConnectionError as error:
        raise RuntimeError("Не удалось подключиться к MinIO") from error
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code", "Unknown")
        if error_code in {"NoSuchKey", "NoSuchObject", "404"}:
            return None
        raise RuntimeError(
            f"Ошибка чтения объекта {object_key} из MinIO: {error_code}"
        ) from error
    except BotoCoreError as error:
        raise RuntimeError(
            f"Не удалось прочитать объект {object_key} из MinIO"
        ) from error

    try:
        data = json.loads(raw_json)
    except (JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError(
            f"Объект {object_key} содержит некорректный JSON"
        ) from error

    return data


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
        byte_data = json.dumps(
            data,
            allow_nan=False,
            ensure_ascii=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"Не удалось сериализовать объект {object_key} в JSON"
        ) from error

    upload_bytes_to_minio(
        client=client,
        bucket=bucket,
        object_key=object_key,
        data=byte_data,
        content_type="application/json",
    )
