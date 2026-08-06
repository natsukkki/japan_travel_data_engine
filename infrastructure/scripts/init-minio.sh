#!/bin/sh
set -eu

echo "Настройка подключения к MinIO"
mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

bucket_name="${MINIO_RAW_BUCKET:-raw-batch}"

echo "Создание бакета ${bucket_name}"
mc mb --ignore-existing "local/${bucket_name}"

echo "Настройка приватного доступа"
mc anonymous set none "local/${bucket_name}"

echo "Инициализация MinIO успешно завершена"
