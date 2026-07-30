#!/bin/sh
set -eu

echo "Настройка подключения к MinIO"
mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

echo "Создание бакета raw-batch"
mc mb --ignore-existing local/raw-batch

echo "Настройка приватного доступа"
mc anonymous set none local/raw-batch

echo "Инициализация MinIO успешно завершена"