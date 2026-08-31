\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS dds_schema
    AUTHORIZATION CURRENT_USER;

CREATE SCHEMA IF NOT EXISTS marts_schema
    AUTHORIZATION CURRENT_USER;

COMMENT ON SCHEMA dds_schema IS
    'Очищенные и нормализованные данные после ETL';

COMMENT ON SCHEMA marts_schema IS
    'Витрины данных, создаваемые с помощью dbt';