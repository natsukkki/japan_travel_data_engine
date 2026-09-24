\set ON_ERROR_STOP on

CREATE TABLE dds_schema.dds_weather_processed_files (
    object_key VARCHAR(100) NOT NULL,
    processed_at TIMESTAMP NOT NULL,

    CONSTRAINT pk_weather_processed_files PRIMARY KEY (object_key)
);

COMMENT ON TABLE dds_schema.dds_weather_processed_files IS
    'Журнал успешно обработанных погодных файлов MinIO; ключи фиксируются в одной транзакции с переносом данных из staging в DDS';

COMMENT ON COLUMN dds_schema.dds_weather_processed_files.object_key IS
    'Уникальный ключ объекта в настроенном raw-бакете MinIO; используется для исключения уже обработанных файлов из следующей загрузки';

COMMENT ON COLUMN dds_schema.dds_weather_processed_files.processed_at IS
    'Время регистрации успешной обработки файла в транзакции загрузки DDS';
