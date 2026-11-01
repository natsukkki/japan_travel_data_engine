\set ON_ERROR_STOP on

CREATE TABLE dds_schema.dds_tourism_processed_files (
    logical_key TEXT NOT NULL,
    object_key TEXT NOT NULL,
    processed_at TIMESTAMP NOT NULL,

    CONSTRAINT pk_dds_tourism_processed_files
        PRIMARY KEY (logical_key)
);
