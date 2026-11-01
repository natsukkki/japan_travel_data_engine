\set ON_ERROR_STOP on

CREATE TABLE dds_schema.dds_staging_tourism_monthly (
    region_id SMALLINT NOT NULL,
    year INT NOT NULL,
    month SMALLINT NOT NULL,
    total_guest_nights INT NOT NULL,
    foreign_guest_nights INT NOT NULL,
    logical_key TEXT NOT NULL,
    release_type VARCHAR(20) NOT NULL,
    object_key TEXT NOT NULL,

    CONSTRAINT pk_dds_staging_tourism_monthly
        PRIMARY KEY (object_key, region_id, year, month)
);
