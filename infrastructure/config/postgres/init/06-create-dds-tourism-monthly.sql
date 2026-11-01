\set ON_ERROR_STOP on

CREATE TABLE dds_schema.dds_tourism_monthly (
    region_id SMALLINT NOT NULL,
    year INT NOT NULL,
    month SMALLINT NOT NULL,
    total_guest_nights INT NOT NULL,
    foreign_guest_nights INT NOT NULL,
    release_type VARCHAR(20) NOT NULL,

    CONSTRAINT pk_dds_tourism_monthly
        PRIMARY KEY (region_id, year, month),

    CONSTRAINT fk_dds_torism_monthly_dds_regions
        FOREIGN KEY (region_id) REFERENCES dds_schema.dds_regions (region_id)
);
