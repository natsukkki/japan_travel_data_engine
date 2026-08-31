\set ON_ERROR_STOP on

CREATE TABLE IF NOT EXISTS dds_schema.dds_regions (
    region_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    region_code VARCHAR(20) NOT NULL,
    prefecture_name VARCHAR(100) NOT NULL,
    city_name VARCHAR(100) NOT NULL, 
    latitude NUMERIC(8, 5) NOT NULL,
    longitude NUMERIC(9, 5) NOT NULL,
    
    CONSTRAINT pk_dds_regions PRIMARY KEY (region_id),

    CONSTRAINT uq_dds_regions_region_code UNIQUE (region_code),

    CONSTRAINT ck_dds_regions_latitude 
        CHECK (latitude BETWEEN -90 AND 90),

    CONSTRAINT ck_dds_regions_longitude
        CHECK (longitude BETWEEN -180 AND 180)
);

COMMENT ON TABLE dds_schema.dds_regions IS 
    'Единый справочник выбранных регионов Японии';

COMMENT ON COLUMN dds_schema.dds_regions.region_id IS
    'Внутренний суррогатный идентификатор региона';

COMMENT ON COLUMN dds_schema.dds_regions.region_code IS
    'Стабильный бизнес-код региона, используемый в исходных данных';

COMMENT ON COLUMN dds_schema.dds_regions.prefecture_name IS
    'Название префектуры на английском языке';

COMMENT ON COLUMN dds_schema.dds_regions.city_name IS
    'Город, представляющий регион в источниках погодных данных и наблюдений';

COMMENT ON COLUMN dds_schema.dds_regions.latitude IS
    'Широта выбранной точки региона в десятичных градусах';

COMMENT ON COLUMN dds_schema.dds_regions.longitude IS
    'Долгота выбранной точки региона в десятичных градусах';
