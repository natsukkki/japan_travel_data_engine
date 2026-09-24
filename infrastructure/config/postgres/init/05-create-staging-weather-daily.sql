\set ON_ERROR_STOP on

CREATE TABLE dds_schema.dds_staging_weather_daily (
    region_id SMALLINT NOT NULL,
    weather_date DATE NOT NULL,
    temperature_mean NUMERIC(5,2) NOT NULL,
    precipitation_sum NUMERIC(7,2) NOT NULL,
    humidity_mean NUMERIC(5,2) NOT NULL,
    wind_speed_mean NUMERIC(6,2) NOT NULL,
    sunshine_duration_seconds INTEGER NOT NULL,
    object_key TEXT NOT NULL,

    CONSTRAINT pk_staging_weather_daily 
        PRIMARY KEY (region_id, weather_date)
); 