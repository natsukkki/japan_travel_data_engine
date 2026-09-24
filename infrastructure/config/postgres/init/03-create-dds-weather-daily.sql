\set ON_ERROR_STOP on

CREATE TABLE IF NOT EXISTS dds_schema.dds_weather_daily (
    region_id SMALLINT NOT NULL,
    weather_date DATE NOT NULL,
    temperature_mean NUMERIC(5,2) NOT NULL,
    precipitation_sum NUMERIC(7,2) NOT NULL,
    humidity_mean NUMERIC(5,2) NOT NULL,
    wind_speed_mean NUMERIC(6, 2) NOT NULL,
    sunshine_duration_seconds INTEGER NOT NULL,

    CONSTRAINT pk_dds_weather_daily PRIMARY KEY (region_id, weather_date),

    CONSTRAINT fk_dds_weather_daily_dds_regions 
        FOREIGN KEY (region_id) REFERENCES dds_schema.dds_regions (region_id)
);

COMMENT ON TABLE dds_schema.dds_weather_daily IS
    'Очищенные ежедневные погодные наблюдения по выбранным регионам Японии';

COMMENT ON COLUMN dds_schema.dds_weather_daily.region_id IS
    'Идентификатор региона из справочника dds_regions';

COMMENT ON COLUMN dds_schema.dds_weather_daily.weather_date IS
    'Календарная дата погодного наблюдения';

COMMENT ON COLUMN dds_schema.dds_weather_daily.temperature_mean IS
    'Средняя суточная температура воздуха в градусах Цельсия';

COMMENT ON COLUMN dds_schema.dds_weather_daily.precipitation_sum IS
    'Суммарное количество осадков за сутки в миллиметрах';

COMMENT ON COLUMN dds_schema.dds_weather_daily.humidity_mean IS
    'Средняя суточная относительная влажность воздуха в процентах';

COMMENT ON COLUMN dds_schema.dds_weather_daily.wind_speed_mean IS
    'Средняя суточная скорость ветра на высоте 10 метров в метрах в секунду';

COMMENT ON COLUMN dds_schema.dds_weather_daily.sunshine_duration_seconds IS
    'Фактическая продолжительность солнечного сияния за сутки в секундах';
