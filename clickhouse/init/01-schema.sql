CREATE DATABASE IF NOT EXISTS agro_marts;

CREATE TABLE IF NOT EXISTS agro_marts.sync_cursors
(
    source    LowCardinality(String),
    watermark DateTime64(3, 'UTC'),
    updated   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated)
ORDER BY source;

-- Измерение поле / сезон / организация; centroid_* — задел под future map
CREATE TABLE IF NOT EXISTS agro_marts.dim_field
(
    field_id          String,
    field_name        String,
    season_id         String,
    season_name       String,
    organization_id   String,
    contour_count     UInt32 DEFAULT 0,
    field_area_ha     Nullable(Float64),
    centroid_lat      Nullable(Float64),
    centroid_lon      Nullable(Float64),
    synced_at         DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(synced_at)
ORDER BY (organization_id, field_id);

CREATE TABLE IF NOT EXISTS agro_marts.fact_dzz_scene
(
    organization_id   String,
    field_id          String,
    season_id         Nullable(String),
    contour_id        String,
    scene_id          String,
    scene_date        Date,
    collection_name   String,
    sensor            String,
    cloud_cover       Nullable(Float64),
    ndvi              Nullable(Float64),
    evi               Nullable(Float64),
    ndwi              Nullable(Float64),
    msavi             Nullable(Float64),
    updated_at        DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (organization_id, field_id, ifNull(season_id, ''), contour_id, scene_id);

CREATE TABLE IF NOT EXISTS agro_marts.fact_meteo_observation
(
    organization_id      String,
    field_id             String,
    date_time            DateTime64(3, 'UTC'),
    temperature          Nullable(Float64),
    humidity             Nullable(Float64),
    wind_speed           Nullable(Float64),
    precipitation        Nullable(Float64),
    soil_moisture_0_1cm  Nullable(Float64)
)
ENGINE = ReplacingMergeTree(date_time)
ORDER BY (organization_id, field_id, date_time);

-- Севооборот: витрина для отчётности владельца (культура, сроки, привязка к полю/сезону/контуру)
CREATE TABLE IF NOT EXISTS agro_marts.fact_crop_rotation
(
    organization_id    String,
    crop_rotation_id   String,
    field_id           String,
    field_name         String,
    season_id          String,
    season_name        String,
    contour_id         String,
    contour_name       String,
    culture            String,
    cultivar           String,
    start_date         Date,
    end_date           Nullable(Date),
    contour_area_ha       Nullable(Float64),
    description           Nullable(String),
    harvest_yield_t_per_ha Nullable(Float64),
    synced_at             DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(synced_at)
ORDER BY (organization_id, crop_rotation_id);

-- Витрины mart_* (pre-aggregate) можно добавить позже через MV / Spark без смены фактов.
-- Пример для NDVI по дням: AggregatingMergeTree + avgState(assumeNotNull(ndvi)) после фильтра.
