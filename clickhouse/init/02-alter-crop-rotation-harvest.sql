-- Существующие кластеры без колонки в fact_crop_rotation
ALTER TABLE agro_marts.fact_crop_rotation
    ADD COLUMN IF NOT EXISTS harvest_yield_t_per_ha Nullable(Float64);
