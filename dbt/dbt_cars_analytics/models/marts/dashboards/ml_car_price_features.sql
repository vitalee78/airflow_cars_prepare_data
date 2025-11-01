{{ config(materialized='table', schema='mart') }}

with cars as (
    select
        id_car,
        id_brand,
        id_model,
        id_carbody,
        "cost" as target_price,
        year_release,
        mileage,
        transmission,
        drive_type,
        fuel_type,
        lot_date
    from {{ source('raw', 'cars_raw') }}
    where "cost" is not null and "cost" > 0
),

brands as (
    select id_brand, brand
    from {{ source('raw', 'brands_raw') }}
),

models as (
    select id_model, id_brand, model
    from {{ source('raw', 'models_raw') }}
),

carbodies as (
    select id_carbody, carbody
    from {{ source('raw', 'carbodies_raw') }}
),

-- Простой пример: добавим возраст авто
base_features as (
    select
        c.*,
        extract(year from current_date) - c.year_release as car_age
    from cars c
),

-- Здесь можно добавить агрегаты по рынку (по брендам, моделям и т.д.)
-- Пока оставим только базовые признаки для примера
final as (
    select
        id_car,
        id_brand,
        id_model,
        id_carbody,
        target_price,
        year_release,
        car_age,
        mileage,
        transmission,
        drive_type,
        fuel_type,

        -- Заглушки для числовых признаков (заменить на реальные агрегаты позже)
        -- Например, через window functions или join с агрегированными таблицами
        0::float as market_avg_price_3m,
        0::float as market_median_price_3m,
        0::float as market_min_price_3m,
        0::float as market_max_price_3m,
        0::float as market_std_price_3m,
        0::int as market_listing_count_3m,
        0::float as market_avg_mileage_3m,
        0::float as market_median_mileage_3m,
        0::float as brand_avg_price_3m,
        0::int as brand_listing_count_3m,
        0::float as mileage_vs_market_pct,
        0::float as model_vs_brand_price_ratio,
        0::int as low_mileage_count_3m,
        0::int as medium_mileage_count_3m,
        0::int as high_mileage_count_3m

    from base_features c
)

select * from final