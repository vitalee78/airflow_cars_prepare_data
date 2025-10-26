{{ config(materialized='table', schema='mart') }}

with auction as (
    select * from {{ ref('stg_auction_cars') }}
),
stats as (
    select * from {{ source('raw', 'cars_raw') }}
),
enriched_with_closest as (
    select
        a.id_car,
        a.id_brand,
        a.id_model,
        a.id_carbody,
        a.year_release,
        a.mileage as auction_mileage,
        a.transmission,
        a.drive_type,
        a.fuel_type,
        a.source_lot_id,
        a.link_source,
        a.auction_date,
        a.rate,
        a.equipment,
        s."cost" as closest_mileage_price,
        s.mileage as stat_mileage,
        row_number() over (
            partition by a.id_car
            order by abs(a.mileage - s.mileage)
        ) as rn
    from auction a
    inner join stats s
        on a.id_brand    = s.id_brand
        and a.id_model   = s.id_model
        and a.id_carbody = s.id_carbody
        and a.year_release = s.year_release
        and a.rate       = s.rate
),
closest_match as (
    select *
    from enriched_with_closest
    where rn = 1
),
brands as (select id_brand, brand from {{ ref('stg_brands') }}),
models as (select id_model, model from {{ ref('stg_models') }}),
carbodies as (select id_carbody, carbody from {{ ref('stg_carbodies') }})

select
    cm.id_car as id,
    b.brand,
    m.model,
    cb.carbody,
    cm.year_release,
    cm.auction_mileage as mileage,
    cm.transmission,
    cm.drive_type,
    cm.fuel_type,
    cm.closest_mileage_price as price,
    cm.source_lot_id,
    cm.link_source,
    cm.auction_date,
    cm.rate,
    cm.equipment
from closest_match cm
left join brands b      on cm.id_brand = b.id_brand
left join models m      on cm.id_model = m.id_model
left join carbodies cb  on cm.id_carbody = cb.id_carbody