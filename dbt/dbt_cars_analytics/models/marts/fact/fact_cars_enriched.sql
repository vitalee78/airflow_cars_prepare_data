{{ config(materialized='table', schema='fact') }}

with auction as (
    select * from {{ ref('stg_auction_cars') }}
),
brands as (
    select id_brand, brand from {{ ref('stg_brands') }}
),
models as (
    select id_model, model from {{ ref('stg_models') }}
),
carbodies as (
    select id_carbody, carbody from {{ ref('stg_carbodies') }}
)

select
    auction.id_car as id,
    brands.brand,
    models.model,
    carbodies.carbody,
    auction.year_release,
    auction.mileage,
    auction.transmission,
    auction.drive_type,
    auction.fuel_type,
    auction.start_price,
    auction.final_price as cost,
    auction.source_lot_id,
    auction.link_source,
    auction.auction_date,
    auction.rate,
    auction.equipment
from auction
left join brands    on auction.id_brand    = brands.id_brand
left join models    on auction.id_model    = models.id_model
left join carbodies on auction.id_carbody  = carbodies.id_carbody