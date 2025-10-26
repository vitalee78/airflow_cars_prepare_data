{{ config(materialized='view', schema='mart_cars') }}

with current_vld as (
    select (now() at time zone 'Asia/Novosibirsk')::date as today
),
date_range as (
    select
        current_vld.today + 1 as min_auction_date,
        current_vld.today + 4 as max_auction_date
    from current_vld
),
filtered_lots as (
    select
        s.brand,
        s.carbody,
        s.cost,
        s.year_release,
        s.rate,
        s.link_source,
        s.auction_date,
        s.source_lot_id
    from {{ ref('fact_cars_enriched') }} s
    cross join date_range dr
    where s.auction_date between dr.min_auction_date and dr.max_auction_date
      and s.cost is not null
),
ranked as (
    select
        *,
        row_number() over (
            partition by brand, carbody, year_release, rate
            order by cost, auction_date
        ) as rn
    from filtered_lots
)
select
    brand,
    carbody as "Кузов",
    cost as "Минимальная цена",
    year_release as "Год выпуска",
    rate as "Балл",
    link_source,
    auction_date as "Дата ауциона",
    source_lot_id as "Номер лота"
from ranked
where rn = 1
order by brand, carbody