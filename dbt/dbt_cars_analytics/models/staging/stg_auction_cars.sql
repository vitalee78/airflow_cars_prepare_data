select
    id_car,
    id_brand,
    id_model,
    id_carbody,
    year_release,
    mileage,
    transmission,
    drive_type,
    fuel_type,
    start_price,
    final_price,
    source_lot_id,
    link_source,
    auction_date,
    rate,
    equipment
from {{ source('raw', 'auction_cars_raw') }}