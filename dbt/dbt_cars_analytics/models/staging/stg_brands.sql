select
    id_brand,
    brand
from {{ source('raw', 'brands_raw') }}