select
    id_carbody,
    id_model,
    carbody,
    description
from {{ source('raw', 'carbodies_raw') }}