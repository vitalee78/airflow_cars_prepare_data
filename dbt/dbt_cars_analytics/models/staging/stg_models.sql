select
    id_model,
    id_brand,
    model
from {{ source('raw', 'models_raw') }}