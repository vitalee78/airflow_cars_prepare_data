SELECT id_car
	,id_brand
	,id_model
	,id_carbody
	,"cost"
	,year_release
	,rate
	,mileage
	,transmission
	,drive_type
	,fuel_type
	,source_lot_id
	,link_source
	,lot_date
	,equipment
	,created_at
	,updated_at
	,id
FROM {{ source('raw', 'cars_raw') }}