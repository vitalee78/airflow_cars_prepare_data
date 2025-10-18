import json

import pandas as pd
from sqlalchemy import text

from scripts.cars.common.db_postgres import get_engine
from scripts.cars.reference.loader import ReferenceLoader


class LoaderAuctions:
    def __init__(self, airflow_mode: bool = True):
        self.airflow_mode = airflow_mode

    def save_auctions_to_db(self, df: pd.DataFrame):
        if df.empty:
            return

        ref = ReferenceLoader(airflow_mode=self.airflow_mode)
        engine = get_engine(airflow_mode=self.airflow_mode)

        # Подготавливаем данные
        records = []
        for _, row in df.iterrows():
            brand_id = ref.get_or_create_brand(row['brand'])
            model_id = ref.get_or_create_model(brand_id, row['model'])
            carbody_id = ref.get_or_create_carbody(model_id, row.get('carbody'))

            records.append({
                "id_brand": brand_id,
                "id_model": model_id,
                "id_carbody": carbody_id,
                "id_car": row['id_car'],
                "equipment": row.get('equipment'),
                "year_release": row['year'],
                "mileage": row.get('mileage'),
                "source_lot_id": row['source_lot_id'],
                "link_source": row['link_source'],
                "auction_date": str(row['lot_date']),
                "rate": row.get('rate')
            })

        json_str = json.dumps(records, ensure_ascii=False)

        with engine.begin() as conn:
            conn.execute(
                text("SELECT public.fn_upsert_auction_cars(:data)"),
                {"data": json_str}
            )