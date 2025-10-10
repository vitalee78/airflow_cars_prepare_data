import pandas as pd
from sqlalchemy import text

from scripts.cars.reference.loader import ReferenceLoader
from scripts.cars.common.db_postgres import get_engine


class LotsLoader:
    def __init__(self, airflow_mode: bool = True):
        self.airflow_mode = airflow_mode

    def save_lots_to_db(self, df: pd.DataFrame):
        if df.empty:
            return

        ref = ReferenceLoader(airflow_mode=self.airflow_mode)
        engine = get_engine(airflow_mode=self.airflow_mode)

        with engine.begin() as conn:
            for _, row in df.iterrows():
                # 1. Получаем или создаём справочники
                brand_id = ref.get_or_create_brand(row['brand'])
                model_id = ref.get_or_create_model(brand_id, row['model'])
                carbody_id = ref.get_or_create_carbody(model_id, row.get('carbody'))

                # 2. Вставка или обновление лота в f_cars
                result = conn.execute(
                    text("""
                        INSERT INTO f_cars (
                            id_brand, id_model, id_carbody,
                            cost, year_release, rate, mileage,
                            source_lot_id, link_source,
                            created_at, updated_at, lot_date
                        ) VALUES (
                            :id_brand, :id_model, :id_carbody,
                            :cost, :year_release, :rate, :mileage,
                            :source_lot_id, :link_source,
                            NOW(), NOW(), :lot_date
                        )
                        ON CONFLICT (source_lot_id) DO UPDATE
                        SET
                            cost = EXCLUDED.cost,
                            mileage = EXCLUDED.mileage,
                            updated_at = NOW()
                        RETURNING id
                    """),
                    {
                        "id_brand": brand_id,
                        "id_model": model_id,
                        "id_carbody": carbody_id,
                        "cost": row['cost'],
                        "year_release": row['year'],
                        "rate": row.get('rate'),
                        "mileage": row.get('mileage'),
                        "source_lot_id": row['source_lot_id'],
                        "link_source": row['link_source'],
                        "lot_date": row['lot_date']
                    }
                ).fetchone()

                car_id = result[0]

                # 3. Добавляем запись в историю цен
                # conn.execute(
                #     text("""
                #         INSERT INTO f_cost_hist (car_id, cost, cost_date)
                #         VALUES (:car_id, :cost, NOW())
                #     """),
                #     {"car_id": car_id, "cost": row['cost']}
                # )