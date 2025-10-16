import pandas as pd
from sqlalchemy import text

from scripts.cars.reference.loader import ReferenceLoader
from scripts.cars.common.db_postgres import get_engine


class LoaderAuctions:
    def __init__(self, airflow_mode: bool = True):
        self.airflow_mode = airflow_mode

    def save_auctions_to_db(self, df: pd.DataFrame):
        if df.empty:
            return

        ref = ReferenceLoader(airflow_mode=self.airflow_mode)
        engine = get_engine(airflow_mode=self.airflow_mode)

        # 1. Собираем текущие ID лотов
        current_lot_ids = df['source_lot_id'].tolist()

        with engine.begin() as conn:
            # 2. Обрабатываем каждый лот — UPSERT как 'active'
            for _, row in df.iterrows():
                brand_id = ref.get_or_create_brand(row['brand'])
                model_id = ref.get_or_create_model(brand_id, row['model'])
                carbody_id = ref.get_or_create_carbody(model_id, row.get('carbody'))

                conn.execute(
                    text("""
                        INSERT INTO f_auction_cars (
                            id_brand, id_model, id_carbody, id_car,
                            equipment,
                            year_release, mileage,
                            source_lot_id, link_source, auction_date,
                            created_at, updated_at, status, rate
                        ) VALUES (
                            :id_brand, :id_model, :id_carbody, :id_car,
                            :equipment,
                            :year_release, :mileage,
                            :source_lot_id, :link_source, :auction_date,
                            NOW(), NOW(), 'active', :rate
                        )
                        ON CONFLICT (id_car) DO UPDATE
                        SET
                            mileage = EXCLUDED.mileage,
                            link_source = EXCLUDED.link_source,
                            equipment = EXCLUDED.equipment,
                            rate = EXCLUDED.rate,
                            updated_at = NOW(),
                            status = 'active'  -- возвращаем в active, если снова появился
                        RETURNING id
                    """),
                    {
                        "id_brand": brand_id,
                        "id_model": model_id,
                        "id_carbody": carbody_id,
                        "id_car": row['id_car'],
                        "equipment": row['equipment'],
                        "year_release": row['year'],
                        "mileage": row.get('mileage'),
                        "source_lot_id": row['source_lot_id'],
                        "link_source": row['link_source'],
                        "auction_date": row['lot_date'],
                        "rate": row.get('rate')
                    }
                )

            # 3. Помечаем как 'sold' лоты, которые:
            #    - были 'active'
            #    - НЕ в списке current_lot_ids
            #    - аукцион уже прошёл (auction_date <= сегодня)
            # if current_lot_ids:
            #     conn.execute(
            #         text("""
            #             UPDATE f_auction_cars
            #             SET status = 'sold', updated_at = NOW()
            #             WHERE status = 'active'
            #               AND source_lot_id NOT IN :current_ids
            #               AND auction_date <= CURRENT_DATE
            #         """),
            #         {"current_ids": tuple(current_lot_ids)}
            #     )
            # else:
            #     # Если парсинг вернул 0 лотов — все активные лоты с прошедших аукционов → sold
            #     conn.execute(
            #         text("""
            #             UPDATE f_auction_cars
            #             SET status = 'sold', updated_at = NOW()
            #             WHERE status = 'active'
            #               AND auction_date <= CURRENT_DATE
            #         """)
            #     )