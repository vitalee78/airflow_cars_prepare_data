# scripts/loader_lots.py

from sqlalchemy import text
from scripts.cars.common.db_postgres import get_engine


class ReferenceLoader:
    def __init__(self, airflow_mode: bool = True):
        self.airflow_mode = airflow_mode
        self.engine = get_engine(airflow_mode=self.airflow_mode)

        # Кэши для справочников (в пределах одного экземпляра)
        self._brand_cache = {}          # brand_name → id_brand
        self._model_cache = {}          # (brand_id, model_name) → id_model
        self._carbody_cache = {}        # (model_id, carbody) → id_carbody

    def get_or_create_brand(self, brand_name: str) -> int:
        if brand_name in self._brand_cache:
            return self._brand_cache[brand_name]

        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO ref_brands (brand)
                    VALUES (:brand)
                    ON CONFLICT (brand) DO NOTHING
                    RETURNING id_brand
                """),
                {"brand": brand_name}
            ).fetchone()

            if result:
                brand_id = result[0]
            else:
                result = conn.execute(
                    text("SELECT id_brand FROM ref_brands WHERE brand = :brand"),
                    {"brand": brand_name}
                ).fetchone()
                brand_id = result[0]

        self._brand_cache[brand_name] = brand_id
        return brand_id

    def get_or_create_model(self, brand_id: int, model_name: str) -> int:
        key = (brand_id, model_name)
        if key in self._model_cache:
            return self._model_cache[key]

        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO ref_models (id_brand, model)
                    VALUES (:brand_id, :model)
                    ON CONFLICT (id_brand, model) DO NOTHING
                    RETURNING id_model
                """),
                {"brand_id": brand_id, "model": model_name}
            ).fetchone()

            if result:
                model_id = result[0]
            else:
                result = conn.execute(
                    text("""
                        SELECT id_model
                        FROM ref_models
                        WHERE id_brand = :brand_id AND model = :model
                    """),
                    {"brand_id": brand_id, "model": model_name}
                ).fetchone()
                model_id = result[0]

        self._model_cache[key] = model_id
        return model_id

    def get_or_create_carbody(self, model_id: int, carbody: str | None) -> int | None:
        if carbody is None:
            return None

        # Нормализация
        carbody_clean = carbody.strip()
        if not carbody_clean:
            return None
        carbody_upper = carbody_clean.upper()


        key = (model_id, carbody)
        if key in self._carbody_cache:
            return self._carbody_cache[key]

        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO ref_carbodies (id_model, carbody)
                    VALUES (:model_id, :carbody)
                    ON CONFLICT (id_model, carbody) DO NOTHING
                    RETURNING id_carbody
                """),
                {"model_id": model_id, "carbody": carbody_upper}
            ).fetchone()

            if result:
                carbody_id = result[0]
            else:
                result = conn.execute(
                    text("""
                        SELECT id_carbody
                        FROM ref_carbodies
                        WHERE id_model = :model_id AND carbody = :carbody
                    """),
                    {"model_id": model_id, "carbody": carbody_upper}
                ).fetchone()
                if not result:
                    raise RuntimeError(f"Carbody not found after insert: {model_id}, {carbody_upper}")
                carbody_id = result[0]

        self._carbody_cache[key] = carbody_id
        return carbody_id