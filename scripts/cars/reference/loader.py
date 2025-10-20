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
                text("SELECT fact.fn_get_or_insert_brand(:brand)"),
                {"brand": brand_name}
            ).scalar()

        self._brand_cache[brand_name] = result
        return result

    def get_or_create_model(self, brand_id: int, model_name: str) -> int:
        key = (brand_id, model_name)
        if key in self._model_cache:
            return self._model_cache[key]

        with self.engine.begin() as conn:
            result = conn.execute(
                text("SELECT fact.fn_get_or_insert_model(:brand_id, :model)"),
                {"brand_id": brand_id, "model": model_name}
            ).scalar()

        self._model_cache[key] = result
        return result

    def get_or_create_carbody(self, model_id: int, carbody: str | None) -> int | None:
        if carbody is None:
            return None

        # Нормализация
        carbody_clean = carbody.strip()
        if not carbody_clean:
            return None
        carbody_upper = carbody_clean.upper()

        key = (model_id, carbody_upper)
        if key in self._carbody_cache:
            return self._carbody_cache[key]

        with self.engine.begin() as conn:
            result = conn.execute(
                text("SELECT fact.fn_get_or_insert_carbody(:model_id, :carbody)"),
                {"model_id": model_id, "carbody": carbody_upper}
            ).scalar()

        self._carbody_cache[key] = result
        return result