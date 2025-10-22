# ml/train_model.py
import logging
import pickle
import warnings
from pathlib import Path
from typing import Dict, Any

import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder

from scripts.cars.common.db_postgres import get_engine

logger = logging.getLogger(__name__)

# Отключаем FutureWarning о downcasting
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.set_option('future.no_silent_downcasting', True)

# Признаки для модели
NUMERICAL_FEATURES = [
    'year_release', 'car_age', 'mileage',
    'market_avg_price_3m', 'market_median_price_3m',
    'market_min_price_3m', 'market_max_price_3m',
    'market_std_price_3m', 'market_listing_count_3m',
    'market_avg_mileage_3m', 'market_median_mileage_3m',
    'brand_avg_price_3m', 'brand_listing_count_3m',
    'mileage_vs_market_pct', 'model_vs_brand_price_ratio',
    'low_mileage_count_3m', 'medium_mileage_count_3m', 'high_mileage_count_3m'
]

CATEGORICAL_FEATURES = ['id_brand', 'id_model', 'id_carbody', 'transmission', 'drive_type', 'fuel_type']


class Train:
    def __init__(self, airflow_mode: bool = True):
        self.airflow_mode = airflow_mode
        # Определяем корень проекта относительно текущего файла
        self.project_root = Path(__file__).parent.parent
        self.models_dir = self.project_root / "models"
        self.models_dir.mkdir(exist_ok=True)

    def prepare_features(self, df: pd.DataFrame):
        """Подготовка признаков с one-hot encoding"""
        # Числовые признаки
        X_numerical = df[NUMERICAL_FEATURES].fillna(0)

        # One-hot encoding для категориальных признаков
        encoder = OneHotEncoder(drop='first', sparse_output=False, handle_unknown='ignore')
        X_categorical = encoder.fit_transform(df[CATEGORICAL_FEATURES])

        # Названия колонок после кодирования
        categorical_columns = []
        for i, feature in enumerate(CATEGORICAL_FEATURES):
            for category in encoder.categories_[i][1:]:
                categorical_columns.append(f"{feature}_{category}")

        X_categorical_df = pd.DataFrame(X_categorical, columns=categorical_columns, index=df.index)

        # Объединяем все признаки
        X_processed = pd.concat([X_numerical, X_categorical_df], axis=1)

        return X_processed, encoder

    def train_and_evaluate(self) -> Dict[str, Any]:
        """
        Обучает модель и возвращает метрики + путь к модели.
        Не вызывает sys.exit — исключения пробрасываются выше для обработки Airflow.
        """
        engine = get_engine(airflow_mode=self.airflow_mode)

        try:
            connection = engine.raw_connection()
            try:
                df = pd.read_sql("""
                    SELECT * 
                    FROM mart.v_ml_car_price_features 
                    WHERE target_price IS NOT NULL 
                          AND target_price > 0;
                """, connection)
            finally:
                connection.close()

            if df.empty:
                raise ValueError("Нет данных для обучения модели")

            logger.info(f"Загружено {len(df)} записей для обучения")

            # Подготавливаем признаки
            X, encoder = self.prepare_features(df)
            y = df['target_price']

            logger.info(f"Всего признаков после кодирования: {X.shape[1]}")

            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42, shuffle=True
            )

            # Улучшенная модель
            model = RandomForestRegressor(
                n_estimators=200,
                max_depth=25,
                min_samples_split=5,
                min_samples_leaf=2,
                max_features='sqrt',
                random_state=42,
                n_jobs=-1
            )

            model.fit(X_train, y_train)

            # Предсказания и метрики
            y_pred_train = model.predict(X_train)
            y_pred_test = model.predict(X_test)

            mae_train = mean_absolute_error(y_train, y_pred_train)
            mae_test = mean_absolute_error(y_test, y_pred_test)
            r2_train = r2_score(y_train, y_pred_train)
            r2_test = r2_score(y_test, y_pred_test)

            logger.info(f"Размер тренировочной выборки: {len(X_train)}")
            logger.info(f"Размер тестовой выборки: {len(X_test)}")
            logger.info(f"MAE (train): {mae_train:,.0f} руб, MAE (test): {mae_test:,.0f} руб")
            logger.info(f"R² (train): {r2_train:.3f}, R² (test): {r2_test:.3f}")

            # Генерируем имя модели с временной меткой или версией
            model_filename = "car_price_model_v4.pkl"
            model_path = self.models_dir / model_filename

            # Сохраняем модель
            model_artifact = {
                'model': model,
                'encoder': encoder,
                'numerical_features': NUMERICAL_FEATURES,
                'categorical_features': CATEGORICAL_FEATURES,
                'all_feature_columns': list(X.columns),
                'metrics': {
                    'mae_train': mae_train,
                    'mae_test': mae_test,
                    'r2_train': r2_train,
                    'r2_test': r2_test
                },
                'training_date': pd.Timestamp.now(),
                'training_samples': len(X_train),
                'feature_importance': dict(zip(X.columns, model.feature_importances_))
            }

            with open(model_path, "wb") as f:
                pickle.dump(model_artifact, f)

            logger.info(f"Модель успешно сохранена в: {model_path}")

            # Обновляем latest_model.txt
            latest_file = self.models_dir / "latest_model.txt"
            with open(latest_file, "w") as f:
                f.write(model_filename)
            logger.info(f"Активная модель обновлена: {model_filename}")

            # Топ-15 самых важных признаков
            feature_importance = sorted(
                zip(X.columns, model.feature_importances_),
                key=lambda x: x[1],
                reverse=True
            )[:15]

            for feature, importance in feature_importance:
                logger.info(f"  {feature}: {importance:.3f}")

            # Возвращаем результат для Airflow (можно отправить в XCom)
            return {
                "model_path": str(model_path),
                "metrics": model_artifact["metrics"],
                "training_samples": len(X_train),
                "status": "success"
            }

        except Exception as e:
            logger.error(f"Ошибка при обучении модели: {e}")
            raise  # Пробрасываем исключение — Airflow сам обработает как failure


if __name__ == "__main__":
    train = Train(airflow_mode=False)
    result = train.train_and_evaluate()
    print("Обучение завершено успешно:", result["model_path"])
