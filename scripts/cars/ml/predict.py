# scripts/cars/ml/predict.py
import logging
import pickle
import sys
import warnings
from pathlib import Path
from typing import Optional, List, Any, Dict

import numpy as np
import pandas as pd
from sklearn.preprocessing import OneHotEncoder

from scripts.cars.common.db_postgres import get_engine

logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore")

# Конфиг аукциона (можно переопределить через аргумент)
AUCTION_CONFIG = {
    'start_price_ratio': 0.7,
    'min_start_price': 100000,
    'max_start_price': 5000000,
    'start_price_source': 'market',
    'filters': {
        'min_year': None,
        'max_year': None,
        'top': None
    }
}


class Predict:
    def __init__(self, airflow_mode: bool = True):
        self.airflow_mode = airflow_mode
        # Определяем корень проекта относительно текущего файла
        self.project_root = Path(__file__).parent.parent  # scripts/cars/ml → корень проекта
        self.models_dir = self.project_root / "models"

    def _load_latest_model(self):
        """Загружает последнюю активную модель по latest_model.txt"""
        latest_file = self.models_dir / "latest_model.txt"
        if not latest_file.exists():
            raise FileNotFoundError(
                f"Файл {latest_file} не найден. Обучите модель сначала (он должен содержать имя .pkl файла)."
            )

        with open(latest_file, "r") as f:
            model_filename = f.read().strip()

        model_path = self.models_dir / model_filename
        if not model_path.exists():
            raise FileNotFoundError(f"Модель {model_path} не найдена.")

        logger.info(f"Загружается модель: {model_filename}")
        with open(model_path, "rb") as f:
            return pickle.load(f)

    def prepare_prediction_features(
            self,
            df: pd.DataFrame,
            numerical_features: List[str],
            categorical_features: List[str],
            encoder: OneHotEncoder,
            all_feature_columns: List[str]
    ) -> pd.DataFrame:
        """Подготовка признаков для предикта (аналогично обучению)"""
        # Числовые признаки
        X_numerical = df[numerical_features].fillna(0)

        # One-hot для категориальных
        X_categorical = encoder.transform(df[categorical_features])

        # Воссоздаём колонки
        categorical_columns = []
        for i, feature in enumerate(categorical_features):
            for category in encoder.categories_[i][1:]:
                categorical_columns.append(f"{feature}_{category}")

        X_categorical_df = pd.DataFrame(X_categorical, columns=categorical_columns, index=df.index)

        # Объединяем
        X_processed = pd.concat([X_numerical, X_categorical_df], axis=1)

        # Приводим к тому же порядку колонок, что и при обучении
        X_processed = X_processed.reindex(columns=all_feature_columns, fill_value=0)

        return X_processed

    def calculate_start_prices(self, df: pd.DataFrame, config: dict) -> pd.DataFrame:
        """Рассчитывает стартовую цену аукциона"""
        # Пример реализации — адаптируйте под вашу логику
        df['start_price'] = (df['predicted_price'] * config['start_price_ratio']).clip(
            lower=config['min_start_price'],
            upper=config['max_start_price']
        )
        return df

    def calculate_confidence(self, predictions: np.ndarray, df: pd.DataFrame) -> pd.Series:
        """Пример: доверие на основе отклонения от рыночной цены"""
        # Заглушка — замените на вашу логику
        return pd.Series([0.95] * len(predictions), index=df.index)

    def save_predictions_to_db(self, df: pd.DataFrame, engine):
        """Сохраняет прогнозы в БД"""
        # Реализуйте по вашей схеме
        logger.info("Сохранение прогнозов в БД...")
        # Пример:
        # df.to_sql("mart.ml_predictions", engine, if_exists="append", index=False)

    def display_results(self, df: pd.DataFrame, min_year=None, max_year=None, top_count=None):
        """Вывод результатов в консоль"""
        filtered = df.copy()
        if min_year:
            filtered = filtered[filtered['year_release'] >= min_year]
        if max_year:
            filtered = filtered[filtered['year_release'] <= max_year]
        if top_count:
            filtered = filtered.head(top_count)

        print("\n=== ПРОГНОЗЫ ===")
        for _, row in filtered.iterrows():
            print(
                f"{row['car_info']} → прогноз: {row['predicted_price']:,.0f} руб, старт: {row['start_price']:,.0f} руб")

    def predict_auction_prices(
            self,
            config: Optional[dict] = None,
            save_to_db: bool = False,
            show_display: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Прогнозирование цен для будущих аукционов"""
        # Обновляем конфиг
        results_info = []
        current_config = AUCTION_CONFIG.copy()
        if config:
            current_config.update(config)

        logger.info("Начало прогнозирования цен...")
        engine = get_engine(airflow_mode=self.airflow_mode)

        # Загружаем модель через latest_model.txt
        try:
            model_data = self._load_latest_model()
        except Exception as e:
            logger.error(f"Ошибка загрузки модели: {e}")
            raise  # Airflow сам обработает как failure

        model = model_data['model']
        encoder = model_data['encoder']
        numerical_features = model_data['numerical_features']
        categorical_features = model_data['categorical_features']
        all_feature_columns = model_data['all_feature_columns']

        logger.info(f"Модель загружена. Признаков: {len(all_feature_columns)}")

        # Загружаем данные
        connection = engine.raw_connection()
        try:
            df = pd.read_sql("""
                SELECT 
                    f.*,
                    b.brand,
                    m.model,
                    cb.carbody,
                    fac.link_source,
                    CONCAT(b.brand, ' ', m.model, ' ', f.year_release::text, ' (', cb.carbody, ')') as car_info
                FROM mart.ml_car_price_features f
                LEFT JOIN raw.brands_raw b ON f.id_brand = b.id_brand
                LEFT JOIN raw.models_raw m ON f.id_model = m.id_model
                LEFT JOIN raw.carbodies_raw cb ON f.id_carbody = cb.id_carbody
                LEFT JOIN raw.auction_cars_raw fac ON fac.id_car = f.id_car;
            """, connection)
        finally:
            connection.close()

        if df.empty:
            logger.info("Нет будущих лотов для прогнозирования")
            return None

        logger.info(f"Загружено лотов: {len(df)}")

        # Подготавливаем признаки
        X_pred = self.prepare_prediction_features(
            df, numerical_features, categorical_features, encoder, all_feature_columns
        )

        # Прогноз
        predictions = model.predict(X_pred)
        df['predicted_price'] = predictions
        df['prediction_date'] = pd.Timestamp.now()

        # Стартовые цены и доверие
        df = self.calculate_start_prices(df, current_config)
        df['prediction_confidence'] = self.calculate_confidence(predictions, df)

        # Сохранение и вывод
        if save_to_db:
            self.save_predictions_to_db(df, engine)

        if show_display:
            filters = current_config.get('filters', {})
            self.display_results(
                df,
                min_year=filters.get('min_year'),
                max_year=filters.get('max_year'),
                top_count=filters.get('top')
            )

        logger.info("Прогнозирование завершено успешно")

        results_info.append({
            "count_lots": len(df),
            "feature_count": len(all_feature_columns)
        })

        return {
            "status": "success",
            "details": results_info
        }

    def ensure_numeric_types(self, df):
        """Убедиться, что числовые колонки имеют правильный тип"""
        numeric_columns = ['mileage', 'market_avg_price_3m', 'start_price', 'predicted_price']

        for col in numeric_columns:
            if col in df.columns:
                # Преобразуем в numeric, ошибки заменяем на NaN, затем заполняем 0
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        return df

    def calculate_price_difference(self, results):
        """Безопасное вычисление разницы в процентах"""
        if 'start_price' in results.columns and 'predicted_price' in results.columns:
            # Убедимся, что колонки числовые
            start_price = pd.to_numeric(results['start_price'], errors='coerce').fillna(0)
            predicted_price = pd.to_numeric(results['predicted_price'], errors='coerce').fillna(0)

            # Вычисляем разницу в процентах, избегая деления на ноль
            with np.errstate(divide='ignore', invalid='ignore'):
                price_diff_pct = np.where(
                    start_price > 0,
                    (predicted_price - start_price) / start_price * 100,
                    0
                )

            results['price_difference_pct'] = np.round(price_diff_pct, 1)
        else:
            results['price_difference_pct'] = 0

        return results

    def calculate_start_prices(self, df, config):
        """Расчет стартовых цен согласно конфигурации"""

        ratio = config['start_price_ratio']
        min_price = config['min_start_price']
        max_price = config['max_start_price']
        source = config['start_price_source']

        if source == 'market':
            # От рыночной цены
            base_prices = pd.to_numeric(df['market_avg_price_3m'], errors='coerce').fillna(0)
            df['start_price'] = (base_prices * ratio).clip(min_price, max_price)
            df['start_price_source'] = 'market_based'

        elif source == 'predicted':
            # От прогнозируемой цены
            base_prices = pd.to_numeric(df['predicted_price'], errors='coerce').fillna(0)
            df['start_price'] = (base_prices * ratio).clip(min_price, max_price)
            df['start_price_source'] = 'predicted_based'

        elif source == 'fixed':
            # Фиксированная цена для всех
            df['start_price'] = min_price
            df['start_price_source'] = 'fixed_price'

        # Статистика
        logger.info(f"  Рассчитано стартовых цен: {len(df)}")
        logger.info(f"  Средняя стартовая цена: {df['start_price'].mean():,.0f} руб")

        return df

    def prepare_prediction_features(self, df, numerical_features, categorical_features, encoder, all_feature_columns):
        """Подготовка признаков для прогнозирования"""

        # Числовые признаки
        X_numerical = df[numerical_features].copy()

        # Заполняем пропущенные значения в числовых признаках
        for col in numerical_features:
            if col in X_numerical.columns:
                # Преобразуем в numeric, ошибки заменяем на NaN, затем заполняем 0
                X_numerical[col] = pd.to_numeric(X_numerical[col], errors='coerce').fillna(0)
            else:
                # Если колонка отсутствует, создаем с нулевыми значениями
                X_numerical[col] = 0

        # One-hot encoding для категориальных признаков
        if encoder and categorical_features:
            try:
                # Проверяем наличие всех категориальных признаков
                for col in categorical_features:
                    if col not in df.columns:
                        logger.info(f"Создаем отсутствующий категориальный признак: {col}")
                        df[col] = 0

                # Преобразуем категориальные признаки
                X_categorical = encoder.transform(df[categorical_features])

                # Создаем DataFrame с правильными названиями колонок
                categorical_columns = []
                for i, feature in enumerate(categorical_features):
                    for category in encoder.categories_[i][1:]:  # пропускаем первую категорию
                        categorical_columns.append(f"{feature}_{category}")

                X_categorical_df = pd.DataFrame(X_categorical, columns=categorical_columns, index=df.index)

            except Exception as e:
                logger.error(f"Ошибка при кодировании категориальных признаков: {e}")
                # Создаем пустой DataFrame с нужными колонками
                categorical_columns = []
                for feature in categorical_features:
                    if encoder and hasattr(encoder, 'categories_'):
                        idx = categorical_features.index(feature)
                        for category in encoder.categories_[idx][1:]:
                            categorical_columns.append(f"{feature}_{category}")

                X_categorical_df = pd.DataFrame(0, columns=categorical_columns, index=df.index)
        else:
            # Если нет encoder, создаем пустые колонки
            X_categorical_df = pd.DataFrame(index=df.index)
            for col in all_feature_columns:
                if col not in numerical_features and any(col.startswith(cat) for cat in categorical_features):
                    X_categorical_df[col] = 0

        # Объединяем все признаки
        X_processed = pd.concat([X_numerical, X_categorical_df], axis=1)

        # Убеждаемся, что все данные числовые
        X_processed = X_processed.astype(np.float64)

        logger.info(f"Подготовлено признаков: {X_processed.shape[1]}")
        return X_processed

    def calculate_confidence(self, predictions, df):
        """Расчет доверительной оценки прогноза"""
        confidence_scores = []

        for i, pred in enumerate(predictions):
            try:
                # Безопасно получаем рыночную цену
                market_price = df.iloc[i].get('market_avg_price_3m')
                listing_count = df.iloc[i].get('market_listing_count_3m', 0)

                # Преобразуем в numeric на случай, если это строки
                market_price = pd.to_numeric(market_price, errors='coerce')
                listing_count = pd.to_numeric(listing_count, errors='coerce')

                # Проверяем валидность market_price
                if market_price is not None and market_price > 0 and not np.isnan(market_price):
                    price_diff_ratio = abs(pred - market_price) / market_price
                    price_confidence = max(0, 1 - price_diff_ratio)
                else:
                    price_confidence = 0.3

                # Учитываем количество наблюдений на рынке
                listing_count = listing_count if not np.isnan(listing_count) else 0
                count_confidence = min(1.0, (listing_count or 0) / 100)

                # Общая уверенность
                overall_confidence = (price_confidence * 0.7 + count_confidence * 0.3)
                confidence_scores.append(round(overall_confidence, 2))

            except (TypeError, ValueError, ZeroDivisionError) as e:
                # В случае ошибки устанавливаем низкое доверие
                confidence_scores.append(0.1)

        return confidence_scores

    def display_results(self, df, min_year=None, max_year=None, top_count=None):
        """Компактный вывод по брендам с фильтрацией по году выпуска"""

        df = self.ensure_numeric_types(df)
        df = self.calculate_price_difference(df)

        # Применяем фильтрацию по году выпуска
        if min_year is not None:
            df = df[df['year_release'] >= min_year]
            logger.info(f"Фильтр: год выпуска от {min_year}")

        if max_year is not None:
            df = df[df['year_release'] <= max_year]
            logger.info(f"Фильтр: год выпуска до {max_year}")

        profitable_df = df[df['price_difference_pct'] > 0]

        print(f"\n=== ВЫГОДНЫЕ ПРЕДЛОЖЕНИЯ ПО БРЕНДАМ ===")

        # Статистика по годам
        if not profitable_df.empty:
            min_year_actual = profitable_df['year_release'].min()
            max_year_actual = profitable_df['year_release'].max()
            print(f"Диапазон годов в выборке: {min_year_actual}-{max_year_actual}")

        for brand, group in profitable_df.groupby('brand'):
            top_N = group.nlargest(top_count, 'price_difference_pct')

            print(f"\n{brand.upper()} ({len(top_N)} лучших):")

            for i, (idx, row) in enumerate(top_N.iterrows(), 1):
                # Формируем информацию
                model_year = f"{row.get('model', 'N/A')} {row.get('year_release', 'N/A')}"
                carbody = row.get('carbody', '')
                if carbody:
                    model_info = f"{model_year} ({carbody})"
                else:
                    model_info = model_year

                # Стоимости
                start_price = f"{row['start_price']:,.0f}"
                predicted_price = f"{row['predicted_price']:,.0f}"
                profit_info = f"+{row['price_difference_pct']:.1f}%"
                confidence = f"{row.get('prediction_confidence', 0):.0%}"

                # Выводим
                print(f"  {i}. {model_info}")
                print(f"     Старт: {start_price:>10} руб -> Прогноз: {predicted_price:>10} руб ({profit_info})")
                print(f"     Доверие: {confidence}")

                # Пробег если есть
                mileage = row.get('mileage', 0)
                if mileage and mileage > 0:
                    print(f"     Пробег: {mileage:,.0f} км")

                # Ссылка
                link_source = row.get('link_source')
                if link_source and pd.notna(link_source):
                    print(f"     Ссылка: {link_source}")

                print()  # пустая строка между автомобилями

        # Общая статистика
        if not profitable_df.empty:
            print(f"\n=== СТАТИСТИКА ===")
            print(f"Всего выгодных предложений: {len(profitable_df)}")
            print(f"Средняя выгода: +{profitable_df['price_difference_pct'].mean():.1f}%")
            print(f"Медианная выгода: +{profitable_df['price_difference_pct'].median():.1f}%")

    def save_predictions_to_db(self, df, engine):
        try:
            # Используем raw connection для полного контроля
            connection = engine.raw_connection()
            try:
                cursor = connection.cursor()

                # Очищаем таблицу
                cursor.execute("TRUNCATE TABLE mart.ml_predictions")
                connection.commit()
                logger.info("✅ Таблица ml_predictions очищена")

                # Подготавливаем данные для сохранения
                save_data = []

                for _, row in df.iterrows():
                    prediction_record = (
                        row.get('id_car'),
                        row.get('auction_date'),
                        row.get('brand'),
                        row.get('model'),
                        row.get('year_release'),
                        row.get('carbody'),
                        row.get('mileage'),
                        row.get('start_price'),
                        row.get('predicted_price'),
                        row.get('price_difference_pct', 0),
                        row.get('prediction_confidence', 0),
                        row.get('link_source'),
                        pd.Timestamp.now()
                    )
                    save_data.append(prediction_record)

                # Вставляем данные через executemany
                insert_query = """
                        INSERT INTO mart.ml_predictions 
                        (id_car, auction_date, brand, model, year_release, carbody, mileage, 
                         start_price, predicted_price, price_difference_pct, prediction_confidence, 
                         link_source, prediction_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

                cursor.executemany(insert_query, save_data)
                connection.commit()

                logger.info(f"✅ Новые прогнозы сохранены в БД: {len(save_data)} записей")

            finally:
                connection.close()

        except Exception as e:
            logger.error(f"❌ Ошибка при сохранении прогнозов в БД: {e}")
            sys.exit(1)

    def load_current_predictions(self, brand=None, min_confidence=0.0):
        """Загрузка текущих прогнозов из БД"""

        engine = get_engine(airflow_mode=self.airflow_mode)

        query = """
            SELECT 
                brand, model, year_release, carbody, mileage,
                start_price, predicted_price, price_difference_pct,
                prediction_confidence, link_source, prediction_date
            FROM mart.ml_predictions 
            WHERE 1=1
        """

        params = []

        if brand:
            query += " AND brand = %s"
            params.append(brand)

        if min_confidence > 0:
            query += " AND prediction_confidence >= %s"
            params.append(min_confidence)

        query += " ORDER BY price_difference_pct DESC"

        try:
            df = pd.read_sql(query, engine, params=params)
            logger.info(f"Загружено {len(df)} текущих прогнозов из БД")
            return df
        except Exception as e:
            logger.error(f"Ошибка при загрузке прогнозов из БД: {e}")
            return pd.DataFrame()


if __name__ == "__main__":
    config = {
        'filters': {'min_year': 2016, 'max_year': 2018, 'top': 3}
    }
    predict = Predict(airflow_mode=False)
    predict.predict_auction_prices(config, save_to_db=False, show_display=False)
